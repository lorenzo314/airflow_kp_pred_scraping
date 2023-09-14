import json
import os.path
import urllib.request
import pandas as pd
import datetime as dtm

from airflow.decorators import task

from google.cloud import storage


def __checkdate__(starttime, endtime):
    if starttime > endtime:
        raise NameError("Error! Start time must be before or equal to end time")
    return True


def __checkIndex__(index):
    if index not in ['Kp', 'ap', 'Ap', 'Cp', 'C9', 'Hp30', 'Hp60', 'ap30', 'ap60', 'SN', 'Fobs', 'Fadj']:
        raise IndexError(
            "Error! Wrong index parameter!"
            "\nAllowed are only the string parameter:"
            "'Kp', 'ap', 'Ap', 'Cp', 'C9', 'Hp30', 'Hp60', 'ap30', 'ap60', 'SN', 'Fobs', 'Fadj'"
        )
    return True


def __checkstatus__(status):
    if status not in ['all', 'def']:
        raise IndexError("Error! Wrong option parameter! \nAllowed are only the string parameter: 'def'")
    return True


def __addstatus__(url, status):
    if status == 'def':
        url = url + '&status=def'
    return url


def getKpindex(starttime, endtime, index, status='all'):
    """
    ---------------------------------------------------------------------------------
    download 'Kp', 'ap', 'Ap', 'Cp', 'C9', 'Hp30', 'Hp60', 'ap30', 'ap60', 'SN', 'Fobs' or 'Fadj'
    index data from kp.gfz-potsdam.de
    date format for starttime and endtime is 'yyyy-mm-dd' or 'yyyy-mm-ddTHH:MM:SSZ'
    optional 'def' parameter to get only definitive values
    (only available for 'Kp', 'ap', 'Ap', 'Cp', 'C9', 'SN')
    Hpo index and Fobs/Fadj does not have the status info
    example: (time, index, status) = getKpindex('2021-09-29', '2021-10-01','Ap','def')
    example: (time, index, status) = getKpindex('2021-09-29T12:00:00Z', '2021-10-01T12:00:00Z','Kp')
    ---------------------------------------------------------------------------------
    """
    result_t = 0
    result_index = 0
    result_s = 0

    if len(starttime) == 10 and len(endtime) == 10:
        starttime = starttime + 'T00:00:00Z'
        endtime = endtime + 'T23:59:00Z'

    try:
        d1 = dtm.datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%SZ')
        d2 = dtm.datetime.strptime(endtime, '%Y-%m-%dT%H:%M:%SZ')

        __checkdate__(d1, d2)
        __checkIndex__(index)
        __checkstatus__(status)

        time_string =\
            "start=" +\
            d1.strftime('%Y-%m-%dT%H:%M:%SZ') +\
            "&end=" +\
            d2.strftime('%Y-%m-%dT%H:%M:%SZ')

        url = 'https://kp.gfz-potsdam.de/app/json/?' + time_string + "&index=" + index
        if index not in ['Hp30', 'Hp60', 'ap30', 'ap60', 'Fobs', 'Fadj']:
            url = __addstatus__(url, status)

        web_url = urllib.request.urlopen(url)
        binary = web_url.read()
        text = binary.decode('utf-8')

        try:
            data = json.loads(text)
            result_t = tuple(data["datetime"])
            result_index = tuple(data[index])
            if index not in ['Hp30', 'Hp60', 'ap30', 'ap60', 'Fobs', 'Fadj']:
                result_s = tuple(data["status"])
        except Exception:
            print(text)

    except NameError as er:
        print(er)
    except IndexError as er:
        print(er)
    except ValueError:
        print("Error! Wrong datetime string")
        print("Both dates must be the same format.")
        print("Datetime strings must be in format yyyy-mm-dd or yyyy-mm-ddTHH:MM:SSZ")
    except urllib.error.URLError:
        print("Connection Error\nCan not reach " + url)
    finally:
        return result_t, result_index, result_s


@task()
def prep_args():
    passed_param_dict = {"days_to_take": 5}
    passed_param_dict["hours_to_take"] = 24 * passed_param_dict["days_to_take"]

    passed_param_dict["raw_data_path"] =\
        "/home/lorenzo/spaceable/airflow_kp_scraping/raw_data"

    x = dtm.datetime.strftime(dtm.datetime.now(), '%Y%m%dT%H%M%SZ')
    raw_data_file = f"{x}_kp_raw_data.txt"
    passed_param_dict["raw_data_file"] = raw_data_file

    return passed_param_dict


# %% DEFINING THE FUNCTION THAT GET KP DATA
@task()
def getKp(passed_param_dict: dict):
    """
    Download Kp index data according a number of hours to keep and a time_format.
    Sources : kp.gfz-potsdam.de

    Parameters
    ----------
    passed_param_dict : dict
        A dict with a key called "hours_to_take":
        The number of hours to scrape that will be display on the platform.

    Return
    -------
    dataframe containing the good GOES data.

    """

    # days_to_take = passed_param_dict["days_to_take"]
    hours_to_take = passed_param_dict["hours_to_take"]

    print('Process Kp data (' + str(hours_to_take) + 'h data history)')
    today = dtm.datetime.today() - dtm.timedelta(hours=2)  # We put a delay of 2 hours to match ACE data
    date_to_scrape = today - dtm.timedelta(days=int(hours_to_take / 24))

    kp_index, kp_data, _ = getKpindex(
        str(date_to_scrape.date()) +
        'T' +
        str(date_to_scrape.time().hour) +
        ':00:00Z',
        str(today.date()) +
        'T' +
        str(today.time().hour) +
        ':00:00Z', "Kp"
    )

    kp = pd.DataFrame([kp_index, kp_data])
    kp = kp.transpose()
    kp.columns = ['date', 'value']
    kp['date'] = kp['date'].apply(
        lambda df: dtm.datetime.strptime(df, '%Y-%m-%dT%H:%M:%SZ'))
    kp.set_index('date', inplace=True, drop=True)

    passed_param_dict["kp"] = kp
    return passed_param_dict


@task()
def save_kp_data_locally(passed_param_dict: dict):
    kp = passed_param_dict["kp"]
    x = os.path.join(
        passed_param_dict["raw_data_path"], passed_param_dict["raw_data_file"]
    )
    kp.to_csv(x)

    return passed_param_dict


@task()
def upload_raw(passed_arguments_dict: dict):
    bucket_name = passed_arguments_dict["bucket_name"]

    output_file = os.path.join(
        passed_arguments_dict["raw_data_path"], passed_arguments_dict["raw_data_file"]
    )

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    destination_blob_name = output_file
    blob = bucket.blob(destination_blob_name)

    generation_match_precondition = 0

    source_file_name = output_file

    blob.upload_from_filename(
        source_file_name,
        if_generation_match=generation_match_precondition
    )

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

    return passed_arguments_dict
