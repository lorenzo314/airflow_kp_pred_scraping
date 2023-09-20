import json
import os.path
from urllib import request
import pandas as pd
import numpy as np
import datetime as dtm
import requests

from airflow.decorators import task

from google.cloud import storage


def is_url(url):
    """
    Check if the passed url exists or not

    Parameters
    ----------
    url : string
        URL to test.

    Return
    -------
    True if the url exists, False if not
    """

    r = requests.get(url)
    if r.status_code == 429:
        print('Retry URL checking (429)')
        time.sleep(5)
        return is_url(url)
    elif r.status_code == 404:
        return False
    else:
        return True


@task
def getKpPred(passed_param_dict: dict):
    """
    Download Kp prediction over 3 days according a time_format.
    Sources : https://services.swpc.noaa.gov/text/3-day-forecast.txt
    The Kp prediction data in the file are presented like this :
                 Feb 17       Feb 18       Feb 19
    00-03UT       1.67         6.00 (G2)    3.33
    03-06UT       3.00         5.33 (G1)    2.33
    06-09UT       2.00         4.00         4.00
    09-12UT       1.33         3.33         4.00
    12-15UT       2.00         3.00         3.33
    15-18UT       2.67         2.33         1.67
    18-21UT       3.67         3.00         1.67
    21-00UT       5.00 (G1)    3.67         2.67

    Parameters
    ----------
    passed_param_dict a dict
    Return
    -------
    dataframe containing the Kp prediction over 3 days.
    """
    print('Get Kp prediction over 3 days...')

    # Check if the url of the file exists
    url = 'https://services.swpc.noaa.gov/text/3-day-forecast.txt'
    check_url = is_url(url)

    if check_url:
        # Open the file
        file = request.urlopen(url)
        data = pd.read_fwf(file)
    else:
        print("The url doesn't work anymore. (" + url + ")")
        exit()

    # Skiprows : remove the first useless rows of the file
    ind = 0
    while data.iloc[ind, 0][0] != '0':
        ind += 1
    data.drop(range(ind-1), axis=0, inplace=True)
    data.reset_index(inplace=True, drop=True)

    # Skipfooter : remove the last useless rows of file
    data.drop(range(9, data.index[-1]+1), axis=0, inplace=True)

    # Only keep the first column of the file
    data = pd.DataFrame(data.values[1:, 0], columns=[data.values[0, 0]])

    # Get the 3 dates of the Kp prediction in a list
    cols = data.columns[0].split()
    current_dates = [cols[i] + ' ' + cols[i + 1] for i in np.arange(0, 6, 2)]

    # Loop every line of the data (should be every 3h => 8 times)
    Kp_pred = pd.Series()
    for i in range(len(data.values)):
        current_line = data.iloc[i, 0].split()

        k = 0
        # Remove the label of Kp tempest if there is one ((G1), (G2), etc.)
        while k < len(current_line):
            if current_line[k][0] == '(':
                del current_line[k]
                k -= 1
            k += 1

        current_hour = int(current_line[0][:2])
        for j in range(len(current_dates)):
            current_day = dtm.datetime.strptime(current_dates[j], '%b %d')
            current_date = dtm.date(year=dtm.datetime.today().year, month=current_day.month, day=current_day.day)
            current_datetime = dtm.datetime(year=current_date.year, month=current_date.month, day=current_date.day, hour=current_hour, minute=0)
            Kp_pred[current_datetime] = float(current_line[j+1])

    # Sort the index because we read the file line per line and not column per column
    Kp_pred.sort_index(inplace=True)
    Kp_pred.index.rename('date', inplace=True)

    Kp_pred.name = 'value'
    Kp_pred = Kp_pred.to_frame()
    passed_param_dict["Kp_pred"] = Kp_pred
    return passed_param_dict


@task()
def prep_args():
    passed_param_dict = {"days_to_take": 5}
    passed_param_dict["hours_to_take"] = 24 * passed_param_dict["days_to_take"]

    passed_param_dict["raw_data_path"] =\
        "/home/lorenzo/spaceable/airflow_kp_pred_scraping/raw_data"

    x = dtm.datetime.strftime(dtm.datetime.now(), '%Y%m%dT%H%M%SZ')
    raw_data_file = f"{x}_kp_raw_data.txt"
    passed_param_dict["raw_data_file"] = raw_data_file

    return passed_param_dict


@task()
def save_kp_data_locally(passed_param_dict: dict):
    kp = passed_param_dict["Kp_pred"]
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
