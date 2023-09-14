import pendulum

from airflow.decorators import dag

import kp_utils_raw as kpur
import ace_utils_raw as aur


@dag(
    schedule="35 8 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="UTC"),
    catchup=False,
    tags=["kp_raw"]
)
def getKp_taskraw():
    # Get the start and en dates and the directory path
    # Retrieves the current day as start date and One as end date since
    # this is done in automatic mode
    # The directory path is a local one
    # passed_arguments_dict = kpur.initialize_date_and_directory_path()
    # Get the date range to query
    # In automatic mode it is just the current date
    # passed_arguments_dict = \
    #     kpur.get_dates_in_time_interval(passed_arguments_dict)
    # Get the measuring devices
    # passed_arguments_dict = \
    #     kpur.get_measuring_devices(passed_arguments_dict)
    # Get the list of URLs
    # passed_arguments_dict = kpur.define_url_format(passed_arguments_dict)
    # Download the data
    # passed_arguments_dict = kpur.download_data(passed_arguments_dict)
    # Get the bucket name
    # passed_arguments_dict = kpur.get_bucket_name(passed_arguments_dict)
    # Upload on cloud
    # passed_arguments_dict = kpur.upload_raw(passed_arguments_dict)
    # Save parameters on local file
    # passed_arguments_dict = kpur.save_passed_arguments_locally(passed_arguments_dict)

    passed_param_dict = kpur.prep_args()
    passed_param_dict = kpur.getKp(passed_param_dict)
    passed_param_dict = kpur.save_kp_data_locally(passed_param_dict)
    passed_param_dict = aur.get_bucket_name(passed_param_dict)
    passed_param_dict = kpur.upload_raw(passed_param_dict)


getKp_taskraw()
