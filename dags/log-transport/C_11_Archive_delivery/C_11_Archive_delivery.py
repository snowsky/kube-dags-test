"""
This is a DAG to move logs
"""

import os
import sys
import time
from datetime import timedelta, datetime
import typing

from airflow import XComArg
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    params={
        "quality_check_delivery": "1",
        "script_name": "ARCHIVE_DELIVERY__L_68",
        "days_offset": 0,  # Set to -13 for example to start 13 days ago
        "error_file_location": "\source\C-11\Run_Errors.txt",
    },
)
def c_11_archive_delivery():
    """
    DAG to copy logs
    """

    import pandas as pd

    @task
    def get_restart_trigger() -> pd.DataFrame:
        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        restart_trigger = hook.get_pandas_df("SELECT * FROM restart_trigger;")
        return restart_trigger

    @task
    def get_schedule_jobs() -> pd.DataFrame:
        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        schedule_jobs = hook.get_pandas_df("SELECT * FROM schedule_jobs;")
        return schedule_jobs

    @task
    def parse_sourced_schedule_jobs_task(
        script_name: str, restart_trigger: XComArg, schedule_jobs: XComArg
    ):
        print("Parsing sourced schedule jobs")
        print(f"Script name: {script_name}")
        print(f"Restart trigger: {restart_trigger}")
        print(f"Schedule jobs: {schedule_jobs}")
        restart_trigger_df = typing.cast(pd.DataFrame, restart_trigger)
        schedule_jobs_df = typing.cast(pd.DataFrame, schedule_jobs)

        parsed_schedule_jobs = schedule_jobs_df.where(
            schedule_jobs_df["script_name"] == script_name
        )
        parsed_schedule_jobs = parsed_schedule_jobs.dropna()
        parsed_schedule_jobs["trigger_name"] = parsed_schedule_jobs["server"]
        print("Parsed schedule jobs")
        print(parsed_schedule_jobs)
        parsed_schedule_jobs = parsed_schedule_jobs.merge(
            restart_trigger_df,
            left_on="trigger_name",
            right_on="trigger_name",
            how="inner",
        )

        print("Parsed schedule jobs")
        print(parsed_schedule_jobs)

        return parsed_schedule_jobs

    parsed_sourced_schedule_jobs = parse_sourced_schedule_jobs_task(
        script_name="{{ params.script_name }}",
        restart_trigger=get_restart_trigger(),
        schedule_jobs=get_schedule_jobs(),
    )

    @task
    def get_overlap_check(script_name: str) -> pd.DataFrame:
        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        overlap_check = hook.get_pandas_df(
            f"SELECT * FROM job_triggers_airflow where associated_table = '{script_name.lower()}';"
        )
        return overlap_check

    @task
    def append_to_error_log(error_file_location: str, error_details: str):
        """
        This function appends an event to the error log.
        """
        localtime = str(time.asctime(time.localtime(time.time())))
        titleString = os.path.basename(sys.argv[0])
        errorFileLocation = error_file_location
        print(f"Error File Location: {errorFileLocation}, Title String {titleString}, Local Time: {localtime}")
        f = open(errorFileLocation, "a")
        f.write(
            "Time: "
            + localtime
            + " | Error Details: "
            + error_details
            + " - "
            + titleString
            + " | Last HSQL Query: "
            + "\n"
        )
        print(f"Filename: {f}")
        f.close()

    @task
    def update_trigger_status(script_name: str, trigger_status: str):
        """
        This function updates the trigger status.
        """

        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        hook.run(
            f"UPDATE job_triggers_airflow SET trigger_status = '{trigger_status}' WHERE associated_table = '{script_name.lower()}';"
        )

        print("Updated table as state of running")

    @task(retries=5, retry_delay=timedelta(minutes=1))
    def get_parser_check() -> pd.DataFrame:
        try:
            hook = MySqlHook(mysql_conn_id="prd-az1-sqlw2-airflowconnection")
            parser_check = hook.get_pandas_df(
                "SELECT * FROM _dashboard_maintenance.parser_controller;"
            )
            return parser_check
        except:
            raise ValueError("Error in getting parser check...retrying in 1 minute")

    @task
    def value_error(error_details: str):
        """
        This function raises a ValueError.
        """
        raise ValueError(error_details)

    @task.branch
    def check_parser_status(parser_check: XComArg):
        """
        This function checks the parser status.
        """

        context = get_current_context()
        ti = context["ti"]  # type: ignore

        parser_check_df = typing.cast(pd.DataFrame, parser_check)

        if parser_check_df["enabled"][0] == 0:
            ti.xcom_push(
                key="error_details",
                value="ERTA - Parser Disabled | File: ARCHIVE_DELIVERY__l_68",
            )

            return ["append_to_error_log", "value_error"]

    @task.branch
    def check_schedule_jobs_and_overlap(
        parsed_sourced_schedule_jobs: XComArg,
        overlap_check: XComArg,
        days_offset: str,
    ):
        """
        This function prepares the dataframes and merges them.
        """

        context = get_current_context()
        ti = context["ti"]  # type: ignore

        tasks_to_return = []

        trigger_status_to_return = ""
        error_details_to_return = ""

        parsed_sourced_schedule_jobs_df = typing.cast(
            pd.DataFrame, parsed_sourced_schedule_jobs
        )

        overlap_check_df = typing.cast(pd.DataFrame, overlap_check)

        if parsed_sourced_schedule_jobs_df["switch"][0] == 0:
            print("Running")
            timestamp_condition = pd.Timestamp(
                overlap_check_df["updated_at"][0]
            ).tz_localize(None) <= pd.Timestamp(datetime.today() + timedelta(days=-1))

            overlap_check_latest_trigger_status = overlap_check_df["trigger_status"][0]

            if overlap_check_latest_trigger_status == "0" or timestamp_condition:
                print("Running")
                if timestamp_condition:
                    error_details_to_return = "ARCHIVE_DELIVERY__l_68_prod.py Job Stalled for over a day | File: ARCHIVE_DELIVERY__l_68"

                    trigger_status_to_return = overlap_check_latest_trigger_status

                    tasks_to_return.append("append_to_error_log")
                    tasks_to_return.append("update_trigger_status")

                days_offset_absolute = abs(int(days_offset))

                if days_offset_absolute == 0:
                    days_offset_absolute = 1

                for day in range(days_offset_absolute):
                    print(f"On loop: {day} of {days_offset_absolute}", end="\r")

                    if overlap_check_latest_trigger_status == "1":
                        print("Already Running - Continuing to next loop")
                        continue

                    trigger_status_to_return = "1"
                    tasks_to_return.append("update_trigger_status")

                    tasks_to_return.append("get_parser_check")

            ti.xcom_push(key="error_details", value=error_details_to_return)

            ti.xcom_push(
                key="overlap_check_latest_trigger_status",
                value=trigger_status_to_return,
            )

            return tasks_to_return

    check_schedule_jobs_and_overlap_instance = check_schedule_jobs_and_overlap(
        parsed_sourced_schedule_jobs=parsed_sourced_schedule_jobs,
        overlap_check=get_overlap_check(script_name="{{ params.script_name }}"),
        days_offset="{{ params.days_offset }}",
    )

    append_to_error_log_instance = append_to_error_log(
        error_file_location="{{ params.error_file_location }}",
        error_details="{{ ti.xcom_pull(task_ids='check_schedule_jobs_and_overlap', key='error_details') }}",
    )

    update_trigger_status_instance = update_trigger_status(
        script_name="{{ params.script_name }}",
        trigger_status="{{ ti.xcom_pull(task_ids='check_schedule_jobs_and_overlap', key='overlap_check_latest_trigger_status') }}",
    )

    get_parser_check_instance = get_parser_check()
    check_parser_status_instance = check_parser_status(get_parser_check_instance)

    check_schedule_jobs_and_overlap_instance >> [
        append_to_error_log_instance,
        update_trigger_status_instance,
        get_parser_check_instance,
    ]

    value_error_instance = value_error(
        "{{ ti.xcom_pull(task_ids='check_parser_status', key='error_details') }}"
    )

    parser_check_append_to_error_log_instance = append_to_error_log(
        error_file_location="{{ params.error_file_location }}",
        error_details="{{ ti.xcom_pull(task_ids='check_parser_status', key='error_details') }}",
    )

    get_parser_check_instance >> check_parser_status_instance

    check_parser_status_instance >> [
        value_error_instance,
        parser_check_append_to_error_log_instance,
    ]

    @task
    def get_authorized_destination(day: str) -> pd.DataFrame:
        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        authorized_destination = hook.get_pandas_df(
            """select authorized_identifier_oid
            , delivery_paused
            , authorized_filter_file_beginning_pattern
            , authorization_reference
            , participant_client_name
            , source_base_path
            , destination_path_override
            , client_folder_name
            from archive_delivery__l_68 group by authorized_identifier_oid
            , authorized_filter_file_beginning_pattern
            , delivery_paused
            ,authorization_reference
            , participant_client_name
            ,source_base_path
            , destination_path_override
            , client_folder_name
            ;"""
        )

        authorized_destination["authorized_identifier_oid"]

        print("Authorized Destination")
        print(authorized_destination)

        return authorized_destination

    @task
    def deliver_to_authorized_destination(authorized_destination: XComArg, day: str):
        """
        This function delivers the logs to the authorized destination.
        """
        print("Delivering to authorized destination")
        print(f"Authorized destination: {authorized_destination}")
        print(f"Days to deliver archive: {day}")

        authorized_destination_df = typing.cast(pd.DataFrame, authorized_destination)

        if authorized_destination_df["delivery_paused"] != 0:
            return

        folder_name = authorized_destination_df["client_folder_name"].strip()
        base_oid = authorized_destination_df["authorized_identifier_oid"].strip()

        print(f"Folder name: {folder_name}")
        print(f"Base OID: {base_oid}")

        archive_delivery_destination_path_override = authorized_destination_df[
            "destination_path_override"
        ].strip()
        archive_delivery_destination_path = authorized_destination_df[
            "source_base_path"
        ].strip()
        archive_delivery_participant_client_name = authorized_destination_df[
            "participant_client_name"
        ].strip()
        archive_delivery_authorized_filter_file_beginning_pattern = (
            authorized_destination_df[
                "authorized_filter_file_beginning_pattern"
            ].strip()
        )

        print(f"Source base path: {archive_delivery_destination_path}")

    @task
    def get_days_to_deliver_archive(dayOffset: str) -> list[str]:
        # Making sure on the day change that it reruns yesterday just to make sure it gets all the files from the end of the day

        print("Trigger Overlap Allowing Script to Run")
        # Making sure on the day change that it reruns yesterday just to make sure it gets all the files from the end of the day

        today = datetime.today() + timedelta(
            days=int(dayOffset)
        )  ## Allows a rewind if days were missed
        # today = datetime.today() #+ timedelta(days=-2) ## Allows a rewind if days were missed
        yesterday = today + timedelta(days=-1)
        remove_day = today + timedelta(days=-2)
        today_date_marker = today.strftime("%Y%m%d")
        yesterday_date_marker = yesterday.strftime("%Y%m%d")
        remove_date_marker = remove_day.strftime("%Y%m%d")
        daysRun = [today_date_marker, yesterday_date_marker]

        return daysRun


c_11_archive_delivery_dag = c_11_archive_delivery()

if __name__ == "__main__":
    c_11_archive_delivery_dag.test()
