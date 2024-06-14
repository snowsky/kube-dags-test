"""
This is a DAG to deliver archive logs to authorized destinations.
"""

import os
import sys
import time
from datetime import timedelta, datetime
import typing

from airflow import XComArg
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
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
        "error_file_location": "/data/biakonzasftp/C-11/Run_Errors.txt",
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
        print(
            f"Error File Location: {errorFileLocation}, Title String {titleString}, Local Time: {localtime}"
        )
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
        except Exception as exception:
            raise ValueError(
                f"Error in getting parser check...retrying in 1 minute: {exception} "
            )

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
    ]  # type: ignore

    value_error_instance = value_error(
        "{{ ti.xcom_pull(task_ids='check_parser_status', key='error_details') }}"
    )

    parser_check_append_to_error_log_instance = append_to_error_log(
        error_file_location="{{ params.error_file_location }}",
        error_details="{{ ti.xcom_pull(task_ids='check_parser_status', key='error_details') }}",
    )

    get_parser_check_instance >> check_parser_status_instance  # type: ignore

    check_parser_status_instance >> [
        value_error_instance,
        parser_check_append_to_error_log_instance,
    ]  # type: ignore

    @task
    def get_authorized_destinations() -> pd.DataFrame:
        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        authorized_destinations = hook.get_pandas_df(
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

        authorized_destinations["authorized_identifier_oid"]

        print("Authorized Destination")
        print(authorized_destinations)

        return authorized_destinations

    @task
    def get_authorized_destinations_details(
        authorized_destination: XComArg,
    ) -> dict | None:
        """
        This function delivers the logs to the authorized destination.
        """
        print("Delivering to authorized destination")
        print(f"Authorized destination: {authorized_destination}")
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
        archive_delivery_source_base_path = authorized_destination_df[
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

        print(f"Source base path: {archive_delivery_source_base_path}")

        return {
            "folder_name": folder_name,
            "base_oid": base_oid,
            "archive_delivery_destination_path_override": archive_delivery_destination_path_override,
            "archive_delivery_source_base_path": archive_delivery_source_base_path,
            "archive_delivery_participant_client_name": archive_delivery_participant_client_name,
            "archive_delivery_authorized_filter_file_beginning_pattern": archive_delivery_authorized_filter_file_beginning_pattern,
        }

    @task
    def get_sftp_details(authorized_destination_details: XComArg) -> dict:
        authorized_destination_details_cast = typing.cast(
            dict, authorized_destination_details
        )

        folder_name = authorized_destination_details_cast["folder_name"]

        hook = MySqlHook(mysql_conn_id="C-11_sftp_credentials_parent_mgmt")

        sftp_details = hook.get_pandas_df(
            f"SELECT * FROM sftp_credentials_parent_mgmt where client_security_groupings_admins_name ='{folder_name}';"
        )
        quality_review_location = sftp_details["quality_review_location"][0]
        sftp_root_directory = sftp_details["sftp_root_directory"][0]

        return {
            "quality_review_location": quality_review_location,
            "sftp_root_directory": sftp_root_directory,
        }

    @task
    def get_days_to_deliver_archive(dayOffset: str) -> dict:
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
        days_run = [today_date_marker, yesterday_date_marker]

        return {
            "days_run": days_run,
            "today_date_marker": today_date_marker,
            "remove_date_marker": remove_date_marker,
        }

    @task
    def get_directory_list(
        days_to_deliver_archive: XComArg,
        sftp_details: XComArg,
        authorized_destination_details: XComArg,
    ) -> dict | None:
        days_to_deliver_archive_cast = typing.cast(dict, days_to_deliver_archive)
        sftp_details_cast = typing.cast(dict, sftp_details)
        authorized_destination_details_cast = typing.cast(
            dict, authorized_destination_details
        )

        sftp_root_directory = sftp_details_cast["sftp_root_directory"]
        archive_delivery_destination_path_override = (
            authorized_destination_details_cast["destination_path_override"]
        )
        archive_delivery_source_base_path = authorized_destination_details_cast[
            "source_base_path"
        ]
        today_date_marker = days_to_deliver_archive_cast["today_date_marker"]
        base_oid = authorized_destination_details_cast["authorized_identifier_oid"]

        definitive_path = (
            archive_delivery_destination_path_override
            if archive_delivery_destination_path_override
            else sftp_root_directory
        )
        definitive_source_path = archive_delivery_source_base_path
        directory_list = []

        if archive_delivery_source_base_path == "//PRD-AZ1-SQLW2/HL7InV3/":
            day_source_path = f"{archive_delivery_source_base_path}{today_date_marker}"

            try:
                directory_list = os.listdir(day_source_path)
            except Exception as e:
                ValueError(
                    f"Error in getting directory list on path {day_source_path}: {e}"
                )

            if len(directory_list) == 0:
                directory_list = ["", ""]

            for directory in directory_list:
                print(f"Directory: {directory}")

                sub_directory_path = f"{day_source_path}/{directory}"

                try:
                    sub_directory_list = os.listdir(sub_directory_path)
                except Exception as e:
                    ValueError(
                        f"Error in getting sub directory list on the path {sub_directory_path}: {e}"
                    )

                for sub_directory in sub_directory_list:
                    print(f"Sub Directory: {sub_directory}")

                    if sub_directory != base_oid:
                        print(
                            f"Found no BaseOIDs matching innerSubDirectory Path - continuing to next OID for this day {today_date_marker}"
                        )
                        continue

                    print("Found one")
                    definitive_source_path = (
                        f"{day_source_path}/{directory}/{sub_directory}"
                    )

        if definitive_source_path == "//PRD-AZ1-SQLW2/HL7InV3/":
            print(
                "skip - baseSourcePath must be reassigned above or it means none were found in the if statement"
            )
            return

        return {
            "definitive_path": definitive_path,
            "definitive_source_path": definitive_source_path,
            "directory_list": directory_list,
        }

    @task
    def check_base_source_path(
        days_to_deliver_archive: XComArg,
        authorized_destinations: XComArg,
        directory_list: XComArg,
        authorized_destination_details: XComArg,
    ) -> pd.DataFrame | None:
        scanned_count = 0
        dataframe_counter = 0
        archive_files_df = pd.DataFrame()
        days_to_deliver_archive_cast = typing.cast(dict, days_to_deliver_archive)
        authorized_destinations_cast = typing.cast(
            pd.DataFrame, authorized_destinations
        )
        directory_list_cast = typing.cast(dict, directory_list)
        authorized_destination_details_cast = typing.cast(
            dict, authorized_destination_details
        )

        today_date_marker = days_to_deliver_archive_cast["today_date_marker"]
        destination_path = directory_list_cast["definitive_path"]

        base_source_path = authorized_destination_details_cast["source_base_path"]
        destination_path = authorized_destination_details_cast[
            "destination_path_override"
        ]
        folder_name = authorized_destination_details_cast["client_folder_name"]
        authorized_filter_file_beginning_pattern = authorized_destination_details_cast[
            "authorized_filter_file_beginning_pattern"
        ]

        base_source_path_walker = os.walk(base_source_path, topdown=False)

        for root, directories, files in base_source_path_walker:
            if len(files) > 0:
                print(f"No Files in path - would move on {base_source_path}")
                continue

            dataframe_counter += 1

            print(
                "{0:.0%}".format(dataframe_counter / len(files))
                + " or "
                + str(dataframe_counter)
                + " of "
                + str(len(base_source_path_walker))  # type: ignore
                + " files... ",
                end="\r",
            )

            if len(files) > 0:
                source_path = str(root + str(directories))[:-2]
                if today_date_marker not in source_path:
                    print(
                        f"{source_path} is not a folder from today: {today_date_marker}"
                    )
                    continue

            extra_check = 0

            for index, row in authorized_destinations_cast.iterrows():
                base_oid = row["authorized_identifier_oid"]

                try:
                    if base_oid in source_path:
                        print(f"Found {base_oid} in {source_path}")
                        extra_check += 1
                except NameError:
                    print("sourcePath variable empty - not approving the extra check")

            if extra_check == 0:
                print(
                    "OS.WALK is pulling from an invalid directory - noted Office Practicum has some of these that should be skipped"
                )
                continue

            scanned_count = scanned_count + len(files)
            dataframe_counter = 0

            for file in files:
                dataframe_counter += 1

                print(
                    "{0:.0%}".format(dataframe_counter / len(files))
                    + " or "
                    + str(dataframe_counter)
                    + " of "
                    + str(len(files))
                    + " files... ",
                    end="\r",
                )

                if authorized_filter_file_beginning_pattern:
                    if authorized_filter_file_beginning_pattern not in file:
                        print(f"Skipping {file} as it does not match the pattern")
                        continue

                try:
                    does_file_exist = os.path.isfile(
                        os.path.join(root, file)
                    )  # This is to check if the file is a file and not a directory

                    if does_file_exist:
                        archive_file_record = {
                            "Path": source_path,
                            "Filename": [file],
                            "file_specific_FolderDestination": [destination_path],
                            "file_specific_client_folder": [folder_name],
                            "Modified Time": [
                                time.ctime(os.path.getmtime(os.path.join(root, file)))
                            ],
                            "Modified Y-M-D": [
                                datetime.utcfromtimestamp(
                                    os.path.getmtime(os.path.join(root, file))
                                ).strftime("%Y%m%d")
                            ],
                            "UnixTime": [os.path.getmtime(os.path.join(root, file))],
                        }
                        archive_files_df.append(archive_file_record)  # type: ignore

                except FileNotFoundError:
                    if file[-4:] != ".xml":
                        break

                    continue

                if file[-4:] != ".xml":
                    break
        if archive_files_df.empty:
            print(
                f"No files matched the scanned pattern {authorized_filter_file_beginning_pattern} in the base source path {base_source_path}"
            )
            return
        return archive_files_df

    @task
    def deduplicate_archive_files(archive_files: XComArg) -> pd.DataFrame:
        """
        This function deduplicates the archive files dataframe.
        """

        archive_files_df = typing.cast(pd.DataFrame, archive_files)

        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        already_archived_files_df = hook.get_pandas_df(
            "SELECT * FROM archive_delivery_todays_delivery__l_68;"
        )

        already_archived_files_df["combo"] = (
            already_archived_files_df["Path"] + already_archived_files_df["Filename"]
        )
        # archive_files_df[archive_files_df['Path'].isin(already_archived_files_df['Path'])]
        # if not archive_files_df[archive_files_df['Path'].isin(already_archived_files_df['Path'])].empty:
        #    print("Found some that were in the table! Use line below instead of 167 to omit")
        archive_files_df["combo"] = (
            archive_files_df["Path"] + archive_files_df["Filename"]
        )
        archive_files_prelimit_df = archive_files_df
        # archive_files_df = archive_files_prelimit_df
        # archive_files_prelimit_df[0]
        archive_files_df = archive_files_df[
            ~archive_files_df["combo"].isin(already_archived_files_df["combo"])
        ]
        if not archive_files_prelimit_df[
            archive_files_prelimit_df["combo"].isin(already_archived_files_df["combo"])
        ].empty:
            print(
                "Found "
                + str(len(archive_files_prelimit_df) - len(archive_files_df))
                + " that were in the table to omit! Adding the other "
                + str(len(archive_files_df))
            )
            # continue

        return archive_files_df

    @task(retries=5, retry_delay=timedelta(minutes=1))
    def copy_file_to_destination(
        file_to_copy: XComArg,
    ):
        """
        This function copies the file to the destination.
        """

        file_to_copy_cast = typing.cast(dict, file_to_copy)

        current_progress = file_to_copy_cast["index"] + 1

        print(
            "{0:.0%}".format(current_progress / file_to_copy_cast["length"])
            + " or "
            + str(current_progress)
            + " of "
            + str(file_to_copy_cast["length"])
            + " files... ",
            end="\r",
        )

        print(
            f"Copying file from {file_to_copy_cast['origin_path']} to {file_to_copy_cast['destination_path']}"
        )

        hook = SFTPHook(ssh_conn_id="C-11_sftp_credentials_parent_mgmt")

        try:
            hook.store_file(
                remote_full_path=file_to_copy_cast["destination_path"],
                local_full_path=file_to_copy_cast["origin_path"],
            )
        except Exception as e:
            raise ValueError(
                f"Error in copying file from {file_to_copy_cast['origin_path']} to {file_to_copy_cast['destination_path']} {e}"
            )

    @task
    def get_files_to_copy(
        file_list: XComArg,
        directory_list: XComArg,
    ) -> list[dict] | None:
        """
        This function copies the files to the destination.
        """

        file_list_df = typing.cast(pd.DataFrame, file_list)
        directory_list_cast = typing.cast(dict, directory_list)
        destination_path = directory_list_cast["definitive_path"]

        files_to_copy = []

        if len(file_list_df) == 0:
            print("No Files in path - would move on ")
            return

        for index, file_record in file_list_df.iterrows():
            # I:\HL7InV3_CDA_KONZA_SFTP_Retrieval__L_69\Glenwood Systems\20210721\2.16.840.1.113883.3.225.10.109
            files_to_copy.append(
                {
                    "origin_path": f"{file_record['Path']}/{file_record['Filename']}",
                    "destination_path": f"{destination_path}/{file_record['Filename']}",
                    "index": index,
                    "length": len(file_list_df),
                }
            )

        return files_to_copy

    @task
    def update_archive_files_table(
        archive_files: XComArg,
    ):
        """
        This function updates the archive files table.
        """

        archive_files_df = typing.cast(pd.DataFrame, archive_files)

        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")

        archive_files_df = archive_files_df.drop(["combo"], axis=1)

        archive_files_df.to_sql(
            "archive_delivery_todays_delivery__l_68",
            hook.get_sqlalchemy_engine(),
            if_exists="append",
            index=False,
        )

        archive_files_df = pd.read_sql(
            "archive_delivery_todays_delivery__l_68",
            hook.get_sqlalchemy_engine(),
        )

        return archive_files_df

    @task
    def check_quality_check_delivery(quality_check_delivery: str) -> str | None:
        return "store_quality_check_delivery" if quality_check_delivery == "1" else None

    @task(retries=5, retry_delay=timedelta(minutes=1))
    def store_quality_check_delivery(
        archive_files: XComArg,
        days_to_deliver_archive: XComArg,
        **context,
    ):
        """
        This function stores the quality check delivery.
        """

        archive_files_df = typing.cast(pd.DataFrame, archive_files)

        days_to_deliver_archive_cast = typing.cast(dict, days_to_deliver_archive)
        today_date_marker = days_to_deliver_archive_cast["today_date_marker"]

        dag_run = context["dag_run"]
        quality_check_destination = f"//PRD-AZ1-OPS1/Analytic Reports/Quality Review/L-68/delivered_{today_date_marker}-{dag_run.id}.csv"

        try:
            archive_files_df.to_csv(
                quality_check_destination,
            )
        except Exception as e:
            raise ValueError(f"Error in storing quality check delivery: {e}")

    @task
    def publish_message_volume(
        archive_files: XComArg,
        days_to_deliver_archive: XComArg,
        authorized_destination_details: XComArg,
    ):
        days_to_deliver_archive_cast = typing.cast(dict, days_to_deliver_archive)
        today_date_marker = days_to_deliver_archive_cast["today_date_marker"]
        archive_files_df = typing.cast(pd.DataFrame, archive_files)

        authorized_destination_details_cast = typing.cast(
            dict, authorized_destination_details
        )

        today_date_marker = days_to_deliver_archive_cast["today_date_marker"]

        source_base_path = authorized_destination_details_cast["source_base_path"]
        folder_name = authorized_destination_details_cast["client_folder_name"]

        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        dataframe_counter = 0

        for index, row in archive_files_df.iterrows():
            dataframe_counter = dataframe_counter + 1
            if len(archive_files_df) == 0:
                print("No Files in path - would move on ")
                continue
            print(
                "{0:.0%}".format(dataframe_counter / len(archive_files_df))
                + " or "
                + str(dataframe_counter)
                + " of "
                + str(len(archive_files_df))
                + " files... ",
                end="\r",
            )
            hook.run(
                """INSERT INTO public.message_volume(
                id,day_timestamp, count, stage, stage_level)
                VALUES (
                nextval('message_volume_id'),
                '"""
                + today_date_marker
                + """',
                1,
                '"""
                + source_base_path
                + """',
                '"""
                + "3 - Message Directed to"
                + folder_name
                + """')
                    ON CONFLICT (day_timestamp,stage, stage_level) DO
                    UPDATE
                    SET count = message_volume.count + 1
                ;"""
            )

    @task
    def delete_check_query(days_to_deliver_archive: XComArg):
        days_to_deliver_archive_cast = typing.cast(dict, days_to_deliver_archive)
        today_date_marker = days_to_deliver_archive_cast["today_date_marker"]
        yesterday_date_marker = days_to_deliver_archive_cast["remove_date_marker"]

        sqlLikeToday = "%\\" + today_date_marker + "%"
        sqlLikeYesterday = "%\\" + yesterday_date_marker + "%"

        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        hook.run(
            """delete from archive_delivery_todays_delivery__l_68 where "Path" not like '"""
            + sqlLikeToday
            + """' AND "Path" not like '"""
            + sqlLikeYesterday
            + """';"""
        )

    @task
    def update_trigger_status_completed(
        script_name: str,
    ):
        """
        This function updates the trigger status to completed so a new run can begin.
        """

        hook = PostgresHook(postgres_conn_id="prd-az1-ops1-airflowconnection")
        hook.run(
            f"UPDATE job_triggers_airflow SET trigger_status = '0' WHERE associated_table = '{script_name.lower()}';"
        )

        print("Updated table as state of completed")

    authorized_destinations = get_authorized_destinations()

    authorized_destinations_details = get_authorized_destinations_details.expand(
        authorized_destination=authorized_destinations,
    )

    sftps_details = get_sftp_details.expand(
        authorized_destination_details=authorized_destinations_details
    )

    days_to_deliver_archive = get_days_to_deliver_archive(
        dayOffset="{{ params.days_offset }}"
    )

    directory_lists = get_directory_list.partial(
        days_to_deliver_archive=days_to_deliver_archive,
    ).expand(
        sftp_details=sftps_details,
        authorized_destination_details=authorized_destinations_details,
    )

    archive_files_dfs = check_base_source_path.partial(
        days_to_deliver_archive=days_to_deliver_archive,
        authorized_destinations=authorized_destinations,
    ).expand(
        directory_list=directory_lists,
        authorized_destination_details=authorized_destinations_details,
    )

    deduplicate_archive_files_dfs = deduplicate_archive_files.expand(
        archive_files=archive_files_dfs
    )

    files_to_copy = get_files_to_copy.expand(
        file_list=deduplicate_archive_files_dfs,
        directory_list=directory_lists,
    )

    copy_files_to_destination = copy_file_to_destination.expand(
        file_to_copy=files_to_copy,
    )

    update_archive_files_table_instance = update_archive_files_table.expand(
        archive_files=deduplicate_archive_files_dfs,
    )

    check_quality_check_delivery_instance = check_quality_check_delivery(
        quality_check_delivery="{{ params.quality_check_delivery }}"
    )

    (
        check_parser_status_instance
        >> authorized_destinations
        >> authorized_destinations_details
        >> sftps_details
        >> days_to_deliver_archive
        >> directory_lists
        >> archive_files_dfs
        >> deduplicate_archive_files_dfs
        >> files_to_copy
        >> copy_files_to_destination
        >> update_archive_files_table_instance
        >> check_quality_check_delivery_instance
    )  # type: ignore

    store_quality_check_delivery_instance = store_quality_check_delivery.partial(
        days_to_deliver_archive=days_to_deliver_archive,
    ).expand(
        archive_files=update_archive_files_table_instance,
    )

    check_quality_check_delivery_instance >> [
        store_quality_check_delivery_instance,
    ]  # type: ignore

    publish_message_volume_instance = publish_message_volume.partial(
        days_to_deliver_archive=days_to_deliver_archive,
    ).expand(
        authorized_destination_details=authorized_destinations_details,
    )

    delete_check_query_instance = delete_check_query(
        days_to_deliver_archive=days_to_deliver_archive,
    )

    update_trigger_status_completed_instance = update_trigger_status_completed(
        script_name="{{ params.script_name }}",
    )

    (
        check_quality_check_delivery_instance
        >> publish_message_volume_instance
        >> delete_check_query_instance
        >> update_trigger_status_completed_instance
    )  # type: ignore


c_11_archive_delivery_dag = c_11_archive_delivery()

if __name__ == "__main__":
    c_11_archive_delivery_dag.test()
