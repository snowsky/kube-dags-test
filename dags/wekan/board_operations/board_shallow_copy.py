"""
This is a DAG to copy a wekan board from one server to another.
This executes a shallow copy, meaning that it only copies:

- The board itself
  - The swimlanes
  - The lists
    - The cards
      - The comments
      - The checklists
        - The checklist items

The cards contain the original description if possible, but falls back
to a linked reference to the original card on the origin server.

The card comments are posted by the robot user in order with the metadata
(author & dates) of the original comment.

This assumes that you have configured the following variables:

- source_hostname: The hostname of the source server.
- target_hostname: The hostname of the target server.
- source_username: The username to login with.
- source_password: The password to login with.
- target_username: The username to login with.
- target_password: The password to login with.
- source_board_id: The board ID of the source board.
- target_board_id: The board ID of the target board.

This also assumes that the target board is empty (No swimlanes, lists or cards).

Notes:
- The attachments are not copied.
- The users are not copied.
- The archived cards are not copied.
"""

import sys
sys.path.insert(0, '/opt/airflow/dags/repo/dags')

import json
from typing import TypedDict
from datetime import timedelta, datetime
import typing
from airflow import XComArg

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from lib.wekan.controllers.boards import WekanConfiguration


class WekanConfigurations(TypedDict):
    source_configuration: WekanConfiguration
    target_configuration: WekanConfiguration


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    params={
        "source_hostname": "https://boards.ertanalytics.com",
        "target_hostname": "http://wekan.wekan.svc:8080",
        "source_username": "erta_robot",
        "source_password": "source_password",
        "target_username": "erta_robot",
        "target_password": "target_password",
        "source_board_id": "source_board_id",
        "target_board_id": "target_board_id",
    },
)
def board_shallow_copy():
    """
    This DAG copies a wekan board from one server to another.
    """

    @task
    def login_users(
        source_hostname: str,
        target_hostname: str,
        source_username: str,
        source_password: str,
        target_username: str,
        target_password: str,
    ):
        """
        This function logs in the users to the source and target servers.
        """

        from lib.wekan.controllers.login import login

        source_response = login(
            hostname=source_hostname, username=source_username, password=source_password
        )

        target_response = login(
            hostname=target_hostname, username=target_username, password=target_password
        )

        error = (
            source_response.get("error")
            if isinstance(source_response, dict)
            else (
                target_response.get("error")
                if isinstance(target_response, dict)
                else None
            )
        )

        if error:
            print(f"error: {error}")
            print(f"source_response: {source_response} {type(source_response)}")
            print(f"target_response: {target_response} {type(target_response)}")
            error_dict = {
                "status_code": error,
                "detail": {
                    "source_response": json.dumps(source_response),
                    "target_response": json.dumps(target_response),
                },
            }
            raise AirflowException(error_dict)

        output = {
            "source_configuration": source_response,
            "target_configuration": target_response,
        }

        return output

    @task
    def get_populated_board(hostname: str, board_id: str, configuration: XComArg):
        """
        Function to get a populated board.
        """

        from lib.wekan.controllers.boards import get_populated_board

        parsed_configurations = typing.cast(WekanConfigurations, configuration)

        parsed_configuration = parsed_configurations.get("source_configuration")

        board = get_populated_board(
            hostname, board_id, configuration=parsed_configuration
        )

        return board

    @task
    def shallow_copy_board(
        source_hostname: str,
        target_hostname: str,
        source_board_id: str,
        target_board_id: str,
        configurations: XComArg,
        populated_board: XComArg,
    ):
        """
        Function to shallow copy a board.
        """

        from lib.wekan.controllers.boards import copy_populated_board

        parsed_configurations = typing.cast(WekanConfigurations, configurations)

        source_configuration = parsed_configurations.get("source_configuration")
        target_configuration = parsed_configurations.get("target_configuration")

        copy_response = copy_populated_board(
            source_hostname,
            target_hostname,
            source_board_id,
            target_board_id,
            source_configuration,
            target_configuration,
            raw_populated_board=populated_board,
        )

        return copy_response

    configurations = login_users(
        source_hostname="{{params.source_hostname}}",
        target_hostname="{{params.target_hostname}}",
        source_username="{{params.source_username}}",
        source_password="{{params.source_password}}",
        target_username="{{params.target_username}}",
        target_password="{{params.target_password}}",
    )

    populated_board = get_populated_board(
        hostname="{{params.source_hostname}}",
        board_id="{{params.source_board_id}}",
        configuration=configurations,
    )

    shallow_copy_board(
        source_hostname="{{params.source_hostname}}",
        target_hostname="{{params.target_hostname}}",
        source_board_id="{{params.source_board_id}}",
        target_board_id="{{params.target_board_id}}",
        configurations=configurations,
        populated_board=populated_board,
    )


board_shallow_copy_dag = board_shallow_copy()

if __name__ == "__main__":
    board_shallow_copy_dag.test()
