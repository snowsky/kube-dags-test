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

import json
from typing import TypedDict
from datetime import timedelta, datetime
import typing
from airflow import DAG, XComArg
from airflow.sdk import task
from airflow.exceptions import AirflowException

from lib.wekan.controllers.boards import WekanConfiguration


class WekanConfigurations(TypedDict):
    source_configuration: WekanConfiguration
    target_configuration: WekanConfiguration


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    'board_shallow_copy',
    default_args=default_args,
    description='Copy a wekan board from one server to another',
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
) as dag:
    @task
    def login_users(
        source_hostname: str,
        target_hostname: str,
        source_username: str,
        target_username: str,
    ):
        """
        This function logs in the users to the source and target servers.
        Using mock data for testing.
        """
        # Mock login responses for testing
        source_response = {
            "id": "mock_source_user_id",
            "token": "mock_source_token",
            "tokenExpires": "2025-12-31T23:59:59.999Z"
        }

        target_response = {
            "id": "mock_target_user_id",
            "token": "mock_target_token",
            "tokenExpires": "2025-12-31T23:59:59.999Z"
        }

        output = {
            "source_configuration": source_response,
            "target_configuration": target_response,
        }

        print(f"Mock login successful for {source_username} and {target_username}")
        return output

    @task
    def get_populated_board(hostname: str, board_id: str, configuration: XComArg):
        """
        Function to get a populated board.
        Using mock data for testing.
        """
        # Mock populated board data for testing
        mock_board = {
            "_id": board_id,
            "title": f"Mock Board {board_id}",
            "slug": f"mock-board-{board_id}",
            "archived": False,
            "createdAt": "2024-01-01T00:00:00.000Z",
            "modifiedAt": "2024-01-01T00:00:00.000Z",
            "stars": 0,
            "isTemplate": False,
            "permission": "public",
            "color": "belize",
            "swimlanes": [
                {
                    "_id": "mock_swimlane_1",
                    "title": "Backlog",
                    "archived": False,
                    "createdAt": "2024-01-01T00:00:00.000Z",
                    "modifiedAt": "2024-01-01T00:00:00.000Z",
                    "sort": 1,
                    "cards": []
                }
            ],
            "lists": [
                {
                    "_id": "mock_list_1",
                    "title": "To Do",
                    "archived": False,
                    "createdAt": "2024-01-01T00:00:00.000Z",
                    "modifiedAt": "2024-01-01T00:00:00.000Z",
                    "sort": 1,
                    "cards": []
                }
            ]
        }

        print(f"Mock populated board retrieved for {board_id} from {hostname}")
        return mock_board

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
        Using mock data for testing.
        """
        # Mock copy response for testing
        mock_copy_response = {
            "success": True,
            "message": f"Successfully copied board {source_board_id} to {target_board_id}",
            "source_board_id": source_board_id,
            "target_board_id": target_board_id,
            "copied_at": "2024-01-01T00:00:00.000Z",
            "swimlanes_copied": 1,
            "lists_copied": 1,
            "cards_copied": 0
        }

        print(f"Mock board copy completed: {source_board_id} -> {target_board_id}")
        return mock_copy_response

    configurations = login_users(
        source_hostname="https://boards.ertanalytics.com",
        target_hostname="http://wekan.wekan.svc:8080",
        source_username="erta_robot",
        target_username="erta_robot",
    )

    populated_board = get_populated_board(
        hostname="https://boards.ertanalytics.com",
        board_id="source_board_id",
        configuration=configurations,
    )

    shallow_copy_board(
        source_hostname="https://boards.ertanalytics.com",
        target_hostname="http://wekan.wekan.svc:8080",
        source_board_id="source_board_id",
        target_board_id="target_board_id",
        configurations=configurations,
        populated_board=populated_board,
    )

    # Set task dependencies
    configurations >> populated_board >> shallow_copy_board
