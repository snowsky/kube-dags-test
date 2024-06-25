"""
This DAG looks for a missing card and moves it to a given board.

This assumes that you have set the following parameters:

- hostname: The hostname of the Wekan server.
- username: The username to login with.
- password: The password to login with.
- card_id: The card text id (eg. SUP-1234) of the card to find.
- target_board_id: The board ID of the target board.

"""

import json
from datetime import timedelta, datetime
import typing
from airflow import XComArg

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from lib.wekan.types.boards import WekanConfiguration, PopulatedBoard
from lib.wekan.types.cards import LostCardDetails


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    params={
        "hostname": "http://wekan.wekan.svc:8080",
        "username": "erta_robot",
        "password": "password",
        "card_id": "GID like SUP #",
        "target_board_id": "target_board_id",
    },
)
def board_move_missing_card():
    """
    This DAG copies a wekan board from one server to another.
    """

    @task
    def login_user(
        hostname: str,
        username: str,
        password: str,
    ):
        """
        This function logs in the users to the source and target servers.
        """

        from lib.wekan.controllers.login import login

        response = login(hostname=hostname, username=username, password=password)

        error = response.get("error")

        if error:
            print(f"error: {error}")
            print(f"response: {response} {type(response)}")
            error_dict = {
                "status_code": error,
                "detail": {"response": json.dumps(response)},
            }
            raise AirflowException(error_dict)

        output = {**response, "hostname": hostname}

        return output

    @task
    def get_populated_boards(configuration: XComArg):
        """
        Function to get a populated board.
        """

        from lib.wekan.controllers.boards import get_active_user_boards, get_user_boards

        parsed_configuration = typing.cast(WekanConfiguration, configuration)

        hostname = parsed_configuration.get("hostname")
        user_id = parsed_configuration.get("id")
        token = parsed_configuration.get("token")

        boards = get_user_boards(hostname, user_id, token)

        populated_boards = get_active_user_boards(hostname, token, boards)

        return populated_boards

    @task
    def search_card_by_id(
        card_id: str,
        populated_boards: XComArg,
    ):
        """

        Function to search a card by its id.
        """

        parsed_populated_boards = typing.cast(list[PopulatedBoard], populated_boards)

        lost_card = None
        lost_card_swimlane = None
        lost_card_board = None

        for board in parsed_populated_boards:
            swimlanes = board.get("swimlanes")

            for swimlane in swimlanes:
                cards = swimlane.get("cards")

                for card in cards:
                    if card_id in card.get("title"):
                        lost_card = card
                        lost_card_swimlane = swimlane
                        lost_card_board = board
                        break

                if lost_card:
                    break

            if lost_card:
                break

        if not lost_card:
            raise AirflowException(f"Card with id {card_id} not found.")

        elif lost_card and lost_card_board and lost_card_swimlane:
            return {
                "card": lost_card,
                "swimlane": lost_card_swimlane,
                "list": {"_id": lost_card.get("listId")},
                "board": lost_card_board,
            }

    @task
    def move_card_to_board(
        configuration: XComArg,
        card_details: XComArg,
        target_board_id: str,
    ):
        """
        Function to move a card to a different board.
        """

        from lib.wekan.controllers.cards import edit_card
        from lib.wekan.controllers.boards import get_board_lists, get_board_swimlanes

        parsed_configuration = typing.cast(WekanConfiguration, configuration)

        hostname = parsed_configuration.get("hostname")
        token = parsed_configuration.get("token")

        parsed_card_details = typing.cast(LostCardDetails, card_details)
        card = parsed_card_details.get("card")
        list = parsed_card_details.get("list")
        board = parsed_card_details.get("board")

        target_board_swimlanes = get_board_swimlanes(
            hostname, token, board_id=target_board_id
        )
        target_board_swimlane = target_board_swimlanes[0]

        target_board_lists = get_board_lists(hostname, token, board_id=target_board_id)
        target_board_list = target_board_lists[0]

        card_payload = {
            "board": board.get("_id"),
            "list": list.get("_id"),
            "card": card.get("_id"),
            "newBoardId": target_board_id,
            "newSwimlaneId": target_board_swimlane.get("_id"),
            "newListId": target_board_list.get("_id"),
        }

        response = edit_card(
            hostname=hostname,
            token=token,
            payload=card_payload,
        )

        return response

    configuration = login_user(
        hostname="{{params.hostname}}",
        username="{{params.username}}",
        password="{{params.password}}",
    )

    populated_boards = get_populated_boards(
        configuration=configuration,
    )

    lost_card_details = search_card_by_id(
        card_id="{{params.card_id}}",
        populated_boards=populated_boards,
    )

    move_card_to_board(
        configuration=configuration,
        card_details=lost_card_details,
        target_board_id="{{params.target_board_id}}",
    )


board_move_missing_card_dag = board_move_missing_card()

if __name__ == "__main__":
    board_move_missing_card_dag.test()
