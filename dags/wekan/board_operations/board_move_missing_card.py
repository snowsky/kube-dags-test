import json
from datetime import timedelta, datetime
import typing
from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from airflow.sdk import get_current_context

from lib.wekan.types.boards import WekanConfiguration, PopulatedBoard
from lib.wekan.types.cards import LostCardDetails

@dag(
    dag_id="board_move_missing_card",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    params={
        "hostname": "http://wekan.wekan.svc:8080",
        "username": "c354e7f1-2afc-40e0-b554-7e26c57cbdb4",
        "password": "password",
        "card_id": "SUP-1234",
        "target_board_id": "ijzp326Etg3EZStWb",
    },
)
def board_move_missing_card():

    @task
    def login_user():
        context = get_current_context()
        params = context["params"]

        from lib.wekan.controllers.login import login

        response = login(
            hostname=params["hostname"],
            username=params["username"],
            password=params["password"],
        )

        error = response.get("error")
        if error:
            raise AirflowException({
                "status_code": error,
                "detail": {"response": json.dumps(response)},
            })

        return {**response, "hostname": params["hostname"]}

    @task
    def get_populated_boards(configuration: dict):
        from lib.wekan.controllers.boards import get_active_user_boards, get_user_boards

        parsed_configuration = typing.cast(WekanConfiguration, configuration)
        hostname = parsed_configuration.get("hostname")
        user_id = parsed_configuration.get("id")
        token = parsed_configuration.get("token")

        boards = get_user_boards(hostname, user_id, token)
        populated_boards = get_active_user_boards(hostname, token, boards)

        return populated_boards

    @task
    def search_card_by_id(populated_boards: list):
        context = get_current_context()
        card_id = context["params"]["card_id"]

        parsed_populated_boards = typing.cast(list[PopulatedBoard], populated_boards)

        for board in parsed_populated_boards:
            for swimlane in board.get("swimlanes", []):
                for card in swimlane.get("cards", []):
                    if card_id in card.get("title", ""):
                        return {
                            "card": card,
                            "swimlane": swimlane,
                            "list": {"_id": card.get("listId")},
                            "board": board,
                        }

        raise AirflowException(f"Card with id {card_id} not found.")

    @task
    def move_card_to_board(configuration: dict, card_details: dict):
        context = get_current_context()
        target_board_id = context["params"]["target_board_id"]

        from lib.wekan.controllers.cards import edit_card
        from lib.wekan.controllers.boards import get_board_lists, get_board_swimlanes

        parsed_configuration = typing.cast(WekanConfiguration, configuration)
        hostname = parsed_configuration.get("hostname")
        token = parsed_configuration.get("token")

        parsed_card_details = typing.cast(LostCardDetails, card_details)
        card = parsed_card_details.get("card")
        list_ = parsed_card_details.get("list")
        board = parsed_card_details.get("board")

        target_board_swimlanes = get_board_swimlanes(hostname, token, board_id=target_board_id)
        target_board_swimlane = target_board_swimlanes[0]

        target_board_lists = get_board_lists(hostname, token, board_id=target_board_id)
        target_board_list = target_board_lists[0]

        card_payload = {
            "board": board.get("_id"),
            "list": list_.get("_id"),
            "card": card.get("_id"),
            "newBoardId": target_board_id,
            "newSwimlaneId": target_board_swimlane.get("_id"),
            "newListId": target_board_list.get("_id"),
        }

        response = edit_card(hostname=hostname, token=token, payload=card_payload)
        return response

    # DAG flow
    configuration = login_user()
    populated_boards = get_populated_boards(configuration)
    lost_card_details = search_card_by_id(populated_boards)
    move_card_to_board(configuration, lost_card_details)

# In Airflow 3.0, the @dag decorator automatically registers the DAG
# No need to create a module-level variable - this can cause serialization issues
