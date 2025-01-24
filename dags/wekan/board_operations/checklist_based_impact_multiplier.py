import json
import logging
import typing
from datetime import timedelta, datetime
from airflow import XComArg
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

from lib.wekan.types.boards import WekanConfiguration, PopulatedBoard
from lib.wekan.types.cards import WekanCard

@dag(
    dag_id="checklist_based_impact_multiplier",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    params={
        "hostname": "http://wekan.wekan.svc:8080",
        "username": "c354e7f1-2afc-40e0-b554-7e26c57cbdb4",
        "password": "password",
    },
)
def list_boards_and_checklists():
    """
    This DAG lists the boards and checklists from a Wekan server.
    """

    @task
    def login_user(hostname: str, username: str, password: str):
        """
        This function logs in the user to the Wekan server.
        """
        from lib.wekan.controllers.login import login

        response = login(hostname=hostname, username=username, password=password)
        error = response.get("error")

        if error:
            logging.info(f"error: {error}")
            print(f"error: {error}")
            logging.info(f"response: {response} {type(response)}")
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
        Function to get populated boards.
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
    def list_checklists(populated_boards: XComArg):
        """
        Function to list checklists from the boards.
        """
        parsed_populated_boards = typing.cast(list[PopulatedBoard], populated_boards)
        checklists = []
        
        for board in parsed_populated_boards:
            logging.info(f'Board: {board}')
            for swimlane in board.get("swimlanes", []):
                logging.info(f'Swimlane: {swimlane}')
                for card in swimlane.get("cards", []):
                    card_checklists = card.get("checklists", [])
                    logging.info(f'Card_checklists: {card_checklists}')
                    checklists.extend(card_checklists)
        logging.info(f'Checklists: {checklists}')
        return checklists

    configuration = login_user(
        hostname="{{params.hostname}}",
        username="{{params.username}}",
        password="{{params.password}}",
    )

    populated_boards = get_populated_boards(configuration=configuration)
    checklists = list_checklists(populated_boards=populated_boards)
    logging.info(f'Checklists: {checklists}')
    print(f'Checklists: {checklists}')

list_boards_and_checklists_dag = list_boards_and_checklists()

if __name__ == "__main__":
    list_boards_and_checklists_dag.test()
