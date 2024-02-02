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
        
The cards contain the original description if possible, but falls back to a linked reference to the original card on the origin server.

The card comments are posted by the robot user in order with the metadata (author & dates) of the original comment.

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

from datetime import timedelta, datetime

from airflow import DAG, AirflowException
from airflow.operators.python import PythonVirtualenvOperator
from airflow.models import Variable


def login_users_async(
    source_hostname,
    target_hostname,
    source_username,
    source_password,
    target_username,
    target_password,
):
    """
    This function is a wrapper to run the login_users function in an asyncio event loop.
    """

    import json
    import asyncio

    from konza.wekan.board_operations.src.controllers.login import login

    async def login_users(
        source_hostname,
        target_hostname,
        source_username,
        source_password,
        target_username,
        target_password,
    ):
        """
        Function to login a users and return the tokens as a Xcom.
        """

        source_response = await login(
            hostname=source_hostname, username=source_username, password=source_password
        )

        target_response = await login(
            hostname=target_hostname, username=target_username, password=target_password
        )

        error = (
            source_response.get("error")
            if isinstance(source_response, dict)
            else target_response.get("error")
            if isinstance(target_response, dict)
            else None
        )

        if error:
            error_dict = {
                "status_code": error,
                "detail": {source_response, target_response},
            }
            raise AirflowException(error_dict)

        xcom_output = {
            "source_configuration": source_response,
            "target_configuration": target_response,
        }

        return xcom_output

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(
        login_users(
            source_hostname,
            target_hostname,
            source_username,
            source_password,
            target_username,
            target_password,
        )
    )

    return json.dumps(result)


def shallow_copy_board_async(
    source_hostname, target_hostname, source_board_id, target_board_id, configuration
):
    """
    This function is a wrapper to run the shallow_copy_board function in an asyncio event loop.
    """

    import json
    import asyncio

    from konza.wekan.board_operations.src.controllers.boards import copy_populated_board

    async def shallow_copy_board(
        source_hostname,
        target_hostname,
        source_board_id,
        target_board_id,
        configuration,
    ):
        """
        Function to copy a board from one wekan server to another.
        """

        parsed_configuration = json.loads(configuration)

        source_configuration = parsed_configuration.get("source_configuration")
        target_configuration = parsed_configuration.get("target_configuration")

        copy_response = await copy_populated_board(
            source_hostname,
            target_hostname,
            source_board_id,
            target_board_id,
            source_configuration,
            target_configuration,
        )

        return copy_response

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(
        shallow_copy_board(
            source_hostname,
            target_hostname,
            source_board_id,
            target_board_id,
            configuration,
        )
    )
    return result


with DAG(
    "C-7__Wekan_board_shallow_copy",
    catchup=False,
    start_date=datetime(2024, 1, 29),
    dagrun_timeout=timedelta(minutes=120),
):
    login_users_task = PythonVirtualenvOperator(
        task_id="login_users",
        python_callable=login_users_async,
        requirements=["httpx==0.26.0"],
        system_site_packages=True,
        python_version="3.11",
        op_kwargs={
            "source_hostname": Variable.get("source_hostname"),
            "target_hostname": Variable.get("target_hostname"),
            "source_username": Variable.get("source_username"),
            "source_password": Variable.get("source_password"),
            "target_username": Variable.get("target_username"),
            "target_password": Variable.get("target_password"),
        },
    )

    source_hostname = Variable.get("source_hostname")
    target_hostname = Variable.get("target_hostname")
    source_board_id = Variable.get("source_board_id")
    target_board_id = Variable.get("target_board_id")

    shallow_copy_board_task = PythonVirtualenvOperator(
        task_id="shallow_copy_board",
        python_callable=shallow_copy_board_async,
        requirements=["httpx==0.26.0"],
        system_site_packages=True,
        python_version="3.11",
        op_kwargs={
            "source_hostname": source_hostname,
            "target_hostname": target_hostname,
            "source_board_id": source_board_id,
            "target_board_id": target_board_id,
            "configuration": '{{ ti.xcom_pull(task_ids="login_users") }}',
        },
    )

    login_users_task >> shallow_copy_board_task
