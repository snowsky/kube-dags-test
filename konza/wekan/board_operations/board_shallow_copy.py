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

import asyncio
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, TaskInstance

from konza.wekan.board_operations.src.controllers.login import login
from konza.wekan.board_operations.src.controllers.boards import copy_populated_board


async def login_users(ti: TaskInstance):
    """
    Function to login a users and return the tokens as a Xcom.
    """

    source_hostname = str(Variable.get("source_hostname"))
    target_hostname = str(Variable.get("target_hostname"))
    source_username = str(Variable.get("source_username"))
    source_password = str(Variable.get("source_password"))
    target_username = str(Variable.get("target_username"))
    target_password = str(Variable.get("target_password"))

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
        raise Exception(status_code=error, detail={source_response, target_response})

    ti.xcom_push(key="source_configuration", value=source_response)
    ti.xcom_push(key="target_configuration", value=target_response)


def login_users_async(ti: TaskInstance):
    """
    This function is a wrapper to run the login_users function in an asyncio event loop.
    """
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(login_users(ti))
    return result


async def shallow_copy_board(ti: TaskInstance):
    """
    Function to copy a board from one wekan server to another.
    """

    source_hostname = Variable.get("source_hostname")
    target_hostname = Variable.get("target_hostname")
    source_board_id = Variable.get("source_board_id")
    target_board_id = Variable.get("target_board_id")

    source_configuration = ti.xcom_pull(
        key="source_configuration", task_ids="login_users"
    )
    target_configuration = ti.xcom_pull(
        key="target_configuration", task_ids="login_users"
    )

    copy_response = await copy_populated_board(
        source_hostname,
        target_hostname,
        source_board_id,
        target_board_id,
        source_configuration,
        target_configuration,
    )

    return copy_response


def shallow_copy_board_async(ti: TaskInstance):
    """
    This function is a wrapper to run the shallow_copy_board function in an asyncio event loop.
    """
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(shallow_copy_board(ti))
    return result


with DAG(
    "C-7__Wekan_board_shallow_copy",
    dagrun_timeout=timedelta(minutes=1),
):
    login_users_task = PythonOperator(
        task_id="login_users", provide_context=True, python_callable=login_users_async
    )

    shallow_copy_board_task = PythonOperator(
        task_id="shallow_copy_board",
        provide_context=True,
        python_callable=shallow_copy_board_async,
    )

    login_users_task >> shallow_copy_board_task
