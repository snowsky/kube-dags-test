""" 
This module contains utilitary functions to interact with the Wekan API.
"""

import json
from httpx import Response

from airflow import AirflowException


def get_wekan_api_response_status_code(
    response: Response, json_response, default_status_code=500
):
    """
    Function to get the status code from a Wekan API response.
    """
    response_status_code = (
        json_response.get("statusCode")
        if type(json_response) == dict and json_response.get("statusCode")
        else response.status_code
        if response.status_code
        else default_status_code
    )

    return response_status_code


def get_wekan_api_response_error_message(error):
    """
    Function to get the error message from a Wekan API response.
    """

    (error_dict) = error.args[0]

    response_error_message = (
        error.message
        if hasattr(error, "message")
        else error_dict
        if error_dict
        else str(error)
    )

    return response_error_message


async def get_user_auth_record(hostname: str, username: str):
    """
    Function to get the user token from Redis.
    """

    if not hostname or not username:
        error_dict = {
            "status_code": 400,
            "detail": "Missing hostname, username or board_id.",
        }
        raise AirflowException(error_dict)

    """ authorization_record = await redis_client.get(
        f"{settings['REDIS_RECORD_PREFIX']}_{hostname}_{username}"
    ) """

    authorization_record = None

    if not authorization_record:
        error_dict = {"status_code": 401, "detail": "User not logged in."}
        raise AirflowException(error_dict)

    final_authorization_record = json.loads(authorization_record)

    return final_authorization_record


async def get_user_mapping(hostname: str, key: str, value: str, users: list = []):
    """
    Function to get the user mapping from Redis.
    """

    """ print(f"hostname: {hostname}")

    print(f"k: {key}")
    print(f"v: {value}") """

    if not hostname or not key or not value:
        error_dict = {
            "status_code": 400,
            "detail": "Missing hostname, mapping key or value.",
        }
        raise AirflowException(error_dict)

    """ users_record = await redis_client.get(
        f"{settings['REDIS_RECORD_PREFIX']}_{hostname}_users"
    ) """

    if not users:
        error_dict = {"status_code": 400, "detail": "User not logged in."}
        raise AirflowException(error_dict)

    """ print(f"users: {users}") """

    user_mapping = next((user for user in users if user[key] == value), None)

    if not user_mapping:
        error_dict = {"status_code": 404, "detail": "User not found."}
        raise AirflowException(error_dict)

    return user_mapping


async def get_user_list_mapping(
    source_hostname: str, target_hostname: str, user_list: list
):
    """
    Function to get the user list mapping from Redis.
    """

    if not source_hostname or not target_hostname or not type(user_list) == list:
        error_dict = {"status_code": 400, "detail": "Missing hostname or user_list."}
        raise AirflowException(error_dict)

    final_user_id_list = []

    for user_id in user_list:
        source_user_mapping = await get_user_mapping(source_hostname, "_id", user_id)

        if not source_user_mapping:
            error_dict = {"status_code": 404, "detail": "Missing user."}
            raise AirflowException(error_dict)

        username = source_user_mapping.get("username")  # type: ignore

        target_user_mapping = await get_user_mapping(
            target_hostname, "username", username
        )

        target_user_id = target_user_mapping.get("_id")  # type: ignore

        if not target_user_id:
            error_dict = {"status_code": 404, "detail": "User not found."}
            raise AirflowException(error_dict)

        final_user_id_list.append(target_user_id)

    return final_user_id_list
