"""
This module contains utilitary functions to interact with the Wekan API.
"""

import json

from requests import Response


def get_wekan_api_response_status_code(
    response: Response, json_response, default_status_code=500
):
    """
    Function to get the status code from a Wekan API response.
    """
    response_status_code = (
        json_response.get("statusCode")
        if isinstance(json_response, dict) and json_response.get("statusCode")
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


def get_user_auth_record(hostname: str, username: str):
    """
    Function to get the user token from Redis.
    """

    if not hostname or not username:
        raise Exception("Missing hostname, username or board_id.")

    """ authorization_record = redis_client.get(
        f"{settings['REDIS_RECORD_PREFIX']}_{hostname}_{username}"
    ) """

    authorization_record = None

    if not authorization_record:
        raise Exception("User not logged in.")

    final_authorization_record = json.loads(authorization_record)

    return final_authorization_record


def get_user_mapping(hostname: str, key: str, value: str, users: list = []):
    """
    Function to get the user mapping from Redis.
    """

    """ print(f"hostname: {hostname}")

    print(f"k: {key}")
    print(f"v: {value}") """

    if not hostname or not key or not value:
        raise Exception("Missing hostname, mapping key or value.")

    """ users_record = redis_client.get(
        f"{settings['REDIS_RECORD_PREFIX']}_{hostname}_users"
    ) """

    if not users:
        raise Exception("User not logged in.")

    """ print(f"users: {users}") """

    user_mapping = next((user for user in users if user[key] == value), None)

    if not user_mapping:
        raise Exception("User not found.")

    return user_mapping


def get_user_list_mapping(source_hostname: str, target_hostname: str, user_list: list):
    """
    Function to get the user list mapping from Redis.
    """

    if not source_hostname or not target_hostname or not isinstance(user_list, list):
        raise Exception("Missing hostname or user_list.")

    final_user_id_list = []

    for user_id in user_list:
        source_user_mapping = get_user_mapping(source_hostname, "_id", user_id)

        if not source_user_mapping:
            raise Exception("Missing user.")

        username = source_user_mapping.get("username")  # type: ignore

        target_user_mapping = get_user_mapping(target_hostname, "username", username)

        target_user_id = target_user_mapping.get("_id")  # type: ignore

        if not target_user_id:
            raise Exception("User not found.")

        final_user_id_list.append(target_user_id)

    return final_user_id_list
