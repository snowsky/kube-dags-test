"""
This module contains all the functions related to the users.
"""
import os
from typing import TypedDict
from lib.wekan.utils.api import api_get_request

os.environ["no_proxy"] = "*"


class User(TypedDict):
    _id: str
    createdAt: str
    username: str
    emails: list[dict[str, str]]


def get_user(hostname: str, token: str, user_id: str):
    """
    Function to get a user.
    """

    url = f"{hostname}/api/users/{user_id}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return api_get_request(url, headers)


def get_users(hostname: str, token: str) -> list[User]:
    """
    Function to get all the users.
    """

    url = f"{hostname}/api/users"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    users = api_get_request(url, headers)
    populated_users = []

    for user in users:
        populated_user = get_user(hostname, token, user["_id"])

        populated_users.append(populated_user)

    return populated_users
