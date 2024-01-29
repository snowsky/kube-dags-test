""" 
This module contains all the functions related to the users.
"""
import os
from konza.wekan.board_operations.src.utils.api import api_get_request

os.environ["no_proxy"] = "*"


async def get_user(hostname: str, token: str, user_id: str):
    """
    Function to get a user.
    """

    url = f"{hostname}/api/users/{user_id}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_get_request(url, headers)


async def get_users(hostname: str, token: str):
    """
    Function to get all the users.
    """

    url = f"{hostname}/api/users"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    users = await api_get_request(url, headers)
    populated_users = []

    for user in users:
        populated_user = await get_user(hostname, token, user["_id"])

        populated_users.append(populated_user)

    return populated_users
