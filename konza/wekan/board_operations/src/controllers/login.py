""" 
This module contains the function to sign in.
"""

from konza.wekan.board_operations.src.controllers.users import get_users
from konza.wekan.board_operations.src.utils.api import api_post_request


async def login(hostname: str, username: str, password: str):
    """
    Function to sign in.
    """

    url = f"{hostname}/users/login"
    payload = {"username": username, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "*/*"}

    login_response = await api_post_request(
        url, headers, payload, payload_type="form-data"
    )

    token = login_response["token"]

    instance_users = await get_users(hostname, token)

    return {**login_response, "users": instance_users}
