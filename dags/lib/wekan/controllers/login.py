"""
This module contains the function to sign in.
"""

from lib.wekan.controllers.users import get_users
from lib.wekan.utils.api import api_post_request


def login(hostname: str, username: str, password: str):
    """
    Function to sign in.
    """

    url = f"{hostname}/users/login"
    payload = {"username": username, "password": password}
    headers = {"Content-Type": "application/json", "Accept": "*/*"}

    login_response = api_post_request(url, headers, payload, payload_type="json")

    token = login_response["token"]

    instance_users = get_users(hostname, token)

    return {**login_response, "users": instance_users}
