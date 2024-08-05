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
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "*/*"}

    login_response = api_post_request(url, headers, payload, payload_type="form-data")

    token = login_response["token"]

    instance_users = get_users(hostname, token)

    return {**login_response, "users": instance_users}
