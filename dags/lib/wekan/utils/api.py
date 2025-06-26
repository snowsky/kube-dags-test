"""
This module contains functions to make API requests to Wekan.
"""

import os

from airflow.exceptions import AirflowException
import requests

from lib.wekan.utils.dict import convert_dict_to_html_entities
from lib.wekan.utils.wekan import (
    get_wekan_api_response_error_message,
    get_wekan_api_response_status_code,
)

os.environ["no_proxy"] = "*"


def api_get_request(url: str, headers: dict, timeout: int = 10, params=None):
    """
    Function to make an API GET request.
    """

    response_status_code = 200

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout,
            params=params
        )

        print(f"api_get_request: {response}")

        final_response = response.json()

        # print(f"api_get_request: {final_response}")

        response_status_code = get_wekan_api_response_status_code(
            response, final_response
        )

        if response_status_code == 200:
            return final_response

        else:
            raise RuntimeError(final_response)

    except Exception as error:
        print(f"api_get_request: {error}")

        error_dict = {
            "status_code": response_status_code,
            "detail": get_wekan_api_response_error_message(error),
        }

        raise AirflowException(error_dict) from error


def api_post_request(
    url: str,
    headers: dict,
    payload: dict,
    timeout: int = 10,
    payload_type: str = "json",
):
    """
    Function to make an API POST request.
    """

    response_status_code = 200

    try:
        # final_payload = convert_dict_to_html_entities(payload)
        final_payload = payload

        # print(f"api_post_request: {url}")
        # print(f"api_post_request: {final_payload}")

        response = (
            requests.post(
                url,
                headers=headers,
                json=final_payload,
                timeout=timeout,
            )
            if payload_type == "json"
            else requests.post(
                url,
                headers=headers,
                data=final_payload,
                timeout=timeout,
            )
        )

        print(f"api_post_request: {response}")

        final_response = response.json()

        # print(f"api_post_request: {final_response}")

        response_status_code = get_wekan_api_response_status_code(
            response, final_response
        )

        if response_status_code == 200:
            return final_response

        else:
            raise RuntimeError(final_response)

    except Exception as error:
        print(f"api_post_request: {error}")
        error_dict = {
            "status_code": response_status_code,
            "detail": get_wekan_api_response_error_message(error),
        }

        raise AirflowException(error_dict) from error


def api_put_request(
    url: str,
    headers: dict,
    payload: dict,
    timeout: int = 10,
):
    """
    Function to make an API PUT request.
    """

    response_status_code = 200

    try:
        final_payload = convert_dict_to_html_entities(payload)

        # print(f"api_put_request: {url}")
        # print(f"api_put_request: {final_payload}")

        response = requests.put(
            url,
            headers=headers,
            json=final_payload,
            timeout=timeout,
        )

        print(f"api_put_request: {response}")

        final_response = response.json()

        # print(f"api_put_request: {final_response}")

        response_status_code = get_wekan_api_response_status_code(
            response, final_response
        )

        if response_status_code == 200:
            return final_response

        else:
            raise RuntimeError(final_response)

    except Exception as error:
        print(f"api_put_request: {error}")
        error_dict = {
            "status_code": response_status_code,
            "detail": get_wekan_api_response_error_message(error),
        }

        raise AirflowException(error_dict) from error
