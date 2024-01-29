""" 
This module contains functions to make API requests to Wekan.
"""

import os
import httpx

from airflow import AirflowException

from konza.wekan.board_operations.src.utils.dict import convert_dict_to_html_entities
from konza.wekan.board_operations.src.utils.wekan import (
    get_wekan_api_response_error_message,
    get_wekan_api_response_status_code,
)

os.environ["no_proxy"] = "*"


async def api_get_request(url: str, headers: dict, timeout: int = 10):
    """
    Function to make an API GET request.
    """

    response_status_code = 200

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url,
                headers=headers,
                timeout=timeout,
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


async def api_post_request(
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
        async with httpx.AsyncClient() as client:
            # final_payload = convert_dict_to_html_entities(payload)
            final_payload = payload

            # print(f"api_post_request: {url}")
            # print(f"api_post_request: {final_payload}")

            response = (
                await client.post(
                    url,
                    headers=headers,
                    json=final_payload,
                    timeout=timeout,
                )
                if payload_type == "json"
                else await client.post(
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


async def api_put_request(
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
        async with httpx.AsyncClient() as client:
            final_payload = convert_dict_to_html_entities(payload)

            # print(f"api_put_request: {url}")
            # print(f"api_put_request: {final_payload}")

            response = await client.put(
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
