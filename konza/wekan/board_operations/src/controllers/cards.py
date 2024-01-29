""" 
This module contains all the functions related to the boards cards.
"""

from konza.wekan.board_operations.src.utils.api import (
    api_get_request,
    api_post_request,
    api_put_request,
)


async def create_card(
    hostname: str, token: str, board_id: str, list_id: str, card_payload: dict
):
    """
    Function to create a board card.
    """

    if not hostname or not token or not board_id or not list_id or not card_payload:
        raise Exception(
            status_code=400,
            detail="Missing hostname, token, board_id, list_id or card_payload.",
        )

    url = f"{hostname}/api/boards/{board_id}/lists/{list_id}/cards"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_post_request(url, headers, card_payload)


async def get_card(
    hostname: str, token: str, board_id: str, list_id: str, card_id: str
):
    """
    Function to get a board card.
    """

    url = f"{hostname}/api/boards/{board_id}/lists/{list_id}/cards/{card_id}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_get_request(url, headers)


async def create_card_comment(
    hostname: str, token: str, board_id: str, card_id: str, comment_payload: dict
):
    """
    Function to create a board card comment.
    """

    if not hostname or not token or not board_id or not card_id or not comment_payload:
        raise Exception(
            status_code=400,
            detail="Missing hostname, token, board_id, card_id or comment_payload.",
        )

    url = f"{hostname}/api/boards/{board_id}/cards/{card_id}/comments"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_post_request(url, headers, comment_payload)


async def get_card_comment(
    hostname: str, token: str, board_id: str, card_id: str, comment_id: str
):
    """
    Function to get a board card comment.
    """

    url = f"{hostname}/api/boards/{board_id}/cards/{card_id}/comments/{comment_id}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_get_request(url, headers)


async def get_card_comments(hostname: str, token: str, board_id: str, card_id: str):
    """
    Function to get all available card comments of a given card.
    """

    url = f"{hostname}/api/boards/{board_id}/cards/{card_id}/comments"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    card_comments = await api_get_request(url, headers)
    final_card_comments = []

    for comment in card_comments:
        comment_id = comment.get("_id")

        comment_record = await get_card_comment(
            hostname, token, board_id, card_id, comment_id
        )

        final_card_comment = {
            **comment,
            **comment_record,
        }

        final_card_comments.append(final_card_comment)

    return final_card_comments


async def create_checklist(
    hostname: str, token: str, board_id: str, card_id: str, checklist_payload: dict
):
    """
    Function to create a board card checklist.
    """

    if (
        not hostname
        or not token
        or not board_id
        or not card_id
        or not checklist_payload
    ):
        raise Exception(
            status_code=400,
            detail="Missing hostname, token, board_id, card_id or checklist_payload.",
        )

    url = f"{hostname}/api/boards/{board_id}/cards/{card_id}/checklists"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_post_request(url, headers, checklist_payload)


async def edit_checklist_item(
    hostname: str,
    token: str,
    board_id: str,
    card_id: str,
    checklist_id: str,
    checklist_item_id: str,
    checklist_item_payload: dict,
):
    """
    Function to edit a board card checklist item.
    """

    if (
        not hostname
        or not token
        or not board_id
        or not card_id
        or not checklist_id
        or not checklist_item_id
        or not checklist_item_payload
    ):
        raise Exception(
            status_code=400,
            detail="Missing hostname, token, board_id, card_id, checklist_id, checklist_item_id or checklist_item_payload.",
        )

    url = f"{hostname}/api/boards/{board_id}/cards/{card_id}/checklists/{checklist_id}/items/{checklist_item_id}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_put_request(url, headers, checklist_item_payload)


async def get_checklist_items(
    hostname: str, token: str, board_id: str, card_id: str, checklist_id: str
):
    """
    Function to get all available card comments of a given card.
    """

    if not hostname or not token or not board_id or not card_id or not checklist_id:
        raise Exception(
            status_code=400,
            detail="Missing hostname, token, board_id, card_id or checklist_id.",
        )

    url = f"{hostname}/api/boards/{board_id}/cards/{card_id}/checklists/{checklist_id}"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    response = await api_get_request(url, headers)

    return response


async def get_populated_card_checklists(
    hostname: str, token: str, board_id: str, card_id: str
):
    """
    Function to get all available card checklists of a given card.
    """

    url = f"{hostname}/api/boards/{board_id}/cards/{card_id}/checklists"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    card_checklists = await api_get_request(url, headers)

    for checklist in card_checklists:
        checklist_id = checklist.get("_id")

        checklist_items_record = await get_checklist_items(
            hostname, token, board_id, card_id, checklist_id
        )

        checklist_items = checklist_items_record.get("items") or []

        checklist["items"] = checklist_items

    return card_checklists
