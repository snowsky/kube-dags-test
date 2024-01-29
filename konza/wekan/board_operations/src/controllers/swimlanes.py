""" 
This module contains all the functions related to the boards.
"""

from konza.wekan.board_operations.src.controllers.cards import (
    get_card,
    get_card_comments,
    get_populated_card_checklists,
)
from konza.wekan.board_operations.src.utils.api import (
    api_get_request,
    api_post_request,
)


async def create_swimlane(hostname: str, token: str, board_id: str, swimlane_name: str):
    """
    Function to create a board swimlane.
    """

    if not hostname or not token or not board_id or not swimlane_name:
        raise Exception(
            status_code=400,
            detail="Missing hostname, token, board_id or list_name.",
        )

    url = f"{hostname}/api/boards/{board_id}/swimlanes"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_post_request(url, headers, {"title": swimlane_name})


async def get_board_swimlanes(hostname: str, token: str, board_id: str):
    """
    Function to get all available board populated swimlanes.
    """

    url = f"{hostname}/api/boards/{board_id}/swimlanes"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_get_request(url, headers)


async def get_swimlane_cards(
    hostname: str, token: str, swimlane_id: str, board_id: str
):
    """
    Function to get all available board populated swimlanes.
    """

    url = f"{hostname}/api/boards/{board_id}/swimlanes/{swimlane_id}/cards"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    swimlane_cards = await api_get_request(url, headers)

    populated_swimlane_cards = []

    for card in swimlane_cards:
        card_id = card.get("_id")
        list_id = card.get("listId")

        populate_card = await get_card(hostname, token, board_id, list_id, card_id)

        populated_swimlane_cards.append(populate_card)

    return populated_swimlane_cards


async def get_populated_board_swimlanes(hostname: str, token: str, board_id: str):
    """
    Function to get all available board populated swimlanes.
    """

    board_swimlanes = await get_board_swimlanes(hostname, token, board_id)

    for swimlane in board_swimlanes:
        swimlane_id = swimlane.get("_id")
        swimlane_cards = await get_swimlane_cards(
            hostname, token, swimlane_id, board_id
        )

        for card in swimlane_cards:
            card_id = card.get("_id")

            card_comments = await get_card_comments(hostname, token, board_id, card_id)
            card["comments"] = card_comments

            card_checklists = await get_populated_card_checklists(
                hostname, token, board_id, card_id
            )
            card["checklists"] = card_checklists

        swimlane["cards"] = swimlane_cards

    return board_swimlanes
