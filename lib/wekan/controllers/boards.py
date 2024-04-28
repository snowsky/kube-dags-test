"""
This module contains all the functions related to the boards.
"""

from typing import TypedDict
import typing

from airflow import XComArg

from lib.wekan.controllers.swimlanes import (
    create_swimlane,
    get_populated_board_swimlanes,
)
from lib.wekan.controllers.cards import (
    create_card,
    create_card_comment,
    create_checklist,
    edit_checklist_item,
    get_checklist_items,
)
from lib.wekan.controllers.users import User

from lib.wekan.utils.api import api_get_request, api_post_request
from lib.wekan.utils.wekan import (
    get_user_mapping,
)


class WekanConfiguration(TypedDict):
    id: str
    token: str
    tokenExpires: str
    users: list[User]


class WekanList(TypedDict):
    _id: str
    title: str


class WekanComments(TypedDict):
    _id: str
    text: str
    comment: str
    authorId: str
    createdAt: str
    modifiedAt: str
    boardId: str
    cardId: str
    userId: str


class WekanCard(TypedDict):
    _id: str
    title: str
    comments: list[WekanComments]


class WekanSwimlane(TypedDict):
    _id: str
    title: str
    cards: list[WekanCard]


class PopulatedBoard(TypedDict):
    _id: str
    lists: list[WekanList]


def get_user_boards(hostname: str, user_id: str, token: str):
    """
    Function to get all the boards the user has access to.
    """

    url = f"{hostname}/api/users/{user_id}/boards"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return api_get_request(url, headers)


def get_active_user_boards(hostname: str, token: str, boards: list):
    """
    Function to get all the active boards the user has access to.
    """

    try:
        active_boards = []
        for board in boards:
            populated_board = get_board_details(hostname, token, board.get("_id"))

            board_archived = populated_board.get("archived")

            if not board_archived:
                active_boards.append(populated_board)
        return active_boards

    except Exception as error:
        (error_dict) = error.args[0]

        return error.message if hasattr(error, "message") else error_dict if error_dict else str(error)  # type: ignore


def create_board_list(hostname: str, token: str, board_id: str, list_name: str):
    """
    Function to create a board list.
    """

    if not hostname or not token or not board_id or not list_name:
        raise Exception("Missing hostname, token, board_id or list_name.")

    url = f"{hostname}/api/boards/{board_id}/lists"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return api_post_request(url, headers, {"title": list_name})


def get_board_list(hostname: str, token: str, board_id: str, list_id: str):
    """
    Function to get a board list.
    """

    url = f"{hostname}/api/boards/{board_id}/lists/{list_id}"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return api_get_request(url, headers)


def get_board_lists(hostname: str, token: str, board_id: str):
    """
    Function to get all available board lists.
    """

    url = f"{hostname}/api/boards/{board_id}/lists"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    board_lists = api_get_request(url, headers)

    for board_list in board_lists:
        list_id = board_list.get("_id")
        board_list = {
            **board_list,
            **get_board_list(hostname, token, board_id, list_id),
        }

    return board_lists


def get_board_details(hostname: str, token: str, board_id: str):
    """
    Function to get all available board details.
    """

    url = f"{hostname}/api/boards/{board_id}"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    board_details = api_get_request(url, headers)

    board_details["lists"] = get_board_lists(hostname, token, board_id)

    return board_details


def get_board_export(hostname: str, token: str, board_id: str):
    """
    Function to get a board export.
    """

    url = f"{hostname}/api/boards/{board_id}/export?authToken={token}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return api_get_request(url, headers)


def get_populated_board(
    hostname: str, board_id: str, configuration: WekanConfiguration
):
    """
    Function to get a populated board including all the cards with their lists, cards, and comments.
    """

    if not hostname or not board_id or not configuration:
        raise Exception("Missing hostname, board id or configuration.")

    auth_record = configuration

    token = auth_record.get("token")

    board_details = get_board_details(hostname, token, board_id)

    board_swimlanes = get_populated_board_swimlanes(
        hostname=hostname, token=token, board_id=board_id
    )

    populated_board = {
        **board_details,
        "swimlanes": board_swimlanes,
    }

    return populated_board


def copy_populated_board(
    source_hostname: str,
    target_hostname: str,
    source_board_id: str,
    target_board_id: str,
    source_configuration: WekanConfiguration,
    target_configuration: WekanConfiguration,
    raw_populated_board: XComArg,
):
    """
    Function to copy a board from one instance to another. Including all the cards with their lists, cards, and comments.
    """

    if (
        not source_hostname
        or not target_hostname
        or not source_board_id
        or not target_board_id
        or not source_configuration
        or not target_configuration
    ):
        raise Exception(
            "Missing source hostname, target hostname, board id, source token or target token."
        )

    creator_auth_record = target_configuration

    creator_id = creator_auth_record.get("id")

    source_auth_record = source_configuration

    source_users = source_auth_record.get("users")

    target_auth_record = target_configuration

    target_token = target_auth_record.get("token")

    populated_board = typing.cast(PopulatedBoard, raw_populated_board)

    created_board_lists = []

    populated_board_lists = (
        populated_board.get("lists") if populated_board.get("lists") else []
    )

    populated_board_lists_length = len(populated_board_lists)  # type: ignore

    for index, board_list in enumerate(populated_board.get("lists")):  # type: ignore
        print(f"Copying list {index + 1} of {populated_board_lists_length}")

        board_list_title = board_list.get("title")

        list_creation = create_board_list(
            target_hostname, target_token, target_board_id, board_list_title
        )

        list_creation_id = list_creation.get("_id")

        created_board_lists.append(
            {
                "_id": list_creation_id,
                "title": board_list_title,
                "old_id": board_list.get("_id"),
            }
        )

    populated_board_swimlanes = (
        populated_board.get("swimlanes") if populated_board.get("swimlanes") else []
    )

    populated_board_swimlanes_length = len(populated_board_swimlanes)  # type: ignore

    for index, swimlane in enumerate(populated_board_swimlanes):  # type: ignore
        print(f"Copying swimlane {index + 1} of {populated_board_swimlanes_length}")

        swimlane_title = swimlane.get("title")

        target_swimlane = create_swimlane(
            target_hostname, target_token, target_board_id, swimlane_title
        )

        swimlane_id = target_swimlane.get("_id")

        swimlane_cards = swimlane.get("cards") if swimlane.get("cards") else []

        swimlane_cards_length = len(swimlane_cards)

        for index, card in enumerate(swimlane_cards):
            print(f"Copying card {index + 1} of {swimlane_cards_length}")
            for i in range(0,100):
                try:
                    old_card_list_id = card.get("listId")
        
                    target_card_list = next(
                        (
                            list
                            for list in created_board_lists
                            if list.get("old_id") == old_card_list_id
                        ),
                    )
        
                    target_card_list_id = target_card_list.get("_id")
        
                    card_title = card.get("title")
                    card_created_at = card.get("createdAt")
                    card_updated_at = card.get("updatedAt")
        
                    # source_card_author_id = card.get("author")
        
                    # card_author = (
                    #     get_user_mapping(source_hostname, "_id", source_card_author_id)
                    #     if source_card_author_id
                    #     else False
                    # )
        
                    # card_author_id = card_author.get("_id") if card_author else creator_id
        
                    card_author_id = creator_id
        
                    card_description = card.get("description", "")
        
                    final_card_description = f"{card_description}\n\n\nCreated at {card_created_at}.\nUpdated at: {card_updated_at}.\nOriginal card: {source_hostname}/b/{source_board_id}/{populated_board.get('slug')}/{card.get('_id')}."
        
                    card_payload = {
                        "authorId": card_author_id,
                        "title": card_title,
                        "description": final_card_description,
                        "swimlaneId": swimlane_id,
                    }
        
                    print(
                        f"Creating card: {target_hostname} {target_token} {target_board_id} {target_card_list_id} {card_payload}"
                    )
        
                    card_creation_response = create_card(
                        target_hostname,
                        target_token,
                        target_board_id,
                        target_card_list_id,
                        card_payload,
                    )
        
                    target_card_id = card_creation_response.get("_id")
                except:
                    continue
                break

            card_comments = card.get("comments") if card.get("comments") else []

            card_comments.reverse()

            card_comments_length = len(card_comments)

            for index, comment in enumerate(card_comments):
                for i in range(0,100):
                    try:
                        print(f"Copying comment {index + 1} of {card_comments_length}")

                        comment_text = comment.get("comment")

                        source_comment_author_id = comment.get("authorId")
                        source_comment_created_at = comment.get("createdAt")
                        source_comment_modified_at = comment.get("modifiedAt")

                        source_comment_author = get_user_mapping(
                            source_hostname, "_id", source_comment_author_id, source_users
                        )

                        source_comment_author_email = source_comment_author.get("emails")[
                            0
                        ].get("address")

                        source_comment_author_name = (
                            source_comment_author.get("profile").get("fullname") or None
                        )

                        final_comment_text = f"{comment_text}\n\n\nWrote by: {f'{source_comment_author_name} ' if source_comment_author_name else ''} {source_comment_author_email}\nCreated at: {source_comment_created_at}.\nModified at: {source_comment_modified_at}."

                        comment_payload = {
                            "comment": final_comment_text,
                            "authorId": creator_id,
                        }

                        print(
                            f"Creating comment: {target_hostname} {target_token} {target_board_id} {target_card_id} {comment_payload}"
                        )

                        create_card_comment(
                            target_hostname,
                            target_token,
                            target_board_id,
                            target_card_id,
                            comment_payload,
                        )
                    except:
                        continue
                    break

            card_checklists = card.get("checklists") if card.get("checklists") else []

            card_checklists_length = len(card_checklists)

            for index, checklist in enumerate(card_checklists):
                for attempt in range(1,5):
                    try:
                        print(f"Copying checklist {index + 1} of {card_checklists_length}")
        
                        checklist_title = checklist.get("title")
        
                        checklist_items = checklist.get("items")
        
                        final_checklist_items = []
        
                        for item in checklist_items:
                            item_title = item.get("title")
        
                            final_checklist_items.append(item_title)
        
                        checklist_payload = {
                            "title": checklist_title,
                            "items": final_checklist_items,
                        }
        
                        print(
                            f"Creating checklist: {target_hostname} {target_token} {target_board_id} {target_card_id} {checklist_payload}"
                        )
        
                        target_checklist = create_checklist(
                            target_hostname,
                            target_token,
                            target_board_id,
                            target_card_id,
                            checklist_payload,
                        )
        
                        populated_checklist = get_checklist_items(
                            target_hostname,
                            target_token,
                            target_board_id,
                            target_card_id,
                            target_checklist.get("_id"),
                        )
        
                        for index, checklist_item in enumerate(checklist_items):
                            checklist_item_title = checklist_item.get("title")
                            checklist_item_is_checked = checklist_item.get("isFinished")
        
                            populated_checklist_item = populated_checklist.get("items")[index]
        
                            if checklist_item_is_checked is True:
                                edit_checklist_item(
                                    target_hostname,
                                    target_token,
                                    target_board_id,
                                    target_card_id,
                                    target_checklist.get("_id"),
                                    populated_checklist_item.get("_id"),
                                    {"title": checklist_item_title, "isFinished": True},
                                )
                        break
                    except:
                        print(
                            f"RETRYING - Creating checklist TIMEOUT: {target_hostname} {target_token} {target_board_id} {target_card_id} {checklist_payload}"
                        )
                        
