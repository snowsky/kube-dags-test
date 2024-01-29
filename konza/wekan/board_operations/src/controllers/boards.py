""" 
This module contains all the functions related to the boards.
"""

from airflow import AirflowException

from konza.wekan.board_operations.src.controllers.swimlanes import (
    create_swimlane,
    get_populated_board_swimlanes,
)
from konza.wekan.board_operations.src.controllers.cards import (
    create_card,
    create_card_comment,
    create_checklist,
    edit_checklist_item,
    get_checklist_items,
)

from konza.wekan.board_operations.src.utils.api import api_get_request, api_post_request
from konza.wekan.board_operations.src.utils.wekan import (
    get_user_mapping,
)


async def get_user_boards(hostname: str, user_id: str, token: str):
    """
    Function to get all the boards the user has access to.
    """

    url = f"{hostname}/api/users/{user_id}/boards"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_get_request(url, headers)


async def get_active_user_boards(hostname: str, token: str, boards: list):
    """
    Function to get all the active boards the user has access to.
    """

    try:
        active_boards = []
        for board in boards:
            populated_board = await get_board_details(hostname, token, board.get("_id"))

            board_archived = populated_board.get("archived")

            if not board_archived:
                active_boards.append(populated_board)
        return active_boards

    except Exception as error:
        (error_dict) = error.args[0]

        return error.message if hasattr(error, "message") else error_dict if error_dict else str(error)  # type: ignore


async def create_board_list(hostname: str, token: str, board_id: str, list_name: str):
    """
    Function to create a board list.
    """

    if not hostname or not token or not board_id or not list_name:
        error_dict = {
            "status_code": 400,
            "detail": "Missing hostname, token, board_id or list_name.",
        }
        raise AirflowException(error_dict)

    url = f"{hostname}/api/boards/{board_id}/lists"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_post_request(url, headers, {"title": list_name})


async def get_board_list(hostname: str, token: str, board_id: str, list_id: str):
    """
    Function to get a board list.
    """

    url = f"{hostname}/api/boards/{board_id}/lists/{list_id}"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_get_request(url, headers)


async def get_board_lists(hostname: str, token: str, board_id: str):
    """
    Function to get all available board lists.
    """

    url = f"{hostname}/api/boards/{board_id}/lists"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    board_lists = await api_get_request(url, headers)

    for board_list in board_lists:
        list_id = board_list.get("_id")
        board_list = {
            **board_list,
            **await get_board_list(hostname, token, board_id, list_id),
        }

    return board_lists


async def get_board_details(hostname: str, token: str, board_id: str):
    """
    Function to get all available board details.
    """

    url = f"{hostname}/api/boards/{board_id}"

    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    board_details = await api_get_request(url, headers)

    board_details["lists"] = await get_board_lists(hostname, token, board_id)

    return board_details


async def get_board_export(hostname: str, token: str, board_id: str):
    """
    Function to get a board export.
    """

    url = f"{hostname}/api/boards/{board_id}/export?authToken={token}"
    headers = {
        "Accept": "*/*",
        "Authorization": f"Bearer {token}",
    }

    return await api_get_request(url, headers)


async def copy_populated_board(
    source_hostname: str,
    target_hostname: str,
    source_board_id: str,
    target_board_id: str,
    source_configuration: dict[str, str],
    target_configuration: dict[str, str],
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
        error_dict = {
            "status_code": 400,
            "detail": "Missing source hostname, target hostname, board id, source token or target token.",
        }
        raise AirflowException(error_dict)

    creator_auth_record = target_configuration

    creator_id = creator_auth_record.get("id")

    source_auth_record = source_configuration

    source_token = source_auth_record.get("token")
    source_users = source_auth_record.get("users")

    target_auth_record = target_configuration

    target_token = target_auth_record.get("token")

    board_details = await get_board_details(
        source_hostname, source_token, source_board_id
    )

    board_swimlanes = await get_populated_board_swimlanes(
        hostname=source_hostname, token=source_token, board_id=source_board_id
    )

    populated_board = {
        **board_details,
        "swimlanes": board_swimlanes,
    }

    created_board_lists = []

    populated_board_lists = (
        populated_board.get("lists") if populated_board.get("lists") else []
    )

    populated_board_lists_length = len(populated_board_lists)  # type: ignore

    for index, board_list in enumerate(populated_board.get("lists")):  # type: ignore
        print(f"Copying list {index + 1} of {populated_board_lists_length}")

        board_list_title = board_list.get("title")

        list_creation = await create_board_list(
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

        target_swimlane = await create_swimlane(
            target_hostname, target_token, target_board_id, swimlane_title
        )

        swimlane_id = target_swimlane.get("_id")

        swimlane_cards = swimlane.get("cards") if swimlane.get("cards") else []

        swimlane_cards_length = len(swimlane_cards)

        for index, card in enumerate(swimlane_cards):
            print(f"Copying card {index + 1} of {swimlane_cards_length}")

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
            #     await get_user_mapping(source_hostname, "_id", source_card_author_id)
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

            card_creation_response = await create_card(
                target_hostname,
                target_token,
                target_board_id,
                target_card_list_id,
                card_payload,
            )

            target_card_id = card_creation_response.get("_id")

            card_comments = card.get("comments") if card.get("comments") else []

            card_comments.reverse()

            card_comments_length = len(card_comments)

            for index, comment in enumerate(card_comments):
                print(f"Copying comment {index + 1} of {card_comments_length}")

                comment_text = comment.get("comment")

                source_comment_author_id = comment.get("authorId")
                source_comment_created_at = comment.get("createdAt")
                source_comment_modified_at = comment.get("modifiedAt")

                source_comment_author = await get_user_mapping(
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

                await create_card_comment(
                    target_hostname,
                    target_token,
                    target_board_id,
                    target_card_id,
                    comment_payload,
                )

            card_checklists = card.get("checklists") if card.get("checklists") else []

            card_checklists_length = len(card_checklists)

            for index, checklist in enumerate(card_checklists):
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

                target_checklist = await create_checklist(
                    target_hostname,
                    target_token,
                    target_board_id,
                    target_card_id,
                    checklist_payload,
                )

                populated_checklist = await get_checklist_items(
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
                        await edit_checklist_item(
                            target_hostname,
                            target_token,
                            target_board_id,
                            target_card_id,
                            target_checklist.get("_id"),
                            populated_checklist_item.get("_id"),
                            {"title": checklist_item_title, "isFinished": True},
                        )
