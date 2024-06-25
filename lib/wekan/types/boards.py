from typing import TypedDict

from lib.wekan.types.users import User


class WekanConfiguration(TypedDict):
    hostname: str
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
    listId: str
    checklists: list


class WekanSwimlane(TypedDict):
    _id: str
    title: str
    cards: list[WekanCard]


class PopulatedBoard(TypedDict):
    _id: str
    lists: list[WekanList]
    swimlanes: list[WekanSwimlane]
