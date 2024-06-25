from typing import TypedDict


class User(TypedDict):
    _id: str
    createdAt: str
    username: str
    emails: list[dict[str, str]]
