from typing import TypedDict

from lib.wekan.types.boards import WekanCard, WekanList, WekanSwimlane


class LostCardDetails(TypedDict):
    card: WekanCard
    swimlane: WekanSwimlane
    list: WekanList
    board: dict
