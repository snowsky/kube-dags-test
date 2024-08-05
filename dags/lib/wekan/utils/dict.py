from lib.wekan.utils.strings import (
    convert_string_to_html_entities,
)


def convert_dict_to_html_entities(dictionary):
    """
    Converts a dictionary to HTML entities.
    """

    for key in dictionary:
        dictionary[key] = (
            convert_string_to_html_entities(dictionary[key])
            if isinstance(dictionary[key], str)
            else dictionary[key]
        )

    return dictionary
