""" 
This module contains functions for string operations.
"""

import html


def convert_string_to_html_entities(string):
    """
    Converts a string to HTML entities.
    """
    string = html.escape(string)
    # ET Add Character Exceptions Here - Search for the character in google and its corresponding HTML entity or HTML code
    entities = {
        "|": "&#124;",
        "$": "&#36;",
        "%": "&#37;",
        "^": "&#94;",
        "@": "&#64;",
        "/": "&#47;",
        " ": "&nbsp;",
        "\xa0": "&nbsp;",
        "route": "&#x72;&#x6f;&#x75;&#x74;&#x65;",
    }

    for entity, value in entities.items():
        string = string.replace(entity, value)

    return string
