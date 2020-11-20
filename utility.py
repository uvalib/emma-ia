# utility.py

# noinspection PyUnresolvedReferences
import logging
import math
import sys

# noinspection PyUnresolvedReferences
from typing import Union, Dict, List, Optional, ItemsView, KeysView, ValuesView
from pprint import PrettyPrinter


# =============================================================================
# Constants
# =============================================================================


PP_NARROW = 1
PP_WIDE   = 120
PP_WIDTH  = PP_NARROW
PP_INDENT = 4
PP_KWARGS = {'width': PP_WIDTH, 'indent': PP_INDENT, 'sort_dicts': False}

CONSOLE_WIDTH = 80


# =============================================================================
# Variables
# =============================================================================


pp      = PrettyPrinter(**PP_KWARGS)
pp_wide = PrettyPrinter(**{**PP_KWARGS, 'width': PP_WIDE})


# =============================================================================
# General
# =============================================================================


def is_blank(value) -> bool:
    """
    Indicate whether the argument is a non-value.  True only if value is not a
    bool, int, float, non-blank str or non-empty list.
    """
    return False if value in (0, 0.0, False) else not value


def is_present(value) -> bool:
    """
    Indicate whether the argument is a non-blank value.
    """
    return not is_blank(value)


def to_list(value, default=None) -> list:
    """
    Transform value to a list, or the default if value is blank.
    """
    if isinstance(value, list):
        return value
    elif isinstance(value, tuple):
        return list(value)
    elif is_present(value):
        return [value]
    elif default is None:
        return []
    else:
        return default if isinstance(default, list) else [default]


# =============================================================================
# Console output
# =============================================================================


def show(value, width=None, indent=None, pp_override=None):
    """
    Pretty-print value.

    :param any           value:         Item to display.
    :param int           width:         Override width.
    :param int           indent:        Override indent.
    :param PrettyPrinter pp_override:   Override default pp.

    """
    if isinstance(value, (str, int)):
        print(value)
    elif pp_override:
        pp_override.pprint(value)
    elif width or indent:
        kwargs = PP_KWARGS.copy()
        if width:
            kwargs.update(width=width)
        if indent:
            kwargs.update(indent=indent)
        PrettyPrinter(**kwargs).pprint(value)
    else:
        pp.pprint(value)


def show_section(value, width=CONSOLE_WIDTH, gap=2):
    """
    Output a major section heading.

    :param str value:   Heading label.
    :param int width:   Width of output line.
    :param int gap:     Number of blanks on either side of label.

    """
    width -= 1
    length = len(value)
    if length >= (width + (2 * gap)):
        label = value
    else:
        stars = '*' * (math.floor((width - length) / 2) - gap)
        space = ' ' * gap
        label = space.join([stars, value, stars])
        label += '*' * (width - len(label))  # Fill out line if necessary.
    _show_lines('', label)


def show_header(value, width=CONSOLE_WIDTH):
    """
    Output a heading.

    :param str value: Heading label.
    :param int width: Width of output line.

    """
    width -= 1
    h_bar = '-' * min(len(value), width)
    _show_lines('', value, h_bar)


def _show_lines(*lines):
    """
    Output a line for each argument.

    :param str lines:

    """
    for line in lines:
        sys.stdout.write(line + "\n")
