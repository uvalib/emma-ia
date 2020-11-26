# common.py
#
# Common definitions.

# noinspection PyUnresolvedReferences
import logging
import os

# noinspection PyUnresolvedReferences
from typing import Union, Dict, List, Optional, ItemsView, KeysView, ValuesView


# =============================================================================
# Constants
# =============================================================================


DEPLOYMENT = os.environ.get('DEPLOYMENT', 'staging')

FALSE_VALUES = ['0', 'no',  'false', 'off']
TRUE_VALUES  = ['1', 'yes', 'true',  'on']


# =============================================================================
# Functions
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


def is_lambda(item):
    """
    Indicate whether the argument is a lambda.
    """
    return isinstance(item, type(lambda: 0))


def is_false(item):
    """
    Indicate whether the argument is or represents a False value.
    """
    if isinstance(item, str):
        return item.casefold() in FALSE_VALUES
    else:
        return isinstance(item, bool) and not item


def is_true(item):
    """
    Indicate whether the argument is or represents a True value.
    """
    if isinstance(item, str):
        return item.casefold() in TRUE_VALUES
    else:
        return isinstance(item, bool) and item


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
