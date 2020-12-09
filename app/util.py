# app/util.py
#
# Utility functions.


import typing


# =============================================================================
# Constants
# =============================================================================


FALSE_VALUES = ('0', 'no',  'false', 'off')
TRUE_VALUES  = ('1', 'yes', 'true',  'on')
ENUMERABLE   = (list, tuple, set, typing.AbstractSet)


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


def is_lambda(item) -> bool:
    """
    Indicate whether the argument is a lambda.
    """
    return isinstance(item, type(lambda: 0))


def is_false(item) -> bool:
    """
    Indicate whether the argument is or represents a False value.
    """
    if isinstance(item, str):
        return item.casefold() in FALSE_VALUES
    else:
        return isinstance(item, bool) and not item


def is_true(item) -> bool:
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
    if isinstance(value, ENUMERABLE):   return list(value)
    if is_present(value):               return [value]
    if default is None:                 return []
    return to_list(default)


def to_tuple(value, default=None) -> tuple:
    """
    Transform value to a tuple, or the default if value is blank.
    """
    if isinstance(value, ENUMERABLE):   return tuple(value)
    if is_present(value):               return tuple(to_list(value))
    if default is None:                 return tuple()
    return to_tuple(default)


def pluralize(value: str, count: int = 0) -> str:
    value  = value.strip() if value else ''
    single = (count == 1) or not value or value.casefold().endswith('s')
    suffix = '' if single else 'S' if value[-1].isupper() else 's'
    return f"{value}{suffix}"
