# app/output.py
#
# Console output and logging support.


import logging
import math
import sys

from pprint import PrettyPrinter

from app.var  import APPLICATION_DEPLOYED
from app.util import to_list

# =============================================================================
# Constants
# =============================================================================


PP_NARROW = 1
PP_WIDE   = 120
PP_WIDTH  = PP_NARROW
PP_INDENT = 4
PP_KWARGS = {'width': PP_WIDTH, 'indent': PP_INDENT, 'sort_dicts': False}

CONSOLE_WIDTH = 80

# To select only the log entries for this service:
#
# https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups/log-group/uva-docker-production-0/log-events/messages$3Fstart$3D-43200000$26filterPattern$3D$2522EMMA:$2522
#
LOG_PREFIX = 'EMMA:'


# =============================================================================
# Variables
# =============================================================================


pp      = PrettyPrinter(**PP_KWARGS)
pp_wide = PrettyPrinter(**{**PP_KWARGS, 'width': PP_WIDE})


# =============================================================================
# Log output
# =============================================================================


def log_error(message, *args, **kwargs):
    prefix  = _line_prefix()
    message = message if isinstance(message, str) else repr(message)
    logging.error(f" {prefix}{message}", *args, **kwargs)


# =============================================================================
# Console output
# =============================================================================


def show(value, width=None, indent=None, prefix=None, pp_override=None):
    """
    Pretty-print value.

    :param any           value:         Item to display.
    :param int           width:         Override width.
    :param int           indent:        Override indent.
    :param str|None      prefix:        String prepended to every output line.
    :param PrettyPrinter pp_override:   Override default pp.

    """
    if isinstance(value, (str, int)):
        lines = value
    elif pp_override:
        lines = pp_override.pformat(value)
    elif width or indent:
        kwargs = PP_KWARGS.copy()
        if width:
            kwargs.update(width=width)
        if indent:
            kwargs.update(indent=indent)
        lines = PrettyPrinter(**kwargs).pformat(value)
    else:
        lines = pp.pformat(value)
    lines = lines.split("\n") if isinstance(value, str) else to_list(lines)
    _show_lines(*lines, prefix=prefix)


def show_section(value=None, width=CONSOLE_WIDTH, gap=2, prefix=None):
    """
    Output a major section heading.

    :param str      value:  Heading label.
    :param int      width:  Width of output line.
    :param int      gap:    Number of blanks on either side of label.
    :param str|None prefix: String prepended to every output line.

    """
    width -= 1
    value  = value or ''
    length = len(value)
    if length >= (width + (2 * gap)):
        label = value
    else:
        stars = '*' * (math.floor((width - length) / 2) - gap)
        space = ' ' * gap if value else ''
        label = space.join([stars, value, stars])
        label += '*' * (width - len(label))  # Fill out line if necessary.
    if APPLICATION_DEPLOYED:
        lines = [label]
    else:
        lines = ['', label, '']
    _show_lines(*lines, prefix=prefix)


def show_header(value, width=CONSOLE_WIDTH, prefix=None):
    """
    Output a heading.

    :param str      value:  Heading label.
    :param int      width:  Width of output line.
    :param str|None prefix: String prepended to every output line.

    """
    width -= 1
    h_bar = '-' * min(len(value), width)
    if APPLICATION_DEPLOYED:
        lines = [value, h_bar]
    else:
        lines = ['', value, h_bar]
    _show_lines(*lines, prefix=prefix)


# =============================================================================
# Internal functions
# =============================================================================


def _line_prefix(prefix=None):
    """
    The string to prepend to every output line.

    :param str prefix:  Default: LOG_PREFIX when deployed; '' on the desktop.

    :return: Either '' or a string ending with ' '.
    :rtype:  str

    """
    prefix = prefix or LOG_PREFIX if APPLICATION_DEPLOYED else ''
    return prefix if not prefix or prefix.endswith(' ') else f"{prefix} "


def _show_lines(*lines, prefix=None):
    """
    Output a line for each argument.

    :param str      lines:  Each is assumed to have no embedded newlines.
    :param str|None prefix: String prepended to every output line.

    """
    prefix = _line_prefix(prefix)
    for line in lines:
        sys.stdout.write(f"{prefix}{line}\n")
