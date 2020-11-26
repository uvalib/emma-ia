# tests/sys.py
#
# Operating environment trials.

# noinspection PyUnresolvedReferences
from common import *
from output import *


# =============================================================================
# Tests
# =============================================================================


def trials():

    # List environment variables.
    if False:
        show_header('Environment variables')
        for k, v in os.environ.items():
            show(f'{k} = "{v}"')
