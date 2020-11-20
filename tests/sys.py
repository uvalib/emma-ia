# Operating environment tests.

import os  # for environment variables

from utility import *


# =============================================================================
# Tests
# =============================================================================


def trials():

    # List environment variables.
    if False:
        show_header('Environment variables')
        for k, v in os.environ.items():
            show(f'{k} = "{v}"')
