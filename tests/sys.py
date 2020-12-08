# tests/sys.py
#
# Operating environment trials.


from app.common import *


# =============================================================================
# Functions
# =============================================================================


def show_env_vars():
    for k, v in os.environ.items():
        show(f'{k} = "{v}"')


# =============================================================================
# Tests
# =============================================================================


def trials():
    show_section('SYSTEM VALUES')

    if True:
        show_header('Environment variables')
        show_env_vars()

    show_section()


if __name__ == '__main__':
    trials()
