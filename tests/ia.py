# tests/ia.py
#
# InternetArchive interface trials.

from utility import *
from ia      import ia_get_session, ia_get_files, ia_search


# =============================================================================
# Tests
# =============================================================================


def trials():
    show_section('IA TRIALS')
    session = ia_get_session()

    # IA session.
    if True:
        show_header('IA session')
        show(f'user_email   = {session.user_email}')
        show(f'access_key   = {session.access_key}')
        show(f'secret_key   = {session.secret_key}')
        show(f'host         = {session.host}')
        show(f'headers      = {session.headers}')
        show(f'adapter args = {session.http_adapter_kwargs}')

    # IA search.
    if True:
        terms = 'emma'
        count = 10
        show_header(f'SEARCH for "{terms}" ({count}):')
        for item in ia_search(terms, count, session=session):
            show('')
            show(item, pp_override=pp_wide)
            if True:  # Show the files associated with the entry.
                ia_id = item['identifier']
                files = ia_get_files(ia_id)
                show(files)

    # IA catalog.
    if True:
        show_header('CATALOG (all queued or running tasks):')
        try:
            cat = session.get_my_catalog()
            show(cat)
        except Exception as error:
            show(f'\tERROR: {error}')

    # IA tasks.
    if True:
        show_header('TASKS (all tasks):')
        try:
            tasks = session.get_tasks()
            show(tasks)
        except Exception as error:
            show(f'\tERROR: {error}')
