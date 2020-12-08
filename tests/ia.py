# tests/ia.py
#
# InternetArchive interface trials.


from app.ia import *


# =============================================================================
# Functions
# =============================================================================


def show_ia_session(session=None):
    """
    :param ArchiveSession|None session:
    """
    session = session or ia_get_session()
    show(f'user_email   = {session.user_email}')
    show(f'access_key   = {session.access_key}')
    show(f'secret_key   = {session.secret_key}')
    show(f'host         = {session.host}')
    show(f'headers      = {session.headers}')
    show(f'adapter args = {session.http_adapter_kwargs}')


def show_ia_search(terms, count, session=None, show_files=True):
    """
    :param str                 terms:       Search term(s).
    :param int                 count:       Number of results to fetch.
    :param ArchiveSession|None session:
    :param bool                show_files:  Show each entries files.
    """
    session = session or ia_get_session()
    for item in ia_search(terms, count, session=session):
        show('')
        show(item, width=PP_WIDE)
        if show_files:
            ia_id = item['identifier']
            files = ia_get_files(ia_id)
            show(files)


def show_ia_catalog(session=None):
    """
    :param ArchiveSession|None session:
    """
    session = session or ia_get_session()
    try:
        show(session.get_my_catalog())
    except Exception as error:
        show(f'\tERROR: {error}')


def show_ia_tasks(session=None):
    """
    :param ArchiveSession|None session:
    """
    session = session or ia_get_session()
    try:
        show(session.get_tasks())
    except Exception as error:
        show(f'\tERROR: {error}')


# =============================================================================
# Trials
# =============================================================================


def trials():
    show_section('IA TRIALS')
    session = ia_get_session()

    if True:
        show_header('IA session')
        show_ia_session(session)

    if True:
        terms = 'emma'
        count = 10
        show_header(f'SEARCH for "{terms}" ({count}):')
        show_ia_search(terms, count, session)

    if True:
        show_header('CATALOG (all queued or running tasks):')
        show_ia_catalog(session)

    if True:
        show_header('TASKS (all tasks):')
        show_ia_tasks(session)

    show_section()


if __name__ == '__main__':
    trials()
