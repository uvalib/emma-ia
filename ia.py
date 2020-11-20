# InternetArchive interface

import internetarchive

from internetarchive.session import ArchiveSession

from utility import *


# =============================================================================
# Constants
# =============================================================================


IA_DEBUG = True


# =============================================================================
# Functions
# =============================================================================


def ia_get_session() -> ArchiveSession:
    return internetarchive.get_session()


def ia_get_files(identifier, **kwargs):
    """
    Retrieve information about the files associated with the given IA item.

    :param str identifier: IA title identifier.
    :param kwargs:

    :rtype: list[dict]

    """
    if 'on_the_fly' not in kwargs:
        kwargs['on_the_fly'] = True
    result = internetarchive.get_files(identifier, **kwargs)
    return list(result)


def ia_search(terms, count=10, fields=None, session=None):
    """
    :param str            terms:
    :param int            count:
    :param list           fields:   Fields to return (def: identifier, title)
    :param ArchiveSession session:

    :return: Search result items.
    :rtype:  list
    """
    session = session or ia_get_session()
    params  = {'rows': count, 'page': 1}
    fields  = to_list(fields, default=['identifier', 'title'])
    try:
        # noinspection PyTypeChecker
        result = session.search_items(terms, params=params, fields=fields)
        return list(result)
    except Exception as error:
        IA_DEBUG and show(f'\tERROR: {error}')
        logging.error(error)
        return []


# =============================================================================
# Testing
# =============================================================================


if __name__ == '__main__':
    from tests.ia import trials
    trials()
    show('')
    show('DONE')
