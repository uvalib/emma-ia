# app/ia.py
#
# Internet Archive interface definitions.


import internetarchive
import tempfile

from internetarchive import ArchiveSession
from internetarchive import Item
from requests        import Response
from requests.models import PreparedRequest

from app.common import *


# =============================================================================
# Constants
# =============================================================================


IA_CONFIG = {
    's3': {
        'access': os.getenv('IA_ACCESS'),
        'secret': os.getenv('IA_SECRET')
    },
    'logging': {
        'level': 'DEBUG' if IA_DEBUG else 'INFO',
        'file':  os.path.join(tempfile.gettempdir(), 'ia.log')
    },
    'cookies': {
        'logged-in-user': os.getenv('IA_USER_COOKIE'),
        'logged-in-sig':  os.getenv('IA_SIG_COOKIE')
    }
}


# =============================================================================
# Functions
# =============================================================================


def ia_get_session() -> ArchiveSession:
    """
    Get an IA session based on the configuration supplied via environment
    variables.

    Because get_session() starts with values found in ~/.ia for the current
    user then merges in the "additional" supplied values, the configuration
    file reference is explicitly eliminated. For desktop testing, this
    guarantees that the application has the same dependence on environment
    variables as it would when deployed.

    """
    return internetarchive.get_session(IA_CONFIG, config_file='/dev/null')


def ia_get_files(identifier, **kwargs):
    """
    Retrieve information about the files associated with the given IA item.

    :param str identifier:  IA title identifier.
    :param kwargs:          Passed to internetarchive.get_files().

    :rtype: list[dict]

    """
    if 'on_the_fly' not in kwargs:
        kwargs['on_the_fly'] = True
    result = internetarchive.get_files(identifier, **kwargs)
    return list(result)


def ia_search(terms, count=10, fields=None, session=None):
    """
    Search for items on Archive.org.

    :param str            terms:    Search term(s).
    :param int            count:    Number of results to fetch.
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
        log_error(error)
        return []


def ia_upload(
        target,
        files,
        metadata=None,
        delete=False,
        overwrite=False,
        dry_run=False,
        session=None):
    """
    Upload one or more files to be associated with the given Internet Archive
    title entry.

    :param str|Item       target:       IA title identifier or Item instance.
    :param str|list       files:        One or more file paths.
    :param dict           metadata:
    :param bool           delete:       Delete local files after use.
    :param bool           overwrite:    Force re-upload of an existing item [1]
    :param bool           dry_run:      Don't actually send to IA.
    :param ArchiveSession session:      Used if *target* is an identifier.

    :return: Success.
    :rtype:  bool

    == Notes
    [1] As long as the object key for the submitted data file is based on the
        submission ID, the checking for duplicates won't work.  For that reason
        there's no point in requesting a checksum be performed because it's
        guaranteed that upload() will not find a matching filename uploaded to
        IA's S3 storage.

    """
    success      = False
    files        = to_list(files)
    # checksum     = True   # Guard against re-upload by default.
    checksum     = False  # TODO: See Note [1] above.
    remove_files = False  # By default, upload() will remove each temp file.
    if overwrite:
        checksum = False
        if delete:
            # If *delete* is True then upload() has a feature(?) where checksum
            # is set to True unconditionally.  To avoid that, handle temp file
            # cleanup here.
            remove_files = True
            delete       = False
    try:
        if isinstance(target, str):
            session = session or ia_get_session()
            item = session.get_item(target)
        else:
            item = target
        result = item.upload(
            files,
            metadata=metadata,
            queue_derive=False,
            verbose=True,
            delete=delete,
            checksum=checksum,
            debug=dry_run
        )
        success = (len(result) == len(files))
        if dry_run:
            for request in result:     # type: PreparedRequest
                _show_prepared_request(request)
        elif success:
            for response in result:    # type: Response
                success = success and response.ok
    except Exception as error:
        log_error(error)
        success = False
    finally:
        if success and remove_files and not dry_run:
            for file in files:
                os.remove(file)
    return success


def _show_prepared_request(item: PreparedRequest):
    show(f"{item.method} {item.url}")
    show(dict(item.headers), width=PP_WIDE)


# =============================================================================
# Testing
# =============================================================================


if __name__ == '__main__':
    from tests.ia import trials
    trials()
