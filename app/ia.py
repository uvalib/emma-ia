# app/ia.py
#
# Internet Archive interface definitions.


import internetarchive
import tempfile

from internetarchive import ArchiveSession
from internetarchive import Item
from requests        import Response, PreparedRequest

from app.common import *


# =============================================================================
# Constants
# =============================================================================


# In general, it's probably better to avoid attempting to change title-level
# metadata for the IA item -- specific remediation metadata should be
# associated with the file.
UPDATE_IA_TITLE_METADATA = False

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

IA_METADATA_FIELDS = [
    # All IA fields given on:
    # @see https://archive.org/services/docs/api/metadata-schema/index.html?highlight=metadata%20fields
    #
    # NOTES:
    # [1] See on https://archive.org/metadata/twoviewsofgettys0000unse
    #
    # FIELD                     RANGE/PATTERN                       INTERNAL
    # =======================   ==================================  ===========
    'access-restricted',        # True                                yes
    'access-restricted-item',   # True
    'adaptive_ocr',             # True
    'addeddate',                # YYYY-MM-DD HH:MM:SS YYYY-MM-DD
    'aspect_ratio',             # N:M
#   'associated-names',         # NOTE: [1]
    'audio_codec',              # (str)
    'audio_sample_rate',        # (float)
#   'backup_location',          # NOTE: [1]
    'betterpdf',                # True
#   'bookplateleaf',            # NOTE: [1]
    'bookreader-defaults',      # mode/1up mode/2up mode/thumb
    'boxid',                    # IA######                            yes
    'bwocr',                    # (page range)
    'call_number',              # (string)
    'camera',                   # (string)
    'ccnum',                    # cc# asr ocr #
    'closed_captioning',        # "yes"/"no"
    'collection',               # valid identifier
#   'collection_set',           # NOTE: [1]
    'color',                    # (str)
    'condition',                # "Near Mint", "Very Good", "Good", "Fair", "Worn", "Poor", "Fragile", "Incomplete"
    'condition-visual',         # "Near Mint", "Very Good", "Good", "Fair", "Worn", "Poor", "Fragile"
    'contributor',              # (str)
    'coverage',                 # (str)
    'creator',                  # (str)
    'creator-alt-script',
    'curation',                 # (str)                               yes
    'date',                     # (str)
    'description',              # (str)
    'external-identifier',      # (str)
    'firstfiledate',
    'fixed-ppi',                # (float)
    'foldoutcount',             # (int)
    'frames_per_second',        # (float)
    'geo_restricted',           # e.g. "US"
    'hidden',                   # True                                yes
    'identifier',               # (str)
#   'identifier-access',        # NOTE: [1]
    'identifier-ark',           # ark:/NAAN/Name
    'identifier-bib',           # (str)
    'imagecount',               # (int)
#   'invoice',                  # NOTE: [1]
    'isbn',                     # ISBN
    'issn',                     # ISSN
    'language',                 # ISO
    'lastfiledate',
    'lccn',                     # LCCN
    'licenseurl',               # (URL)
    'mediatype',                # "texts", "etree", "audio", "movies", "software", "image", "data", "web", "collection", "account"
    'neverindex',               # True
    'next_item',
    'no_ol_import',
    'noindex',                  # True                                yes
    'notes',                    # (str)
    'oclc-id',                  # (str)
    'ocr',                      # (str)
#   'old_pallet',               # NOTE: [1]
    'openlibrary',
    'openlibrary_author',       # OL#A
    'openlibrary_edition',
    'openlibrary_subject',
    'openlibrary_work',
    'operator',
    'page-progression',
    'possible-copyright-status',
    'ppi',
    'previous_item',
    'public-format',            # (str)                               yes
    'publicdate',
    'publisher',                # (str)
    'related_collection',
    'related-external-id',
    'repub_state',
    'republisher',
    'republisher_date',
    'republisher_operator',
    'republisher_time',
    'rights',
    'runtime',
    'scandate',
    'scanfee',
    'scanner',
    'scanningcenter',
#   'scribe3_search_catalog',   # NOTE: [1]
#   'scribe3_search_id',        # NOTE: [1]
    'show_related_music_by_track',
    'skip_ocr',
    'sort-by',
    'sound',
    'source',
    'source_pixel_height',
    'source_pixel_width',
    'sponsor',
    'sponsordate',
    'start_localtime',
    'start_time',
    'stop_time',
    'subject',
    'summary',
    'title',
    'title_message',
#   'tts_version',              # NOTE: [1]
#   'uploader',                 # NOTE: [1]
]

# IA metadata fields that should be treated as file-level not title-level
# metadata.
IA_FILE_METADATA_FIELDS = to_tuple('contributor')


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


def ia_upload_file(
        target,
        file,
        metadata=None,
        delete=False,
        overwrite=False,
        dry_run=False,
        session=None):
    """
    Upload a file to be associated with the given Internet Archive title entry.

    :param str|Item       target:       IA title identifier or Item instance.
    :param str            file:         A file path.
    :param dict           metadata:     A mix of title- and file-level metadata
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
    success   = False
    # checksum  = True   # Guard against re-upload by default.
    checksum  = False    # TODO: See Note [1] above.
    cleanup   = dry_run  # By default, upload() will remove each temp file.
    if overwrite:
        checksum = False
        if delete:
            # If *delete* is True then upload() has a feature(?) where checksum
            # is set to True unconditionally.  To avoid that, handle temp file
            # cleanup here.
            cleanup = True
            delete  = False
    show_results = dry_run or (IA_DEBUG and not APPLICATION_DEPLOYED)

    # Associate non-title-level metadata with the file.
    [title_metadata, file_metadata] = ia_partition_metadata(metadata)
    file_metadata.update(name=file)  # Needed for a dict argument to upload().

    try:
        if isinstance(target, str):
            session = session or ia_get_session()
            item = session.get_item(target)
        else:
            item = target
        result = item.upload(
            file_metadata,          # NOTE: file_metadata['name'] is the file
            metadata=None,          # NOTE: must use modify_metadata() below
            queue_derive=False,
            verbose=True,
            delete=delete,
            checksum=checksum,
            debug=dry_run
        )
        success = is_present(result)
        cleanup = cleanup or not success
        for part in result:
            show_results and _show_response(part)
            if 'ok' in dir(part):
                success = success and part.ok
        if success and UPDATE_IA_TITLE_METADATA:
            result = item.modify_metadata(title_metadata, debug=dry_run)
            if show_results:
                for part in result:
                    _show_response(part)
    except Exception as error:
        log_error(error)
        cleanup = cleanup or not success
        success = False
    finally:
        if cleanup:
            os.remove(file)
    return success


def ia_partition_metadata(metadata):
    """
    Separate a mix of title-level and file-level metadata.

    :param dict metadata:

    :return: Title-level metadata then file-level metadata.
    :rtype:  list[dict,dict]

    """
    title_metadata = {}
    file_metadata  = {}
    for k, v in metadata.items():
        if k in IA_FILE_METADATA_FIELDS or k not in IA_METADATA_FIELDS:
            file_metadata[k]  = v
        else:
            title_metadata[k] = v
    return [title_metadata, file_metadata]


def _show_response(item, prefix=None):
    """
    :param Response|PreparedRequest item:
    :param str|None                 prefix:     Prefix for heading line.
    """
    prefix = '' if prefix is None else prefix
    prefix = prefix if not prefix or prefix.endswith(' ') else f"{prefix} "
    tag    = f"{item.method} {item.url}" if 'method' in dir(item) else item.url
    show(f">>> {prefix}{tag}")
    show(dict(item.headers), width=PP_WIDE)


# =============================================================================
# Testing
# =============================================================================


if __name__ == '__main__':
    from tests.ia import trials
    trials()
