# app/emma.py
#
# EMMA metadata definitions.


import re
import io

import xml.etree.ElementTree as Xml

from app.common import *


# =============================================================================
# Constants
# =============================================================================


EMMA_FIELD_MAP: Dict[str, Union[str, Dict[str, Union[str, Dict]]]] = {

    # DIRECT MAPPINGS
    #
    # EMMA field                IA field
    # ------------------------  -----------------------------------------------
    'emma_collection':          'collection',
    'rem_source':               'contributor',
    'dc_creator':               'creator',
    'dcterms_dateCopyright':    'date',
    'dc_description':           'description',
    'emma_repositoryRecordId':  'identifier',
    'dc_language':              'language',
    'dc_publisher':             'publisher',
    'dc_subject':               'subject',
    'dc_title':                 'title',

    # DIRECT TRANSLATIONS
    #
    # * field:  The equivalent IA metadata field.
    # * map:    A mapping of EMMA field values to IA field values.
    # -------------------------------------------------------------------------
    'dc_type': {
        'field': 'mediatype',
        'map':   {'text': 'texts', 'sound': 'audio', 'dataset': 'data'}
    },

    # EMMA-UNIQUE MAPPINGS
    #
    # These are EMMA metadata fields mapped on to the columns defined by IA in
    # the CSV files they have generated in order to support EMMA bulk-upload.
    #
    # According to a discussion with IA, although these are not listed at the
    # URL below, they will be retained with metadata associated with the
    # uploaded file, and, in fact, should be available for inclusion in the
    # index feed to Benetech.
    #
    # EMMA field                IA CSV column name
    # ------------------------  -----------------------------------------------
    'rem_metadataSource':       'metadata_source',
    'rem_coverage':             'portion_description',
    'rem_remediation':          'remediated_aspects',
    'rem_remediatedBy':         'remediated_by',
    'emma_lastRemediationNote': 'remediation_comments',
    'rem_status':               'remediation_status',
    'bib_seriesType':           'series_type',
    'rem_quality':              'text_quality',
    'bib_version':              'version',
    'bib_volume':               'volume',

    # INDIRECT TRANSLATIONS
    #
    # * field:     The equivalent IA metadata field.
    # * transform: A mapping of EMMA field values to IA field values.
    # -------------------------------------------------------------------------
    'rem_complete': {
        'field':     'portion',
        'transform': lambda v: str(v).casefold() != 'true',
    },

}

IDENTIFIER_MAP = {
    'oclc': 'ocld-id',
    'isbn': 'isbn',
    'issn': 'issn',
    'lccn': 'lccn'
}


# =============================================================================
# Functions
# =============================================================================


def ia_metadata(emma_metadata):
    """
    Translate EMMA metadata into IA metadata.

    :param dict[str, str|bool|list[str]] emma_metadata: Metadata from the SIP.

    :return: Metadata for use with IA API functions.
    :rtype:  dict[str, str|bool|list[str]]

    """
    # noinspection PyTypeChecker
    result = ia_identifier_metadata(emma_metadata.get('dc_identifier'))
    for field, entry in EMMA_FIELD_MAP.items():
        if field in emma_metadata:
            ia_field = entry
            ia_value = emma_metadata[field]
            if isinstance(entry, dict):
                ia_field = entry['field']
                if 'map' in entry:
                    ia_value = entry['map'][ia_value]
                elif 'transform' in entry:
                    ia_value = entry['transform'](ia_value)
            if is_present(ia_value):
                result[ia_field] = ia_value
    EMMA_DEBUG and show(result, width=PP_WIDE)
    return result


def ia_identifier_metadata(dc_identifier):
    """
    Translate one or more EMMA standard identifier values into the equivalent
    IA field(s).

    :param str|bool|list[str] dc_identifier:

    :return A dict with IA metadata fields derived from the EMMA values.
    :rtype: dict[str, list[str]]

    """
    result = {}
    for identifier in to_list(dc_identifier):
        part      = identifier.split(':', 1)
        id_scheme = part.pop(0)
        id_scheme = IDENTIFIER_MAP.get(id_scheme, id_scheme)
        id_value  = ':'.join(part)
        if id_scheme in result:
            result[id_scheme].append(id_value)
        else:
            result[id_scheme] = [id_value]
    return result


# =============================================================================
# Functions
# =============================================================================


def sip_parse(source):
    """
    Parse EMMA Submission Information Package XML into metadata values.

    :param str|io.BytesIO|io.StringIO source: object

    :rtype: dict

    """
    result = {}
    text = source if isinstance(source, str) else source.getvalue()
    root = Xml.fromstring(text)
    for child in root:
        name = child.tag
        # attr = child.attrib
        field = re.sub(r'{[^}\n]+}', '', name)  # Remove namespace prefix.
        value = sip_node_value(child)
        if EMMA_DEBUG:
            v = f'"{value}"'   if isinstance(value, str) else value
            v = f"{v} [blank]" if is_blank(value) else v
            show(f'name = "{field}" | value = {v}')
        if is_present(value):
            result[field] = value
    return result


def sip_node_value(node):
    """
    Extract the value of an XML node from a Submission Information Package.

    :param Xml.Element node: An XML node.

    :return: The value contained by the node.
    :rtype:  str|number|bool|list

    """
    if len(node):
        result = []
        for child in node:
            value = sip_node_value(child)
            if is_present(value):
                result.append(value)
        return result
    else:
        text   = node.text
        value  = text.casefold() if isinstance(text, str) else None
        return True if value == 'true' else False if value == 'false' else text


# =============================================================================
# Command-line tests.
# =============================================================================


if __name__ == '__main__':
    from tests.emma import trials
    trials()
