# main.py
#
# Core functionality.

import os
import tempfile

from aws       import *
from emma      import *
from ia        import *
from sip_table import *


# =============================================================================
# Constants
# =============================================================================


DEBUG = AWS_DEBUG or EMMA_DEBUG or IA_DEBUG


# =============================================================================
# Functions
# =============================================================================


def get_bucket(bucket=None) -> s3.Bucket:
    """
    :param str|s3.Bucket|None bucket:   S3 bucket name or instance.
    """
    bucket = bucket or IA_BUCKET
    return get_s3_bucket(bucket)


def get_submissions(bucket=None):
    """
    Retrieve all submissions present in an out-bound EMMA queue on AWS S3.

    :param str|s3.Bucket|None bucket:   S3 bucket name or instance.

    :returns: All un-retrieved submissions IDs with their related files.
    :rtype:   SipTable

    """
    result    = SipTable()
    s3_bucket = get_bucket(bucket)
    for entry in s3_bucket.objects.all():
        file = entry.key
        sid  = re.sub(r'\.[^.]+$', '', file)
        item = 'package' if re.search(r'\.xml$', file) else 'data_file'
        if item in result[sid]:
            error = f'{item} already found for "{sid}"'
            logging.error(error)
            DEBUG and show(f'\tERROR: {error}')
        else:
            result[sid][item] = file
    if DEBUG:
        show_header(f"AWS S3 BUCKET {s3_bucket.name} CONTENTS:")
        show(result)
    return result


def parse_submissions(submissions, bucket=None):
    """
    For each submission, download its submission information package and
    extract metadata values.

    :param SipTable submissions:
    :param str|s3.Bucket|None bucket:   S3 bucket name or instance.

    :returns: Metadata for each submission ID.
    :rtype:   dict

    """
    s3_bucket = get_bucket(bucket)
    for sid, submission in submissions.items():
        DEBUG and show_header(f"ENTRY {sid}:")
        sip = submission.package
        bio = io.BytesIO()
        s3_bucket.Object(sip).download_fileobj(bio)
        submission.metadata = sip_parse(bio)

    result = {}
    for sid, submission in submissions.items():
        result[sid] = submission.metadata
    return result


def upload_submissions(submissions, bucket=None):
    """
    For each submission, upload file and metadata to IA.

    :param SipTable submissions:
    :param str|s3.Bucket|None bucket:   S3 bucket name or instance.

    :return: The list of completed submission IDs.
    :rtype:  list[str]

    """
    session   = None
    s3_bucket = get_bucket(bucket)
    for sid, submission in submissions.items():
        DEBUG and show_header(f"ENTRY {sid} METADATA:")

        # Transform SIP metadata into IA metadata.
        emma_metadata = submission.metadata
        metadata = ia_metadata(emma_metadata)

        # Download a copy of the submitted data file.
        file = submission.data_file
        obj  = s3_bucket.Object(file)  # type: s3.Object
        size = obj.content_length
        tmp  = tempfile.mktemp()
        obj.download_file(tmp)

        # Upload the submitted data file to IA.
        ia_id = emma_metadata.get('emma_repositoryRecordId')
        if DEBUG:
            show_header(f'SUBMIT "{ia_id}" ({file}, size: {size} bytes) TO IA')
        try:
            session = session or ia_get_session()
            item = session.get_item(ia_id)
            item.upload(
                tmp,
                key=file,
                metadata=metadata,
                queue_derive=False,
                verbose=True,
                delete=False,
                debug=True,
            )
            submission.completed = True
        except Exception as error:
            DEBUG and show(f'\tERROR: {error}')
            logging.error(error)
        finally:
            os.remove(tmp)

    completed = []
    for sid, submission in submissions.items():
        if submission.completed:
            completed.append(sid)
    return completed


def remove_submissions(submissions, bucket=None):
    """
    Remove completed submissions from the AWS S3 bucket.

    :param SipTable submissions:
    :param str|s3.Bucket|None bucket:   S3 bucket name or instance.

    :return: The list of removed files (AWS object keys).
    :rtype:  list[str]

    """
    object_keys = []
    for submission in submissions.values():
        if submission.completed:
            object_keys.append(submission.package)
            object_keys.append(submission.data_file)
    if DEBUG:
        show_header(f"DELETING {get_bucket(bucket).name} OBJECTS:")
        show(object_keys)
    if is_present(object_keys):
        s3_bucket = get_bucket(bucket)
        delete_from_s3_bucket(object_keys, s3_bucket)
    return object_keys


def process():
    """
    Retrieve submission(s) from the designated AWS bucket, upload them to IA,
    and removed completed submissions.

    :return: The number of submissions removed from the AWS bucket.
    :rtype:  int

    """
    bucket      = get_bucket()
    table       = get_submissions(bucket)
    _metadata   = parse_submissions(table, bucket)
    _completed  = upload_submissions(table, bucket)
    removed     = remove_submissions(table, bucket)
    submissions = []
    for object_key in removed:
        if object_key.endswith('.xml'):
            submissions.append(object_key)
    return len(submissions)


# =============================================================================
# Main program
# =============================================================================


if __name__ == '__main__':
    if 'trials' in sys.argv:
        from tests import *
        sys_trials()
        aws_trials()
        ia_trials()
        emma_trials()
        show('')
        show('DONE')
    else:
        count = process()
        show('')
        show(f"{count} SUBMISSION{'S' if count != 1 else ''} PROCESSED")
