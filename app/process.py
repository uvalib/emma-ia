# app/process.py
#
# Core functionality.


from enum import Enum, auto

from app.aws_s3    import *
from app.emma      import *
from app.ia        import *
from app.sip_table import *


# =============================================================================
# Classes
# =============================================================================


class Action(Enum):
    Clear  = auto()
    Pause  = auto()
    Resume = auto()


# =============================================================================
# Constants
# =============================================================================


PAUSE_KEY_TEMPLATE  = 'control/paused-{repo}-{deployment}'
RESUME_KEY_TEMPLATE = 'control/active-{repo}-{deployment}'


# =============================================================================
# Variables
# =============================================================================


_s3_bucket: Optional[s3.Bucket] = None


# =============================================================================
# Functions
# =============================================================================


def _run(action, repo=None, deployment=None):
    """
    :param Action   action:
    :param str|None repo:           Member repository (def: DEF_REPO).
    :param str|None deployment:     One of DEPLOYMENTS (def: DEF_DEPLOYMENT).
    """
    repo       = str(repo).casefold()       if repo       else DEF_REPO
    deployment = str(deployment).casefold() if deployment else DEF_DEPLOYMENT
    bucket     = s3_bucket_name('emma', deployment)
    pause_key  = PAUSE_KEY_TEMPLATE.format(repo=repo, deployment=deployment)
    resume_key = RESUME_KEY_TEMPLATE.format(repo=repo, deployment=deployment)
    if action == Action.Pause:
        if not s3_object_rename(resume_key, pause_key, bucket):
            DEBUG and show(f"CREATING {pause_key}")
            upload_to_s3_bucket('/dev/null', bucket, object_key=pause_key)
    elif action == Action.Resume:
        s3_object_rename(pause_key, resume_key, bucket)
    elif action == Action.Clear:
        delete_from_s3_bucket([pause_key, resume_key], bucket)
    else:
        raise ValueError(f"{action}: invalid action")


def is_paused(repo=None, deployment=None) -> bool:
    """
    If the control file exists in the appropriate bucket then processing of IA
    staging submissions will not proceed.

    :param str|None repo:           Member repository (def: DEF_REPO).
    :param str|None deployment:     One of DEPLOYMENTS (def: DEF_DEPLOYMENT).

    """
    repo       = str(repo).casefold()       if repo       else DEF_REPO
    deployment = str(deployment).casefold() if deployment else DEF_DEPLOYMENT
    bucket     = s3_bucket_name('emma', deployment)
    key        = PAUSE_KEY_TEMPLATE.format(repo=repo, deployment=deployment)
    return s3_object_exists(key, bucket)


def get_repo_bucket(repo=None, deployment=None, bucket=None) -> s3.Bucket:
    """
    :param str|None           repo:         Member repository.
    :param str|None           deployment:   One of DEPLOYMENTS.
    :param str|s3.Bucket|None bucket:       S3 bucket name or instance.
    """
    bucket = s3_bucket_name(repo, deployment) if repo or deployment else bucket
    return get_s3_bucket(bucket)


def get_submissions(prefix='', bucket=None):
    """
    Retrieve all submissions present in an out-bound EMMA queue on AWS S3.

    :param str|None           prefix:   If '' then keys that have any prefix
                                            are skipped; if None then any/all
                                            prefixes are allowed.
    :param str|s3.Bucket|None bucket:   S3 bucket or name (default: _s3_bucket)

    :returns: All un-retrieved submissions IDs with their related files.
    :rtype:   SipTable

    """
    result    = SipTable()
    prefix    = f"{prefix}/" if prefix and not prefix.endswith('/') else prefix
    s3_bucket = _s3_bucket or get_repo_bucket(bucket=bucket)
    for entry in s3_bucket.objects.all():  # type: s3.Object
        file = entry.key
        if prefix is None or prefix_of(file) == prefix:
            sid = re.sub(r'\.[^.]+$', '', file)
            item = 'package' if re.search(r'\.xml$', file) else 'data_file'
            if item in result[sid]:
                log_error(f'{item} already found for "{sid}"')
            else:
                result[sid][item] = file
    if DEBUG and (result or not APPLICATION_DEPLOYED):
        show_header(f"AWS S3 BUCKET {s3_bucket.name} CONTENTS:")
        show(result)
    return result


def parse_submissions(submissions, bucket=None):
    """
    For each submission, download its submission information package and
    extract metadata values.

    :param SipTable submissions:
    :param str|s3.Bucket|None bucket:   S3 bucket or name (default: _s3_bucket)

    :returns: Metadata for each submission ID.
    :rtype:   dict

    """
    s3_bucket = _s3_bucket or get_repo_bucket(bucket=bucket)
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
    :param str|s3.Bucket|None bucket:   S3 bucket or name (default: _s3_bucket)

    :return: The list of completed submission IDs.
    :rtype:  list[str]

    """
    session   = None
    s3_bucket = _s3_bucket or get_repo_bucket(bucket=bucket)
    for sid, submission in submissions.items():
        DEBUG and show_header(f"ENTRY {sid} METADATA:")

        # Transform SIP metadata into IA metadata.
        emma_metadata = submission.metadata
        metadata = ia_metadata(emma_metadata)

        # Determine the target IA item.
        ia_id = metadata.get('identifier')
        if not ia_id:
            log_error(f"empty emma_repositoryRecordId for {sid}")
            continue

        # Download a copy of the submitted data file.
        file = submission.data_file
        obj  = s3_bucket.Object(file)  # type: s3.Object
        size = obj.content_length
        tmp  = f"{ia_id}_emma_{file}"
        obj.download_file(tmp)

        # Upload the submitted data file to IA.
        if DEBUG:
            _to = '[DRY RUN]' if DRY_RUN else 'TO IA'
            show_header(f'SUBMIT "{ia_id}" (file {file} - {size} bytes) {_to}')
        session = session or ia_get_session()
        submission.completed = ia_upload_file(
            target=ia_id,
            file=tmp,
            metadata=metadata,
            delete=True,
            dry_run=DRY_RUN,
            session=session
        )

    completed = []
    for sid, submission in submissions.items():
        if submission.completed:
            completed.append(sid)
    return completed


def remove_submissions(submissions, bucket=None):
    """
    Remove completed submissions from the AWS S3 bucket.

    :param SipTable submissions:
    :param str|s3.Bucket|None bucket:   S3 bucket or name (default: _s3_bucket)

    :return: The list of removed files (AWS object keys).
    :rtype:  list[str]

    """
    object_keys = []
    for submission in submissions.values():
        if submission.completed:
            object_keys.append(submission.package)
            object_keys.append(submission.data_file)
    bucket = _s3_bucket or bucket
    if DEBUG and (object_keys or not APPLICATION_DEPLOYED):
        bucket_name = bucket.name if is_s3_bucket(bucket) else bucket
        deleting    = 'ELIGIBLE FOR DELETION' if DRY_RUN else 'DELETING'
        show_header(f"{deleting} {bucket_name} OBJECTS:")
        show(object_keys or 'NONE')
    if object_keys and not DRY_RUN:
        delete_from_s3_bucket(object_keys, bucket)
    return object_keys


def process(repo=None, deployment=None):
    """
    Retrieve submission(s) from the designated AWS bucket, upload them to IA,
    and removed completed submissions.

    :param str|None repo:           Member repository (def: DEF_REPO).
    :param str|None deployment:     One of DEPLOYMENTS (def: DEF_DEPLOYMENT).

    :return: The number of submissions removed from the AWS bucket.
    :rtype:  int

    """
    global _s3_bucket
    _s3_bucket  = get_repo_bucket(repo, deployment)
    table       = get_submissions()
    _metadata   = parse_submissions(table)
    _completed  = upload_submissions(table)
    removed     = remove_submissions(table)
    submissions = []
    for object_key in removed:
        if object_key.endswith('.xml'):
            submissions.append(object_key)
    _s3_bucket = None
    return len(submissions)


# =============================================================================
# Main program
# =============================================================================


def main():
    repos = []
    deployments = []
    checking = clearing = pausing = resuming = all_repos = None

    # Process command-line arguments.
    for arg in sys.argv[1:]:
        if arg in ('check', 'check_pause'):
            checking = True
        elif arg in ('clear', 'reset'):
            clearing = True
        elif arg in ('halt', 'pause'):
            pausing = True
        elif arg in ('resume', 'unpause'):
            resuming = True
        elif arg == 'all':
            all_repos = True
        elif arg in ALL_REPOS:
            repos.append(arg)
        elif arg in DEPLOYMENTS:
            deployments.append(arg)
        else:
            raise RuntimeError(f"{arg}: invalid command-line option")
    if all_repos:
        repos = ALL_REPOS

    # Prepare queue description for logging.
    _queue = '{repo} QUEUE {deployment}'
    _queue = f"{_queue} [DRY RUN]" if DRY_RUN else _queue

    # Process all indicated repository/deployment combinations.
    for repo in repos or TARGET_REPOS:
        for deployment in deployments or DEPLOYMENTS:
            leader = "\n" if DEBUG and not APPLICATION_DEPLOYED else ''
            queue  = _queue.format(repo=repo, deployment=deployment).upper()
            paused = is_paused(repo, deployment)
            if clearing:
                memo = 'CLEARING'
            elif checking:
                memo = 'PAUSED'         if paused else 'NOT PAUSED'
            elif pausing:
                memo = 'ALREADY PAUSED' if paused else 'PAUSING'
            elif resuming:
                memo = 'RESUMING'       if paused else 'ALREADY NOT PAUSED'
            else:
                memo = 'PAUSED'         if paused else None
            if memo:
                show(f"{leader}*** {memo} *** - {queue}")
                if clearing:
                    _run(Action.Clear, repo, deployment)
                elif paused and resuming:
                    _run(Action.Resume, repo, deployment)
                elif not paused and pausing:
                    _run(Action.Pause, repo, deployment)
            else:
                count       = process(repo, deployment)
                submissions = pluralize('SUBMISSION', count)
                show(f"{leader}{count} {submissions} PROCESSED - {queue}")


if __name__ == '__main__':
    main()
