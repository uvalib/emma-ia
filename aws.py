# aws.py
#
# AWS interface definitions.

import re
import boto3
import boto3_type_annotations.s3      as s3
import boto3_type_annotations.sqs     as sqs
import boto3_type_annotations.lambda_ as lam

from botocore.exceptions import ClientError

# noinspection PyUnresolvedReferences
from common import *
from output import *


# =============================================================================
# Constants
# =============================================================================


AWS_DEBUG = is_true(os.environ.get('AWS_DEBUG', True))

REPO_TABLE = {
    'ia': 'archive',
    'ht': 'hathi',
    'bs': 'bookshare'
}

DEF_QUEUE_DELAY = 0  # 2


# =============================================================================
# AWS S3 repository buckets
# =============================================================================


def emma_bucket_name(deployment=None) -> str:
    return bucket_name('emma', deployment)


def bs_bucket_name(deployment=None) -> str:
    return bucket_name('bs', deployment)


def ht_bucket_name(deployment=None) -> str:
    return bucket_name('ht', deployment)


def ia_bucket_name(deployment=None) -> str:
    return bucket_name('ia', deployment)


def bucket_name(repo, deployment=None) -> str:
    repo = str(repo).casefold()
    if repo == 'emma':
        area = 'storage'
    elif repo in REPO_TABLE:
        area = f"{repo}-queue"
    else:
        for code, name in REPO_TABLE.items():
            if re.search(name, repo):
                repo = code
                break
        area = f"{repo}-queue"
    deployment = deployment or DEPLOYMENT
    return f"emma-{area}-{deployment}"


# =============================================================================
# AWS S3
# =============================================================================


def is_s3_client(item) -> bool:
    return 'list_objects' in dir(item)


def is_s3_resource(item) -> bool:
    return 'buckets' in dir(item)


def is_s3_bucket(item) -> bool:
    return 'objects' in dir(item)


def is_s3_object(item) -> bool:
    return '_bucket_name' in dir(item)


def s3_client(item) -> s3.Client:
    if is_s3_client(item):
        return item
    elif is_s3_resource(item) or is_s3_bucket(item):
        return item.meta.client
    else:
        return boto3.client('s3')


def s3_resource(item) -> s3.ServiceResource:
    if is_s3_resource(item):
        return item
    else:
        return boto3.resource('s3')


def get_s3_bucket(bucket, s3_res=None) -> s3.Bucket:
    """
    :param str|s3.Bucket      bucket:   Bucket name or instance.
    :param s3.ServiceResource s3_res:
    """
    if isinstance(bucket, str):
        s3_res = s3_resource(s3_res)
        bucket = s3_res.Bucket(bucket)
    return bucket


def create_s3_bucket(name, region=None, s3_item=None) -> bool:
    """
    Create an S3 bucket in a specified region.

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    @see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html

    :param str name:                    Bucket to create,
    :param str region:                  Region for bucket in, e.g., 'us-west-2'
    :param s3.Client|s3.ServiceResource s3_item:

    :return: Whether the bucket was created.

    """
    s3_item = s3_item or s3_client(s3_item)
    config  = {}
    if region:
        config['LocationConstraint'] = region
    try:
        s3_item.create_bucket(Bucket=name, CreateBucketConfiguration=config)
    except ClientError as error:
        AWS_DEBUG and show(f'\tERROR: {error}')
        logging.error(error)
        return False
    return True


def copy_to_s3_bucket(file_path, bucket, s3_res=None):
    """
    :param str                file_path:
    :param str|s3.Bucket      bucket:   Destination bucket name or instance.
    :param s3.ServiceResource s3_res:
    """
    key    = os.path.basename(file_path)
    stream = open(file_path, 'rb')
    bucket = get_s3_bucket(bucket, s3_res)
    bucket.put_object(Key=key, Body=stream)


def upload_to_s3_bucket(file_path, bucket, object_key=None, s3_item=None):
    """
    Upload a file to an S3 bucket.

    @see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html

    :param str           file_path:     Local file to upload.
    :param str|s3.Bucket bucket:        Destination bucket name or instance.
    :param str           object_key:    Destination S3 object name. If not
                                            given then file_name is used.
    :param s3.Client|s3.ServiceResource s3_item:

    :return: Whether the file was uploaded.
    :rtype:  bool

    """
    object_key = object_key or os.path.basename(file_path)
    try:
        if is_s3_client(s3_item):
            bucket_name = bucket if isinstance(bucket, str) else bucket.name
            s3_item.upload_file(file_path, bucket_name, object_key)
        else:
            bucket = get_s3_bucket(bucket, s3_item)
            bucket.upload_file(file_path, object_key)
    except ClientError as error:
        AWS_DEBUG and show(f'\tERROR: {error}')
        logging.error(error)
        return False
    return True


def download_from_s3_bucket(object_key, bucket, file_path=None, s3_item=None):
    """
    Download a file from an S3 bucket.

    @see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html

    :param str           object_key:    Source S3 object name.
    :param str|s3.Bucket bucket:        Source bucket name or instance.
    :param str           file_path:     Local name for the downloaded file;
                                            defaults to object_key.
    :param s3.Client|s3.ServiceResource s3_item:

    :return: Whether file was downloaded.
    :rtype:  bool

    """
    file_path = file_path or object_key
    try:
        if is_s3_client(s3_item):
            bucket_name = bucket if isinstance(bucket, str) else bucket.name
            s3_item.download_file(bucket_name, object_key, file_path)
        else:
            bucket = get_s3_bucket(bucket, s3_item)
            bucket.download_file(object_key, file_path)
    except ClientError as error:
        AWS_DEBUG and show(f'\tERROR: {error}')
        logging.error(error)
        return False
    return True


def delete_from_s3_bucket(object_keys, bucket, s3_item=None):
    """
    Remove a file from an S3 bucket.

    :param str|list[str] object_keys:   Target S3 object name(s).
    :param str|s3.Bucket bucket:        Target bucket name or instance.
    :param s3.Client|s3.ServiceResource s3_item:

    :return: Whether the file(s) were removed.
    :rtype:  bool

    """
    object_list = []
    keys = [object_keys] if isinstance(object_keys, str) else object_keys
    for key in keys:
        object_list.append({'Key': key})
    if object_list:
        try:
            if is_s3_client(s3_item):
                bucket = bucket if isinstance(bucket, str) else bucket.name
                s3_item.delete_objects(bucket, Delete={'Objects': object_list})
            else:
                bucket = get_s3_bucket(bucket, s3_item)
                bucket.delete_objects(Delete={'Objects': object_list})
        except ClientError as error:
            AWS_DEBUG and show(f'\tERROR: {error}')
            logging.error(error)
            return False
    return True


# =============================================================================
# AWS SQS
# =============================================================================


def is_sqs_client(item) -> bool:
    return 'list_queues' in dir(item)


def is_sqs_resource(item) -> bool:
    return 'queues' in dir(item)


def sqs_client(item) -> sqs.Client:
    if is_sqs_client(item):
        return item
    elif is_sqs_resource(item):
        return item.meta.client
    else:
        return boto3.client('sqs')


def sqs_resource(item) -> sqs.ServiceResource:
    if is_sqs_resource(item):
        return item
    else:
        return boto3.resource('sqs')


def sqs_queue_name(queue: sqs.Queue) -> str:
    return queue.attributes['QueueArn'].split(':')[-1]


def get_sqs_queue(queue_name, sqs_res=None) -> Optional[sqs.Queue]:
    """
    :param str queue_name:
    :param sqs.ServiceResource sqs_res:
    """
    sqs_res = sqs_resource(sqs_res)
    try:
        queue = sqs_res.get_queue_by_name(QueueName=queue_name)
    except ClientError as error:
        AWS_DEBUG and show(f'\tERROR: {error}')
        logging.error(error)
        queue = None
    return queue


def create_sqs_queue(queue_name, delay=DEF_QUEUE_DELAY, sqs_obj=None):
    """
    :param str queue_name:
    :param str|int delay:   Queue update delay.
    :param sqs.Client|sqs.ServiceResource sqs_obj:

    :return: The queue instance.
    :rtype:  sqs.Queue|None

    """
    sqs_obj = sqs_obj or sqs_resource(sqs_obj)
    attr    = {'DelaySeconds': str(delay)}
    try:
        queue = sqs_obj.create_queue(QueueName=queue_name, Attributes=attr)
    except ClientError as error:
        AWS_DEBUG and show(f'QUEUE ALREADY EXISTS? - %{error}')
        queue = get_sqs_queue(queue_name, sqs_obj)
    if queue and AWS_DEBUG:
        show(sqs_queue_name(queue))
        show({**{'url': queue.url}, **queue.attributes})
    return queue


def delete_sqs_queue(queue_name, sqs_obj=None):
    """
    :param str queue_name:
    :param sqs.Client|sqs.ServiceResource sqs_obj:
    """
    try:
        queue = get_sqs_queue(queue_name, sqs_obj)
    except ClientError as error:
        AWS_DEBUG and show(f'\tERROR: {error}')
        queue = None
    if queue:
        sqs_cli = sqs_client(sqs_obj)
        sqs_cli.delete_queue(QueueUrl=queue.url)


# =============================================================================
# AWS Lambda
# =============================================================================


# @see https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html
LAMBDA_RUNTIMES = [
    'nodejs12.x',
    'nodejs10.x',
    'python3.8',
    'python3.7',
    'python3.6',
    'python2.7',
    'ruby2.7',
    'ruby2.5',
    'java11',
    'java8.a12',
    'java8',
    'go1.x',
    'dotnetcore3.1',
    'dotnetcore2.1',
    'provided.al2',     # Custom runtime - Amazon Linux 2
    'provided',         # Custom runtime - Amazon Linux
]


def get_lambda_client() -> lam.Client:
    return boto3.client('lambda')


def get_lambda_functions(cli=None, all_versions=False) -> List[Dict]:
    """
    :param lam.Client cli:
    :param bool       all_versions:
    """
    cli = cli or get_lambda_client()
    kwargs = {}
    if all_versions:
        kwargs['FunctionVersion'] = 'ALL'
    result = cli.list_functions(**kwargs)
    result = result.get('Functions', ['FAIL'])
    return result


def get_lambda_function_names(cli=None) -> List[str]:
    """
    :param lam.Client cli:
    """
    result = []
    for entry in get_lambda_functions(cli):
        name = entry.get('FunctionName')
        if name:
            result.append(name)
    return result


# =============================================================================
# Command-line tests.
# =============================================================================


if __name__ == '__main__':
    from tests.aws import trials
    trials()
    show('')
    show('DONE')
