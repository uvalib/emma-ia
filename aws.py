# AWS interface.

import boto3

# noinspection PyUnresolvedReferences
from boto3_type_annotations.s3  import *
from boto3_type_annotations.sqs import *
from botocore.exceptions        import ClientError

from utility import *


# =============================================================================
# Constants
# =============================================================================


AWS_DEBUG = True

BS_BUCKET = 'emma-bs-queue-staging'
HT_BUCKET = 'emma-ht-queue-staging'
IA_BUCKET = 'emma-ia-queue-staging'

DEF_QUEUE_DELAY = 0  # 2


# =============================================================================
# AWS S3
# =============================================================================


def is_s3_client(item) -> bool:
    return 'list_objects' in dir(item)


def is_s3_resource(item) -> bool:
    return 'Bucket' in dir(item)


def is_s3_bucket(item) -> bool:
    return 'Policy' in dir(item)


def s3_client(item) -> Client:
    if is_s3_client(item):
        return item
    elif is_s3_resource(item) or is_s3_bucket(item):
        return item.meta.client
    else:
        return boto3.client('s3')


def s3_resource(item):
    # :rtype: ServiceResource
    if is_s3_resource(item):
        return item
    else:
        return boto3.resource('s3')


def get_s3_bucket(bucket, s3_res=None):
    """
    :param str|Bucket      bucket:      Bucket name or Bucket instance.
    :param ServiceResource s3_res:      S3 service resource instance to use.
    :rtype: Bucket
    """
    if isinstance(bucket, str):
        s3_res = s3_resource(s3_res)
        bucket = s3_res.Bucket(bucket)
    return bucket


def create_s3_bucket(name, region=None, s3=None):
    """
    Create an S3 bucket in a specified region.

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    @see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html

    :param str name:                    Bucket to create,
    :param str region:                  Region for bucket in, e.g., 'us-west-2'
    :param Client|ServiceResource s3:

    :return: Whether the bucket was created.
    :rtype:  bool

    """
    s3 = s3 or s3_client(s3)
    config = {}
    if region:
        config['LocationConstraint'] = region
    try:
        s3.create_bucket(Bucket=name, CreateBucketConfiguration=config)
    except ClientError as error:
        logging.error(error)
        return False
    return True


def copy_to_s3_bucket(file, bucket, s3_res=None):
    """
    :param str             file:
    :param str|Bucket      bucket:      Bucket to copy to.
    :param ServiceResource s3_res:      S3 service resource instance to use.
    """
    fd = open(file, 'rb')
    bucket = get_s3_bucket(bucket, s3_res)
    bucket.put_object(Key=file, Body=fd)


def upload_to_s3_bucket(file_name, bucket, object_key=None, s3=None):
    """
    Upload a file to an S3 bucket.

    @see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html

    :param str        file_name:    File to upload.
    :param str|Bucket bucket:       Bucket to upload to.
    :param str        object_key:   S3 object name. If not given then file_name
                                        is used.
    :param Client|ServiceResource s3:

    :return: Whether the file was uploaded.
    :rtype:  bool

    """
    object_key = object_key or file_name
    try:
        if is_s3_client(s3):
            bucket_name = bucket if isinstance(bucket, str) else bucket.name
            s3.upload_file(file_name, bucket_name, object_key)
        else:
            bucket = get_s3_bucket(bucket, s3)
            bucket.upload_file(file_name, object_key)
    except ClientError as error:
        logging.error(error)
        return False
    return True


def download_from_s3_bucket(object_key, bucket, file_name=None, s3=None):
    """
    Download a file from an S3 bucket.

    @see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html

    :param str        object_key:   S3 object name.
    :param str|Bucket bucket:       Bucket to download from.
    :param str        file_name:    File to download. If not given then
                                        object_key is used.
    :param Client|ServiceResource s3:

    :return: Whether file was downloaded.
    :rtype:  bool

    """
    file_name = file_name or object_key
    try:
        if is_s3_client(s3):
            bucket_name = bucket if isinstance(bucket, str) else bucket.name
            s3.download_file(bucket_name, object_key, file_name)
        else:
            bucket = get_s3_bucket(bucket, s3)
            bucket.download_file(object_key, file_name)
    except ClientError as error:
        AWS_DEBUG and show(f'\tERROR: {error}')
        logging.error(error)
        return False
    return True


def delete_from_s3_bucket(object_keys, bucket, s3=None):
    """
    Remove a file from an S3 bucket.

    :param str|list[str]   object_keys: S3 object name(s).
    :param str|Bucket      bucket:      Bucket holding the object(s).
    :param Client|ServiceResource s3:

    :return: Whether the file(s) were removed.
    :rtype:  bool

    """
    object_list = []
    keys = [object_keys] if isinstance(object_keys, str) else object_keys
    for key in keys:
        object_list.append({'Key': key})
    if object_list:
        try:
            if is_s3_client(s3):
                bucket = bucket if isinstance(bucket, str) else bucket.name
                s3.delete_objects(bucket, Delete={'Objects': object_list})
            else:
                bucket = get_s3_bucket(bucket, s3)
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


def sqs_client(item) -> Client:
    if is_sqs_client(item):
        return item
    elif is_sqs_resource(item):
        return item.meta.client
    else:
        return boto3.client('sqs')


def sqs_resource(item):
    # :rtype: ServiceResource
    if is_sqs_resource(item):
        return item
    else:
        return boto3.resource('sqs')


def sqs_queue_name(queue: Queue) -> str:
    return queue.attributes['QueueArn'].split(':')[-1]


def get_sqs_queue(name, sqs_res=None):
    """
    :param str             name:        AWS SQS queue name.
    :param ServiceResource sqs_res:     SQS service resource instance to use.

    :return: The queue instance.
    :rtype:  Queue|None
    """
    sqs_res = sqs_resource(sqs_res)
    try:
        queue = sqs_res.get_queue_by_name(QueueName=name)
    except ClientError as error:
        AWS_DEBUG and show(f'\tERROR: {error}')
        logging.error(error)
        queue = None
    return queue


def create_sqs_queue(name, delay=DEF_QUEUE_DELAY, sqs=None):
    """
    :param str                    name:     AWS SQS queue name.
    :param str|int                delay:    Queue update delay.
    :param Client|ServiceResource sqs:      Optional SQS client/resource.

    :return: The queue instance.
    :rtype:  Queue|None
    """
    sqs  = sqs or sqs_resource(sqs)
    attr = {'DelaySeconds': str(delay)}
    try:
        queue = sqs.create_queue(QueueName=name, Attributes=attr)
    except ClientError as error:
        AWS_DEBUG and show(f'QUEUE ALREADY EXISTS? - %{error}')
        queue = get_sqs_queue(name, sqs)
    if queue and AWS_DEBUG:
        show(sqs_queue_name(queue))
        show({**{'url': queue.url}, **queue.attributes})
    return queue


def delete_sqs_queue(name, sqs=None):
    """
    :param str    name:                 AWS SQS queue name.
    :param Client|ServiceResource sqs:  Optional SQS client/resource.
    """
    try:
        queue = get_sqs_queue(name, sqs)
    except ClientError as error:
        AWS_DEBUG and show(f'\tERROR: {error}')
        queue = None
    if queue:
        sqs_cli = sqs_client(sqs)
        sqs_cli.delete_queue(QueueUrl=queue.url)


# =============================================================================
# Command-line tests.
# =============================================================================


if __name__ == '__main__':
    from tests.aws import trials
    trials()
    show('')
    show('DONE')
