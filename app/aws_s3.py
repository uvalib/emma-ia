# app/aws_s3.py
#
# AWS S3 interface definitions.


import re
import boto3
import boto3_type_annotations.s3 as s3

from botocore.exceptions import ClientError

from app.common import *


# =============================================================================
# AWS S3 class instances
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


# =============================================================================
# AWS S3 buckets
# =============================================================================


def s3_bucket_name(repo, deployment) -> str:
    """
    Return with the S3 bucket associated with the given repository for the
    given deployment.

    :param str|None repo:           Member repository (def: DEF_REPO).
    :param str|None deployment:     One of DEPLOYMENTS (def: DEF_DEPLOYMENT).

    """
    repo = str(repo).casefold() if repo else DEF_REPO
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
    deployment = str(deployment).casefold() if deployment else DEF_DEPLOYMENT
    return f"emma-{area}-{deployment}"


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
        log_error(error)
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
        log_error(error)
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
        log_error(error)
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
    for key in to_list(object_keys):
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
            log_error(error)
            return False
    return True


# =============================================================================
# AWS S3 objects
# =============================================================================


def prefix_of(obj):
    """
    Extract the prefix of an object key.

    :param str|s3.Object obj:   An object key or prefix (ending with '/').

    :return: Either '' or a string ending with '/'.
    :rtype:  str

    """
    obj   = obj.key if is_s3_object(obj) else obj or ''
    parts = obj.split('/')
    parts.pop()
    return '/'.join(parts) + '/' if parts else ''


def s3_object_count(obj, bucket=None, s3_item=None) -> int:
    """
    The number of matching objects in the bucket.

    :param str|s3.Object obj:       An object key or prefix (ending with '/').
    :param str|s3.Bucket bucket:    Bucket name.
    :param s3.Client|s3.ServiceResource s3_item:

    """
    if is_s3_object(obj):
        name    = obj.key
        bucket  = obj.bucket_name
    else:
        name    = obj
        bucket  = get_s3_bucket(bucket, s3_item).name
    response = s3_client(s3_item).list_objects_v2(Bucket=bucket, Prefix=name)
    return response.get('KeyCount') or 0


def s3_object_exists(obj, bucket=None, s3_item=None) -> bool:
    """
    Indicate whether the bucket has any matching objects.

    :param str|s3.Object obj:       An object key or prefix (ending with '/').
    :param str|s3.Bucket bucket:    Bucket name.
    :param s3.Client|s3.ServiceResource s3_item:

    """
    return s3_object_count(obj, bucket, s3_item) > 0


def s3_object_rename(obj, new_key, bucket=None, s3_item=None):
    """
    Rename an object by replacing it with an object of the given object key.
    
    :param str|s3.Object obj:       Object with the original key name.
    :param str           new_key:   Name for the replacement object.
    :param str|s3.Bucket bucket:    Bucket name.
    :param s3.Client|s3.ServiceResource s3_item:
    
    :return: The renamed (replacement) object.
    :rtype:  s3.Object|None
    
    """
    result = None
    try:
        if is_s3_object(obj):
            obj_key = obj.key
            bucket  = bucket or obj.bucket_name
            bucket  = get_s3_bucket(bucket, s3_item)
        else:
            obj_key = obj
            bucket  = get_s3_bucket(bucket, s3_item)
            obj     = bucket.Object(obj_key)
        new_obj = bucket.Object(new_key)
        new_obj.copy_from(CopySource={'Bucket': bucket.name, 'Key': obj_key})
        obj.delete()
        result = new_obj
    except ClientError as error:
        DEBUG and show(f'\tERROR: {error} (obj_key = "{obj_key}")')
    return result
