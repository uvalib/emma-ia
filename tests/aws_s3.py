# tests/aws_s3.py
#
# AWS S3 interface trials.


from app.aws_s3 import *


# =============================================================================
# Functions
# =============================================================================


def list_s3_buckets(s3_obj=None):
    """
    :param s3.Client|s3.ServiceResource s3_obj:
    """
    s3_cli   = s3_client(s3_obj)
    response = s3_cli.list_buckets()
    for bucket in response.get('Buckets', []):
        show(f"{bucket['Name']} {'=' * 40}")
        show(bucket)


def show_s3_bucket_names(s3_obj=None):
    """
    :param s3.Client|s3.ServiceResource s3_obj:
    """
    s3_res = s3_resource(s3_obj)
    for bucket in s3_res.buckets.all():
        show(bucket.name)


# =============================================================================
# Trials
# =============================================================================


def trials(via_client=True):
    """
    :param bool via_client: Use variants which operate via the client directly.
    """

    # List S3 buckets.
    if True:
        show_header('S3 buckets')
        if via_client:
            list_s3_buckets()
        else:
            show_s3_bucket_names()

    # Copy file to bucket.
    if False:
        src = 'Pipfile'
        bkt = s3_bucket_name('emma', 'staging')
        show_header(f'Copy to S3 bucket "{bkt}"')
        copy_to_s3_bucket(src, bkt)


if __name__ == '__main__':
    trials()
