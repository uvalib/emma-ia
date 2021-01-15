# app/aws_sqs.py
#
# AWS SQS interface definitions.


import boto3
import boto3_type_annotations.sqs as sqs

from botocore.exceptions import ClientError

from app.common import *


# =============================================================================
# Constants
# =============================================================================


DEF_QUEUE_DELAY = 0


# =============================================================================
# AWS SQS class instances
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


# =============================================================================
# AWS SQS queues
# =============================================================================


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
        log_error(error)
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
