# AWS interface tests.

from utility import *
from aws     import s3_client, s3_resource
from aws     import sqs_client, sqs_resource
from aws     import sqs_queue_name, get_sqs_queue, create_sqs_queue


# =============================================================================
# Constants
# =============================================================================


AWS_TEST_QUEUE = 'emma-test3-staging'


# =============================================================================
# AWS S3
# =============================================================================


def list_s3_buckets(s3=None):
    """
    :param Client|ServiceResource s3:
    """
    s3_cli   = s3_client(s3)
    response = s3_cli.list_buckets()
    for bucket in response.get('Buckets', []):
        show(f"{bucket['Name']} {'=' * 40}")
        show(bucket)


def show_s3_bucket_names(s3=None):
    """
    :param Client|ServiceResource s3:
    """
    s3_res = s3_resource(s3)
    for bucket in s3_res.buckets.all():
        show(bucket.name)


def s3_trials(via_client=True):
    """
    Exercise AWS S3 methods.

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
        src = 'emma.py'
        show_header('Copy to S3')
        copy_to_s3_bucket(src, IA_BUCKET)


# =============================================================================
# AWS SQS
# =============================================================================


def show_sqs_queue(queue):
    """
    :param Queue queue:                 SQS service resource instance to use.
    """
    show(sqs_queue_name(queue))
    show({**{'url': queue.url}, **queue.attributes})


def show_sqs_queue_names(sqs_res=None):
    """
    :param ServiceResource sqs_res:     SQS service resource instance to use.
    """
    sqs_res = sqs_resource(sqs_res)
    for queue in sqs_res.queues.all():
        show(sqs_queue_name(queue))


def show_sqs_queues(match=None, sqs_res=None):
    """
    :param str             match:   If given, only list queues whose name
                                        matches the pattern.
    :param ServiceResource sqs_res: SQS service resource instance to use.
    """
    sqs_res = sqs_resource(sqs_res)
    for queue in sqs_res.queues.all():
        if match in sqs_queue_name(queue):
            show_sqs_queue(queue)
            show('--------------')


def show_sqs_queues_client(match=None, sqs_cli=None):
    """
    :param str    match:        If given, only list queues with matching URLs.
    :param Client sqs_cli:      Optional S3 sqs_cli.
    """
    sqs_cli  = sqs_client(sqs_cli)
    response = sqs_cli.list_queues()
    for url in response.get('QueueUrls', []):
        if match is None or match in url:
            show(url)


def sqs_trials(via_client=True, send_msgs=True, recv_msgs=True):
    """
    Exercise AWS SQS methods.

    :param bool via_client: Use variants which operate via the client directly.
    :param bool send_msgs:  Execute functions which send SQS messages.
    :param bool recv_msgs:  Execute functions which receive SQS messages.

    """

    # List SQS queues.
    pattern = 'emma'
    if True:
        show_header('SQS queues')
        show_sqs_queues(match=pattern)
        if via_client:
            show_sqs_queues_client(match=pattern)
        else:
            show_sqs_queues(match=pattern)

    # Delete SQS queue(s).
    if False:
        # noinspection PyShadowingNames
        queues = ['emma-test-staging', 'emma-test2-staging', 'emma-test3-staging']
        show_header('Delete SQS queues:')
        for name in queues:
            show(name)
            delete_sqs_queue(name)

    # Create an SQS queue.
    q_name = AWS_TEST_QUEUE
    if True:
        show_header(f'Create SQS queue "{q_name}"')
        create_sqs_queue(q_name)

    # Get an SQS queue.
    if True:
        show_header(f'Get SQS queue "{q_name}"')
        q = get_sqs_queue(q_name)
        show(q.url)
        show('delay: %s' % q.attributes.get('DelaySeconds'))

    # Send a message to an SQS queue.
    if send_msgs:
        show_header(f'Send a message to SQS queue "{q_name}"')
        body = 'MESSAGE BODY'
        attr = {
            'Author': {
                'StringValue': 'AUTHOR NAME',
                'DataType':    'String',
            },
            'Count': {
                'StringValue': '18',
                'DataType':    'Number',
            }
        }
        response = q.send_message(MessageBody=body, MessageAttributes=attr)
        show('RESPONSE:')
        show(response)

    # Send a message to an SQS queue.
    if send_msgs:
        count = 2
        show_header(f'Send {count} messages to SQS queue "{q_name}"')
        entries = []
        for i in range(1, (count + 1)):
            entry = {
                'Id': str(i),
                'MessageBody': f'MESSAGE {i} BODY',
                'MessageAttributes': {
                    'Author': {
                        'StringValue': f'AUTHOR {i} NAME',
                        'DataType':    'String',
                    },
                    'Count': {
                        'StringValue': f'{18 + i}',
                        'DataType':    'Number',
                    }
                }
            }
            entries.append(entry)
        response = q.send_messages(Entries=entries)
        show('RESPONSE:')
        show(response)

    # Receive SQS messages.
    # @see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
    if recv_msgs:
        show_header(f'Receive messages from SQS queue "{q_name}"')
        args = {
            'AttributeNames':           ['All'],
            'MessageAttributeNames':    ['All'],
            'MaxNumberOfMessages':      10,  # The maximum max.
            'VisibilityTimeout':        0,
        }
        for message in q.receive_messages(**args):
            # Get the custom author message attribute if it was set.
            author_text = ''
            if message.message_attributes:
                author = message.message_attributes.get('Author')
                author_name = author and author.get('StringValue')
                if author_name:
                    author_text = f' ({author_name})'
            # Print out the body and author (if set).
            show(f'Hello, {message.body}!{author_text}')
            # Let the queue know that the message is processed.
            message.delete()


# =============================================================================
# Tests
# =============================================================================


def trials():
    show_section('AWS TRIALS')
    s3_trials()
    sqs_trials(send_msgs=False)
