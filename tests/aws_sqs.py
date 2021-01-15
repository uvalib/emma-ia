# tests/aws_sqs.py
#
# AWS SQS interface trials.


from app.aws_sqs import *


# =============================================================================
# Constants
# =============================================================================


AWS_TEST_QUEUE = 'emma-test3-staging'


# =============================================================================
# Functions
# =============================================================================


def show_sqs_queue(queue):
    """
    :param sqs.Queue queue:
    """
    show(sqs_queue_name(queue))
    show({**{'url': queue.url}, **queue.attributes})


def show_sqs_queue_names(sqs_res=None):
    """
    :param sqs.ServiceResource sqs_res:
    """
    sqs_res = sqs_resource(sqs_res)
    for queue in sqs_res.queues.all():
        show(sqs_queue_name(queue))


def show_sqs_queues(match=None, sqs_res=None):
    """
    :param str match:   Only list queues with names matching the pattern.
    :param sqs.ServiceResource sqs_res:
    """
    sqs_res = sqs_resource(sqs_res)
    for queue in sqs_res.queues.all():
        if match in sqs_queue_name(queue):
            show_sqs_queue(queue)
            show('--------------')


def show_sqs_queues_client(match=None, sqs_obj=None):
    """
    :param str match:   Only list queues with URLs matching the pattern.
    :param sqs.Client|sqs.ServiceResource sqs_obj:
    """
    sqs_cli  = sqs_client(sqs_obj)
    response = sqs_cli.list_queues()
    for url in response.get('QueueUrls', []):
        if match is None or match in url:
            show(url)


# =============================================================================
# Trials
# =============================================================================


def trials(via_client=True, send_msgs=True, recv_msgs=True):
    """
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


if __name__ == '__main__':
    trials()
