import boto3
from botocore.exceptions import ClientError


def client():
    return boto3.resource(
        "sqs",
        endpoint_url="http://localhost:9324",
        region_name="elasticmq",
        aws_secret_access_key="x",
        aws_access_key_id="x",
        use_ssl=False,
    )


def default_queue(client=client(), queue_name="default"):
    return client.get_queue_by_name(QueueName=queue_name)


def create_queue(client=client(), name="default", attributes=None):
    """
    Creates an Amazon SQS queue.

    :param name: The name of the queue. This is part of the URL assigned to the queue.
    :param attributes: The attributes of the queue, such as maximum message size or
                       whether it's a FIFO queue.
    :return: A Queue object that contains metadata about the queue and that can be used
             to perform queue operations like sending and receiving messages.
    """
    if not attributes:
        attributes = {}

    try:
        queue = client.create_queue(QueueName=name, Attributes=attributes)
        print("Created queue '%s' with URL=%s", name, queue.url)
    except ClientError as error:
        print("Couldn't create queue named '%s'.", name)
        raise error
    else:
        return queue


def send_message(message_body, queue=None, message_attributes=None):
    """
    Send a message to an Amazon SQS queue.

    :param queue: The queue that receives the message.
    :param message_body: The body text of the message.
    :param message_attributes: Custom attributes of the message. These are key-value
                               pairs that can be whatever you want.
    :return: The response from SQS that contains the assigned message ID.
    """
    if queue is None:
        queue = default_queue()

    if not message_attributes:
        message_attributes = {}

    try:
        response = queue.send_message(
            MessageBody=message_body, MessageAttributes=message_attributes
        )
    except ClientError as error:
        print("Send message failed: %s", message_body)
        raise error
    else:
        return response


def receive_messages(max_number, wait_time, queue=None, delete=True):
    """
    Receive a batch of messages in a single request from an SQS queue.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual number
                       of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning. When
                      this number is greater than zero, long polling is used. This
                      can result in reduced costs and fewer false empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
    if queue is None:
        queue = default_queue()

    try:
        messages = queue.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time,
        )
        for msg in messages:
            print("Received message: %s: %s", msg.message_id, msg.body)
            if delete:
                delete_message(message=msg)
    except ClientError as error:
        print("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        return messages


def delete_message(message):
    """
    Delete a message from a queue. Clients must delete messages after they
    are received and processed to remove them from the queue.

    :param message: The message to delete. The message's queue URL is contained in
                    the message's metadata.
    :return: None
    """
    try:
        message.delete()
        print("Deleted message: %s", message.message_id)
    except ClientError as error:
        print("Couldn't delete message: %s", message.message_id)
        raise error


def pack_message(msg_path, msg_body, msg_line):
    return {
        "body": msg_body,
        "attributes": {
            "path": {"StringValue": msg_path, "DataType": "String"},
            "line": {"StringValue": str(msg_line), "DataType": "String"},
        },
    }


def unpack_message(msg):
    return (
        msg.message_attributes["path"]["StringValue"],
        msg.body,
        int(msg.message_attributes["line"]["StringValue"]),
    )
