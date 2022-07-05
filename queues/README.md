# Test SQS

1. Start the docker using: `docker-compose run aws_sqs`
2. To start the observer:  `python queues/read_msgs.py`
3. To send one msg: `python queues/send_msg.py test`
