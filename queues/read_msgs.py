from queues.actions import receive_messages, create_queue


if __name__ == "__main__":
    try:
        create_queue()
    except Exception:
        pass

    while 1:
        receive_messages(max_number=1, wait_time=1)
