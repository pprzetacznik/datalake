import sys
from functools import wraps
import logging
from config import Config


def setup_logger():
    if Config.VERBOSE:
        logger = logging.getLogger("kafka")
        logger.addHandler(logging.StreamHandler(sys.stdout))
        logger.setLevel(logging.DEBUG)


def read_file(filename):
    with open(filename) as f:
        return f.read()


def input_loop(fun):
    @wraps(fun)
    def handle(*args, **kwargs):
        print("Please insert a description --> 'stop' to exit")
        description = input()
        index = 0
        while description != "stop":
            kwargs["index"] = index
            kwargs["description"] = description
            key, data = fun(*args, **kwargs)
            print(f"Sending data: {key}: {data}")
            print("Insert new data ('stop' to exit)")
            index += 1
            description = input()

    return handle


@input_loop
def produce_message_loop(producer, index, description):
    key = f"product{index}"
    data = [
        {
            "order_id": index,
            "customer": "John Doe",
            "description": f"{description}",
            "price": 99.98,
            "products": [
                {"name": "shoes", "price": 49.99},
                {"name": "t-shirt", "price": 49.99},
            ],
        }
    ]
    producer.produce(key=key, value=data)
    return key, data


def consume_loop(fun):
    @wraps(fun)
    def handle(*args, **kwargs):
        while True:
            try:
                result = fun(*args, **kwargs)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(e)

    return handle


@consume_loop
def consume_message_loop(consumer):
    message = consumer.consume()
    if message:
        print(f"{message.value()}")
