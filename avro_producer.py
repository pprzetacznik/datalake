import os
import sys
import logging
from io import BytesIO
from kafka import KafkaProducer
from avro.io import DatumWriter, BinaryEncoder
from avro.schema import parse

if os.getenv("VERBOSE", False) in ("True", True):
    logger = logging.getLogger("kafka")
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.setLevel(logging.DEBUG)


def read_file(filename):
    with open(filename) as f:
        return f.read()


KAFKA_URL = os.environ["KAFKA_URL"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
KAFKA_TOPIC_NAME = os.environ["KAFKA_TOPIC"]
AVRO_SCHEMA_FILE = os.getenv("AVRO_SCHEMA_FILE")
value_schema_str = read_file(AVRO_SCHEMA_FILE)
schema = parse(value_schema_str)


def serialize_message(message: str) -> bytes:
    writer = DatumWriter(schema)
    bytes_writer = BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(message, encoder)
    return bytes_writer.getvalue()


producer = KafkaProducer(
    bootstrap_servers=KAFKA_URL,
    value_serializer=serialize_message,
    ssl_check_hostname=True,
    # ssl_cafile="./cacert.pem",
    # ssl_certfile="./certfile.pem",
    # ssl_keyfile="./keyfile.pem",
    # security_protocol="SSL",
)


print("Please insert a description --> 'stop' to exit")
description = input()
index = 0

while description != "stop":
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
    producer.send(KAFKA_TOPIC_NAME, key=key.encode("utf-8"), value=data)
    producer.flush()
    print(f"Sending data: {data}")
    index += 1
    print("Insert new data (stop to exit)")
    input_user = input()
