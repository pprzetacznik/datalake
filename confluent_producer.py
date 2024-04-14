import os
import sys
import logging
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

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

schema_registry_client = SchemaRegistryClient(
    conf={
        "url": f"http://{SCHEMA_REGISTRY_URL}",
    }
)

avro_serializer = AvroSerializer(
    schema_str=value_schema_str,
    schema_registry_client=schema_registry_client,
    to_dict=None,
)

producer_conf = {
    "bootstrap.servers": KAFKA_URL,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
}

producer = SerializingProducer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


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
    producer.produce(
        KAFKA_TOPIC_NAME, value=data, key=key, on_delivery=delivery_report
    )
    producer.flush()
    print(f"Sending data: {data}")
    index += 1
    print("Insert new data (stop to exit)")
    description = input()
