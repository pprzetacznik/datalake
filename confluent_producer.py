from os import path

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config import Config
from utils import setup_logger, read_file

setup_logger()


value_schema_str = read_file(
    path.join(Config.WORKSPACE_DIR, Config.AVRO_SCHEMA_FILE)
)

schema_registry_client = SchemaRegistryClient(
    conf={
        "url": f"http://{Config.SCHEMA_REGISTRY_URL}",
    }
)

avro_serializer = AvroSerializer(
    schema_str=value_schema_str,
    schema_registry_client=schema_registry_client,
    to_dict=None,
)

producer_conf = {
    "bootstrap.servers": Config.KAFKA_URL,
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
        Config.KAFKA_TOPIC_NAME,
        value=data,
        key=key,
        on_delivery=delivery_report,
    )
    producer.flush()
    print(f"Sending data: {data}")
    index += 1
    print("Insert new data (stop to exit)")
    description = input()
