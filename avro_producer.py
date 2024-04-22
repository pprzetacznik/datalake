from os import path
from io import BytesIO

from kafka import KafkaProducer
from avro.io import DatumWriter, BinaryEncoder
from avro.schema import parse

from config import Config
from utils import setup_logger, read_file


setup_logger()


value_schema_str = read_file(
    path.join(Config.WORKSPACE_DIR, Config.AVRO_SCHEMA_FILE)
)
schema = parse(value_schema_str)


def serialize_message(message: str) -> bytes:
    writer = DatumWriter(schema)
    bytes_writer = BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(message, encoder)
    return bytes_writer.getvalue()


producer = KafkaProducer(
    bootstrap_servers=Config.KAFKA_URL,
    value_serializer=serialize_message,
    ssl_check_hostname=True,
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
    producer.send(Config.KAFKA_TOPIC_NAME, key=key.encode("utf-8"), value=data)
    producer.flush()
    print(f"Sending data: {data}")
    index += 1
    print("Insert new data (stop to exit)")
    description = input()
