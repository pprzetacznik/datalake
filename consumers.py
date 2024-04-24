from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from config import Config


class ConfluentConsumer:
    def __init__(self, config: Config, value_schema_str: str):
        self.config = config
        avro_deserializer = AvroDeserializer(
            schema_str=value_schema_str,
            schema_registry_client=SchemaRegistryClient(
                conf={"url": f"http://{Config.SCHEMA_REGISTRY_URL}"}
            ),
            from_dict=None,
        )
        string_deserializer = StringDeserializer("utf_8")

        def handle_errors(*args, **kwargs):
            print(args)
            print(kwargs)

        consumer_conf = {
            "bootstrap.servers": config.KAFKA_URL,
            "key.deserializer": string_deserializer,
            "value.deserializer": avro_deserializer,
            "group.id": "speedwell.confluent_kafka.consumer",
            "auto.offset.reset": "earliest",
            "error_cb": handle_errors,
        }
        self.consumer = DeserializingConsumer(consumer_conf)
        self.consumer.subscribe([config.KAFKA_TOPIC_NAME])

    def consume(self):
        return self.consumer.poll(1.0)

    def close(self):
        self.consumer.close()
