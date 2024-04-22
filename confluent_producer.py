from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config import Config
from utils import setup_logger, read_file, produce_message_loop

setup_logger()


class ConfluentProducer:
    def __init__(self, config: Config):
        self.config = config
        value_schema_str = read_file(config.AVRO_SCHEMA_PATH)
        schema_registry_client = SchemaRegistryClient(
            conf={
                "url": f"http://{config.SCHEMA_REGISTRY_URL}",
            }
        )
        avro_serializer = AvroSerializer(
            schema_str=value_schema_str,
            schema_registry_client=schema_registry_client,
            to_dict=None,
        )
        producer_conf = {
            "bootstrap.servers": config.KAFKA_URL,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
        self.producer = SerializingProducer(producer_conf)

    def produce(self, key: str, value: dict):
        self.producer.produce(
            self.config.KAFKA_TOPIC_NAME,
            value=value,
            key=key,
            on_delivery=self._delivery_report,
        )
        self.producer.flush()

    def _delivery_report(self, err, msg):
        if not err:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        else:
            print(f"Message delivery failed: {err}")


def main():
    config = Config()
    producer = ConfluentProducer(config)
    produce_message_loop(producer)


if __name__ == "__main__":
    main()
