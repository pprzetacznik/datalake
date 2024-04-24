from io import BytesIO
from kafka import KafkaProducer
from avro.io import DatumWriter, BinaryEncoder
from avro.schema import parse

from config import Config
from utils import setup_logger, read_file, produce_message_loop

setup_logger()


class AvroProducer:
    def __init__(
        self,
        config: Config,
        value_schema_str: str,
    ):
        self.config = config
        self.schema = parse(value_schema_str)
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_URL,
            value_serializer=self._serialize_message,
            ssl_check_hostname=True,
        )

    def produce(self, key: str, value: dict):
        self.producer.send(
            self.config.KAFKA_TOPIC_NAME, key=key.encode("utf-8"), value=value
        )
        self.producer.flush()

    def _serialize_message(self, message: str) -> bytes:
        writer = DatumWriter(self.schema)
        bytes_writer = BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(message, encoder)
        return bytes_writer.getvalue()


def main():
    config = Config()
    producer = AvroProducer(
        config, value_schema_str=read_file(config.AVRO_SCHEMA_PATH)
    )
    produce_message_loop(producer)


if __name__ == "__main__":
    main()
