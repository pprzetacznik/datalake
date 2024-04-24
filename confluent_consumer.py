from config import Config
from utils import setup_logger, read_file, consume_message_loop
from consumers import ConfluentConsumer


def main():
    setup_logger()

    config = Config()
    consumer = ConfluentConsumer(
        config, value_schema_str=read_file(config.AVRO_SCHEMA_PATH)
    )
    consume_message_loop(consumer)
    consumer.close()


if __name__ == "__main__":
    main()
