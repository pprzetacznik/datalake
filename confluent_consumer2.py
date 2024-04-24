from json import dumps
from common import monkey_patch_avro, get_all_types_schema
from config import Config
from utils import setup_logger, consume_message_loop
from consumers import ConfluentConsumer


def main():
    setup_logger()
    monkey_patch_avro()

    config = Config()
    all_types_schema = get_all_types_schema()
    print(all_types_schema.schema_str)
    print(dumps(all_types_schema.resolve_schema(), indent=4))
    consumer = ConfluentConsumer(
        config, value_schema_str=all_types_schema.schema_str
    )
    consume_message_loop(consumer)
    consumer.close()


if __name__ == "__main__":
    main()
