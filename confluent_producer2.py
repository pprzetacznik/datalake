from json import dumps
from config import Config
from common import monkey_patch_avro, get_all_types_schema
from confluent_producer import ConfluentProducer
from utils import setup_logger, input_loop

setup_logger()
monkey_patch_avro()


@input_loop
def produce_message_loop(producer, index, description):
    key = f"customer{index}"
    data = (
        {
            "oneof_type": {
                "customer_id": 5,
                "customer_name": key,
                "customer_email": "client@speedwell.pl",
                "customer_address": description,
            }
        }
        if index % 2 == 1
        else {
            "oneof_type": {
                "product_id": 5,
                "product_name": f"{key}-{description}",
                "product_price": 5.5,
            }
        }
    )
    producer.produce(key=key, value=data)
    return key, data


def main():
    config = Config()
    all_types_schema = get_all_types_schema()
    print(dumps(all_types_schema.resolve_schema(), indent=4))

    def strategy(*args):
        print(args)
        return f"{Config.KAFKA_TOPIC_NAME}-io.confluent.examples.avro.AllTypes"

    serializer_conf = {
        "auto.register.schemas": False,
        # "use.latest.version": True,
        "subject.name.strategy": strategy,
    }
    producer = ConfluentProducer(
        config,
        value_schema_str=dumps(all_types_schema.resolve_schema()),
        serializer_conf=serializer_conf,
    )
    produce_message_loop(producer)


if __name__ == "__main__":
    main()
