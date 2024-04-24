from json import loads
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    Schema,
    SchemaReference,
    record_subject_name_strategy,
    topic_subject_name_strategy,
)
from fastavro import parse_schema
from config import Config
from utils import read_file


def monkey_patch_avro():
    # https://github.com/confluentinc/confluent-kafka-python/blob/master/src/confluent_kafka/schema_registry/avro.py
    def parse_schema(schema, named_schemas):
        from fastavro import parse_schema

        customer_schema_str = read_file(Config.CUSTOMER_SCHEMA_PATH)
        product_schema_str = read_file(Config.PRODUCT_SCHEMA_PATH)

        customer_schema = loads(customer_schema_str)
        product_schema = loads(product_schema_str)
        return parse_schema(
            schema,
            named_schemas={
                "io.confluent.examples.avro.Customer": customer_schema,
                "io.confluent.examples.avro.Product": product_schema,
            },
        )

    import confluent_kafka.schema_registry.avro

    confluent_kafka.schema_registry.avro.parse_schema = parse_schema


class SchemaProperties:
    def __init__(self, filename, dependencies=None):
        self.schema_registry_client = SchemaRegistryClient(
            conf={"url": f"http://{Config.SCHEMA_REGISTRY_URL}"}
        )
        self.dependencies = dependencies if dependencies else []
        self.schema_str = read_file(filename)
        self.schema_dict = loads(self.schema_str)
        self.schema_name = self._get_schema_name()
        self.subject = self._get_subject()
        self.schema_registered = self._get_schema_registered()
        self.schema_reference = self._get_schema_reference()

    def _get_schema_name(self):
        return f"{self.schema_dict['namespace']}.{self.schema_dict['name']}"

    def _get_subject(self):
        return f"{Config.KAFKA_TOPIC_NAME}-{self.schema_name}"

    def _get_schema_registered(self):
        schema = Schema(
            self.schema_str,
            "AVRO",
            [dependency.schema_reference for dependency in self.dependencies],
        )
        self.schema_registry_client.register_schema(self.subject, schema)
        return self.schema_registry_client.lookup_schema(self.subject, schema)

    def _get_schema_reference(self):
        return SchemaReference(
            self.schema_name,
            self.subject,
            self.schema_registered.version,
        )

    def resolve_schema(self):
        return parse_schema(
            loads(self.schema_str),
            named_schemas={
                dependency.schema_name: loads(dependency.schema_str)
                for dependency in self.dependencies
            },
            expand=True,
            _write_hint=False,
        )


def get_all_types_schema():
    customer_schema = SchemaProperties(Config.CUSTOMER_SCHEMA_PATH)
    product_schema = SchemaProperties(Config.PRODUCT_SCHEMA_PATH)
    all_types_schema = SchemaProperties(
        Config.ALL_TYPES_SCHEMA_PATH,
        dependencies=[customer_schema, product_schema],
    )
    return all_types_schema
