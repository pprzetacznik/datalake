import os


class Config:
    VERBOSE = os.getenv("VERBOSE", False) in ("True", True)

    INGESTION_DATA_PATH = os.getenv("INGESTION_DATA_PATH", "/tmp")
    IVY_CACHE_PATH = os.path.join(INGESTION_DATA_PATH, "ivy")
    PARQUET_DB_PATH = os.path.join(INGESTION_DATA_PATH, "parquet")
    PARQUET_DLQ_DB_PATH = os.path.join(INGESTION_DATA_PATH, "parquet-dlq")
    CHECKPOINTS_PATH = os.path.join(INGESTION_DATA_PATH, "checkpoints")

    KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")
    KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC", "dev.orders")
    KAFKA_TOPIC_NAME2 = os.getenv("KAFKA_TOPIC", "dev-union.orders")

    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "localhost:8081")

    WORKSPACE_DIR = os.getenv("WORKSPACE_DIR", ".")
    AVRO_SCHEMA_FILE = os.getenv("AVRO_SCHEMA_FILE", "orders.avsc")

    WRITE_MODE = "append"
