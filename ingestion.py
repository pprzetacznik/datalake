import os
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode_outer, col

INGESTION_DATA_PATH = os.getenv("INGESTION_DATA_DIR", "/tmp")
IVY_CACHE_PATH = os.path.join(INGESTION_DATA_PATH, "ivy")
PARQUET_DB_PATH = os.path.join(INGESTION_DATA_PATH, "parquet")
PARQUET_DLQ_DB_PATH = os.path.join(INGESTION_DATA_PATH, "parquet-dlq")
CHECKPOINTS_PATH = os.path.join(INGESTION_DATA_PATH, "checkpoints")
KAFKA_URL = os.environ["KAFKA_URL"]
KAFKA_TOPIC_NAME = os.environ["KAFKA_TOPIC"]
AVRO_SCHEMA_FILE = os.getenv("AVRO_SCHEMA_FILE")

packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2",
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.2",
    "org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.0.2",
    "org.apache.spark:spark-avro_2.12:3.0.2",
]


def read_file(filename):
    with open(filename) as f:
        return f.read()


spark = (
    SparkSession.builder.appName("datalake-ingestion")
    .config("spark.jars.packages", ",".join(packages))
    .config("spark.jars.ivy", IVY_CACHE_PATH)
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("subscribe", KAFKA_TOPIC_NAME)
    .option("startingOffsets", "earliest")
    # .option("kafka.security.protocol", "SSL")
    # .option("kafka.ssl.truststore.location", "../kafka/truststore.jks")
    # .option("kafka.ssl.truststore.password", "changeit")
    # .option("kafka.ssl.keystore.location", "../kafka/keystore.jks")
    # .option("kafka.ssl.keystore.password", "changeit")
    # .option("kafka.ssl.key.password", "changeit")
    .option("mode", "PERMISSIVE")
    .load()
)

df.printSchema()


value_schema_str = read_file(AVRO_SCHEMA_FILE)

json_df = (
    df.withColumn("key", df.key.cast("String"))
    .selectExpr(
        "key",
        "partition",
        "offset",
        "topic",
        "timestamp",
        "substring(value, 6) as avro_value",
    )
    .withColumn(
        "record",
        explode_outer(
            from_avro(
                col("avro_value"),
                value_schema_str,
                options={"mode": "PERMISSIVE"},
            )
        ),
    )
    .select("*", "record.*")
    .drop("record")
)

json_df.printSchema()


def process_df(df, id):
    dead_letter_queue_df = df.filter("order_id is null")
    dead_letter_queue_df.write.format("console").save()

    raw_zone_df = df.filter("order_id is not null")
    raw_zone_df.write.format("console").save()
    raw_zone_df.write.format("parquet").mode("overwrite").save(PARQUET_DB_PATH)
    dead_letter_queue_df.write.format("parquet").mode("overwrite").save(
        PARQUET_DLQ_DB_PATH
    )


json_df.writeStream.option("checkpointLocation", CHECKPOINTS_PATH).option(
    "path", PARQUET_DB_PATH
).outputMode("append").foreachBatch(process_df).start().awaitTermination()
