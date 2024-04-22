from os import path
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import explode_outer, col
from config import Config
from utils import read_file


print(Config.KAFKA_TOPIC_NAME)
print(Config.IVY_CACHE_PATH)


packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2",
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.2",
    "org.apache.spark:spark-streaming-kafka-0-10-assembly_2.12:3.0.2",
    "org.apache.spark:spark-avro_2.12:3.0.2",
]


spark = (
    SparkSession.builder.appName("datalake-ingestion")
    .config("spark.jars.packages", ",".join(packages))
    .config("spark.jars.ivy", Config.IVY_CACHE_PATH)
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", Config.KAFKA_URL)
    .option("subscribe", Config.KAFKA_TOPIC_NAME)
    .option("startingOffsets", "earliest")
    .option("mode", "PERMISSIVE")
    .load()
)

df.printSchema()


value_schema_str = read_file(
    path.join(Config.WORKSPACE_DIR, Config.AVRO_SCHEMA_FILE)
)


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
    raw_zone_df.write.format("parquet").mode(Config.WRITE_MODE).save(
        Config.PARQUET_DB_PATH
    )
    dead_letter_queue_df.write.format("parquet").mode(Config.WRITE_MODE).save(
        Config.PARQUET_DLQ_DB_PATH
    )


json_df.writeStream.option(
    "checkpointLocation", Config.CHECKPOINTS_PATH
).option("path", Config.PARQUET_DB_PATH).outputMode(
    Config.WRITE_MODE
).foreachBatch(
    process_df
).start().awaitTermination()
