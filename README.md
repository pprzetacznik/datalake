# Datalake - ingestion

## Table of contents

* [Overview](#overview)
* [Things to improve](#things-to-improve)
* [How to run](#how-to-run)
* [Development](#development)
* [References](#references)

## Overview

This repository shows how to implement datalake ingestion using Spark Structured Streaming and Kafka. Data are streamed to the parquet database. If messages don't match schema, they're saved to dead letter queue. Schema is saved by the confluent_producer to schema registry.

## Things to improve

* For now, the schema matching using schema registry is only on the ingestion side. There is some additional work needed to do to validate schema against schema registry on the consumer side,
* Needed partitioning tuning for parquet database,
* Idempotency to improve guarentee of message delivery.

## How to run

The main component of the ingestion is `ingestion.py`.
To run it as a process you can simply run docker compose.
To test it, I would recommend running producers from your local machine.
You can generate messages using `confluent_producer` or `avro_producer`. Both examples uses different libraries.
To test the produced database in raw zone, you can use pandas. All needed python dependencies are in `requirements-local.txt` file.

### Run datalake

```Bash
docker-compose up
```

### Prepare local producer and data viewer

```Bash
$ mkvirtualenv datalake
$ workon datalake
(datalake) $ pip install -r requirements-local.txt
```

### Produce messages to kafka topic

When you run a producer on your local machine you'll be asked to preapare a new message.

```Bash
$ ./run_producer.sh
...
Please insert a description --> 'stop' to exit
zażółć gęślą jaźń
Message delivered to dev.orders [0]
Sending data: [{'order_id': 0, 'customer': 'John Doe', 'description': 'zażółć gęślą jaźń', 'price': 99.98, 'products': [{'name': 'shoes', 'price': 49.99}, {'name': 't-shirt', 'price': 49.99}]}]
Insert new data (stop to exit)
>
```

You'll be able to see messages in the docker-compose logs.
```Bash
+---+---------+------+-----+---------+----------+--------+--------+-----------+-----+--------+
ingestion          | |key|partition|offset|topic|timestamp|avro_value|order_id|customer|description|price|products|
ingestion          | +---+---------+------+-----+---------+----------+--------+--------+-----------+-----+--------+
ingestion          | +---+---------+------+-----+---------+----------+--------+--------+-----------+-----+--------+
ingestion          |
+--------+---------+------+----------+--------------------+--------------------+--------+--------+-----------------+-----+--------------------+
ingestion          | |     key|partition|offset|     topic|           timestamp|          avro_value|order_id|customer|      description|price|            products|
ingestion          | +--------+---------+------+----------+--------------------+--------------------+--------+--------+-----------------+-----+--------------------+
ingestion          | |product0|        0|     4|dev.orders|2021-02-27 18:39:...|[02 00 10 4A 6F 6...|       0|John Doe|za???? g??l? ja??|99.98|[[shoes, 49.99], ...|
ingestion          | +--------+---------+------+----------+--------------------+--------------------+--------+--------+-----------------+-----+--------------------+
```

### View parquet raw database

The easiest way for reading parquet database is simply by using pandas and pyarrow.
```Python
$ python
>>> import pandas as pd
>>> pd.read_parquet("./ingestion_data/parquet")
        key  partition  offset  ...        description  price                                           products
0  product0          0       0  ...               1234  99.98  [{'name': 'shoes', 'price': 49.99}, {'name': '...
1  product1          0       1  ...            2131345  99.98  [{'name': 'shoes', 'price': 49.99}, {'name': '...
2  product0          0       2  ...               5555  99.98  [{'name': 'shoes', 'price': 49.99}, {'name': '...
3  product1          0       3  ...                AAA  99.98  [{'name': 'shoes', 'price': 49.99}, {'name': '...
4  product0          0       4  ...  zażółć gęślą jaźń  99.98  [{'name': 'shoes', 'price': 49.99}, {'name': '...

[5 rows x 11 columns]
```

### Producing a message that is broken

Although, `confluent_producer.py` is implemented correctly, it doesn't interract with schema registry and doesn't send that information in first 6 bytes of the serialized message. That means, the message won't be deserialized properly by `ingestion` module. We can try it out to test dead letter queue.
```Bash
$ ./run_producer.sh avro
```
After producing a message, we can check that in the parquet database using pandas.
```Bash
>>> pd.read_parquet("./ingestion_data/parquet-dlq")
        key  partition  offset       topic               timestamp                                         avro_value  order_id customer description  price products
0  product0          0       5  dev.orders 2021-02-27 18:59:02.253  b'hn Doe\x1cmy description\x1f\x85\xebQ\xb8\xf...       NaN     None        None    NaN     None

```

## Development

### Rebuild the ingestion image

```Bash
docker build -f ./ingestion/Dockerfile .
```

### Schema registry queries

List all subjects.
```Bash
$ curl http://localhost:8081/subjects
```

Get latest schema for subject `dev.orders-value`.
```Bash
$ curl http://localhost:8081/subjects/dev.orders-value/versions/latest
```

## References

* [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html),
* [ABRiS - Avro Bridge for Spark](https://github.com/AbsaOSS/ABRiS),
* [Schema Evolution and Compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html).

