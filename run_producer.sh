#!/bin/bash

set -xeuo pipefail

export VERBOSE=True
export KAFKA_URL=localhost:9092
export KAFKA_TOPIC=dev.orders
export AVRO_SCHEMA_FILE=./orders.avsc
export SCHEMA_REGISTRY_URL=localhost:8081


MODE=${1:-confluent}

case $MODE in
  avro)
    python avro_producer.py
    ;;
  confluent)
    python confluent_producer.py
    ;;
  *)
    python confluent_producer.py
    ;;
esac



