#!/bin/bash

set -xeuo pipefail

source ./bin/envs.sh

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

