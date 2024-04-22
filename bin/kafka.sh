#!/bin/bash

set -xeuo pipefail

source ./bin/envs.sh

MODE=${1:-list_topics}
CMD="docker exec -it broker"

case $MODE in
  list_topics)
    $CMD kafka-topics --list --bootstrap-server $KAFKA_URL
    ;;
  *)
    $CMD $@
    ;;
esac

