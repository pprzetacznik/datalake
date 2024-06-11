#!/bin/bash

set -xeuo pipefail

source ./bin/envs.sh

MODE=${1:-list_topics}
CMD="docker exec -it broker"

case $MODE in
  list_topics)
    $CMD kafka-topics --list --bootstrap-server $KAFKA_URL
    ;;
  list_messages)
    $CMD kafka-console-consumer --bootstrap-server $KAFKA_URL --topic dev.orders --from-beginning
    ;;
  *)
    $CMD $@
    ;;
esac

