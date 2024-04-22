#!/bin/bash

set -xeuo pipefail

MODE=${1:-up}

case $MODE in
  up)
    docker-compose up -d
    ;;
  up2)
    docker-compose up
    ;;
  down)
    docker-compose down
    ;;
  rebuild)
    docker-compose build
    ;;
  ingestion)
    docker exec -it ingestion /bin/bash
    ;;
  *)
    docker-compose restart
    ;;
esac

