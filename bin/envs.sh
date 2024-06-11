#!/bin/bash

export VERBOSE=True
export KAFKA_URL=localhost:9092
export SCHEMA_REGISTRY_URL=localhost:8081
export WORKSPACE_DIR=.
export DOCKER_DATALAKE_CONTAINER_VERSION="$(cat ./VERSION).$(date '+%Y%m%d%H%M%S')"
