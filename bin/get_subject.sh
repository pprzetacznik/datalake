#!/bin/bash

source ./bin/envs.sh

curl http://$SCHEMA_REGISTRY_URL/subjects/dev.orders-value/versions/latest
