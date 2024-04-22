#!/bin/bash

set -xe

source ./bin/envs.sh

curl http://$SCHEMA_REGISTRY_URL/subjects
