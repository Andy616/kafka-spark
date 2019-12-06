#!/bin/bash

set -e

docker build -f dockerfile-spark-base -t spark-base:2.4.4 .
docker build -f dockerfile-spark-master -t spark-master:2.4.4 .
docker build -f dockerfile-spark-worker -t spark-worker:2.4.4 .
docker build -f dockerfile-spark-submit -t spark-submit:2.4.4 .
