#!/bin/bash

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic products \
  --partitions 3 \
  --replication-factor 1
