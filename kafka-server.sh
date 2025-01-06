#!/bin/bash

# docker run -d  -p 9092:9092 --name broker apache/kafka:latest
docker run -d  \
  -p 9092:9092 \
  --name broker \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_NUM_PARTITIONS=3 \
  apache/kafka:latest

# Wait for the broker to start
sleep 10

docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic topic-one --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3;
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic topic-two --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1;
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic topic-three --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1;
docker exec broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092