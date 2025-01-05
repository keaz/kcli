#!/bin/bash

docker run -d  -p 9092:9092 --name broker apache/kafka:latest

docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic topic-1 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic topic-one --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1;
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic topic-two --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1;
docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic topic-three --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1;
docker exec broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092