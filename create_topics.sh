#!/bin/bash
docker-compose --project-directory . -f docker-compose/kafka.yml exec kafka kafka-topics.sh --create --replication-factor 1 --partitions 1 --zookeeper zookeeper:2181 --config 'max.message.bytes=10000000' --topic "$1"

