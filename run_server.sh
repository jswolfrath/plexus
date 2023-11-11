#!/usr/bin/env bash

source configure.sh

# Cleanup existing tmp files
rm -rf /tmp/kafka-logs /tmp/zookeeper || true
rm -rf /media/join/*

echo "Starting ZooKeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 5

echo "Starting Kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties 
sleep 5
$KAFKA_HOME/bin/kafka-topics.sh --create --topic orchestrator --bootstrap-server localhost:9092

# Fire up the Orchestrator
echo "Starting the Orchestrator..."
make server

echo "Stopping Kafka."
$KAFKA_HOME/bin/./kafka-server-stop.sh

echo "Stopping ZooKeeper."
$KAFKA_HOME/bin/./zookeeper-server-stop.sh

echo "Done."

