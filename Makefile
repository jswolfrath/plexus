
ROOT_DIR=$(shell pwd)

SERVER_TARGET=ServerMain
CLIENT_TARGET=ClientMain

TARGET_JAR=$(ROOT_DIR)/target/scala-2.12/plexus_2.12-1.0.jar
JARS=$(KAFKA_HOME)/libs/kafka-clients-3.2.1.jar,$(ROOT_DIR)/lib/*

C_CONF=spark.ui.enabled=false --driver-memory 4g --master local[*] 
S_CONF=spark.ui.enabled=false --driver-memory 4g --master local[*]

CONFIG_FILE=$(ROOT_DIR)/config/query.conf

all:
	sbt package

client0:
	@spark-submit --class $(CLIENT_TARGET) --conf $(C_CONF) --jars $(JARS) $(TARGET_JAR) $(CONFIG_FILE) s0

client1:
	@spark-submit --class $(CLIENT_TARGET) --conf $(C_CONF) --jars $(JARS) $(TARGET_JAR) $(CONFIG_FILE) s1

client2:
	@spark-submit --class $(CLIENT_TARGET) --conf $(C_CONF) --jars $(JARS) $(TARGET_JAR) $(CONFIG_FILE) s2

server:
	@spark-submit --class $(SERVER_TARGET) --conf $(S_CONF) --jars $(JARS) $(TARGET_JAR) $(CONFIG_FILE)

