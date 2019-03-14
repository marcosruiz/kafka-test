# Kafka test performance

El objetivo de este proyecto es medir el rendimiento del broker de mensajes Kafka a traves de un proyecto de Java.

## How to deploy

On $KAFKA_HOME write on terminal

´´´
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic demo
´´´

On $PROJECT_HOME write on terminal

´´´
mvn clean install
´´´
