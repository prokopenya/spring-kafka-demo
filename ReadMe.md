### Prerequisites
* [Download kafka] (https://kafka.apache.org/quickstart#quickstart_download)

### How to run
* bin/zookeeper-server-start.sh config/zookeeper.properties
* bin/kafka-server-start.sh config/server.properties
* in/kafka-topics.sh --create --bootstrap-server localhost:9093 --replication-factor 1 --partitions 1 --topic kafka-spring-test
* run class SpringKafkaDemoApplication