# Twitter_kafka_spark
### Steps to run
1. Run **Kafkaproducer.java** through IDE
2. Execute **mvn package** in root
4. Start **zookeeper** with **bin/zookeeper-server-start.sh config/zookeeper.properties**
5. Start **kafka** with **bin/kafka-server-start.sh config/server.properties**
6. Execute **spark-submit --class com.skip.kafka_spark.SparkApplication --master local[2] kafka_spark/target/kafka_spark-1.0-SNAPSHOT-jar-with-dependencies.jar**
