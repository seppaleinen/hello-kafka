# hello-kafka

Hacked together between
https://github.com/ches/docker-kafka
https://github.com/code-not-found/spring-kafka/tree/master/spring-kafka-embedded-test



```bash
docker run --rm ches/kafka kafka-topics.sh --create \
    --topic test --replication-factor 1 --partitions 1 --zookeeper localhost:2181


docker run --rm --interactive ches/kafka kafka-console-producer.sh \
    --topic test --broker-list localhost:9092


docker run --rm ches/kafka kafka-console-consumer.sh \
    --topic test --from-beginning --bootstrap-server localhost:9092

```