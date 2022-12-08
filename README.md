# Benchmark Pubsub lib

- Tools
  - [] Kafka
  - [] Redpanda
  - [] Nats
  - [] Postgres
    
- Producer
  - Single producer, no replication
  - Single producer, 3x async replication
  - Single producer, 3x sync replication
  - Three producer, 3x async replication
- Consumer
  - Single consumer
  - Three consumer

- Go Libs
  - segmentio/kafka-go
  - confluentinc/confluent-kafka-go
  - Shopify/sarama

- Python Libs
  - confluentinc/confluent-kafka-python
  - dpkp/kafka-python

- JS Libs
  - tulios/kafkajs

- Rust 
  - fede1024/rust-rdkafka
