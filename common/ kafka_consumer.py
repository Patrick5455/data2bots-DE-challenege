from kafka import KafkaConsumer

# Kafka consumer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'data_topic'

# Create Kafka consumer instance
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)

# Consume data from Kafka
for message in consumer:
    print(message.value.decode('utf-8'))

# Close Kafka consumer
consumer.close()
