from kafka import KafkaProducer

# Kafka producer configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'data_topic'

# Create Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Read data from S3 or any other source
data = ["data1", "data2", "data3"]

# Produce data to Kafka
for item in data:
    producer.send(topic_name, item.encode('utf-8'))

# Close Kafka producer
producer.close()
