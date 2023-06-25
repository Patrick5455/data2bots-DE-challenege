from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Initialize SparkContext and StreamingContext
sc = SparkContext(appName="KafkaStreamingApp")
ssc = StreamingContext(sc, 1)

# Kafka configuration
kafka_params = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kafka_streaming_group',
    'auto.offset.reset': 'latest'
}
topic_name = 'data_topic'

# Create a DStream that connects to Kafka
kafka_stream = KafkaUtils.createDirectStream(ssc, [topic_name], kafka_params)

# Process the Kafka stream
kafka_stream.foreachRDD(lambda rdd: process_kafka_stream(rdd))

# Function to process each RDD from the Kafka stream
def process_kafka_stream(rdd):
    if not rdd.isEmpty():
        # Process the RDD data (apply transformations, save to DB, etc.)
        processed_data = rdd.map(lambda x: x[1].decode('utf-8')).collect()
        for item in processed_data:
            # Perform necessary operations on each item
            print(item)

# Start the streaming context
ssc.start()
ssc.awaitTermination()
