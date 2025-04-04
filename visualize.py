import psycopg2
import simplejson as json
from confluent_kafka import Consumer, SerializingProducer, KafkaError
from scrape import delivery_report
from pyspark.sql import SparkSession
from pyspark.sql.functions import min as _min, max as _max, from_json, col, count, col, to_json, struct
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

conf = {'bootstrap.servers': 'localhost:9092'}

consumer = Consumer(conf | {
    'group.id': 'jobs-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
consumer.subscribe(['jobs-topic'])

producer = SerializingProducer(conf)

while True:
    # Poll for a message (timeout in seconds)
    message = consumer.poll(timeout=1.0)  # 1-second timeout

    if message is None:  # No message received within the timeout
        continue

    if message.error():  # Check for Kafka errors
        print(f"Consumer error: {message.error()}")
        continue

    # Process the message
    print(f"Key: {message.key().decode('utf-8') if message.key() else 'None'}")
    print(f"Value: {message.value().decode('utf-8') if message.value() else 'None'}")
    print(f"Partition: {message.partition()}, Offset: {message.offset()}")
    print("---")

    # Manually commit the offset if auto.commit is disabled
    consumer.commit(message)