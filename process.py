import psycopg2
import pandas as pd
import simplejson as json
from confluent_kafka import Consumer, SerializingProducer, KafkaError
from scrape import delivery_report
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
from pyspark.sql.functions import lit, col, when, regexp_extract, regexp_replace, split

def store_data(conn, cur, data):
    cur.execute(
        """
        INSERT INTO jobs.jobs(source, position, company, salary, address, exp, query_day) VALUES(%s, %s, %s, %f, %s, %f, %s)
        """,
        (data['source'], data['position'], data['company'], data['salary'], data['address'], data['exp'], datetime.today().strftime('%Y-%m-%d'))
    )
    conn.commit()
    return

if __name__ == '__main__':
    try:
        # conn = psycopg2.connect(
        #     dbname="jobs",
        #     user="postgres",
        #     password="root",
        #     host="localhost",
        #     port="5432"
        # )
        # cur = conn.cursor()

        conf = {
            'bootstrap.servers': 'localhost:9092',
        }

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

        # Initialize SparkSession
        # spark = (SparkSession.builder
        #     .appName("JobMarket")
        #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Spark-Kafka integration
        #     .config("spark.driver.extraClassPath", "C:/Users/admin/PycharmProjects/PythonProject/postgresql-42.7.5.jar")  # PostgresSQL driver
        #     .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
        #     .getOrCreate()
        # )

        # Read data from Kafka 'votes_topic' and process it
        # jobs_df = (spark.readStream
        #     .format("kafka")
        #     .option("kafka.bootstrap.servers", "localhost:9092")
        #     .option("subscribe", "jobs-topic")
        #     .option("startingOffsets", "earliest")
        #     .load()
        #     .selectExpr("CAST(value AS STRING)")
        #     .select(from_json(col("value"), jobs_schema).alias("data"))
        #     .select("data.*")
        # )

        # # Aggregate votes per candidate and turnout by location
        # total_jobs_each_source = enriched_jobs_df.groupBy("source").agg(count("source").alias("total_jobs_each_source"))
        # total_jobs_each_address = enriched_jobs_df.groupBy("address").agg(count("address").alias("total_jobs_each_address"))
        #
        # # Write aggregated data to Kafka topics ('aggregated_votes_per_candidate', 'aggregated_turnout_by_location')
        # total_jobs_to_kafka = total_jobs_each_source.selectExpr("to_json(struct(*)) AS value") \
        #     .writeStream \
        #     .format("kafka") \
        #     .option("failOnDataLoss", "false") \
        #     .option("kafka.bootstrap.servers", "localhost:9092") \
        #     .option("topic", "aggregated_votes_per_candidate") \
        #     .option("checkpointLocation", "C:/Users/admin/PycharmProjects/PythonProject/checkpoints/checkpoint1") \
        #     .outputMode("update") \
        #     .start()
        #
        # # Await termination for the streaming queries
        # total_jobs_to_kafka.awaitTermination()

    except Exception as e:
        print(e)