import psycopg2
import simplejson as json
from confluent_kafka import Consumer, SerializingProducer, KafkaError
from scrape import delivery_report
from pyspark.sql import SparkSession
from pyspark.sql.functions import min as _min, max as _max, from_json, col, count, col, to_json, struct
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

def store_data(conn, cur, data):
    cur.execute(
        """
        INSERT INTO jobs.jobs(total_jobs, min_salary, avg_salary, max_salary, query_day) VALUES(%s, %s, %s, %s, %s)
        """,
        (data['source'], data['position'], data['company'], data['salary'], data['address'])
    )
    conn.commit()
    return

if __name__ == '__main__':
    try:
        conn = psycopg2.connect(
            dbname="jobs",
            user="postgres",
            password="root",
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()

        # Initialize SparkSession
        spark = (SparkSession.builder
            .appName("JobMarket")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Spark-Kafka integration
            .config("spark.driver.extraClassPath", "C:/Users/admin/PycharmProjects/PythonProject/postgresql-42.7.5.jar")  # PostgresSQL driver
            .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
            .getOrCreate()
        )

        # Prevents schema mismatches
        job_schema = StructType([
            StructField("position"  , StringType() , True),
            StructField("company"   , StringType() , True),
            StructField("address"   , StringType() , True),
            StructField("source"    , StringType() , True),
            StructField("query_day" , DateType()   , True),
            StructField("min_salary", FloatType()  , True),
            StructField("max_salary", FloatType()  , True),
            StructField("experience", IntegerType(), True)
        ])

        # Read data from Kafka 'jobs_topics'
        jobs_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "jobs-topic")
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), job_schema).alias("data"))
            .select("data.*")
        )

        # Cal total_jobs
        total_jobs = jobs_df.groupBy().count()
        total_jobs_json = total_jobs.select(to_json(struct("count")).alias("value"))

        # Calc each source total jobs
        total_jobs_each_source = jobs_df.groupBy("source").agg(
            _min("min_salary").alias("min_salary"),
            _max("max_salary").alias("max_salary"),
            count("source").alias("total_jobs")
        )
        total_jobs_each_source_json = total_jobs_each_source.select(to_json(struct("*")).alias("value"))

        # Calc each address total jobs
        total_jobs_each_address = jobs_df.groupBy("address").agg(
            _min("min_salary").alias("min_salary"),
            _max("max_salary").alias("max_salary"),
            count("address").alias("total_jobs")
        )
        total_jobs_each_address_json = total_jobs_each_address.select(to_json(struct("*")).alias("value"))

        # Write aggregated data to Kafka topics
        total_jobs_to_kafka = (total_jobs_json
            .writeStream
            .format("kafka")
            .option("failOnDataLoss", "false")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "total_jobs")
            .option("checkpointLocation", "C:/Users/admin/PycharmProjects/PythonProject/checkpoints/checkpoint1")
            .outputMode("update")
            .start()
        )

        # Write aggregated data to Kafka topics
        total_jobs_each_source_to_kafka = (total_jobs_each_source_json
            .writeStream
            .format("kafka")
            .option("failOnDataLoss", "false")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "total_jobs_each_source")
            .option("checkpointLocation", "C:/Users/admin/PycharmProjects/PythonProject/checkpoints/checkpoint2")
            .outputMode("update")
            .start()
        )

        # Write aggregated data to Kafka topics
        total_jobs_each_address_to_kafka = (total_jobs_each_address_json
            .writeStream
            .format("kafka")
            .option("failOnDataLoss", "false")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "total_jobs_each_source")
            .option("checkpointLocation", "C:/Users/admin/PycharmProjects/PythonProject/checkpoints/checkpoint3")
            .outputMode("update")
            .start()
        )

        # Await termination for the streaming queries
        total_jobs_to_kafka.awaitTermination()
        total_jobs_each_source_to_kafka.awaitTermination()
        total_jobs_each_address_to_kafka.awaitTermination()

    except Exception as e:
        print(e)