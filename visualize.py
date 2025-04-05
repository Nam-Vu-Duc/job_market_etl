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

def create_tables(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS source_report (
            id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            query_date date,
            source varchar(255),
            min_salary float,
            max_salary float,
            total_jobs integer
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS address_report (
            id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            query_date date,
            address varchar(255),
            min_salary float,
            max_salary float,
            total_jobs integer
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS exp_report (
            id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            query_date date,
            exp INTEGER,
            min_salary float,
            max_salary float,
            total_jobs integer
        )
        """
    )
    conn.commit()

def fetch_from_postgres(conn, cur):
    cur.execute("""
        select count(*) voters_count from voters
    """)
    voters_count = cur.fetchone()[0]

    cur.execute("""
            select count(*) candidates_count from candidates
        """)
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count

if __name__ == '__main__':
    conn = psycopg2.connect('host=localhost dbname=voting user=postgres password=postgres')
    cur = conn.cursor()

    create_tables(conn, cur)
    fetch_from_postgres(conn, cur)