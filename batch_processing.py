import psycopg2 
import pandas as pd
import os
import time
from kafka import KafkaProducer
from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import avg, count, col, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, DecimalType, ArrayType
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import schedule
import logging


# Suppress verbose logging from Spark NLP
logging.getLogger("com.johnsnowlabs").setLevel(logging.ERROR)
logging.getLogger("org.apache").setLevel(logging.ERROR)

DB_CONFIG = {
    "host": "localhost",
    "database": "voting",
    "user": "postgres",
    "password": "postgres"
}

TABLE_NAME = "sales"
LAST_PROCESSED_FILE = "last_processed.txt"

KAFKA_TOPIC = "sentiment_data"
KAFKA_SERVER = "localhost:9092"
MAIN_RECORDS = None

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))



# Initialize Spark with Spark NLP
spark = SparkSession.builder \
    .appName("SalesAnalyticsPipeline") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0") \
    .getOrCreate()


def connectDB():
    global MAIN_RECORDS
    # CDC take cares of updation, deletion and insertion
    last_processed = "1970-01-01 00:00:00"
    if os.path.exists(LAST_PROCESSED_FILE):
        with open(LAST_PROCESSED_FILE, "r") as file:
            last_processed = file.read().strip()


    print("this",last_processed)
    query = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE timestamp > '{last_processed}'
        ORDER BY timestamp ASC;
    """

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            MAIN_RECORDS = records
        df = pd.DataFrame(records, columns=columns)
        if not df.empty:
            last_processed = str(df["timestamp"].max())
            with open(LAST_PROCESSED_FILE, "w") as file:
                file.write(last_processed)
                print("this is the last processes data writing to a file", last_processed)
        return df
    finally:
        conn.close()

# Function to perform sentiment analysis using Spark NLP
def perform_sentiment_analysis(df):
    selected_columns = ["state", "branch", "product_name", "shopping_experience", "timestamp"]
    df = df[selected_columns]

    schema = StructType([
        StructField("state", StringType(), True),
        StructField("branch", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("shopping_experience", StringType(), True),
        StructField("timestamp", TimestampType(), True),
    ])

    spark_df = spark.createDataFrame(df, schema=schema)
    pipeline = PretrainedPipeline("analyze_sentiment", lang="en")
    
    # Apply Pretrained Sentiment Analysis Pipeline
    sentiment_df = pipeline.transform(spark_df.withColumnRenamed("shopping_experience", "text"))
    sentiment_df = sentiment_df.select(
    col("state"),
    col("branch"),
    col("product_name"),
    col("text"),
    explode(col("sentiment")).alias("sentiment_details")
    ).select(
    col("state"),
    col("branch"),
    col("product_name"),
    col("text"),
    col("sentiment_details.result").alias("sentiment_result"),
    )

    return sentiment_df

def push_to_kafka(spark_df):
    for row in spark_df.collect():
        message = row.asDict()  # Convert each row to a dictionary
        producer.send(KAFKA_TOPIC, value=message)
        print(f"Sent to Kafka: {message}")

# Every Monday batch processing 
def job():
    df = connectDB()
    spark_df = perform_sentiment_analysis(df)
    spark_df.show()

def pushToDB(spark_df):
    pandas_df = spark_df.toPandas()
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("These are the main records", MAIN_RECORDS)

    insert_query = """
    INSERT INTO sales_data (sales_id, state, branch, product_name, text, sentiment_result)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    try:
        count = 0
        for _, row in pandas_df.iterrows():
            # Prepare row data for insertion
            print("this is the count", count)
            print("Getting the ID", MAIN_RECORDS[count][0])
            delete_query = f"""
                DELETE FROM sales_data WHERE sales_id = {MAIN_RECORDS[count][0]};
            """
            cursor.execute(delete_query)
            cursor.execute(insert_query, (
                MAIN_RECORDS[count][0],
                row["state"],
                row["branch"],
                row["product_name"],
                row["text"],
                row["sentiment_result"]
            ))
            count += 1
        conn.commit()
        print(f"Inserted {len(pandas_df)} rows into the database.")
    except Exception as e:
        print("Error inserting data into database:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    


if __name__ == "__main__":
    # Every Monday batch processing 
    # schedule.every().monday.at("10:00").do(job)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)

    while True:
        df = connectDB()
        if not df.empty:
            spark_df = perform_sentiment_analysis(df)
            pushToDB(spark_df)
            spark_df.show()
            # push_to_kafka(spark_df)
        time.sleep(60)




