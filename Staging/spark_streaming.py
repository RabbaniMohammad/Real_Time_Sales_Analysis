# fetch data from the processed_sales_info and do the aggregation and push back to the aggregated_analysis_data topic
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, DecimalType, ArrayType
from pyspark.sql.functions import from_json, col, sum as _sum, lit, explode, collect_list, struct, window, round


def createTopics(obj, partition_name, checkpoint_location):
        obj.withColumn("key", lit(partition_name)).selectExpr("key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "aggregated_analysis_data") \
    .option("checkpointLocation", f"checkpoints/{checkpoint_location}") \
    .start()


if __name__ == "__main__":
    # Initialize SparkSession
    spark = (SparkSession.builder
             .appName("Real Time Sales Analysis")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
             .config("spark.jars", r"C:\Users\shaik\Downloads\Tech Consulting\Projects\RealTimeVotingSystem\postgresql-42.7.4.jar")
             .config('spark.sql.adaptive.enable', 'false')
             .getOrCreate())

    spark.sparkContext.setLogLevel("ERROR")

    # Define the schema for the JSON array elements
    sale_schema = StructType([
        StructField('customer_id', IntegerType(), True),
        # StructField('product_id', IntegerType(), True),
        StructField('product_name', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('state', StringType(), True),
        StructField('city', StringType(), True),
        StructField('branch', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('date', DateType(), True),
        StructField('time', StringType(), True),
        StructField('month', IntegerType(), True),
        StructField('year', IntegerType(), True),
        StructField('shopping_experience', StringType(), True),
        StructField('payment_method', StringType(), True),
        StructField('total_amount', DecimalType(10, 2), True),
    ])

    # Read data from Kafka topic
    sales_df = (
        spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'localhost:9092')
            .option('subscribe', 'processed_sales_info')
            .option('startingOffsets', 'earliest')
            .option('failOnDataLoss', 'false')
            .load()
    )

    # Extract JSON array as a string and parse
    raw_value_df = sales_df.selectExpr("CAST(value AS STRING) as json_array")

    parsed_df = (
        raw_value_df
        .select(explode(from_json(col("json_array"), ArrayType(sale_schema))).alias("data"))
        .select("data.*")  # Flatten the struct fields
    )
    
    enriched_sales_df = parsed_df \
        .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
        .withColumn("total_amount", col("total_amount").cast(DecimalType(10, 2))) \
        .withWatermark("timestamp", "1 minute")

    # Extra code added From Here
    enriched_sales_df = enriched_sales_df.dropna()
    # To Here
    
    # Aggregations
    total_sales_per_location = enriched_sales_df.groupBy("state", "city", "branch").agg(
        _sum("quantity").alias("total_quantity"),
        _sum("total_amount").alias("total_sales")
    )


    # Write aggregated data to Kafka
    createTopics(total_sales_per_location, "total_sales_per_location", "checkpoint_partition_1")

    # Analysis 2: Top Products by Revenue
    top_products_by_revenue = enriched_sales_df.groupBy("product_name").agg(
        _sum("total_amount").alias("total_revenue")
        ).orderBy(col("total_revenue").desc())

    createTopics(top_products_by_revenue, "top_products_by_revenue", "checkpoint_partition_2")

    # Analysis 3:Aggregates sales over 1-hour time windows.
    customer_purchase_trends = enriched_sales_df.groupBy(
        window("timestamp", "1 hour")
        ).agg(
    _sum("total_amount").alias("total_revenue"),
    _sum("quantity").alias("total_quantity")
    )

    createTopics(customer_purchase_trends, "customer_purchase_trends", "checkpoint_partition_3")

    # Analysis 4: Analyzes revenue by payment method.
    payment_method_distribution = enriched_sales_df.groupBy("payment_method").agg(
    _sum("total_amount").alias("total_revenue")
    )

    createTopics(payment_method_distribution, "payment_method_distribution", "checkpoint_partition_4")

    # Analysis 5: Average Customer Spending Per Location
    avg_customer_spending = enriched_sales_df.groupBy("state", "city", "branch").agg(
    round((_sum("total_amount") / _sum("quantity")), 2).alias("avg_spending_per_item")
    )

    createTopics(avg_customer_spending, "avg_customer_spending", "checkpoint_partition_5")

    # Analysis 6: the top-selling products per location (state, city, branch)
    top_selling_products = enriched_sales_df.groupBy("state", "city", "branch", "product_name").agg(
    _sum("quantity").alias("total_sold")
    ).orderBy(col("total_sold").desc())

    createTopics(top_selling_products, "top_selling_products", "checkpoint_partition_6")

    # Write aggregated data to console for debugging
    (customer_purchase_trends.writeStream 
        .outputMode("complete") 
        .format("console") 
        .start() 
        .awaitTermination())

