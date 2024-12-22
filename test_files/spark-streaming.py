from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, DecimalType
from pyspark.sql.functions import from_json, col, sum as _sum, lit, window, round



def createTopics(obj, partition_name, checkpoint_location):
        obj.withColumn("key", lit(partition_name)).selectExpr("key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("complete") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "aggregated_analysis_data") \
    .option("checkpointLocation", f"checkpoints/{checkpoint_location}") \
    .start()

def checkOuput(obj):
        print("This is the ouput ----------------------------------------------------------")
        obj.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start().awaitTermination()


if __name__ == "__main__":
    spark = (SparkSession.builder
         .appName("Real Time Sales Analysis")
         # connecting spark with kafka where spark uses sparksql to read and write data to the kafka
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
         # connecting spark with the postgresql
         .config("spark.jars", r"C:\Users\shaik\Downloads\Tech Consulting\Projects\RealTimeVotingSystem\postgresql-42.7.4.jar")
         .config('spark.sql.adaptive.enable', 'false')
         .getOrCreate())
    
    sale_schema = sales_schema = StructType([
            StructField('id', IntegerType(), False),  # Unique identifier for the sale (Primary Key)
            StructField('customer_id', IntegerType(), False),  # Unique identifier for the customer
            StructField('product_id', IntegerType(), False),  # Unique identifier for the product
            StructField('product_name', StringType(), False),  # Name of the product
            StructField('quantity', IntegerType(), False),  # Quantity of product purchased
            StructField('state', StringType(), False),  # State where the sale occurred
            StructField('city', StringType(), False),  # City where the sale occurred
            StructField('branch', StringType(), False),  # Branch location of the sale
            StructField('timestamp', TimestampType(), True),  # Full timestamp of the purchase
            StructField('date', DateType(), True),  # Purchase date
            StructField('time', StringType(), True),  # Purchase time (stored as time)
            StructField('month', IntegerType(), True),  # Month of the purchase
            StructField('year', IntegerType(), True),  # Year of the purchase
            StructField('shopping_experience', StringType(), False),  # Customer feedback
            StructField('payment_method', StringType(), True),  # Payment method (e.g., Credit, Cash)
            StructField('total_amount', DecimalType(10, 2), True),  # Total amount spent
])
    
    sales_df = (spark.readStream
                .format('kafka').
                option('kafka.bootstrap.servers', 'localhost:9092').
                option('subscribe', 'processed_sales_info').
                option('startingOffsets', 'latest').
                option('failOnDataLoss', 'false').
                load().
                selectExpr("CAST(value AS STRING)").
                select(from_json(col('value'), sale_schema).alias('data'))
                .select('data.*')
                )
    
    # print("This is the sales_df", sales_df)
    checkOuput(sales_df)
    enriched_sales_df = sales_df \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .withColumn("total_amount", col("total_amount").cast(DecimalType()))

    # .withColumn("quantity", col("quantity").cast(IntegerType())) \

    enriched_sales_df = enriched_sales_df.withWatermark("timestamp", "1 minute") 

    # Analysis 1: Total Sales Per Location
    total_sales_per_location = enriched_sales_df.groupBy("state", "city", "branch").agg(
    _sum("customer_id").alias("total_quantity"),
    _sum("total_amount").alias("total_sales")
    )

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
    checkOuput(top_selling_products)

    

    
    


