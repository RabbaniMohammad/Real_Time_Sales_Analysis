from pyspark.sql import SparkSession
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql.functions import from_json, explode, col

spark = SparkSession.builder \
    .appName("SalesAnalyticsPipeline") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.0") \
    .getOrCreate()
df = spark.createDataFrame([
    (0, "I really enjoyed this book."),
    (1, "This movie was terrible.")
], ["id", "text"])

pipeline = PretrainedPipeline("analyze_sentiment", lang="en")
result = pipeline.transform(df)

# Extracting sentiment result
processed_df = result.select(
    col("id"),
    col("text"),
    explode(col("sentiment")).alias("sentiment_details")
).select(
    col("id"),
    col("text"),
    col("sentiment_details.result").alias("sentiment_result"),
)

# Show results
processed_df.show(truncate=False)