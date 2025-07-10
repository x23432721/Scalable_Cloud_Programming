#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from textblob import TextBlob

# 1. Define JSON schema
schema = StructType([
    StructField("review_id",   StringType(),  nullable=False),
    StructField("review_text", StringType(),  nullable=True),
    StructField("label",       IntegerType(), nullable=True),
    StructField("timestamp",   TimestampType(),nullable=True)
])

# 2. Sentiment UDF
def analyze(text):
    blob = TextBlob(text or "")
    pol = blob.sentiment.polarity
    if pol > 0.1:
        return "positive"
    elif pol < -0.1:
        return "negative"
    else:
        return "neutral"
sent_udf = udf(analyze, StringType())

# 3. Spark session
spark = SparkSession.builder.appName("AmazonReviewStream").getOrCreate()

# 4. Read from Kinesis
kinesis_df = spark.readStream \
    .format("kinesis") \
    .option("streamName",       "amazon-reviews") \
    .option("endpointUrl",      "https://kinesis.eu-west-1.amazonaws.com") \
    .option("region",           "eu-west-1") \
    .option("startingposition", "LATEST") \
    .load()

# 5. Parse JSON
json_df = kinesis_df.selectExpr("CAST(data AS STRING) as json_str")
reviews = json_df.select(from_json(col("json_str"), schema).alias("r")).select("r.*")

# 6. Apply UDF
scored = reviews.withColumn("sentiment", sent_udf(col("review_text")))

# 6.1 Add timestamp for windowing
scoredWithWM = scored.withWatermark("timestamp", "2 minutes")
# 7. Windowed aggregation
    # windowed_counts = scored.groupBy(
    #     window(col("timestamp"), "1 minute", "30 seconds"),
    #     col("sentiment")
    # ).count()

windowed_counts = scoredWithWM.groupBy(
     window(col("timestamp"), "1 minute", "30 seconds"),
     col("sentiment")
 ).count()

# 8. Write to S3 as Parquet
query = windowed_counts.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path",             "s3://my-amazon-reviews-bucket1/processed/streaming/") \
    .option("checkpointLocation","s3://my-amazon-reviews-bucket1/processed/streaming/_checkpoints/") \
    .start()

query.awaitTermination()
