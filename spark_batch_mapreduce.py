# spark_batch_mapreduce.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("BatchSentimentMR").getOrCreate()

# 1. Read raw JSON from S3
df = spark.read.json("s3://my-amazon-reviews-bucket1/raw/*.json")

# 2. Word count
words = df.select(explode(split("review_text", " ")).alias("word"))
wc = words.groupBy("word").count()
wc.write.mode("overwrite").parquet("s3://my-amazon-reviews-bucket1/processed/batch/wordcount/")

# 3. Sentiment count
sent_counts = df.groupBy("label").count()
sent_counts.write.mode("overwrite").parquet("s3://my-amazon-reviews-bucket1/processed/batch/sentiment/")

spark.stop()

