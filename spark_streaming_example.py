# spark_streaming_example.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StructuredStreamingExample").getOrCreate()

# Read from rate source at 10 rows/sec
df = spark.readStream.format("rate") \
    .option("rowsPerSecond", 10) \
    .load()

# Select timestamp and value
df2 = df.selectExpr("value", "timestamp")

query = df2.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

