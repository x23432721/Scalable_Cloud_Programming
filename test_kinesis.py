# test_kinesis.py
# ----------------
# Verify the Structured-Streaming Kinesis connector by printing schema + a few records.

from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("TestKinesis").getOrCreate()

    kdf = spark.readStream \
        .format("kinesis") \
        .option("streamName",       "amazon-reviews") \
        .option("endpointUrl",      "https://kinesis.eu-west-1.amazonaws.com") \
        .option("region",           "eu-west-1") \
        .option("startingposition", "LATEST") \
        .load()

    kdf.printSchema()

    query = kdf.writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()

    # run for 30s then exit
    query.awaitTermination(30)
    spark.stop()

if __name__ == "__main__":
    main()

