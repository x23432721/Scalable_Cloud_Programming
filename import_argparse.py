import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def tranform_data(data_source: str, output_uri: str) -> None:
    with SparkSession.buikder.appName("My First Application").getOrCreate() as spark:
        #Load CSV file
        df = spark.read.option("header","true").csv(data_source)


        # Rename columns
        df = df.select(
            col("Name").alias("name"),
            col("Violation Type").alias("violation_type"),
            
        )
            
        #createc an in memory DataFrame
        df.createOrReplaceTempView("restaurent_violations")



        #construct SQL query
        GROUP_BY_QUERY = """
            SELECT name, count(*) AS total_red_violations
            FROM restaurant_violations
            WHERE violation_type = "RED"
            GROUP BY name
        """

        # TRansform Data
        transformed_df = spark.sql(GROUP_BY_QUERY)

        #log in to EMR stout
        print(f"Number of rows in SQL query: {transformed_df.count()}")

        #write our results as parquet files
        transformed_df.write.mode("overwrite").parquet(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumrntParser()
    parser.add_argument('--data_source')
    parser.add_argument('--output_uri')
    args = parser.parse_args()

    transform_data(args.data_source, args.output_uri)



