import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, regexp_replace, split, trim
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()

# Read the data from HDFS
hdfs_nn = "172.31.29.168"  # Replace with your actual HDFS NameNode IP
input_file_name = 'TA_restaurants_curated_cleaned.csv'
df = spark.read.option("header", True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/{input_file_name}')
df.printSchema()

# Preprocess the "Cuisine Style" column to clean it up for splitting
df = df.withColumn("Cuisine Style", regexp_replace(col("Cuisine Style"), "\[|\]", ""))  # Remove the square brackets
df = df.withColumn("Cuisine Style", regexp_replace(col("Cuisine Style"), "'", ""))  # Remove single quotes
df = df.withColumn("Cuisine Style", trim(col("Cuisine Style")))  # Trim whitespace

# Split the "Cuisine Style" into an array of cuisines
df = df.withColumn("Cuisines", split(col("Cuisine Style"), ",\s*").cast("array<string>"))

# Explode the array to have a row for each cuisine per restaurant
df = df.select(col("City"), explode_outer(col("Cuisines")).alias("Cuisine"))

# Clean up exploded cuisines
df = df.withColumn("Cuisine", trim(df.Cuisine))

# Group by City and Cuisine to count occurrences
df_city_cuisine_count = df.groupBy("City", "Cuisine").count()

# Show the results
df_city_cuisine_count.show(truncate=False)

# Write the result to a CSV file in HDFS
df_city_cuisine_count.write.option("header", "true").csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question4/output.csv')
