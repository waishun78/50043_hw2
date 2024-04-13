import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, regexp_replace, split, trim
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()

# Read the data from HDFS
hdfs_nn = "172.31.29.168"  # Replace with your actual HDFS NameNode IP
input_file_name = 'TA_restaurants_curated_cleaned.csv'
df = spark.read.option("header", True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/{input_file_name}')
df.printSchema()

# Preprocess the "Cuisine Style" column to clean it up for splitting
#Convert Cuisine Style to Array to be exploded into a new row for each element in the array
explode_df = df.withColumn("Cuisine", explode(split("Cuisine Style", ", ")))
#Clean up Cuisine Column
cleaned_df = explode_df.withColumn("Cuisine", trim(regexp_replace("Cuisine", "\\'|\\]|\\[", "")))
output_df = cleaned_df.groupBy("City", "Cuisine").count()

output_df.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question4/', header='true')
