import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

# note that we load the text file directly with a local path instead of providing an hdfs url
input_file_name = 'input/TA_restaurants_curated_cleaned.csv'
hdfs_nn ="172.31.29.168" #TODO: Replace with 
df = spark.read.option("header",True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/')
df.printSchema()

df_price_not_null = df.filter(col("Price Range").isNotNull())

windowSpec = Window.partitionBy(["City", "Price Range"]).orderBy("Rating")
df_best_restaurant = df_price_not_null.withColumn("rank", row_number().over(windowSpec)).filter(col("rank")==1)
df_best_restaurant.show(truncate=False)


# df_filtered.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question1/output.csv', header='true')
# df_filtered.show(truncate=False)
