import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, asc, lit
from pyspark.sql.window import Window
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

# note that we load the text file directly with a local path instead of providing an hdfs url
input_file_name = 'input/TA_restaurants_curated_cleaned.csv'
hdfs_nn ="172.31.29.168" #TODO: Replace with 
df = spark.read.option("header",True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/')
df.printSchema()

df_city_cuisine_count = df.groupBy(["City", "Cuisine"]).count()

df_city_cuisine_count.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question4/output.csv', header='true')