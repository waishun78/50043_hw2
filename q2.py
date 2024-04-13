import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dense_rank, desc
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

best_window_spec = Window.partitionBy(["City", "Price Range"]).orderBy(desc("Rating"))
df_best_restaurant = df_price_not_null.withColumn("rank", dense_rank().over(best_window_spec)).filter(col("rank")==1)
#df_best_restaurant.show(truncate=True)
worst_window_spec = Window.partitionBy(["City", "Price Range"]).orderBy("Rating")
df_worst_restaurant = df_price_not_null.withColumn("rank", dense_rank().over(worst_window_spec)).filter(col("rank")==1)
#df_worst_restaurant.show(truncate=True)
#df_worst_restaurant.printSchema()
#df_best_restaurant.printSchema()

df_out = df_worst_restaurant.union(df_best_restaurant)
df_out.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question2/output.csv', header='true')
df_out.show(truncate=False)

