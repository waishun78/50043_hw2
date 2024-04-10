import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode_outer, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, IntegerType 
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

df_cuisine_split = df.withColumn("Cuisine Style",split(regexp_replace("Cuisine Style", "[)]",""), ","))
df_cuisine_split.show()
df_cuisine_select = df_cuisine_split.select(col("City"), col("Cuisine Style").alias("Cuisines"))
df_cuisine_split.show()
df_cuisine_explode = df_cuisine_split.select(df_cuisine_split.City,explode_outer(df_cuisine_split.Cuisines).alias("Cuisine")) 
df_cuisine_explode.show()
df_city_cuisine_count = df_cuisine_explode.groupBy(["City", "Cuisine"]).count()
df_city_cuisine_count.show()

df_city_cuisine_count.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question4/output.csv', header='true')