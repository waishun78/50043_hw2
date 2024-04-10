import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, regexp_replace, split
from pyspark.sql.types import ArrayType, StringType


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
# Preprocess the "Cuisine Style" column to make it valid JSON
df = df.withColumn("Cuisine Style", regexp_replace(col("Cuisine Style"), "^\[ |\]$", ""))  # Trim leading/trailing brackets
df.show()
df = df.withColumn("Cuisine Style", regexp_replace(col("Cuisine Style"), "'", '"'))  # Replace single quotes with double quotes

df.show()
# Convert the JSON string in the DataFrame to a structured column
#df_cuisine_split = df.withColumn("Cuisine Style", from_json(col("Cuisine Style"), ArrayType(StringType())))
#df_cuisine_split.show()

# Select the relevant columns, renaming as necessary
df_cuisine_select = df.select(col("City"), col("Cuisine Style").alias("Cuisines"))
df_cuisine_select.show()
df_cuisine_split = df_cuisine_select.select(col("City"), split(col("Cuisines"), ','))
df_cuisine_split.show()
# Explode the cuisines into separate rows
df_cuisine_explode = df_cuisine_split.select(col("City"), explode_outer(col("Cuisines")).alias("Cuisine"))
df_cuisine_explode.show()

# Group by City and Cuisine to count occurrences
df_city_cuisine_count = df_cuisine_explode.groupBy("City", "Cuisine").count()
df_city_cuisine_count.show()

df_city_cuisine_count.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question4/output.csv', header='true')