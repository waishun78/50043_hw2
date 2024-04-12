import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType
# you may add more import if you need to


# don't change this line
#hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

# note that we load the text file directly with a local path instead of providing an hdfs url
hdfs_nn ="localhost" 
df = spark.read.option("header",True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/')
df.printSchema()

df_nonNullPrice = df.na.drop(subset=["Price Range"])
df_casted = df_nonNullPrice.withColumn("Rating", df_nonNullPrice["Rating"].cast(FloatType()))

windowSpecAsc  = Window.partitionBy("City", "Price Range").orderBy(col("Rating").asc())
windowSpecAgg = Window.partitionBy("City", "Price Range")
worst_rest_df = df_casted.withColumn("rank",row_number().over(windowSpecAsc)) \
    .filter(col("rank") == 1).drop("rank")

windowSpecDesc  = Window.partitionBy("City", "Price Range").orderBy(col("Rating").desc())
best_rest_df = df_casted.withColumn("rank",row_number().over(windowSpecDesc)) \
    .filter(col("rank") == 1).drop("rank")

output_df = worst_rest_df.union(best_rest_df)  

output_df.show()
output_df.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question2/output.csv', header='true')

