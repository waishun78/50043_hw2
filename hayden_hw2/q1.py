import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, array_contains

# you may add more import if you need to


# don't change this line
#hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
hdfs_nn ="localhost" 
df = spark.read.option("header",True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/')
df.printSchema()

df_noNull = df.na.fill(value=0,subset=['Rating','Number of Reviews'])
df_casted = df_noNull.withColumn("Rating",col("Rating").cast(FloatType()))
df_filtered = df_casted.filter(col("Rating")>= 1.0)
df_filtered = df_filtered.filter(col("Number of Reviews") > 0)
#Even when Number of Reviews > 0 , can still be empty reviews q2 sample output
df_filtered = df_filtered.filter(col("Reviews") != '[ [  ], [  ] ]')
df_filtered.write.mode("overwrite").csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question1/output.csv', header=True)
df_filtered.show()