import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col

# you may add more import if you need to


# don't change this line
# hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
hdfs_nn ="172.31.29.168"

# note that we load the text file directly with a local path instead of providing an hdfs url
df = spark.read.option("header",True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/')
df.printSchema()

df_noNull = df.na.fill(value=0,subset=['Rating','Number of Reviews'])
df_casted = df_noNull.withColumn("Rating",col("Rating").cast(FloatType()))
df_filtered = df_casted.filter(col("Rating")>= 1.0)
df_filtered = df_filtered.filter(col("Number of Reviews") > 0)

#Even when Number of Reviews > 0 , can still be empty reviews
df_filtered = df_filtered.filter(col("Reviews") != '[ [  ], [  ] ]')
df_filtered.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question1/output.csv', header=True)
df_filtered.show()

