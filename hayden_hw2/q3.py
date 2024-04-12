import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,asc,desc,lit,col
# you may add more import if you need to


# don't change this line
#hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

# note that we load the text file directly with a local path instead of providing an hdfs url
hdfs_nn ="localhost" #TODO: Replace with 
df = spark.read.option("header",True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/')
df.printSchema()

avg_rating_df = df.groupBy("City").agg(avg("Rating").alias("AverageRating"))
sorted_avg_rating_df = avg_rating_df.orderBy(col("AverageRating").desc())
best_three = sorted_avg_rating_df.limit(3).withColumn("RatingGroup", lit("Top"))

sorted_avg_rating_df = avg_rating_df.orderBy(col("AverageRating").asc())
worst_three = sorted_avg_rating_df.limit(3).withColumn("RatingGroup", lit("Bottom"))

# Combine the two lists and sort them
output = best_three.union(worst_three).orderBy(col("AverageRating").desc())
output.show()
output.write.csv(f'hdfs://{hdfs_nn}:9000/assignment2/output/question3/output.csv', header='true')

