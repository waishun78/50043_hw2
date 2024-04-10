import sys
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW
sc = SparkContext(conf=spark)
spark = SparkSession(sc)

# note that we load the text file directly with a local path instead of providing an hdfs url
input_file_name = 'input/TA_restaurants_curated_cleaned.csv'
hdfs_nn = '54.221.65.222' #TODO: Replace with 
df = spark.read.option("header",True).csv(f'hdfs://{hdfs_nn}:9000/assignment2/part1/input/')
df.printSchema()

df_has_ratings = df.filter(df.Reviews.isNotNull())
df_ratings_int = df_has_ratings.withColumn('Rating_int', df['Rating'].cast(IntegerType()))
df_filtered = df_ratings_int.filter(df_ratings_int.Rating_int>=1).drop(['Rating_int'])

output_folder = './assignment2/output/question1'
df.toPandas().to_csv('hdfs://{hdfs_nn}:9000/assignment2/output/question1/output.csv')

sc.stop()
