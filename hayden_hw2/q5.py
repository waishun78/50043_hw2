import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# you may add more import if you need to

# don't change this line
#hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

hdfs_nn = "localhost"  # Replace with your actual HDFS NameNode IP

df = spark.read.option("header", True).parquet(f'hdfs://{hdfs_nn}:9000/assignment2/part2/input/')
df.printSchema()

cast_schema = ArrayType(
    elementType = StructType([
        StructField('cast_id', StringType()),
        StructField('character', StringType()),
        StructField('credit_id', StringType()),
        StructField('gender', StringType()),
        StructField('id', StringType()),
        StructField('name', StringType()),
        StructField('order', StringType()),
    ])
)

#select the three relevant columns and explode the cast column 
cast_df = df.select("movie_id", "title", "cast").withColumn("cast", from_json("cast", cast_schema)).withColumn("cast", explode("cast"))
cast_df.show(truncate=False,n=5)
#Group all the cast by the movie_id and title into a list of cast
cast_list = cast_df.select("movie_id", "title", "cast.name").groupBy("movie_id", "title").agg(collect_list('name').alias('cast_list')) 

#cast_list.show(n=15)

# Generate list of pairs of actors for each movie
def generate_pairs(cast_list):
    # Create all combinations of pairs in the cast list
    pairs = []
    n = len(cast_list)
    for i in range(n):
        for j in range(i + 1, n):
            pairs.append(tuple(sorted([cast_list[i], cast_list[j]])))  # Sort to avoid duplicates like (Actor1, Actor2) and (Actor2, Actor1)
    return pairs

# Define UDF to apply the generate_pairs function on cast_list
generate_pairs_udf = udf(generate_pairs, ArrayType(ArrayType(StringType())))

# Apply UDF to create a new column 'pairs' with all actor pairs for each movie
cast_pairs_list = cast_list.withColumn("pairs", generate_pairs_udf("cast_list"))

# Explode the pairs column to get individual rows for each pair per movie
exploded_pairs = cast_pairs_list.select("movie_id", "title", explode("pairs").alias("pair"))
exploded_pairs = exploded_pairs.select("movie_id", "title", col("pair").getItem(0).alias("actor1"), col("pair").getItem(1).alias("actor2"))

# Group by actor pairs then count the occurrences
pair_counts = exploded_pairs.groupBy("actor1", "actor2").agg(count("*").alias("count"))
# filter by those pair that appears in at least 2 movies tgt
filtered_pairs = pair_counts.filter(col("count") >= 2)

# Join back to the expanded_pairs to get movie_ids and titles
output = exploded_pairs.join(filtered_pairs, ["actor1", "actor2"]).select("movie_id", "title", "actor1", "actor2")

# Remove duplicates to ensure each movie pair is listed once
output = output.dropDuplicates()
#print(f"Result Count: {output.count()}")
#output.show(truncate=False,n=10)
output.write.option("header", True).mode("overwrite").parquet("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn))