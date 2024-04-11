import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode_outer, regexp_replace, split, trim, count, sort_array, array, desc
from pyspark.sql.types import *
from pyspark.sql.window import Window


# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

hdfs_nn = "172.31.29.168"  # Replace with your actual HDFS NameNode IP
input_file_name = 'tmdb_5000_credits.parquet'

df = spark.read.option("header", True).parquet(f'hdfs://{hdfs_nn}:9000/assignment2/part2/input/{input_file_name}')
df.printSchema()

# Preprocess the "Cuisine Style" column to clean it up for splitting
cast_schema = ArrayType(StructType([StructField("cast_id", IntegerType()), 
                               StructField("character", StringType()), 
                               StructField("credit_id", StringType()),
                               StructField("gender", IntegerType()),
                               StructField("id", IntegerType()),
                               StructField("name", StringType()),
                               StructField("order", IntegerType())]))
# Parse the JSON column and expand into structured format
df_character = df.select(
    col("movie_id"),
    col("title"),
    from_json(col("cast"), cast_schema).alias("cast_split")
)

df_character_indiv = df_character.select(
    col("movie_id"),
    col("title"),
    explode_outer(col("cast_split")).alias("actor")
)

df_name = df_character_indiv.select(
    col("movie_id"),
    col("title"),
    col("actor.name").alias("name")
)

# Create pairs of actors for the same movie, ensuring no pair of the same actor
df_actor_pairs = df_name.alias("a") \
    .join(df_name.alias("b"), "movie_id") \
    .select(
        col("a.movie_id").alias("movie_id"),
        col("a.title").alias("title"),
        col("a.name").alias("actorA"),
        col("b.name").alias("actorB")
    ) \
    .filter(col("actorA") != col("actorB"))

# Sort actor names in each row for consistent ordering and deduplication
df_with_sorted_actors = df_actor_pairs.withColumn(
    "sorted_actors", sort_array(array("actorA", "actorB"))
).select(
    "movie_id", "title", "sorted_actors"
).withColumn(
    "actor1", col("sorted_actors")[0]
).withColumn(
    "actor2", col("sorted_actors")[1]
)

# Remove duplicate pairs and ensure both actor names are non-null
df_duplicates_removed = df_with_sorted_actors.dropDuplicates(["actor1", "actor2"]) \
    .filter(col("actor1").isNotNull() & col("actor2").isNotNull())

# Count how many times each pair appears across different movies
windowSpec = Window.partitionBy("actor1", "actor2")
df_count = df_duplicates_removed.withColumn('count', count('movie_id').over(windowSpec)) \
    .orderBy(desc("count"), "actor1", "actor2")

# Filter pairs that appear together in at least two movies
df_count_filter = df_count.filter(col("count") >= 2)

# Select and show final output
df_out = df_count_filter.select("movie_id", "title", "actor1", "actor2")
df_out.show(truncate=True)
