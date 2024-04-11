import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode_outer, regexp_replace, split, trim, count, sort_array, array
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
schema = ArrayType(StructType([StructField("cast_id", IntegerType()), 
                               StructField("character", StringType()), 
                               StructField("credit_id", StringType()),
                               StructField("gender", IntegerType()),
                               StructField("id", IntegerType()),
                               StructField("name", StringType()),
                               StructField("order", IntegerType())]))
df_character = df.select(col("movie_id"), col("title"), from_json(df.cast, schema).alias("cast_split"))
print("FROM JSON")
df_character.show(truncate=True)
df_character.printSchema()

df_character_indiv = df_character.select(col("movie_id"), col("title"), explode_outer(col("cast_split")).alias("actor"))
print("INDIV")
df_character_indiv.show(truncate=True)
df_character_indiv.printSchema()

df_name = df_character_indiv.select(col("movie_id"), col("title"), col("actor.name").alias("name"))
print("NAME")
df_name.show(truncate=True)
df_name2 = df_name

cond = [df_name.movie_id == df_name2.movie_id, df_name.title == df_name2.title, df_name.name!=df_name2.name]
df_actor_pairs = df_name.alias("a") \
    .join(df_name2.alias("b"), on="movie_id") \
    .select(col("a.movie_id").alias("movie_id"), col("a.title").alias("title"), col("a.name").alias("actorA"), col("b.name").alias("actorB"))
print("ACTOR PAIR")
df_actor_pairs.show(truncate=True)

df_with_sorted_actors = df_actor_pairs.withColumn("sorted_actors", sort_array(array("actorA", "actorB")))
print("SORTED ARRAY")
df_with_sorted_actors.show(truncate=True)

# Adding these sorted actors as separate columns
df_with_sorted_actors = df_with_sorted_actors.withColumn("actor1", df_with_sorted_actors["sorted_actors"][0]) \
                                             .withColumn("actor2", df_with_sorted_actors["sorted_actors"][1])
print("SORTED")
df_with_sorted_actors.show(truncate=True)
# Finding duplicates based on these sorted columns
df_duplicates_removed = df_with_sorted_actors.dropDuplicates(["actor1", "actor2"])
print("DUPLICATE_REMOVE")
df_duplicates_removed.show(truncate=True)

windowSpec = Window.partitionBy("actor1", "actor2")
# actor 1<->actor2
df_count = df_duplicates_removed.withColumn('count', count('movie_id').over(windowSpec)) \
                                .filter(col("count") >= 2) \
                                .filter(col("actor1").isNotNull()) \
                                .filter(col("actor2").isNotNull())
print("COUNT")
df_count.show(truncate=True)
df_out = df_count.drop("count")
print("OUT")
df_out.show(truncate=True)
