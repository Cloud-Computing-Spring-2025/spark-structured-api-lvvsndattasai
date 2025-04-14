from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as max_
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("Task5GenreLoyalty").getOrCreate()

# Load logs and songs metadata
logs = spark.read.option("header", True).csv("listening_logs.csv")
songs = spark.read.option("header", True).csv("songs_metadata.csv")

# Join logs with metadata to get genres
logs = logs.join(songs, "song_id")

# Count plays per user per genre
user_genre_counts = logs.groupBy("user_id", "genre") \
    .agg(count("*").alias("genre_count"))

# Total plays per user
user_total_counts = logs.groupBy("user_id") \
    .agg(count("*").alias("total_count"))

# Join counts and calculate loyalty score
loyalty = user_genre_counts.join(user_total_counts, "user_id") \
    .withColumn("loyalty_score", col("genre_count") / col("total_count"))

# For each user, find their max genre loyalty score
window = Window.partitionBy("user_id")
loyalty_filtered = loyalty.withColumn("max_score", max_("loyalty_score").over(window)) \
    .filter(col("loyalty_score") == col("max_score")) \
    .filter(col("loyalty_score") > 0.2) \
    .select("user_id", "genre", "loyalty_score")

# Save output
loyalty_filtered.write.format("csv").mode("overwrite").save("output/genre_loyalty_scores/")

