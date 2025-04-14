from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, to_timestamp, hour, weekofyear, max as max_

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

logs = spark.read.option("header", "true").csv("listening_logs.csv")
songs = spark.read.option("header", "true").csv("songs_metadata.csv")

logs = logs.withColumn("duration_sec", col("duration_sec").cast("int")) \
           .withColumn("timestamp", to_timestamp("timestamp"))

# Join logs with metadata
enriched = logs.join(songs, on="song_id")

# Example: User's Favorite Genre
favorite_genre = enriched.groupBy("user_id", "genre") \
    .agg(count("*").alias("play_count")) \
    .withColumn("max_play", max_("play_count").over(Window.partitionBy("user_id"))) \
    .filter(col("play_count") == col("max_play")) \
    .drop("max_play")

favorite_genre.write.format("csv").mode("overwrite").save("output/user_favorite_genres/")

