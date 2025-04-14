from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# Start Spark session
spark = SparkSession.builder.appName("Task4HappyRecommendations").getOrCreate()

# Load logs and songs metadata
logs = spark.read.option("header", True).csv("listening_logs.csv")
songs = spark.read.option("header", True).csv("songs_metadata.csv")

# Join logs with song metadata
logs = logs.join(songs, "song_id")

# Filter sad plays
sad_plays = logs.filter(col("mood") == "Sad").groupBy("user_id").agg(count("*").alias("sad_count"))

# Total plays per user
total_plays = logs.groupBy("user_id").agg(count("*").alias("total_count"))

# Users who mostly listen to Sad songs
sad_dominant_users = sad_plays.join(total_plays, "user_id") \
    .withColumn("sad_ratio", col("sad_count") / col("total_count")) \
    .filter(col("sad_ratio") > 0.3) \
    .select("user_id")

# All happy songs
happy_songs = songs.filter(col("mood") == "Happy").select("song_id").distinct()

# Songs each user already listened to
user_played = logs.select("user_id", "song_id").distinct()

# Recommend happy songs not yet listened to
candidates = sad_dominant_users.crossJoin(happy_songs) \
    .join(user_played, ["user_id", "song_id"], how="left_anti") \
    .withColumn("id", monotonically_increasing_id())

# Limit to top 3 recommendations per user
window = Window.partitionBy("user_id").orderBy("id")
recommendations = candidates.withColumn("rank", row_number().over(window)) \
                            .filter(col("rank") <= 3) \
                            .select("user_id", "song_id")

# Save output
recommendations.write.format("csv").mode("overwrite").save("output/happy_recommendations/")

