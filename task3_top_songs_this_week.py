from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, to_timestamp, weekofyear, current_date

# Start Spark session
spark = SparkSession.builder.appName("Task3TopSongsThisWeek").getOrCreate()

# Load logs
logs = spark.read.option("header", True).csv("listening_logs.csv")
logs = logs.withColumn("timestamp", to_timestamp("timestamp"))

# Extract current week number
current_week_num = spark.sql("SELECT weekofyear(current_date()) AS current_week").collect()[0]["current_week"]

# Add week column and filter only current week
logs_with_week = logs.withColumn("week", weekofyear("timestamp"))
current_week_logs = logs_with_week.filter(col("week") == current_week_num)

# Group by song and count plays
top_songs = current_week_logs.groupBy("song_id") \
    .agg(count("*").alias("play_count")) \
    .orderBy(desc("play_count")) \
    .limit(10)

# Save result
top_songs.write.format("csv").mode("overwrite").save("output/top_songs_this_week/")

