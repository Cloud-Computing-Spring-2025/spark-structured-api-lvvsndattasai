from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("Task2AvgListenTime").getOrCreate()

logs = spark.read.option("header", True).csv("listening_logs.csv")
logs = logs.withColumn("duration_sec", col("duration_sec").cast("int"))

avg_duration = logs.groupBy("song_id").agg(avg("duration_sec").alias("avg_listen_time"))

avg_duration.write.format("csv").mode("overwrite").save("output/avg_listen_time_per_song/")

