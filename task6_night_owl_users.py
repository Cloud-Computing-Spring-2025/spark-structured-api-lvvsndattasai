from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, hour

# Start Spark session
spark = SparkSession.builder.appName("Task6NightOwlUsers").getOrCreate()

# Load listening logs
logs = spark.read.option("header", True).csv("listening_logs.csv")

# Convert timestamp column to proper timestamp type
logs = logs.withColumn("timestamp", to_timestamp("timestamp"))

# Extract hour and filter between 0 and 5 (12 AM to 5 AM)
night_logs = logs.filter((hour("timestamp") >= 0) & (hour("timestamp") < 5))

# Get distinct night owl users
night_owl_users = night_logs.select("user_id").distinct()

# Save output
night_owl_users.write.format("csv").mode("overwrite").save("output/night_owl_users/")

