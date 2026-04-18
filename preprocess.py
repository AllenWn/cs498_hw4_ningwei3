from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("HW4Preprocess").getOrCreate()

# Step 1: Read cleaned CSV
df = spark.read.csv("data/taxi_trips_clean.csv", header=True, inferSchema=True)

# Step 2: Add fare_per_minute
df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))

# Step 3: Register temp view
df.createOrReplaceTempView("trips")

# Step 4: Compute company summary using Spark SQL
summary_df = spark.sql("""
SELECT
    company,
    COUNT(*) AS trip_count,
    ROUND(AVG(fare), 2) AS avg_fare,
    ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute
FROM trips
GROUP BY company
ORDER BY trip_count DESC
""")

# Step 5: Save output as JSON
summary_df.write.mode("overwrite").json("processed_data")

# Optional: print to terminal for checking
summary_df.show(truncate=False)

spark.stop()