from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV Stream Processing") \
    .getOrCreate()

# Define input and output directories
input_path = "path/to/in_files"   # Replace with your input path
output_path = "path/to/out_files"  # Replace with your output path

# Read streaming CSV files
df = spark.readStream \
    .option("header", "true") \
    .csv(input_path)

# Add timestamp in HHMMSS format
df_with_timestamp = df.withColumn("timestamp", date_format(current_timestamp(), "HHmmss"))

# Write the results to the output directory in CSV format
query = df_with_timestamp.writeStream \
    .outputMode("append") \
    .option("header", "true") \
    .csv(output_path)

# Start the streaming query
query.awaitTermination()
