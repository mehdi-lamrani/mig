from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, input_file_name, udf, expr
from pyspark.sql.types import StructType, StructField, StringType
import logging

# Initialize Spark session
spark = SparkSession.builder.config("spark.sql.adaptive.enabled", "false").getOrCreate()

# Define input and output directories
input_path = "/data/dummy/in"   # Replace with your input path
output_path = "/data/dummy/out"  # Replace with your output path
checkpoint_path= "/data/dummy/cp"
log_path = "/data/dummy/log"
# Set up logging
logging.basicConfig(filename=log_path+'/stream.log', level=logging.INFO, format='%(asctime)s - %(message)s')

schema = StructType([
    StructField("id", StringType(), True),
    StructField("value", StringType(), True)
])

# Set up logging
logging.basicConfig(filename=log_path+'/stream.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Define a UDF to extract the file name from the full path
get_file_name = udf(lambda path: path.split("/")[-1], StringType())

    # Read streaming CSV files with specified schema
df = spark.readStream \
    .schema(schema) \
    .option("header", "false") \
    .csv(input_path)

# Add timestamp in HHMMSS format and input file name
df_with_metadata = df.withColumn("timestamp", date_format(current_timestamp(), "HHmmss")) \
                     .withColumn("input_file", input_file_name())


# Write the results to the output directory in CSV format and log the processed file name
query = df_with_metadata.writeStream \
    .outputMode("append") \
    .option("header", "true") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query = df_with_metadata.writeStream \
    .outputMode("append") \
    .option("header", "false") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .foreachBatch(lambda df, batch_id: (
        df.write.csv(f"{output_path}/batch_{batch_id}", header=True, mode="append"),
        logging.info(f"Processed files: {', '.join(df.select('file_name').distinct().rdd.flatMap(lambda x: x).collect())}")
    )).start()
# Start the streaming query
query.awaitTermination()
