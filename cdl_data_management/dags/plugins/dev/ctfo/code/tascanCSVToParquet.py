from pyspark.sql import SparkSession
import sys
import gc
from datetime import datetime
import boto3
import os


if len(sys.argv) != 2:
    print("Usage: spark-submit script.py <file_name>")
    sys.exit(1)

file_name = sys.argv[1]
spark = SparkSession.builder.appName("Delete DataFrame Example").getOrCreate()
file_name_l = file_name.lower()
file_name_u = file_name.upper()
file_path = f"s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/tdec_tascan_new_files/20250103/{file_name_u}.csv"

today_date = datetime.now().strftime('%Y%m%d')

df = spark.read.csv(
    file_path,
    header=True,
    quote='"',
    escape='\\',
    multiLine=True,
    inferSchema=True
)

df.coalesce(1).write.parquet(
    f's3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/temp/tascan_cleaned/{file_name_l}/',
    mode='overwrite'
)

# Initialize a session using Amazon S3
s3 = boto3.client('s3')

# Define the source and destination S3 paths
source_bucket = 'aws-a0220-use1-00-d-s3b-shrd-cus-cdl01'
source_prefix = f'clinical-data-lake/temp/tascan_cleaned/{file_name_l}/'
destination_prefix = f'clinical-data-lake/pre-ingestion/TA_SCAN/tascan_{file_name_l}/{file_name_l}_{today_date}.parquet'

# List objects in the source path (excluding the _SUCCESS file)
objects = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

# Filter out the _SUCCESS file
files_to_copy = [obj['Key'] for obj in objects.get('Contents', []) if '_SUCCESS' not in obj['Key']]

# Copy the filtered files to the destination
for file_key in files_to_copy:
    # Construct destination path without the part-xxxx structure
    destination_key = destination_prefix  # Directly use the destination path as the filename

    # Copy file from source to destination
    s3.copy_object(
        Bucket=source_bucket,
        CopySource={'Bucket': source_bucket, 'Key': file_key},
        Key=destination_key
    )
    print(f"Copied: {file_key} to {destination_key}")

# Remove the copied files from the source location
for file_key in files_to_copy:
    s3.delete_object(Bucket=source_bucket, Key=file_key)
    print(f"Deleted: {file_key}")

df.unpersist()
del df
gc.collect()
spark.stop()
