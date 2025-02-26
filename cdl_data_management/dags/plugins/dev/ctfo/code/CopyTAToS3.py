import boto3
import os
from pyspark.sql import SparkSession

def main():
# Define source and destination S3 buckets
    source_bucket = 's3://aws-a0220-use1-00-s-s3b-shrd-shr-sandboxdata01'
    source_prefix = 'clinical-data-lake/temp/TA_SCAN/'
# 's3://aws-a0220-use1-00-s-s3b-shrd-shr-sandboxdata01/clinical-data-lake/temp/TA_SCAN/'
    destination_bucket = 'aws-a0220-use1-00-d-s3b-shrd-cus-cdl01'
    destination_prefix = 'clinical-data-lake/temp/TA_SCAN_COPIED/'

    # Initialize a session using boto3
    s3 = boto3.client('s3')

    # List objects in the source bucket
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            source_key = obj['Key']
            destination_key = destination_prefix + source_key.split('/')[-1]

            # Copy the object
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)

        print(f"Copied all objects from s3://{source_bucket}/{source_prefix} to s3://{destination_bucket}/{destination_prefix}")
    else:
        print("No objects found in the source bucket.")

