import logging
import datetime
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import io

# Initialize a Boto3 S3 client
s3_client = boto3.client('s3')


# Define the ingestion function
def call_ingestion():
    try:
        # Step 1: Get the current date in YYYYMMDD format
        current_date = datetime.datetime.now().strftime("%Y%m%d")
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        # Step 2: Define the S3 paths
        bucket_name = "aws-a0220-use1-00-d-s3b-shrd-cus-cdl01"
        bucket_path = "clinical-data-lake/temp/temp_ihme/Data Explorer - Data - Tuberculosis, HIV_AIDS, Diarrheal diseases, Lower respiratory infections, Upper respi...  2024-12-09 00-00-51.xlsx"
        output_path = f"clinical-data-lake/pre-ingestion/IHME/ihme/ihme_{current_date}.xlsx"

        # Configure logging
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger()

        # Step 3: Read the Excel file from the S3 bucket using Boto3
        response = s3_client.get_object(Bucket=bucket_name, Key=bucket_path)
        file_content = response['Body'].read()

        # Use BytesIO to read the Excel file into a pandas DataFrame
        pddf = pd.read_excel(io.BytesIO(file_content))

        # Step 5: Convert the pandas DataFrame to a Spark DataFrame
        df = spark.createDataFrame(pddf)
        df.registerTempTable('ihme')

        # Step 6: Perform SQL query on the Spark DataFrame
        final_df = spark.sql("""
            SELECT condition,
                   sex,
                   age,       
                   regexp_replace(location, ' County [A-Z]{2}$', '') AS location,
                   measure,
                   unit,
                   year,
                   race,
                   value,
                   lower,
                   upper,
                   data_suite
            FROM ihme a
        """)

        # Step 7: Convert the final Spark DataFrame to a Pandas DataFrame
        final_pd_df = final_df.toPandas()

        # Step 8: Write the Pandas DataFrame to an Excel file in memory
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            final_pd_df.to_excel(writer, sheet_name='Sheet1', index=False)

        # Step 9: Upload the Excel file to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_path,
            Body=excel_buffer.getvalue(),
        )

        logger.info(f"Excel file written to: s3://{bucket_name}/{output_path}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")


def main():
    call_ingestion()
