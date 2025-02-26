import boto3
import datetime
import pandas as pd
from io import BytesIO

# Initialize S3 client
def main():
    s3_client = boto3.client("s3")
    today_date = datetime.datetime.now().strftime("%Y%m%d")  # Get today's date in YYYYMMDD format

    # Define source path with today's date
    source_path = f"s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/tdec_tascan_new_files/{today_date}/"

    # Extract bucket name and prefix from the source path
    source_bucket = source_path.split("/")[2]
    source_prefix = "/".join(source_path.split("/")[3:])

    # List all objects in the source location
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

    # Check if files exist in the source path
    if "Contents" not in response:
        print("No files found in the source path.")
        return

    # Process each file in the source bucket
    for obj in response["Contents"]:
        source_key = obj["Key"]  # Full path of the file in the source bucket

        # Skip folders
        if source_key.endswith("/"):
            continue

        try:
            # Extract the file name and remove extension
            file_name = source_key.split("/")[-1]
            file_name_without_ext = file_name.split(".")[0].lower()  # Convert to lowercase

            # Construct the destination key for Parquet
            destination_key = f"clinical-data-lake/pre-ingestion/TA_SCAN/tascan_{file_name_without_ext}/{file_name_without_ext}_{today_date}.parquet"

            # Read the CSV file from S3
            csv_obj = s3_client.get_object(Bucket=source_bucket, Key=source_key)
            csv_buffer = BytesIO(csv_obj["Body"].read())

            # Read CSV into DataFrame (allowing Pandas to infer data types)
            df = pd.read_csv(csv_buffer)

            # Log the first few rows (for debugging)
            # print(df.head())  # Remove or comment in production code

            # Convert the DataFrame to Parquet format
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False)

            # Upload the Parquet file to the destination bucket
            destination_bucket = "aws-a0220-use1-00-d-s3b-shrd-cus-cdl01"
            s3_client.put_object(
                Bucket=destination_bucket,
                Key=destination_key,
                Body=parquet_buffer.getvalue()
            )

            print(f"Converted and copied {source_key} to {destination_bucket}/{destination_key}")

        except Exception as e:
            print(f"Failed to process {source_key}. Error: {e}")

if __name__ == "__main__":
    main()