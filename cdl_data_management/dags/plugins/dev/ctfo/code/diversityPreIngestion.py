import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import json
import os
import logging
import CommonConstants as CommonConstants

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Load configuration from JSON file

def move_s3_files(s3_client, source_bucket, source_prefix, archive_location):
    """
    Moves all files from the source S3 folder to the destination S3 folder.
    Parameters:
    - s3_client: The Boto3 S3 client.
    - source_bucket: The S3 bucket name for the source files.
    - source_prefix: The source folder path in the S3 bucket.
    - destination_prefix: The destination folder path in the S3 bucket.
    """
    # List all objects in the source folder
    date = datetime.now().strftime('%Y%m%d')
    destination_prefix = archive_location.replace('date', date)
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            source_key = obj['Key']

            # Skip if the key is a folder (ends with '/')
            if not source_key.endswith('/'):
                # Define the destination key (new path for the file)
                destination_key = source_key.replace(source_prefix, destination_prefix, 1)

                # Copy the file to the new location
                s3_client.copy_object(Bucket=source_bucket,
                                      CopySource={'Bucket': source_bucket, 'Key': source_key},
                                      Key=destination_key)

                # Delete the original file from the source folder
                s3_client.delete_object(Bucket=source_bucket, Key=source_key)

                print(f"Moved {source_key} to {destination_key}")
    else:
        print("No files found in the source folder.")
def load_config():
    config_path = os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "diversityPreIngestionParams.json")
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found at {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    with open(config_path, "r") as file:
        config = json.load(file)
    logger.info("Configuration loaded successfully.")
    return config

# List all files in the S3 bucket with pagination support
def list_s3_files(s3_client, bucket_name, prefix=''):
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            files.extend([content['Key'] for content in page['Contents']])
    logger.info(f"Found {len(files)} files with prefix '{prefix}'.")
    print(files)
    return files[1:]

# Process an individual Excel file from S3
def process_excel_file(s3_client, s3_bucket, s3_key, column_rename_map):
    try:
        logger.debug(f"Downloading file {s3_key} from bucket {s3_bucket}.")
        # Download the file
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        file_stream = response['Body'].read()

        # Read the Excel file
        with BytesIO(file_stream) as file:
            xls = pd.ExcelFile(file)
            df_overview = pd.read_excel(xls, sheet_name='Overview', usecols=['Tags', 'NPI Number', 'Person ID', 'City 1', 'State 1', 'Zip Code 1', 'Specialty'])
            df_diversity = pd.read_excel(xls, sheet_name='Diversity')
            df_indication = pd.read_excel(xls, sheet_name='Search Filters')
            indication = df_indication.loc[df_indication['Search Filter'] == 'Search Query', 'Value'].values[0]
            df_diversity.rename(columns=column_rename_map, inplace=True)
            df_merged = pd.merge(df_diversity, df_overview, on='Person ID', how='inner')
            df_merged.columns = [col.lower().replace(' ', '_') for col in df_merged.columns]
            df_merged['indication'] = indication

            logger.debug(f"Processed file {s3_key}.")
            return df_merged
    except Exception as e:
        logger.error(f"Error processing file {s3_key}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

# Main function to orchestrate the process
def main():
    logger.info("Script execution started.")
    s3_client = boto3.client('s3')
    config = load_config()
    columns = config['columns']
    bucket_name = config['bucket_name']
    file_location = config['file_location']
    archive_location = config['archive_location']
    output_file_key = config['output_file_key']
    today_date = datetime.now().strftime('%Y%m%d')
    output_filename = output_file_key.format(timestamp=today_date)
    df_list = []

    try:
        s3_files = list_s3_files(s3_client, bucket_name, prefix=file_location)

        # Process each file and append the result to df_list
        for s3_file in s3_files:
            logger.info(f"Processing file {s3_file}.")
            merged_df = process_excel_file(s3_client, bucket_name, s3_file, columns)
            if not merged_df.empty:
                df_list.append(merged_df)
            else:
                logger.warning(f"No data returned for file {s3_file}.")

        if df_list:
            # Concatenate all DataFrames
            final_df = pd.concat(df_list, ignore_index=True).drop_duplicates()
            print(final_df.head())

            # Save the final DataFrame to an Excel file
            with BytesIO() as buffer:
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    final_df.to_excel(writer, index=False, sheet_name='Merged Data')
                buffer.seek(0)
                # Upload the file to S3
                s3_client.put_object(Bucket=bucket_name, Key=output_filename, Body=buffer.getvalue())
            logger.info(f'File uploaded successfully: {output_filename}')
        else:
            logger.info("No data to process.")
        # source_folder = f"s3://{bucket_name}/{file_location}"
        # destination_folder = f"s3://{bucket_name}/{file_location}Archive/{today_date}/"
        # command = f"aws s3 mv {source_folder} {destination_folder} --recursive"
        # os.system(command)
        move_s3_files(s3_client, bucket_name, file_location, archive_location)
    except Exception as e:
        logger.error(f"An error occurred: {e}")

    logger.info("Script execution completed.")
#
# if __name__ == "__main__":
#     main()
