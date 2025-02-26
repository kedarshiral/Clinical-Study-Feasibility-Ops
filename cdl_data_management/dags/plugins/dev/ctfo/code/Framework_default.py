import json
import logging
import boto3
from botocore.exceptions import ClientError
from io import StringIO
from datetime import datetime
import diversityPreIngestion


# Set up logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize S3 client
s3_client = boto3.client('s3')


def read_s3_file(bucket, key):
    """
    Read a file from S3 and return its contents.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response['Body'].read().decode('utf-8')
        logging.info(f"Successfully read file from s3://{bucket}/{key}")
        return data
    except ClientError as e:
        logging.error(f"Error reading file from S3: {e}")
        raise


def write_s3_file(bucket, key, data):
    """
    Write data to a file in S3.
    """
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        logging.info(f"Successfully wrote file to s3://{bucket}/{key}")
    except ClientError as e:
        logging.error(f"Error writing file to S3: {e}")
        raise


def process_file_operations(data_source, file_master_id, source_file_location, table_name, date_format, file_format):
    """
    Function to handle file operations when the data source is inactive (flag == 'N').
    It reads a file, modifies its location, and writes it back in a specific format.
    """
    logging.info(f"Processing file for data_source: {data_source}, file_master_id: {file_master_id}")

    # Handle special cases for citeline and aact
    payload_id = None
    if data_source == 'citeline':
        payload_id = {201: 1, 202: 3, 203: 4, 204: 5}.get(file_master_id)
    elif data_source == 'aact':
        payload_id = 2

    # Construct the file name
    if payload_id:
        file_name = f"{table_name}_{datetime.strptime(date_str, date_format)}_{payload_id}.{file_format}"
    else:
        file_name = f"{table_name}_{datetime.strptime(date_str, date_format)}.{file_format}"

    logging.info(f"Constructed file name: {file_name}")

    # Step 7: Read and write the file in S3
    input_bucket, input_key = parse_s3_url(source_file_location, file_name)
    output_bucket, output_key = parse_s3_url(source_file_location, file_name)

    try:
        # Read the file from S3
        logging.info(f"Reading file from s3://{input_bucket}/{input_key}")
        data = read_s3_file(input_bucket, input_key)

        # Write the file back to S3 (you can change the location if needed)
        logging.info(f"Writing file to s3://{output_bucket}/{output_key}")
        write_s3_file(output_bucket, output_key, data)

        logging.info(f"File processed and written to s3://{output_bucket}/{output_key}")

    except Exception as e:
        logging.error(f"Error processing file: {e}")


def parse_s3_url(base_path, file_name):
    """
    Parses S3 URL to extract bucket and key from the base path and file name.
    """
    if base_path.startswith("s3://"):
        base_path = base_path[5:]
    bucket, key_prefix = base_path.split('/', 1)
    key = f"{key_prefix}/{file_name}"
    return bucket, key


def main(file_master_id, process_id):
    # Step 1: Load JSON and extract the data source values
    data_source_dtl = json.loads('data_source.json')

    # Step 2: Query MySQL to fetch data_source based on process_id
    data_source = data_source_dtl['datasources'].get(process_id)
    if not data_source:
        logging.error(f"Invalid process_id: {process_id}. No corresponding data source found.")

    logging.info(f"Fetched data_source: {data_source} for process_id: {process_id}")

    # Step 3: Query MySQL to fetch source_file_location and table_name from ctl_dataset_master
    query = f"SELECT file_source_location, file_name_pattern FROM ctl_dataset_master WHERE dataset_id = {file_master_id}"
    result = MySQLConnectionManager().execute_query_mysql(query, False)[0]
    source_file_location = result['file_source_location']
    raw_table_name = result['file_name_pattern']
    table_name = raw_table_name.split('_YYYY')[0]
    table_name = table_name[:-2] if data_source == 'citeline' else table_name
    table_name = table_name.lower()

    logging.info(
        f"Fetched source_file_location: {source_file_location}, table_name: {table_name} for file_master_id: {file_master_id}")

    # Step 4: Check the active flag for the data source from the JSON
    flag, date_format, file_format = data_sources['meta_data_source'][data_source]
    logging.info(f"Data source flag: {flag}, Date format: {date_format}, File format: {file_format}")

    # Step 5: If the flag is 'N', modify source_file_location and process file operations
    if flag == 'N':
        logging.info(
            f"Data source {data_source} is inactive (flag = 'N'). Replacing 'pre-ingestion' with 'default' in source_file_location.")
        source_file_location = source_file_location.replace('pre-ingestion', 'default')

        # Call the new function to handle file operations
        process_file_operations(data_source, file_master_id, source_file_location, table_name, date_format, file_format)
    else:
        if data_source=='diversity':
            diversityPreIngestion.main()
        logging.info(f"Data source {data_source} is active (flag = 'Y'). No file operations will be performed.")


