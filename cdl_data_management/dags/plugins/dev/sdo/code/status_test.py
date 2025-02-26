import pandas as pd
import boto3
import io

s3_file_key = 'clinical-data-lake/uploads/trial_status.csv'
bucket = "aws-a0199-use1-00-d-s3b-snfi-ctf-data01"

s3 = boto3.client('s3')
obj = s3.get_object(Bucket=bucket, Key=s3_file_key)

initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
#print(initial_df)

initial_df.columns =[column.replace(" ", "_") for column in initial_df.columns]
initial_df.query('br_status == "Ongoing"', inplace = True)
#print(ongoing_list)
#initial_df.loc[:, 1:2]
#df.iloc[:, 0:2] 
print(initial_df)

