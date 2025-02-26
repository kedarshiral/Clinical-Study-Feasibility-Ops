import os.path
from datetime import datetime
import os


data_src_files = [
    {
        "src_file_location":"s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/pre-ingestion/CITELINE/CITELINE_TRIALTROVE/ ",
        "destination":"s3://aws-a0220-use1-00-d-s3b-shrd-cus-sdocdl01/clinical-data-lake/pre-ingestion/CITELINE/CITELINE_TRIALTROVE/"
    },
    {
        "src_file_location":"s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/pre-ingestion/CITELINE/CITELINE_PHARMAPROJECTS/ ",
        "destination":"s3://aws-a0220-use1-00-d-s3b-shrd-cus-sdocdl01/clinical-data-lake/pre-ingestion/CITELINE/CITELINE_PHARMAPROJECTS/"
    }
]

try:
    for files in data_src_files:
        src_lc = files["src_file_location"]
        src_lc = str(src_lc)
        ltst_cmd = "aws s3 ls {file_location_1} --recursive | sort | tail -n 1 | awk '{print $4}'".format(
            file_location_1=src_lc)
        des_path = os.system(ltst_cmd)
        src_file = "s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/" + des_path
        aws_copy_cmd = "aws s3 cp {src_file_loc} {destination_loc}".format(src_file_loc=src_file,
                                                                           destination_loc=str(files["destination"]))
        print(aws_copy_cmd)
        os.system(ltst_cmd)
except Exception as er:
    print("Error while copying citeline files")
    raise Exception(er)
