
# !/usr/bin/python
# -*- coding: utf-8 -*-
# __author__ = 'ZS Associates'

######################################################Module Information################################################
#   Module Name         :   DiseaseStandardizationArchiver
#   Purpose             :   Archiver of disease names across all data sources
#   Input Parameters    :
#   Output              :
#   Execution Steps     :
#   Predecessor module  :
#   Successor module    :   NA
#   Last changed on     :   22 02 2022
#   Last changed by     :   Sankarshana Kadambari
#   Reason for change   :
########################################################################################################################

import re
import os
import json
import time
import pandas as pd
import numpy as np
import warnings
from datetime import datetime
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

# all module level constants are defined here
MODULE_NAME = "DiseaseStandardizationArchiver"

class DiseaseStandardizationArchiver(object):

    def __init__(self):

        print("init done")

    def __enter__(self):
        return self

    def __exit__(self):
        pass

    def main(self):
        warnings.filterwarnings('ignore')

        APPLICATION_CONFIG_FILE = "application_config.json"

        configuration = json.load(open(APPLICATION_CONFIG_FILE))

        code_env = configuration["adapter_details"]["generic_config"]["env"]

        initial_time = time.time()

        CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
        configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])
        
        s3_mapping_file_location = "s3://{}/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/disease_mapping.csv".format(
            s3_bucket_name)
        prev_mapping_file_location = "s3://{}/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/Archive/".format(
            s3_bucket_name)

        # creating backup of existing disease mapping file in archive folder
        file_copy_command = "aws s3 cp {} {} --sse".format(
            os.path.join(s3_mapping_file_location),
            os.path.join(prev_mapping_file_location, "disease_mapping_" + CURRENT_DATE + ".csv"))
        print("Command to create backup of file on s3 - {}".format(file_copy_command))
        file_copy_status = os.system(file_copy_command)
        if file_copy_status != 0:
            raise Exception("Failed to create backup of disease mapping file on s3")

        final_time = time.time()

        print("Total Execution time: {} seconds".format(final_time - initial_time))


if __name__ == '__main__':
    Disease_std_arch = DiseaseStandardizationArchiver()
    Disease_std_arch.main()

