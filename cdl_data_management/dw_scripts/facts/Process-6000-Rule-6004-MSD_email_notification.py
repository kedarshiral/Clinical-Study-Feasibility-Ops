#!/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

# ####################################################Module Information################################################
#  Module Name         :   Delta Records Standardization
#  Purpose             :   This module will perform the will check for delta records in sponsor, status , disease and
#                          phase mappings.
#  Input Parameters    :
#  Output Value        :   Mail will be sent
#  Pre-requisites      :
#  Last changed on     :   28th Feb 2022
#  Last changed by     :   Kashish Mogha
#  Reason for change   :   Enhancement to provide restartability at file level
# ######################################################################################################################


# Library and external modules declaration
from CommonUtils import CommonUtils

import json
import traceback
import sys
import os
import datetime
import time

sys.path.insert(0, os.getcwd())
from pyspark.sql import *
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
import MySQLdb
import smtplib
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import pyspark.sql.functions as f
import CommonConstants as CommonConstants
from PySparkUtility import PySparkUtility
from ExecutionContext import ExecutionContext
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from email.mime.base import MIMEBase
from email import encoders
from pyspark.sql.functions import expr
from pyspark.sql.functions import regexp_replace

execution_context = ExecutionContext()
spark = PySparkUtility(execution_context).get_spark_context("DeltaAutomation",
                                                            CommonConstants.HADOOP_CONF_PROPERTY_DICT)
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")
# configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)

email_recipient_list = configuration.get_configuration(
                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "msd_email_type_configurations", "ses_recipient_list"])
print(email_recipient_list)
sender_email = str(configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "msd_email_type_configurations", "ses_sender"]))
client_name = str(configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "client_name"]))
recipient_list = ", ".join(email_recipient_list)

smtp_host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_server"])
smtp_port = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_port"])
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])


def email_delta(subject, attachment_data):
    body = """Hello Team,\n\nBased on our MSD automator, attached are the outputs of all three modules.\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(
        client_name=client_name)
    status = send_email(subject, body, attachment_data)
    if status:
        print("Email sent successfully")
    else:
        print("Check logs")


def send_email(mail_subject, body, attachment_data):
    try:
        # Create a multipart message and set headers
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = recipient_list
        msg["Subject"] = mail_subject
        msg.attach(MIMEText(body, "plain"))


        for attachment in attachment_data:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment["data"])
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={attachment['name']}",
            )
            msg.attach(part)

        server = smtplib.SMTP(host=smtp_host, port=smtp_port)
        server.connect(smtp_host, smtp_port)
        server.starttls()
        server.send_message(msg)
        server.quit()
        return True

    except Exception as e:
        print(f"Issue encountered while sending the email: {str(e)}")
        return False

csv_path_inv = bucket_path + '/applications/commons/temp/km_validation/MSD_cluster_update/archive/output/inv/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'
csv_path_site = bucket_path + '/applications/commons/temp/km_validation/MSD_cluster_update/archive/output/site/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'
csv_path_trial = bucket_path + '/applications/commons/temp/km_validation/MSD_cluster_update/archive/output/trial/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

inv_data_slice = spark.read.format("csv").option("header", "true").load(csv_path_inv)
site_data_slice = spark.read.format("csv").option("header", "true").load(csv_path_site)
trial_data_slice = spark.read.format("csv").option("header", "true").load(csv_path_trial)

attachments = [
    {"name": "MSD_INV.csv", "data": inv_data_slice.toPandas().to_csv(index=False)},
    {"name": "MSD_Site.csv", "data": site_data_slice.toPandas().to_csv(index=False)},
    {"name": "MSD_Trial.csv", "data": trial_data_slice.toPandas().to_csv(index=False)}
]

email_delta("Manual Stewardship Dedupe Output", attachments)

# move input file to archieve
# move input file to archieve

audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)
cursor.execute(
    """select cycle_id ,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = 2000 order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db))

fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
print(latest_stable_data_date, latest_stable_cycle_id)

command = "aws s3 mv {}/applications/commons/temp/km_validation/MSD_cluster_update/identified_records/ {}/applications/commons/temp/km_validation/MSD_cluster_update/archive/identified_records/pt_data_dt={}/pt_cycle_id={}/ --recursive".format(
    bucket_path, bucket_path,latest_stable_data_date, latest_stable_cycle_id)
os.system(command)