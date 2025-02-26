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
import numpy as np
from DrugStandardizationProposed import *

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
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_recipient_list"])
print(email_recipient_list)
sender_email = str(configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_sender"]))
client_name = str(configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "client_name"]))
recipient_list = ", ".join(email_recipient_list)
smtp_host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_server"])
smtp_port = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_port"])


def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf

is_numeric_UDF = udf(lambda x: is_numeric(x))


def email_delta(message, subject, attachment=None):
    if message == "Drug":
        body = """Hello Team ,\n\nBased on our delta automator, attached is the list of Drugs which are missing in the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(
            client_name=client_name)
        attachment_name = 'drug_delta_records.xlsx'
        status = send_email(subject, body, drug_delta_new, attachment_name, type_email="Drug")
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")
    elif message == "No Delta":
        subject_email = subject
        body = """Hello Team ,\n\nBased on our delta automator, no delta exists in the recently ingested data.\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(
            client_name=client_name)
        status = send_email(subject_email, body)
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")
    else:
        body = """Hello Team ,\n\nBased on our delta automator, attached are the {} delta records missing from the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(
            message, client_name=client_name)
        attachment_name = "{}_delta.csv".format(message)
        status = send_email(subject, body, attachment, attachment_name)
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")


def send_email(mail_subject, body, attachment=None, attachment_name=None, type_email=None):
    try:
        subject = mail_subject
        # Create a multipart message and set headers
        msg = MIMEMultipart('alternative')
        msg["From"] = sender_email
        msg["To"] = recipient_list
        msg["Subject"] = subject
        msg.attach(MIMEText(body))
        if type_email == "Drug":
            if (attachment and attachment_name):
                filename = attachment_name
                attachment = open("drug_delta_records.xlsx", "rb")
                part = MIMEBase('application', 'octet-stream')
                part.set_payload((attachment).read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
                msg.attach(part)
        else:
            if (attachment and attachment_name):
                part = MIMEApplication(
                    attachment.toPandas().to_csv(escapechar="\\", doublequote=False, encoding='utf-8', index=False),
                    Name=attachment_name)
                content_dis = "attachment; filename=" + attachment_name
                part['Content-Disposition'] = content_dis
                msg.attach(part)
        server = smtplib.SMTP(host=smtp_host, port=smtp_port)
        server.connect(smtp_host, smtp_port)
        server.starttls()
        server.send_message(msg)
        server.quit()
        return True
    except:
        raise Exception(
            "Issue Encountered while sending the email to recipients --> " + str(traceback.format_exc()))
        return False


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)
delta_env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_automator_env"])
env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
path_conf = bucket_path + '/uploads/delta_automator/automation_mapping_configuration.csv'
automation_mapping_configuration = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load(path_conf)

counter = automation_mapping_configuration.select().where(automation_mapping_configuration.Type == 'Drug').count()
run_filter = ["Drug"]
automation_mapping_generic = automation_mapping_configuration.filter(
    automation_mapping_configuration.Type.isin(run_filter))
dataCollect = automation_mapping_generic.rdd.toLocalIterator()
i = 0
for row in dataCollect:
    print("Executing query to get latest successful batch_id")
    cursor.execute(
        "select batch_id from {orchestration_db_name}.log_batch_dtl where trim(lower(batch_status)) = 'succeeded' and dataset_id={dataset_id} order by batch_start_time desc limit 1 ".format(
            orchestration_db_name=audit_db, dataset_id=row['Dataset_Id']))
    fetch_enable_flag_result = cursor.fetchone()
    latest_stable_batch_id = str(fetch_enable_flag_result['batch_id'])
    print("=====================Running For :", row['Type'], "=====================")
    print("Drug")
    file_path = "{Dataset_Path}/pt_batch_id={pt_batch_id}/".format(pt_batch_id=latest_stable_batch_id,
                                                                   Dataset_Path=row['Dataset_Path']).replace(
        '$$bucket_path',
        bucket_path)
    print(file_path)
    print(type(file_path), type(latest_stable_batch_id))
    sdf = spark.read.parquet(file_path)
    ds = row['Dataset_Name'].lower() + "_data"
    print("Datasourse Name: ", ds)
    sdf.write.mode("overwrite").saveAsTable(ds)
    # Mapping File Read
    mapping_path = row['Mapping_File_Path'].replace("$$bucket_path", bucket_path)
    drug_mapping = spark.read.format('com.crealytics.spark.excel').option("header", "true").load(mapping_path)
    drug_mapping.write.mode("overwrite").saveAsTable("drug_mapping")
    delta_mapping_path = bucket_path + "/uploads/DRUG/Drug_Delta/Drug_Delta.xlsx"
    drug_delta_mapping = spark.read.format('com.crealytics.spark.excel').option("header", "true").load(
        delta_mapping_path)
    drug_delta_mapping.write.mode("overwrite").saveAsTable("drug_delta_mapping")
    drug_mapping_final = spark.sql(""" select drugprimaryname, drugnamesynonyms from drug_mapping
    union select citeline_primary_drug_name as drugprimaryname, raw_drug_name as drugnamesynonyms from drug_delta_mapping """)
    drug_mapping_final.write.mode("overwrite").saveAsTable("drug_mapping_final")
    if row['Data_Source'].lower() == "aact":
        aact_query = """ SELECT distinct 'aact' AS source,  regexp_replace(drug_name,'\\\"','\\\'') AS drug_name 
        FROM default.aact_interventions_data 
        lateral view explode(split(name, '\\\;')) as drug_name
        where lower(trim(intervention_type)) like 'drug' and trim(drug_name) <> '' 
        group by 1,2 """.format(Column_Name=row['Column_Name'])
        df_aact = spark.sql(aact_query)
        df_aact.write.mode("overwrite").saveAsTable("aact_drug")
    elif row['Data_Source'].lower() == "ctms":
        ctms_query = """ SELECT distinct 'ctms' AS source, lower(trim(coalesce(trade_name,product_name))) AS drug_name 
        FROM default.ctms_product_data
        WHERE lower(trim(meta_is_current)) = 'y' 
        """.format(Column_Name=row['Column_Name'])
        df_ctms = spark.sql(ctms_query)
        df_ctms.write.mode("overwrite").saveAsTable("ctms_drug")

    i += 1
    if counter == i:
        temp_union = spark.sql("""select distinct drug_name,source from aact_drug 
                              union select distinct drug_name,source from ctms_drug
                              """)
        temp_union.write.mode("overwrite").saveAsTable("temp_union")
        temp_union = temp_union.withColumn("is_numeric_flag", is_numeric_UDF("drug_name"))
        temp_union.write.mode("overwrite").saveAsTable("temp_union")
        df_union_drug = spark.sql(
            """select distinct drug_name,source from temp_union where is_numeric_flag = 'false' """)
        df_union_drug.write.mode("overwrite").saveAsTable("df_union_drug")
        print("final", row['Data_Source'])
        drug_delta_records = spark.sql("""select distinct datasource,lower(trim(drug_name)) as raw_drug_name  
            from (select a.source as datasource,trim(a.drug_name) as drug_name 
            from  default.df_union_drug a left join
            (select distinct drugprimaryname,drugnamesynonyms from default.drug_mapping_final)   b on
            lower(trim(a.drug_name)) =lower(trim(b.drugnamesynonyms)) where (b.drugnamesynonyms  is null) ) where drug_name is not null and lower(trim(drug_name))<>''
            """)
        drug_delta_records.write.mode("overwrite").saveAsTable("drug_delta_records")
        if (drug_delta_records.count() == 0):
            print("No Delta")
            email_delta("No Delta",
                        "Environment: {env} | [Update] No Action Required In Drug Mapping".format(env=env))
        else:
            drug_delta_records.repartition(1).write.format('csv').option('delimiter', '`').option('header', True).mode(
                'overwrite') \
                .save('/user/hive/warehouse/Drug_Delta/')
            drug_std = DrugStandardizationProposed()
            drug_std.main()
            drug_delta_mapping_path = (
                        bucket_path + "/uploads/DRUG/Temp_Drug_Mapping_Proposed_Holder/drug_delta_ouput.xlsx")
            drug_delta_mapping = spark.read.format('com.crealytics.spark.excel').option('header', 'true').load(
                drug_delta_mapping_path)
            drug_delta_mapping.write.mode('overwrite').saveAsTable("drug_delta_mapping")
            drug_delta_new = spark.sql("""select distinct a.raw_drug_name, b.drugnamesynonyms as citeline_drug_name_synonyms, b.drugprimaryname as citeline_primary_drug_name, similarity 
            from default.drug_delta_records a inner join 
            (select raw_drug_name, drugnamesynonyms, similarity, drugprimaryname from drug_delta_mapping) b 
            on lower(trim(a.raw_drug_name)) =lower(trim(b.raw_drug_name)) 
            group by 1,2,3,4""")
            drug_delta_new.toPandas().to_excel("drug_delta_records.xlsx", index=False)
            # os.system('aws s3 cp drug_delta_records.xlsx /user/hive/warehouse/Drug_Delta_Email/drug_delta_records.xlsx .')
            # drug_delta_new.repartition(1).write.format('com.crealytics.spark.excel').option('header', True).mode('overwrite')\
            #    .save('/user/hive/warehouse/Drug_Delta_Email/drug_delta_records.xlsx')
            # os.system(
            #    "hadoop fs -copyToLocal /user/hive/warehouse/Drug_Delta_Email/drug_delta_records.xlsx " + CommonConstants.AIRFLOW_CODE_PATH + "/")

            email_delta("Drug",
                        "Environment: {env} | [URGENT] Update Delta Records In Drug Mapping".format(env=env))
            print("delta")

