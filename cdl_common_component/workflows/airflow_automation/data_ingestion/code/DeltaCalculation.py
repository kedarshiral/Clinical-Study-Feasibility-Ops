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
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from email.mime.base import MIMEBase
from email import encoders
from pyspark.sql.functions import expr
from pyspark.sql.functions import regexp_replace



execution_context = ExecutionContext()
spark = PySparkUtility(execution_context).get_spark_context("DeltaAutomation",CommonConstants.HADOOP_CONF_PROPERTY_DICT)
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")
#configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH, CommonConstants.ENVIRONMENT_CONFIG_FILE))
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

def email_delta(message, subject, attachment=None):
    if message == "disease":
        body = """Hello Team ,\n\nBased on our delta automator, attached is the list of diseases which are missing in the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(client_name=client_name)
        attachment_name = 'disease_delta_records.xlsx'
        status = send_email(subject, body, delta_new, attachment_name,type_email="Disease")
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")
    elif message == "No Delta":
        subject_email = subject
        body = """Hello Team ,\n\nBased on our delta automator, no delta exists in the recently ingested data.\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(client_name=client_name)
        status = send_email(subject_email, body)
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")
    else:
        body = """Hello Team ,\n\nBased on our delta automator, attached are the {} delta records missing from the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(
            message,client_name=client_name)
        attachment_name = "{}_delta.csv".format(message)
        status = send_email(subject, body, attachment, attachment_name)
        if status:
            print("Email sent Successfully")
        else:
            print("Check Logs")


def send_email(mail_subject, body, attachment=None, attachment_name=None,type_email=None):
    try:
        subject = mail_subject
        # Create a multipart message and set headers
        msg = MIMEMultipart('alternative')
        msg["From"] = sender_email
        msg["To"] =recipient_list
        msg["Subject"] = subject
        msg.attach(MIMEText(body))
        if type_email=="Disease":
            if (attachment and attachment_name):
                filename = attachment_name
                attachment = open("disease_delta.xlsx","rb")
                part = MIMEBase('application', 'octet-stream')
                part.set_payload((attachment).read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
                msg.attach(part)
        else :                
            if (attachment and attachment_name):
                part = MIMEApplication(attachment.toPandas().to_csv(escapechar="\\",doublequote=False,encoding='utf-8',index=False), Name=attachment_name)
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
delta_env=configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_automator_env"])
env=configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "environment"])
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
path_conf = bucket_path + '/uploads/delta_automator/automation_mapping_configuration.csv'
automation_mapping_configuration = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load(path_conf)

result = []
generic_delta_filter=["Mesh","Sponsor","Drug","City"]
automation_mapping_generic=automation_mapping_configuration.filter(~automation_mapping_configuration.Type.isin(generic_delta_filter))
dataCollect = automation_mapping_generic.rdd.toLocalIterator()

for row in dataCollect:
    print("Executing query to get latest successful batch_id")
    cursor.execute(
        "select batch_id from {orchestration_db_name}.log_batch_dtl where trim(lower(batch_status)) = 'succeeded' and dataset_id={dataset_id} order by batch_start_time desc limit 1 ".format(
            orchestration_db_name=audit_db, dataset_id=row['Dataset_Id']))
    fetch_enable_flag_result = cursor.fetchone()
    latest_stable_batch_id = str(fetch_enable_flag_result['batch_id'])
    print("hi")
    res = []
    print(row['Mapping_File_Path'] + "," + row['Type'], "in else")
    # latest_stable_file_id = str(fetch_enable_flag_result['file_id']).replace("-", "")
    path = "{Dataset_Path}/pt_batch_id={pt_batch_id}/".format(pt_batch_id=latest_stable_batch_id,
                                                              Dataset_Path=row['Dataset_Path']).replace('$$bucket_path',bucket_path)
    print(path)
    previous_snapshot_data = spark.read.parquet(path)
    previous_snapshot_data.createOrReplaceTempView("previous_snapshot_data")
    query = """select distinct {Column_Name} as {Column_Name} from previous_snapshot_data where {Column_Name} is not null""".format(
        Column_Name=row['Column_Name'])
    if (row['multiple_values'] == "yes"):
        print("row multiple_values ", row['multiple_values'])
        print("Delim :" + row['delimiter'])
        df = spark.sql(query)
        for colname in df.columns:
            df = df.withColumn(colname, f.trim(f.col(colname)))
        if row['delimiter']=="pipe":
            res=df.withColumn(row['Column_Name'],f.explode(f.split(row['Column_Name'],"\\|"))).rdd.flatMap(lambda x: x).distinct().collect()
        elif row['delimiter']=="semicolon":
            res=df.withColumn(row['Column_Name'],f.explode(f.split(row['Column_Name'],";"))).rdd.flatMap(lambda x: x).distinct().collect()
        if (row['regex_flag']=="yes"):
            res=df.withColumn(row['Column_Name'],f.regexp_replace(row['Column_Name'],row['regex_check'],row['regex_replace'])).rdd.flatMap(lambda x: x).distinct().collect()
    else:
        print("No multiple values")
        if (row['regex_flag']=="yes"):
            res = spark.sql(query).select(regexp_replace(row['Column_Name'],row['regex_check'],row['regex_replace'])).rdd.flatMap(lambda x: x).distinct().collect()
        else:
            res = spark.sql(query).select(row['Column_Name']).rdd.flatMap(lambda x: x).distinct().collect()
    # Trim Spaces
    res = [x.strip() for x in res]
    # Remove Duplicates
    distinct_value = []
    [distinct_value.append(x) for x in res if x not in distinct_value]
    # Remove Empty Space
    distinct_value = [i for i in distinct_value if i]
    mapping_path = row['Mapping_File_Path'].replace("$$bucket_path",bucket_path)
    print(mapping_path)
    df_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',').load(mapping_path)
    df_mapping.createOrReplaceTempView('df_mapping')
    if row['Type'] == 'Age' :
        query_mapping = """select distinct {Column_Name} as {Column_Name} from df_mapping""".format(Column_Name=row['Mapping_File_Column_Name'])
    else :
        query_mapping = """select distinct * from (select distinct {Column_Name} as {Column_Name} from df_mapping
        union
        select distinct {standard_column_name} as {Column_Name}  from df_mapping)""".format(Column_Name=row['Mapping_File_Column_Name'], standard_column_name=row['Mapping_File_standard_Column_Name'])
    print(query_mapping)
    df_mapping_distinct_values = spark.sql(query_mapping).select(row['Mapping_File_Column_Name']).rdd.flatMap(lambda x: x).distinct().collect()
    print("Length of ",row['Type'],"distinct_value", len(distinct_value))
    print(distinct_value)
    print("df_mapping_distinct_values",df_mapping_distinct_values)
    delt=list(set(x.lower() for x in distinct_value) - set(x.lower() for x in df_mapping_distinct_values))
    print("Number of delta","For",row['Type'],len(delt))
    for j in delt:
        result.append([row['Type'], row['Data_Source'], row['Dataset_Id'], row['Dataset_Name'], row['Column_Name'], j,row['Mapping_File_Column_Name']])

if (len(result)):
    Typedf = [row['Type'] for row in automation_mapping_configuration.collect()]
    TypeList = list(set(Typedf))
    print("Delta Exists")
    df = spark.createDataFrame(result,
                               ['Type', 'Data_Source', 'Dataset_Id', 'Dataset_Name', 'Column_Name',
                                'Difference_Value',
                                'Mapping_File_Column_Name'])
    df_type_distinct_values = df.select('Type').rdd.flatMap(lambda x: x).distinct().collect()
    df.createOrReplaceTempView('df')
    for k in df_type_distinct_values:
        query_slice = """select * from df where Type='{k}'""".format(k=k)
        data_slice = spark.sql(query_slice)
        email_delta(k, "Environment: {env} | [URGENT] Update Delta Records In ".format(env=env) + k + " Mapping", data_slice)
        #data_slice.show()
    for each_type in TypeList:
        if each_type=="Mesh" or each_type=="Drug" or each_type=="Sponsor" or each_type=="City" :
            continue
        if each_type not in df_type_distinct_values:
            email_delta("No Delta","Environment: {env} | No Delta Records for ".format(env=env) + each_type + " Mapping")
else:
    print("No Delta Exist")
    Typedf = [row['Type'] for row in automation_mapping_configuration.collect()]
    TypeList = list(set(Typedf))
    # remove Disease from the list
    to_remove = ['Mesh', 'City', 'Sponsor', 'Drug']
    print(to_remove)
    for i in range(len(to_remove)):
        if to_remove[i] in TypeList:
            TypeList.remove(to_remove[i])
    # Updated Mail Subject List
    print('Updated List: ', TypeList)
    str_subject = ','.join(TypeList)
    email_delta("No Delta", "Environment: {env} | [Update] No Action Required For: ".format(env=env)+str(str_subject)+" Mappings")
