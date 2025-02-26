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
from SponsorStandardizationProposed import *

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
    if message == "Sponsor":
        body = """Hello Team ,\n\nBased on our delta automator, attached is the list of sponsors which are missing in the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(
            client_name=client_name)
        attachment_name = 'sponsor_delta_records.xlsx'
        status = send_email(subject, body, delta_new, attachment_name, type_email="Sponsor")
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
        if type_email == "Sponsor":
            if (attachment and attachment_name):
                filename = attachment_name
                attachment = open("sponsor_delta_records.xlsx", "rb")
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

counter = automation_mapping_configuration.select().where(automation_mapping_configuration.Type == 'Sponsor').count()
run_filter = ["Sponsor"]
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
    print("Sponsor")
    file_path = "{Dataset_Path}/pt_batch_id={pt_batch_id}/".format(pt_batch_id=latest_stable_batch_id,
                                                                   Dataset_Path=row['Dataset_Path']).replace(
        '$$bucket_path',
        bucket_path)
    print(file_path)
    print(type(file_path), type(latest_stable_batch_id))
    sdf = spark.read.parquet(file_path)
    ds = row['Dataset_Name'].lower() + "_data"
    print("Datasourse Name : ", ds)
    sdf.write.mode("overwrite").saveAsTable(ds)
    # Mapping File Read
    mapping_path = row['Mapping_File_Path'].replace("$$bucket_path", bucket_path)
    sponsor_mapping = spark.read.format('com.crealytics.spark.excel').option("header", "true").load(mapping_path)
    sponsor_mapping.write.mode("overwrite").saveAsTable("sponsor_mapping")
    delta_mapping_path = bucket_path + "/uploads/SPONSOR/Sponsor_Delta/Sponsor_Delta.xlsx"
    sponsor_delta_mapping = spark.read.format('com.crealytics.spark.excel').option("header", "true").load(
        delta_mapping_path)
    sponsor_delta_mapping.write.mode("overwrite").saveAsTable("sponsor_delta_mapping")
    sponsor_mapping_final = spark.sql(""" 
    select distinct raw_sponsor_name, exploded_sponsor, parent_sponsor from sponsor_mapping
    union
    select distinct raw_sponsor_name, exploded_sponsor, parent_sponsor from sponsor_delta_mapping """)
    sponsor_mapping_final.write.mode("overwrite").saveAsTable("sponsor_mapping_final")
    if row['Data_Source'].lower() == "dqs":
        dqs_query = """SELECT distinct 'ir' AS source,sponsor AS sponsor_name FROM 
        default.dqs_study_data """.format(Column_Name=row['Column_Name'])
        df_dqs = spark.sql(dqs_query)
        df_dqs.write.mode("overwrite").saveAsTable("dqs_sponsor")
    elif row['Data_Source'].lower() == "aact":
        df_aact = spark.sql("""SELECT DISTINCT 'aact' as source,regexp_replace(name,'\\\"','\\\'')  AS sponsor_name
        from default.aact_sponsors_data where lower(trim(agency_class)) in ('nih','industry') 
        """)
        df_aact.write.mode("overwrite").saveAsTable("aact_sponsor")
    i += 1
    if counter == i:
        df_union_sponsor = spark.sql("""select lower(trim(sponsor_name)) as sponsor_name from dqs_sponsor union 
        select lower(trim(sponsor_name)) as sponsor_name from aact_sponsor """)
        df_union_sponsor.write.mode("overwrite").saveAsTable("df_union_sponsor")
        print("final", row['Data_Source'])
        delta_records_temp = spark.sql("""select distinct lower(trim(sponsor_name)) as sponsor_name  from (select trim(a.sponsor_name) as sponsor_name 
            from  default.df_union_sponsor a left join
			(select distinct lower(trim(raw_sponsor_name)) as raw_sponsor_name,
						 lower(trim(exploded_sponsor)) as exploded_sponsor,
						 lower(trim(parent_sponsor)) as parent_sponsor 
			from default.sponsor_mapping_final where exploded_sponsor is not null and trim(exploded_sponsor) <>'')   b on
			lower(trim(a.sponsor_name)) =lower(trim(b.exploded_sponsor)) where ( b.exploded_sponsor  is null )) where sponsor_name is not null and lower(trim(sponsor_name))<>''
		""")
        delta_records_temp.write.mode("overwrite").saveAsTable("delta_records_temp")
        delta_records = spark.sql("""select distinct lower(trim(sponsor_name)) as sponsor_name  from (select trim(a.sponsor_name) as sponsor_name
            from  default.delta_records_temp a left join
			(select distinct lower(trim(raw_sponsor_name)) as raw_sponsor_name,
					     lower(trim(exploded_sponsor)) as exploded_sponsor,
						 lower(trim(parent_sponsor)) as parent_sponsor 
			from default.sponsor_mapping_final where raw_sponsor_name is not null and trim(raw_sponsor_name) <>'')   b on
			lower(trim(a.sponsor_name)) =lower(trim(b.raw_sponsor_name)) where ( b.raw_sponsor_name  is null )) where sponsor_name is not null and lower(trim(sponsor_name))<>''
		""")
        delta_records.write.mode("overwrite").saveAsTable("delta_records")
        if (delta_records.count() == 0):
            print("No Delta")
            email_delta("No Delta",
                        "Environment: {env} | [Update] No Action Required In Sponsor Mapping".format(env=env))
        else:
            sponsor_delta_path = CommonConstants.AIRFLOW_CODE_PATH + "sponsor_delta.xlsx"
            if os.path.exists(sponsor_delta_path):
                os.system("rm " + sponsor_delta_path)
            delta_records.repartition(1).write.format('com.crealytics.spark.excel').option('header', True).mode(
                'overwrite') \
                .save('/user/hive/warehouse/Sponsor_Delta/sponsor_delta.xlsx')
            sponsor_std = SponsorStandardizationProposed()
            sponsor_std.main()
            sponsor_delta_mapping_path = (
                        bucket_path + "/uploads/SPONSOR/Temp_Sponsor_Mapping_Proposed_Holder/sponsor_delta_ouput.xlsx")
            sponsor_delta_mapping = spark.read.format('com.crealytics.spark.excel').option('header', 'true').load(
                sponsor_delta_mapping_path)
            sponsor_delta_mapping.write.mode('overwrite').saveAsTable("sponsor_delta_mapping")
            delta_new = spark.sql("""select distinct lower(trim(a.sponsor_name)) as sponsor_name,
                                            lower(trim(b.exploded_sponsor)) as proposed_sponsor,
                                            lower(trim(parent_sponsor)) as proposed_parent_sponsor, similarity 
            from delta_records a inner join
            (select raw_sponsor_name,exploded_sponsor,parent_sponsor,similarity,citeline_raw_sponsor_name from default.sponsor_delta_mapping) b 
            on lower(trim(a.sponsor_name)) =lower(trim(b.raw_sponsor_name)) 
            group by 1,2,3,4""")
            delta_new_path = CommonConstants.AIRFLOW_CODE_PATH + "sponsor_delta_records.xlsx"
            if os.path.exists(delta_new_path):
                os.system("rm " + delta_new_path)
            delta_new.repartition(1).write.format('com.crealytics.spark.excel').option('header', True).mode('overwrite') \
                .save('/user/hive/warehouse/Sponsor_Delta_Email/sponsor_delta_records.xlsx')
            os.system(
                "hadoop fs -copyToLocal /user/hive/warehouse/Sponsor_Delta_Email/sponsor_delta_records.xlsx " + CommonConstants.AIRFLOW_CODE_PATH + "/")

            email_delta("Sponsor",
                        "Environment: {env} | [URGENT] Update Delta Records In Sponsor Mapping".format(env=env))
            print("delta")

