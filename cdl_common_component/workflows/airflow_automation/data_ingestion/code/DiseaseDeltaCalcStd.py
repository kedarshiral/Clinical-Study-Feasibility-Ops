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
from datetime import datetime
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
from DiseaseStandardizationProposed import DiseaseStandardizationProposed
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
smtp_host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_server"])
smtp_port = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_port"])
recipient_list = ", ".join(email_recipient_list)


def email_delta(message, subject, attachment=None):
    if message == "mesh":
        body = """Hello Team ,\n\nBased on our delta automator, attached is the list of mesh which are missing in the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(
            client_name=client_name)
        attachment_name = 'mesh_delta_records.xlsx'
        status = send_email(subject, body, delta_new, attachment_name, type_email="Mesh")
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
        if type_email == "Mesh":
            if (attachment and attachment_name):
                filename = attachment_name
                attachment = open("disease_delta.xlsx", "rb")
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

counter = automation_mapping_configuration.select().where(automation_mapping_configuration.Type == 'Mesh').count()
run_filter = ["Mesh"]
automation_mapping_generic = automation_mapping_configuration.filter(
    automation_mapping_configuration.Type.isin(run_filter))
dataCollect = automation_mapping_generic.rdd.toLocalIterator()
i = 0
count_citeline = 0
count_aact = 0
for row in dataCollect:
    print("Executing query to get latest successful batch_id")
    cursor.execute(
        "select batch_id from {orchestration_db_name}.log_batch_dtl where trim(lower(batch_status)) = 'succeeded' and dataset_id={dataset_id} order by batch_start_time desc limit 1 ".format(
            orchestration_db_name=audit_db, dataset_id=row['Dataset_Id']))
    fetch_enable_flag_result = cursor.fetchone()
    latest_stable_batch_id = str(fetch_enable_flag_result['batch_id'])
    print(
        "================================================================================================================================Running For :",
        row['Type'], "=======================================================================================")
    print("Mesh")
    # mapping_path = row['Mapping_File_Path'].replace("$$bucket_path",bucket_path)
    # disease_mapping = spark.read.format('csv').option("header", "true").option("delimiter", ",").load(mapping_path)
    cursor.execute(
        "select batch_id from {orchestration_db_name}.log_batch_dtl where trim(lower(batch_status)) = 'succeeded' and dataset_id=1601 order by batch_start_time desc limit 1 ".format(
            orchestration_db_name=audit_db, dataset_id=row['Dataset_Id']))
    dis_fetch_enable_flag_result = cursor.fetchone()
    dis_latest_stable_batch_id = str(dis_fetch_enable_flag_result['batch_id'])
    mesh_mapping_path = bucket_path + "/staging/MAPPING/mesh_mapping/pt_batch_id={pt_batch_id}/".format(
        pt_batch_id=dis_latest_stable_batch_id)
    print("mesh_mapping path for table ingestion", mesh_mapping_path)
    mesh_mapping = spark.read.parquet(mesh_mapping_path)
    mesh_mapping.createOrReplaceTempView("mesh_mapping")
    mesh_mapping_final = spark.sql(
        """ select distinct * from (select mesh as disease, standard_mesh as standard_disease from mesh_mapping union select distinct  standard_mesh, standard_mesh from  mesh_mapping)  """)
    mesh_mapping_final.createOrReplaceTempView("mesh_mapping_final")
    mesh_pandas_df = mesh_mapping_final.toPandas()
    mesh_pandas_df.to_excel("disease_mapping_parexcel.xlsx", index=False)
    file_path = "{Dataset_Path}/pt_batch_id={pt_batch_id}/".format(pt_batch_id=latest_stable_batch_id,
                                                                   Dataset_Path=row['Dataset_Path']).replace(
        '$$bucket_path',
        bucket_path)
    print(file_path)
    print(type(file_path), type(latest_stable_batch_id))
    sdf = spark.read.parquet(file_path)
    ds = row['Dataset_Name'].lower() + "_data"
    print("Datasourse name Sankarsh : ", ds)
    sdf.write.mode("overwrite").saveAsTable(ds)
    if row['Data_Source'].lower() == "dqs":
        dqs_query = """SELECT 'ir' AS source,mesh AS mesh_name FROM 
        (select distinct case when mesh_heading is not null and trim(mesh_heading)!='' then mesh_heading else primary_indication end  as primary_indication from default.dqs_study_data)
        lateral VIEW outer posexplode (split(primary_indication,'\\\;'))three AS pos3,
        mesh
        """.format(Column_Name=row['Column_Name'])
        df_dqs = spark.sql(dqs_query)
        df_dqs.write.mode("overwrite").saveAsTable("dqs_mesh")
    elif row['Data_Source'].lower() == "citeline":

        print('**************', row['Dataset_Name'])
        if row['Dataset_Name'].lower() == "citeline_trialtrove":
            count_citeline += 1
            exp_1 = spark.sql("""SELECT   distinct Trim(mesh)    AS mesh_name
            FROM     (select distinct case when trial_mesh_term_name is not null and trim(trial_mesh_term_name)!='' then trial_mesh_term_name else disease_name end as disease_name from 
            default.citeline_trialtrove_data ) 
            lateral VIEW posexplode (split(disease_name,'\\\|'))two AS pos2,
            mesh
            GROUP BY 1
            """)
            exp_1.createOrReplaceTempView('exp_1')
            # applying explode on disease for citeline
            exp_2_1 = spark.sql("""SELECT   distinct 'citeline' as datasource,
            Trim(mesh)    AS mesh_name
            FROM     exp_1
            lateral view posexplode (split(mesh_name,'\\\^'))two    AS pos2,
            mesh
            GROUP BY 1,2
            """)
            exp_2_1.createOrReplaceTempView('exp_2_1')

            exp_2 = spark.sql("""SELECT  distinct datasource,
            Trim(mesh)    AS mesh_name
            FROM     exp_2_1
            lateral view posexplode (split(mesh_name,'\\\#'))two    AS pos2,
            mesh
            GROUP BY 1,2
            """)
            exp_2.createOrReplaceTempView('exp_2')

            print('trialtrove ran')
            if count_citeline == 2:
                df_citeline = spark.sql("""select 'citeline' as source, mesh_name 
                from exp_2 group by 1,2
                union
                select 'citeline' as source,disease_name as mesh_name
                from citeline_pharma_diseases group by 1,2
                """)
                print('************citeline df ready')

                df_citeline.write.mode("overwrite").saveAsTable("citeline_mesh")

            # exp_2.write.mode('overwrite').saveAsTable('exp_2')
            # creating  trial -TA -disease- patient segment mapping for citeline

            ##
        elif row['Dataset_Name'].lower() == "citeline_pharmaprojects":
            count_citeline += 1
            citeline_pharma_temp = spark.sql("""
            select lower(drugprimaryname) as drg_nm,max(indications_diseasename) as disease_nm
            from 
            default.citeline_pharmaprojects_data citeline_pharma
            where lower(citeline_pharma.ispharmaprojectsdrug) ='true'
            group by 1
            """)
            citeline_pharma_temp.registerTempTable('citeline_pharma_temp')

            citeline_pharma_diseases = spark.sql("""
            select trim(disease_name1)  as disease_name,'' as trial_id,'' as therapeutic_area from citeline_pharma_temp  lateral view
            posexplode(split(regexp_replace(disease_nm,"\;","\\\|"),"\\\|"))one
            as pos1,disease_name1
            """)
            citeline_pharma_diseases.registerTempTable('citeline_pharma_diseases')
            print('pharma ran')
            if count_citeline == 2:
                df_citeline = spark.sql("""select 'citeline' as source, mesh_name 
                from exp_2 group by 1,2
                union
                select 'citeline' as source,disease_name as mesh_name
                from citeline_pharma_diseases group by 1,2
                """)
                print('************citeline df ready')
                df_citeline.write.mode("overwrite").saveAsTable("citeline_mesh")
    elif row['Data_Source'].lower() == "aact":
        if (row['Dataset_Name'].lower() == "aact_conditions"):
            count_aact += 1

            aact_conditions_disease = spark.sql("""SELECT DISTINCT nct_id,'aact' as source,regexp_replace(name,'\\\"','\\\'')  AS  mesh_name
            from default.aact_conditions_data where name is not null and  trim(name)!=''
            """)
            aact_conditions_disease.registerTempTable('aact_conditions_disease')
            if count_aact == 3:
                df_aact = spark.sql("""select distinct 'aact' as source, a.mesh_name
                from aact_conditions_disease a left join  (select distinct nct_id from aact_browse_interventions_disease union select distinct nct_id from aact_browse_conditions_disease) b on lower(trim(a.nct_id))=lower(trim(b.nct_id)) where b.nct_id is null group by 1,2
                union
                select distinct 'aact' as source,mesh_name
                from aact_browse_conditions_disease group by 1,2
                union
                select distinct 'aact' as source,mesh_name
                from aact_browse_interventions_disease group by 1,2
                """)
                print('************aact df ready')

                df_aact.write.mode("overwrite").saveAsTable("aact_mesh")

        if (row['Dataset_Name'].lower() == "aact_browse_interventions"):
            count_aact += 1

            aact_browse_interventions_disease = spark.sql("""SELECT DISTINCT nct_id,'aact' as source,mesh_term as mesh_name
            from default.aact_browse_interventions_data where mesh_term is not null and trim(mesh_term)!=''
            """)
            aact_browse_interventions_disease.registerTempTable('aact_browse_interventions_disease')
            if count_aact == 3:
                df_aact = spark.sql("""select distinct 'aact' as source, a.mesh_name
                from aact_conditions_disease a left join  (select distinct nct_id from aact_browse_interventions_disease union select distinct nct_id from aact_browse_conditions_disease) b on lower(trim(a.nct_id))=lower(trim(b.nct_id)) where b.nct_id is null group by 1,2
                union
                select distinct 'aact' as source,mesh_name
                from aact_browse_conditions_disease group by 1,2
                union
                select distinct 'aact' as source,mesh_name
                from aact_browse_interventions_disease group by 1,2
                """)
                print('************aact df ready')

                df_aact.write.mode("overwrite").saveAsTable("aact_mesh")

        elif (row['Dataset_Name'].lower() == "aact_browse_conditions"):
            count_aact += 1
            aact_browse_conditions_disease = spark.sql("""SELECT DISTINCT nct_id, 'aact' as source,mesh_term as mesh_name
            from default.aact_browse_conditions_data where mesh_term is not null and trim(mesh_term)!=''
            """)

            aact_browse_conditions_disease.registerTempTable('aact_browse_conditions_disease')
            if count_aact == 3:
                df_aact = spark.sql("""select distinct 'aact' as source, a.mesh_name
                from aact_conditions_disease a left join  (select distinct nct_id from aact_browse_interventions_disease union select distinct nct_id from aact_browse_conditions_disease) b on lower(trim(a.nct_id))=lower(trim(b.nct_id)) where b.nct_id is null group by 1,2
                union
                select distinct 'aact' as source,mesh_name
                from aact_browse_conditions_disease group by 1,2
                union
                select distinct 'aact' as source,mesh_name
                from aact_browse_interventions_disease group by 1,2
                """)
                print('************aact df ready')

                df_aact.write.mode("overwrite").saveAsTable("aact_mesh")

    else:
        query2 = "select '{datasource}' as source,{column_name} as mesh_name from " \
                 "default.{source}_data group by 1,2 ".format(source=row['Dataset_Name'].lower(),
                                                              column_name=row['Column_Name'],
                                                              datasource=row['Data_Source'].lower())
        data = spark.sql(query2)
        data.write.mode("overwrite").saveAsTable(row['Data_Source'] + '_mesh')
    if i == 0 and row['Data_Source'].lower() != "citeline" and row['Data_Source'].lower() != "aact":
        query3 = "SELECT source, mesh_name FROM default." + row['Data_Source'] + "_mesh "
        print("inside if i:", i, query3)

    elif i == 0 and ((row['Data_Source'].lower() == "citeline" and count_citeline == 2) or (
            row['Data_Source'].lower() == "aact" and count_aact == 3)):
        query3 = "SELECT source, mesh_name FROM default." + row['Data_Source'] + "_mesh "
        print("inside if i:", i, query3)
    elif i >= 1 and ((count_citeline == 2 and row['Data_Source'].lower() == "citeline") or (
            count_aact == 3 and row['Data_Source'].lower() == "aact")):
        query3 = query3 + " union SELECT source, mesh_name FROM default." + row['Data_Source'] + "_mesh"
        print("inside else i:", i, query3)
    elif i >= 1 and row['Data_Source'].lower() != "citeline" and row['Data_Source'].lower() != "aact":
        query3 = query3 + " union SELECT source, mesh_name FROM default." + row['Data_Source'] + "_mesh"
        print("inside else i:", i, query3)
    i += 1
    print("outside if else", query3)
    df_union = spark.sql(query3)
    df_union.write.mode("overwrite").saveAsTable("df_union_mesh")
    print("after union", i)
    if counter == i:
        print("final", row['Data_Source'])
        # df_union.write.mode("overwrite").saveAsTable("df_union")
        delta_records = spark.sql("""select datasource,trim(mesh_name) as disease_name  from (select a.source as datasource,trim(a.mesh_name) as mesh_name 
            from  default.df_union_mesh a left join
            (select mesh as mesh_name from mesh_mapping where mesh is not null and trim(mesh) <>'')   b on
            lower(trim(a.mesh_name)) =lower(trim(b.mesh_name)) where ( b.mesh_name  is null )) where mesh_name is not null and lower(trim(mesh_name))<>''
            """)
        delta_records.registerTempTable('delta_records')
        delta_records.write.mode("overwrite").saveAsTable("delta_records")
        if (delta_records.count() == 0):
            print("No Delta")
            email_delta("No Delta",
                        "Environment: {env} | [Update] No Action Required In Mesh Mapping".format(env=env))
        else:
            pandas_df = delta_records.toPandas()
            pandas_df.to_excel('delta.xlsx', index=False)
            os.system(
                "hadoop fs -put " + CommonConstants.AIRFLOW_CODE_PATH + "/delta.xlsx /user/hive/warehouse/Delta")
            dis_std = DiseaseStandardizationProposed()
            dis_std.main()
            d_map_path = (
                    bucket_path + "/uploads/FINAL_MESH_MAPPING/temp_mesh_mapping_folder/delt_mesh_mapping_temp.xlsx")
            print(d_map_path)
            # test=pd.read_csv('disease_mapping.csv')
            # test=test.astype(str)
            # disease_mapping=spark.createDataFrame(test)
            mesh_mapping_new = spark.read.format('com.crealytics.spark.excel').option('header', 'true').load(d_map_path)
            mesh_mapping_new.registerTempTable("mesh_mapping_new")
            delta_new = spark.sql("""select a.datasource,lower(trim(disease_name)) as mesh_name,coalesce(b.proposed_mesh,'other') as proposed_mesh,similarity from delta_records a left join 
            (select proposed_mesh,mesh_name, similarity, datasource from mesh_mapping_new ) b on
            lower(trim(a.disease_name)) =lower(trim(b.mesh_name)) and lower(trim(a.datasource))=lower(trim(b.datasource))
            group by 1,2,3,4""")
            delta_new.registerTempTable('delta_new')
            pandas_df = delta_new.toPandas()
            pandas_df.to_excel('disease_delta.xlsx', index=False)

            mesh_union_code_path = CommonConstants.AIRFLOW_CODE_PATH + '/union_mesh.xlsx'
            if os.path.exists(mesh_union_code_path):
                os.system('rm ' + mesh_union_code_path)

            # Save the xlsx file in pre-ingestion folder having similarity>=0.8
            mesh_union = spark.sql(
                """select lower(trim(mesh_name)) as mesh, proposed_mesh as standard_mesh from mesh_mapping_new where similarity >= 0.8""")
            mesh_union.registerTempTable('mesh_union')
            pandas_df_1 = mesh_union.toPandas()
            pandas_df_1.to_excel('union_mesh.xlsx', index=False)

            pre_ingetsion_mapping = "{bucket_path}/pre-ingestion/MAPPING/mesh_mapping/".format(bucket_path=bucket_path)

            CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d")
            file_copy_command = "aws s3 cp union_mesh.xlsx {}".format(
                os.path.join(pre_ingetsion_mapping, "mesh_mapping_" + CURRENT_DATE + ".xlsx"))

            print("Command to create mesh mapping file on s3 - {}".format(file_copy_command))

            file_copy_status = os.system(file_copy_command)
            if file_copy_status != 0:
                raise Exception("Failed to create mesh mapping file on s3 pre-ingestion having similarity >= 0.8")
            # delta_new.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('sep', ',') \
            #    .save('/user/hive/warehouse/Delta_disease')
            # os.system(
            #    "hadoop fs -copyToLocal /user/hive/warehouse/Delta_disease " + CommonConstants.AIRFLOW_CODE_PATH + "/")
            # os.system("mv "
            #         + CommonConstants.AIRFLOW_CODE_PATH
            #          + "/Delta_disease/*.csv "
            #          + CommonConstants.AIRFLOW_CODE_PATH
            #          + "/disease_delta.csv"
            #          )
            # disease_csv = pd.read_csv("disease_delta.csv")
            # disease_csv.to_excel("disease_delta.xlsx", index=False)
            email_delta("mesh",
                        "Environment: {env} | [URGENT] Update Delta Records In Mesh Mapping".format(env=env))
            print("delta")

