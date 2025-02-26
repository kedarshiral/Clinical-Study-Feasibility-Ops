import os
import boto3
import sys
import json
import pandas as pd, numpy as np
from datetime import datetime
import re
import traceback
import builtins as py_builtin
import ast
# import findspark
# findspark.init("/usr/lib/spark/")
from pyspark.sql.functions import year
from pyspark.sql.functions import month

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.functions import lit

from NotificationUtility import NotificationUtility
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import CommonConstants as CommonConstants
from PySparkUtility import PySparkUtility
from CommonUtils import CommonUtils
from pyspark.sql import *
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
import MySQLdb

spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)
    

class DCTMatcher(object):

    def __init__(self):
        self.destination_source = "/user/hadoop/dct_output"
        self.df_intervention = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_interventions """)
        self.df_study = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies """)
        self.df_outcome_measure = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_outcome_measurements """)
        self.df_eligibilities = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_eligibilities """)
        self.df_sponsors = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_sponsors """)
        self.df_brief_summaries = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_brief_summaries """)
        self.df_detailed_description = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_detailed_descriptions """)
        self.df_provided_documents = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_provided_documents """)
        self.df_id_information = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_id_information """)
        self.df_design = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_designs """)
        self.df_facilities = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_facilities """)
        self.df_keywords = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_keywords """)
        self.df_spons = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_sponsors """)
        self.df_design_groups = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_design_groups """)
        self.df_design_outcomes = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_design_outcomes """)
        self.df_conditions = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_conditions """)
        self.df_outcomes1 = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_design_outcomes """)
        self.df_outcomes = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_outcomes """)
        self.df_sponsor_collaborator_mapping = spark.read.format("com.databricks.spark.csv").option("delimiter",",").option("header", "true").option("inferSchema", "true").load("{bucket_path}/uploads/DecentralizedClinicalTrials/Sponsor-Collaborator_Mapping.csv".format(bucket_path=bucket_path))
        self.df_dime_mapping = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").option("inferSchema", "true").load("{bucket_path}/uploads/DecentralizedClinicalTrials/DIME_NCT_ids.csv".format(bucket_path=bucket_path))
        self.df_result_group = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_result_groups """)
        self.df_disease = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").option("inferSchema", "true").load("{bucket_path}/uploads/DecentralizedClinicalTrials/conditions_to_disease_mapping.csv".format(bucket_path=bucket_path))
        self.df_ta = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").option("inferSchema", "true").load("{bucket_path}/uploads/DecentralizedClinicalTrials/ta_mapping.csv".format(bucket_path=bucket_path))
        self.df_dct_keywords = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").option("inferSchema", "true").load("{bucket_path}/uploads/DecentralizedClinicalTrials/dct_keywords.csv".format(bucket_path=bucket_path))
        self.df_dct_keywords = self.df_dct_keywords.withColumn('keywords_list',f.split(f.col('keywords'),','))
        self.dct_category = self.df_dct_keywords.select('dct_category','keywords_list').rdd.collectAsMap()
        self.df_exclude_list = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").option("inferSchema", "true").load("{bucket_path}/uploads/DecentralizedClinicalTrials/Exclude_NCT_ID_List.csv".format(bucket_path=bucket_path))
        self.df_dct_parameters = spark.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header","true").option("inferSchema", "true").load("{bucket_path}/uploads/DecentralizedClinicalTrials/dct_parameters.csv".format(bucket_path=bucket_path))

        parameter_name = self.df_dct_parameters.select('parameter_name').rdd.flatMap(lambda x: x).collect()
        value = self.df_dct_parameters.select('value').rdd.flatMap(lambda x: x).collect()

        self.dct_parameters_dict = {parameter_name[i]:value[i] for i in range(len(parameter_name))}
        self.df_dct_include_list = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header","true").option("inferSchema", "true").load("{bucket_path}/uploads/DecentralizedClinicalTrials/dct_include_list.txt".format(bucket_path=bucket_path))
    
    def check_categories(self, df_dataset, filter_column):

        try:
            def is_similar(column_check, category, category_list):
                try:
                    count = 0
                    category_list = category_list.split(',')
                    for s in category_list:
                        regex = re.compile(
                            "([^a-zA-Z]{0}[^a-zA-Z])|(^{0}$)|(^{0}[^a-zA-Z])|([^a-zA-Z]{0}$)".format(s.lower()))
                        if column_check:
                            if regex.search(column_check.lower()):
                                count = count + 1
                                return True
                            continue
                    else:
                        return False
                except Exception as e:
                    print(" Error in is_similar is", str(e), traceback.format_exc())

            categories_list = list(self.dct_category.keys())
            column_search_udf = udf(lambda row, category, category_list : is_similar(row, category, category_list), BooleanType())

            ##### column search UDF #################
            df_temp = spark.createDataFrame([], StructType([]))
            for each_category in categories_list:
                df_dataset = df_dataset.withColumn(each_category,
                                                   column_search_udf(f.col(filter_column), f.lit(each_category), f.lit(','.join(self.dct_category[each_category]))))
            categories_list.append('nct_id')
            df_temp = df_dataset.select(categories_list)
            return df_temp

        except Exception as e:
            print(" Error in check_categories is", str(e), traceback.format_exc())
            raise e

    def send_email(self, attachment_file_name=None):
        delta_env=configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "env"])
        server_host="10.121.0.176"
        server_port="25"
        print(configuration,delta_env,server_host,server_port)
        #email_recipient_list = configuration.get_configuration(
        #                [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_recipient_list"])
        email_recipient_list="sushant.patil@zs.com,sankarshana.kadambari@zs.com,prakhar.mishra@zs.com"
        print(email_recipient_list)
        sender_email = "zaidyn.studyfeasibility@zs.com"
        #recipient_list = ", ".join(email_recipient_list)
        recipient_list=email_recipient_list
        print("recipient_list:",recipient_list)
        try:
            # Create a multipart message and set headers
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] =recipient_list
            msg["Subject"] = "Decentralized Clinical Trials Output | First Quarter 2023"
            body="Hello Team ,\n\nPlease find the attached Decentralized Clinical Trials output for this quarter. Please reach out to Prakhar/Sushant Patil for further queries. \n\nRegards,\nZaidyn Study Feasibility  Team\nClinical Development Excellence"
            msg.attach(MIMEText(body))
            if (attachment_file_name):
                att = MIMEApplication(open(attachment_file_name, 'rb').read())
                att.add_header('Content-Disposition', 'attachment', filename=attachment_file_name)
                msg.attach(att)

            server = smtplib.SMTP(host=server_host, port=server_port)
            server.connect(server_host, server_port)
            server.starttls()
            server.send_message(msg)
            server.quit()
        except Exception as E:
            print("Failed with Exception:",E)
            raise E

    def main_matcher(self):
        try:
            
            self.df_intervention = self.df_intervention.fillna('', subset=['intervention_type', 'name', 'description'])

            self.df_intervention = self.df_intervention.withColumn('intervention_description',
                                                                   f.concat_ws("|", f.col('intervention_type'),
                                                                               f.col('name'), f.col('description')))
            self.df_intervention = self.df_intervention.withColumn("intervention_description",
                                                                   f.col("intervention_description").cast(StringType()))
            self.df_intervention_grp = self.df_intervention.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_intervention.intervention_description)).alias(
                    "intervention_description"))
            self.df_intervention_grp = self.df_intervention_grp.dropDuplicates(['nct_id'])

            ##################### 2 #########
            self.df_study = self.df_study.fillna('', subset=['brief_title', 'official_title'])
            self.df_study = self.df_study.withColumn('title',
                                                     f.concat_ws("|", f.col('brief_title'), f.col('official_title')))
            self.df_study = self.df_study.withColumn("title", f.col("title").cast(StringType()))
            self.df_study_grp = self.df_study.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_study.title)).alias("title"))
            self.df_study_grp = self.df_study_grp.dropDuplicates(['nct_id'])

            #################### 3 ###############

            self.df_outcome_measure = self.df_outcome_measure.fillna('', subset=['title', 'description'])
            self.df_outcome_measure = self.df_outcome_measure.withColumn('outcome_description',
                                                                         f.concat_ws("|", f.col('title'),
                                                                                     f.col('description')))
            self.df_outcome_measure = self.df_outcome_measure.withColumn("outcome_description",
                                                                         f.col("outcome_description").cast(
                                                                             StringType()))

            self.df_outcome_measure_grp = self.df_outcome_measure.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_outcome_measure.outcome_description)).alias(
                    "outcome_description"))
            self.df_outcome_measure_grp = self.df_outcome_measure_grp.dropDuplicates(['nct_id'])

            ################## 4 ####################

            self.df_eligibilities = self.df_eligibilities.fillna('', subset=['population', 'criteria'])
            self.df_eligibilities = self.df_eligibilities.withColumn('eligibility',
                                                                     f.concat_ws("|", f.col('population'),
                                                                                 f.col('criteria')))
            self.df_eligibilities = self.df_eligibilities.withColumn("eligibility",
                                                                     f.col("eligibility").cast(StringType()))
            self.df_eligibilities_grp = self.df_eligibilities.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_eligibilities.eligibility)).alias("eligibility"))
            #                                                               how='left')
            self.df_eligibilities_grp = self.df_eligibilities_grp.dropDuplicates(['nct_id'])

            ################## 5 ####################

            self.df_brief_summaries = self.df_brief_summaries.fillna('', subset=['description'])

            self.df_brief_summaries = self.df_brief_summaries.withColumn("description",
                                                                         f.col("description").cast(StringType()))
            self.df_brief_summaries_grp = self.df_brief_summaries.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_brief_summaries.description)).alias("description"))
            #                                                           how='left')
            self.df_brief_summaries_grp = self.df_brief_summaries_grp.dropDuplicates(['nct_id'])

           ################## 6 ####################
            self.df_detailed_description = self.df_detailed_description.fillna('', subset=['description'])

            self.df_detailed_description = self.df_detailed_description.withColumn("description",
                                                                                   f.col("description").cast(
                                                                                       StringType()))
            self.df_detailed_description_grp = self.df_detailed_description.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_detailed_description.description)).alias("description"))

            self.df_detailed_description_grp = self.df_detailed_description_grp.dropDuplicates(['nct_id'])

            ################# 7 ####################

            self.df_keywords = self.df_keywords.fillna('', subset=['downcase_name'])

            self.df_keywords_grp = self.df_keywords.groupby("nct_id").agg(f.concat_ws("|", f.collect_list(self.df_keywords.DOWNCASE_NAME)).alias("keywords_list"))

            self.df_keywords_grp = self.df_keywords_grp.dropDuplicates(['nct_id'])

            ##### tried ##########

            self.df_spons = self.df_spons.fillna('', subset=['name'])
            self.df_spons_grp = self.df_spons.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_spons.NAME)).alias("name_list"))

            self.df_spons_grp = self.df_spons_grp.dropDuplicates(['nct_id'])

            self.df_design_groups = self.df_design_groups.fillna('', subset=['title', 'description'])
            self.df_design_groups = self.df_design_groups.withColumn('Arms_list', f.concat_ws("|", f.col('title'),
                                                                                              f.col('description')))

            self.df_design_groups_grp = self.df_design_groups.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_design_groups.Arms_list)).alias("Arms_list"))

            self.df_design_groups_grp = self.df_design_groups_grp.dropDuplicates(['nct_id'])

            self.df_design_outcomes = self.df_design_outcomes.fillna('', subset=['measure', 'description'])
            self.df_design_outcomes = self.df_design_outcomes.withColumn('design_outcomes',
                                                                         f.concat_ws("|", f.col('measure'),
                                                                                     f.col('description')))

            self.df_design_outcomes_grp = self.df_design_outcomes.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_design_outcomes.design_outcomes)).alias("design_outcomes"))
            df_design_outcomes_grp_cols = self.df_design_outcomes.columns
            df_design_outcomes_grp_cols.remove('design_outcomes')
            self.df_design_outcomes_joined = self.df_design_outcomes_grp.join(
                self.df_design_outcomes[df_design_outcomes_grp_cols], on=['nct_id'], how='left')
            self.df_design_outcomes_joined = self.df_design_outcomes_joined.dropDuplicates(['nct_id'])

            ######## 8 #################

            list_tables = [[self.df_design_outcomes_joined, 'design_outcomes'],
                           [self.df_intervention_grp, 'intervention_description'],
                           [self.df_study_grp, 'title'],
                           [self.df_outcome_measure_grp, 'outcome_description'],
                           [self.df_eligibilities_grp, 'eligibility'],
                           [self.df_brief_summaries_grp, 'description'],
                           [self.df_detailed_description_grp, 'description'],
                           [self.df_keywords_grp, 'keywords_list'], [self.df_spons_grp, 'name_list'],
                           [self.df_design_groups_grp, 'Arms_list']]

            print("Step 1 Completed")

            #### check this part later #############
            
            df_final_temp = spark.createDataFrame([], StructType([]))

            for i in list_tables:
                global count
                count = 0
                df_temp = self.check_categories(i[0], i[1])
                print(i[1] + ", Count : " + str(count))
                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                print("date and time =", dt_string)
                if df_final_temp.count() <= 0:
                    df_final_temp = df_temp
                else:
                    df_final_temp = df_final_temp.union(df_temp)
                

            df_final = df_final_temp.cache()
            df_final = df_final.filter(
                (df_final['Home health services'] == 'True') | (df_final['Home Sample collection'] == 'True') |
                (df_final['Medication Shipment'] == 'True') | (df_final['Wearable'] == 'True') | (
                        df_final['Chatbot'] == 'True') |
                (df_final['ePRO'] == 'True') | (df_final['eConsent/enrollment'] == 'True') |
                (df_final['Mobile Apps'] == 'True') | (df_final['Remote Monitoring'] == 'True') | (
                        df_final['Telemedicine'] == 'True') | (
                        df_final['Phone Calls'] == 'True') |
                (df_final['Social Media'] == 'True') | (df_final['eCOA'] == 'True') | (
                        df_final['Augmented reality'] == 'True') |
                (df_final['Digital Health Platform'] == 'True') | (df_final['Decentralized Clinical Trials'] == 'True'))
            #
            df_final = df_final.dropDuplicates()
            # ########## skip step ########
            dict_categories = {'Home health services': 'max', 'Home Sample collection': 'max',
                               'Medication Shipment': 'max',
                               'Wearable': 'max', 'Chatbot': 'max', 'ePRO': 'max', 'eConsent/enrollment': 'max',
                               'Mobile Apps': 'max', 'Remote Monitoring': 'max', 'Telemedicine': 'max', 'Phone Calls': 'max',
                               'Social Media': 'max',
                               'eCOA': 'max', 'Augmented reality': 'max', 'Digital Health Platform': 'max',
                               'Decentralized Clinical Trials': 'max'}
            renaming_dict = dict()
            for i_value, j_value in dict_categories.items():
                new_key_value = "max({})".format(i_value)
                rename_value = i_value
                renaming_dict[new_key_value] = rename_value

            #
            #
            df_final = df_final.groupby(['nct_id']).agg(dict_categories)

            for original_name, new_name in renaming_dict.items():
                df_final = df_final.withColumnRenamed(original_name, new_name)

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate1".format(self.destination_source))

            df_final = df_final.join(self.df_study[
                                         ["nct_id", "brief_title", "acronym", "overall_status", "phase", "study_type",
                                          "enrollment",
                                          "start_date",
                                          "primary_completion_date", "completion_date",
                                          "study_first_posted_date", "results_first_posted_date",
                                          "last_update_posted_date"]],
                                     on=['nct_id'], how='left')

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate2".format(self.destination_source))

            self.df_conditions_1 = self.df_conditions.withColumnRenamed("name", "Condition")
            self.df_conditions_1 = self.df_conditions_1.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_conditions_1.Condition)).alias("Condition"))
            self.df_conditions_1 = self.df_conditions_1.dropDuplicates(['nct_id', 'Condition'])
            self.df_conditions_1 = self.df_conditions_1.select(['nct_id', 'Condition'])

            #
            df_final = df_final.join(self.df_conditions_1, on=['nct_id'], how='left')
            #
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate3".format(self.destination_source))
            # #  # #
            #  # # ###################
            self.df_outcomes1 = self.df_outcomes1.withColumn("measure", f.col("measure").cast(StringType()))
            self.df_outcomes1 = self.df_outcomes1.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_outcomes1.measure)).alias("measure"))
            self.df_outcomes1 = self.df_outcomes1.dropDuplicates(['nct_id', 'measure'])
            self.df_outcomes1 = self.df_outcomes1.withColumnRenamed("measure", "Outcome Measures")
            self.df_outcomes1 = self.df_outcomes1.select(['nct_id', 'Outcome Measures'])
            #
            self.df_outcomes = self.df_outcomes.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_outcomes.TITLE)).alias("title"))
            self.df_outcomes = self.df_outcomes.dropDuplicates(['nct_id', 'title'])
            self.df_outcomes = self.df_outcomes.withColumnRenamed("title", "Outcome Measures")
            self.df_outcomes = self.df_outcomes.select(['nct_id', 'Outcome Measures'])
            # #
            # # ###### check #####
            df1 = self.df_outcomes.union(self.df_outcomes1)
            # # #################
            df1 = df1.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(df1['Outcome Measures'])).alias("Outcome Measures"))
            df1 = df1.dropDuplicates(['nct_id', 'Outcome Measures'])
            df1 = df1.select(['nct_id', 'Outcome Measures'])
            df_final = df_final.join(df1, on=['nct_id'], how='left')
            #
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate4".format(self.destination_source))
            #

            self.df_sponsors = self.df_sponsors.withColumn("name", f.col("name").cast(StringType()))
            df_sponsors_grp_1 = self.df_sponsors.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_sponsors.name)).alias("Sponsor/Collaborators"))

            self.df_sponsors = self.df_sponsors.withColumnRenamed("agency_class", "Funded Bys")
            self.df_sponsors = self.df_sponsors.fillna('', subset=['Funded Bys'])

            df_sponsors_grp_2 = self.df_sponsors.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_sponsors['Funded Bys'])).alias("Funded Bys"))
                
            df_sponsors_grp_3 = self.df_sponsors.withColumn("Sponsor/Collaborator (Pharma/MedTech)",f.when((self.df_sponsors['Funded Bys'].contains(self.dct_parameters_dict['sponsor_type_filter'])),self.df_sponsors['name']).otherwise(lit("")))
            
            df_sponsors_grp_3 = df_sponsors_grp_3.select(['nct_id', 'Sponsor/Collaborator (Pharma/MedTech)']).join(self.df_sponsor_collaborator_mapping[['Sponsor/Collaborator (Pharma/MedTech)','Parent Sponsor/Collaborator', 'Parent Company (short name)', 'Top 20 Pharma']], on=['Sponsor/Collaborator (Pharma/MedTech)'], how='left')
            
            df_sponsors_grp_3 = df_sponsors_grp_3.fillna('', subset=['Parent Sponsor/Collaborator', 'Parent Company (short name)', 'Top 20 Pharma'])
            df_sponsors_grp_3 = df_sponsors_grp_3.groupby("nct_id").agg(f.concat_ws("|", f.collect_list(f.col('Sponsor/Collaborator (Pharma/MedTech)'))).alias("Sponsor/Collaborator (Pharma/MedTech)"), f.concat_ws("|", f.collect_list(f.col('Parent Sponsor/Collaborator'))).alias("Parent Sponsor/Collaborator"), f.concat_ws("|", f.collect_list(f.col('Parent Company (short name)'))).alias("Parent Company (short name)"), f.concat_ws("|", f.collect_list(f.col('Top 20 Pharma'))).alias("Top 20 Pharma"))

            df_sponsors_combined = df_sponsors_grp_1.join(df_sponsors_grp_2, on=['nct_id'], how='left')
            df_sponsors_combined = df_sponsors_combined.join(df_sponsors_grp_3, on=['nct_id'], how='left')
            
            df_final = df_final.join(df_sponsors_combined, on=['nct_id'], how='left')
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate5".format(self.destination_source))

            self.df_intervention = self.df_intervention.withColumn('Intervention',
                                                                   f.concat_ws(":", f.col('intervention_type'),
                                                                               f.col('name')))

            self.df_intervention = self.df_intervention.withColumn("Intervention",
                                                                   f.col("Intervention").cast(StringType()))

            self.df_intervention = self.df_intervention.fillna('', subset=['Intervention'])
            self.df_intervention_grp_1 = self.df_intervention.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_intervention.Intervention)).alias("Intervention"))

            df_intervention_cols = self.df_intervention.columns

            df_intervention_cols.remove('Intervention')
            self.df_intervention_joined = self.df_intervention_grp_1.join(
                self.df_intervention[df_intervention_cols], on=['nct_id'], how='left')

            self.df_intervention_joined = self.df_intervention_joined.dropDuplicates(['nct_id', 'Intervention'])
            df_final = df_final.join(self.df_intervention_joined[['nct_id', 'Intervention']],
                                     on=['nct_id'], how='left')

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate6".format(self.destination_source))

            df_final = df_final.fillna('-',
                                       subset=['Sponsor/Collaborator (Pharma/MedTech)', 'Parent Sponsor/Collaborator',
                                               'Parent Company (short name)', 'Top 20 Pharma'])

            self.df_intervention_1 = df_final.select(['nct_id']).join(self.df_intervention[['nct_id', 'intervention_type']],
                                     on=['nct_id'], how='left')
            df_final_intervention_intermediate = self.df_intervention_1.crosstab('nct_id', 'intervention_type')
            df_final_intervention_intermediate = df_final_intervention_intermediate.drop('null')
            df_final_intervention_intermediate = df_final_intervention_intermediate.withColumnRenamed(
                'nct_id_intervention_type',
                'nct_id')
            df_final_intervention_intermediate_cols = df_final_intervention_intermediate.columns
            df_final_intervention_intermediate_cols.remove('nct_id')
            for i in df_final_intervention_intermediate_cols:
                df_final_intervention_intermediate = df_final_intervention_intermediate.withColumn(i, f.when(
                    (f.col(i)>0), 'Yes').otherwise(''))

            df_final_intervention_intermediate.write.format("csv").mode('overwrite').option("header", True).save(
                "{}/crosstab2".format(self.destination_source))

            df_final = df_final.join(df_final_intervention_intermediate, on=['nct_id'], how='left')
            self.df_eligibilities = self.df_eligibilities.fillna('', subset=['minimum_age', 'maximum_age'])

            self.df_eligibilities = self.df_eligibilities.withColumn('minimum_age',
                                                                     f.regexp_replace('minimum_age', 'N/A', ''))
            self.df_eligibilities = self.df_eligibilities.withColumn('maximum_age',
                                                                     f.regexp_replace('maximum_age', 'N/A', ''))

            get_age_udf = udf(lambda x: '{} to {} '.format(x[0], x[1]) if (x[1] != '' and x[0] != '') else (
                '{} and older'.format(x[0]) if x[0] != '' else (
                    'upto {}'.format(x[1]) if x[1] != '' else "Child, Adult, Older Adult")), StringType())
            #
            self.df_eligibilities = self.df_eligibilities.withColumn('Age',
                                                                     get_age_udf(f.array('minimum_age', 'maximum_age')))

            #
            self.df_eligibilities = self.df_eligibilities.dropDuplicates(["nct_id", "gender", "Age"])

            df_final = df_final.join(self.df_eligibilities[["nct_id", "gender", "Age"]],
                                     on=['nct_id'], how='left')

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate7".format(self.destination_source))

            self.df_facilities = self.df_facilities.select(
                *(f.col(c).cast("string").alias(c) for c in ['nct_id', 'name', 'city', 'state', 'zip', 'country']))

            self.df_facilities = self.df_facilities.withColumn('Locations',
                                                               f.concat_ws(",", f.col('name'), f.col('city'),
                                                                           f.col('state'), f.col('zip'), f.col(
                                                                       'country')))
            #
            self.df_facilities_grp = self.df_facilities.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_facilities['Locations'])).alias("Locations"))

            #
            self.df_facilities_grp = self.df_facilities_grp.dropDuplicates(["nct_id", "Locations"])
            df_final = df_final.join(self.df_facilities_grp[["nct_id", "Locations"]],
                                     on=['nct_id'], how='left')

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate8".format(self.destination_source))

            self.df_provided_documents = self.df_provided_documents.withColumn('Study Documents',
                                                                               f.concat_ws(",", f.col('document_type'),
                                                                                           f.col('url')))
            #
            self.df_provided_documents_grp = self.df_provided_documents.groupby("nct_id").agg(
                f.concat_ws("|", f.collect_list(self.df_provided_documents['Study Documents'])).alias(
                    "Study Documents"))

            self.df_provided_documents_grp = self.df_provided_documents_grp.dropDuplicates(
                ["nct_id", "Study Documents"])
            df_final = df_final.join(self.df_provided_documents_grp[["nct_id", "Study Documents"]],
                                     on=['nct_id'], how='left')

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate9".format(self.destination_source))
            #
            self.df_design = self.df_design.fillna('')
            #
            get_allocation_udf = udf(lambda x: ("Allocation" + ': ' + x) if (x != '') else x, StringType())
            get_intervention_model_udf = udf(lambda x: ("Intervention Model" + ': ' + x) if (x != '') else x,
                                             StringType())
            get_observational_model_udf = udf(lambda x: ("Observational Model" + ': ' + x) if (x != '') else x,
                                              StringType())
            get_primary_purpose_udf = udf(lambda x: ("Primary Purpose" + ': ' + x) if (x != '') else x, StringType())
            get_time_perspective_udf = udf(lambda x: ("Time Perspective" + ': ' + x) if (x != '') else x, StringType())
            get_masking_udf = udf(lambda x: ("Masking" + ': ' + x) if (x != '') else x, StringType())
            #
            self.df_design = self.df_design.withColumn('allocation', get_allocation_udf(f.col('allocation')))
            self.df_design = self.df_design.withColumn('intervention_model',
                                                       get_intervention_model_udf(f.col('intervention_model')))
            self.df_design = self.df_design.withColumn('observational_model',
                                                       get_observational_model_udf(f.col('observational_model')))
            self.df_design = self.df_design.withColumn('primary_purpose',
                                                       get_primary_purpose_udf(f.col('primary_purpose')))
            self.df_design = self.df_design.withColumn('time_perspective',
                                                       get_time_perspective_udf(f.col('time_perspective')))
            self.df_design = self.df_design.withColumn('masking', get_masking_udf(f.col('masking')))

            #
            # ######## check these UDFs ########
            def concat_design_cols(row):
                temp_str = ""
                temp_str_list = [row[i] for i in range(len(row)) if row[i] != '']
                if temp_str_list:
                    temp_str = "|".join(temp_str_list)
                #
                return temp_str

            #
            concat_cols_udf = udf(lambda row: concat_design_cols(row), StringType())
            # #
            self.df_design = self.df_design.withColumn('Study Designs', concat_cols_udf(
                f.array('allocation', 'intervention_model', 'observational_model', 'primary_purpose',
                        'time_perspective',
                        'masking')))
            #
            self.df_design = self.df_design.dropDuplicates(["nct_id", "Study Designs"])
            df_final = df_final.join(self.df_design[["nct_id", "Study Designs"]], on=['nct_id'], how='left')
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate10".format(self.destination_source))

            self.df_id_information = self.df_id_information.withColumnRenamed("id_value", "Other IDs")
            #
            self.df_id_information_grp = self.df_id_information.groupby("nct_id").agg(f.concat_ws("|", f.collect_list(self.df_id_information['Other IDs'])).alias("Other IDs"))

            self.df_id_information_grp = self.df_id_information_grp.dropDuplicates(["nct_id", "Other IDs"])
            df_final = df_final.join(self.df_id_information_grp[["nct_id", "Other IDs"]], on=['nct_id'], how='left')

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate11".format(self.destination_source))
            
            df_final = df_final.filter(df_final['Funded Bys'].contains(self.dct_parameters_dict['sponsor_type_filter']))

            df_final = df_final.withColumn('start_date', f.to_date(f.col("start_date"), "yyyy-mm-dd")).withColumn('Year', f.year(f.col('start_date'))).filter(df_final.start_date > (self.dct_parameters_dict['start_date_filter'])).filter(df_final.start_date < datetime.now().strftime("%Y-%m-%d"))
            df_final = df_final.withColumn('start_date', f.date_format(f.col('start_date'), "d-MMM-yy")) \
                .withColumn('primary_completion_date', f.to_date(f.col("primary_completion_date"), "yyyy-mm-dd")) \
                .withColumn('primary_completion_date', f.date_format(f.col('primary_completion_date'), "d-MMM-yy")) \
                .withColumn('completion_date', f.to_date(f.col("completion_date"), "yyyy-mm-dd")) \
                .withColumn('completion_date', f.date_format(f.col('completion_date'), "d-MMM-yy")) \
                .withColumn('results_first_posted_date', f.to_date(f.col("results_first_posted_date"), "yyyy-mm-dd")) \
                .withColumn('results_first_posted_date', f.date_format(f.col('results_first_posted_date'), "d-MMM-yy")) \
                .withColumn('last_update_posted_date', f.to_date(f.col("last_update_posted_date"), "yyyy-mm-dd")) \
                .withColumn('last_update_posted_date', f.date_format(f.col('last_update_posted_date'), "d-MMM-yy"))

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate12".format(self.destination_source))
            self.df_ta = self.df_ta.withColumn('Therapy Area', f.regexp_replace('Therapy Area', '[^a-zA-Z0-9\s\&]', ''))
            self.df_ta = self.df_ta.withColumn('Therapy Area', f.upper(col("Therapy Area")))
            ta_columns = self.df_ta.select('Therapy Area').distinct().rdd.flatMap(lambda x: x).collect()

            ta_columns.sort()
                
            df_1 = df_final.select('nct_id').join(self.df_conditions, on=['nct_id'], how='left')
            df_1 = df_1.withColumn("name_lower", f.lower(df_1.NAME))
            self.df_ta = self.df_ta.withColumn("disease_lower", f.lower(self.df_ta.Disease))
            df_1 = df_1.join(self.df_ta, col('name_lower') == col('disease_lower'),'left')
            df_1 = df_1.na.drop(subset=['Therapy Area'])
            df_1 = df_1.withColumn("Therapy Area", f.col("Therapy Area").cast(StringType()))
            df_1 = df_1.select('nct_id','Therapy Area')
            df_1 = df_1.withColumn('Therapy Area', f.regexp_replace('Therapy Area', '[^a-zA-Z0-9\s\&]', ''))

            df = df_1.crosstab('nct_id','Therapy Area')
            df = df.drop('null')
            df = df.withColumnRenamed('nct_id_Therapy Area', 'nct_id')
            
            tas = df.columns
            tas.remove('nct_id')
            for i in tas:
                df = df.withColumn(i, f.when((f.col(i) > 0), "Yes").otherwise(""))
            df_final = df_final.join(df, on=['nct_id'], how='left')
            
            
            for each_col in ta_columns:
                if each_col not in df_final.columns:
                    df_final = df_final.withColumn(each_col, lit(""))
                    
            
            list_result_group = self.df_result_group.select("nct_id").rdd.flatMap(lambda x: x).collect()
                                               
            
            def study_result_map(nct):
                if nct in list_result_group:
                    return "Has Results"
                else:
                    return "No Results Available"
                    
            study_udf = udf(lambda x: study_result_map(x), StringType())
            df_final = df_final.withColumn('Study Results', study_udf(f.col('nct_id')))
            
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate13".format(self.destination_source))
            new_col_names = ['NCT Number', 'Title', 'Acronym', 'Status', 'Study Results', 'Condition', 'Interventions',
                             'Outcome Measures', 'Sponsor/Collaborators', 'Parent Company (short name)',
                             'Top 20 Pharma', 'Drug/ Biologic', 'Device', 'Behavioral',
                             'Diagnostic Test',
                             'Genetic', 'Procedure', 'Biological', 'Combination Product', 'Radiation', 'Dietary Supplement', 'Other','Gender', 'Age','Phases', 'Enrollment','Funded Bys','Study Type',
                             'Study Designs','Other IDs','Start Date','Year','Primary Completion Date','Completion Date','First Posted',
                             'Results First Posted','Last Update Posted','Locations','Study Documents','Home Health','At-Home Sample Collection',
                             'Medication Shipment','Chatbot', 'ePRO','eConsent', 'Mobile Apps','Telemedicine (excluding phone call)', 'Phone Calls', 'Remote Monitoring','Social Media', 'eCOA','Augmented reality','Wearables/ Biosensors','DCT (Broadly)','Digital Health Platform'] + ta_columns + ['Total count of DCT elements per clinical trial','URL', 'DiME (Yes/No)']

            col_map_dict = {'nct_id': 'NCT Number', 'brief_title': 'Title', 'acronym': 'Acronym',
                            'overall_status': 'Status',
                            'Study Results': 'Study Results', 'Condition': 'Condition', 'Intervention': 'Interventions',
                            'Outcome Measures': 'Outcome Measures', 'Sponsor/Collaborators': 'Sponsor/Collaborators',
                            'Drug': 'Drug/ Biologic',
                            'gender': 'Gender',
                            'phase': 'Phases', 'enrollment': 'Enrollment', 'Funded Bys': 'Funded Bys',
                            'study_type': 'Study Type', 'Study Designs': 'Study Designs', 'Other IDs': 'Other IDs',
                            'start_date': 'Start Date', 'primary_completion_date': 'Primary Completion Date',
                            'completion_date': 'Completion Date', 'study_first_posted_date': 'First Posted',
                            'results_first_posted_date': 'Results First Posted',
                            'last_update_posted_date': 'Last Update Posted', 'Locations': 'Locations',
                            'Study Documents': 'Study Documents', 'Home health services': 'Home Health',
                            'Home Sample collection': 'At-Home Sample Collection', 'Chatbot': 'Chatbot', 'ePRO': 'ePRO',
                            'eConsent/enrollment': 'eConsent', 'Mobile Apps': 'Mobile Apps',
                            'Telemedicine': 'Telemedicine (excluding phone call)',
                            'Remote Monitoring': 'Remote Monitoring', 'Social Media': 'Social Media', 'eCOA': 'eCOA',
                            'Augmented reality': 'Augmented reality', 'Wearable': 'Wearables/ Biosensors',
                            'Decentralized Clinical Trials': 'DCT (Broadly)',
                            'Digital Health Platform': 'Digital Health Platform'}

            for original_name, new_name in col_map_dict.items():
                df_final = df_final.withColumnRenamed(original_name, new_name)

            df_final = df_final.withColumn('Parent Company (short name)', f.regexp_replace('Parent Company (short name)', '(^\|*)|(\|*$)', ' '))
            df_final = df_final.withColumn('Top 20 Pharma', f.regexp_replace('Top 20 Pharma', '(^\|*)|(\|*$)', ' '))
            
            new_col_list = ['Home Health', 'At-Home Sample Collection', 'Medication Shipment', 'Chatbot', 'ePRO',
                            'eConsent', 'Mobile Apps',
                            'Telemedicine (excluding phone call)', 'Phone Calls', 'Remote Monitoring', 'Social Media', 'eCOA', 'Augmented reality',
                            'Wearables/ Biosensors', 'DCT (Broadly)', 'Digital Health Platform']

            df_final = df_final.fillna('', subset=new_col_list)
            for i in new_col_list:
                df_final = df_final.withColumn(i, f.when((f.col(i) == True), "Yes").otherwise(""))

            def count_dct_ele_trails(row):
                count = 0
                for i in range(len(row)):
                    if row[i] == 'Yes':
                        count += 1
                return count

            dime_nct_list = self.df_dime_mapping.select('NCT Number').rdd.flatMap(lambda x: x).collect()

            def dime_nct_map(nct):
                if nct in dime_nct_list:
                    return "Yes"
                else:
                    return ""

            dime_udf = udf(lambda x: dime_nct_map(x), StringType())
            count_dct_ele_trails_udf = udf(lambda row: count_dct_ele_trails(row), IntegerType())
            dct_url_udf = udf(lambda x: 'https://clinicaltrials.gov/ct2/show/' + x, StringType())
            #
            df_final = df_final.withColumn('Total count of DCT elements per clinical trial', count_dct_ele_trails_udf(
                f.array('Home Health', 'At-Home Sample Collection', 'Medication Shipment', 'Chatbot', 'ePRO',
                        'eConsent', 'Mobile Apps',
                        'Telemedicine (excluding phone call)', 'Phone Calls', 'Remote Monitoring', 'Social Media', 'eCOA', 'Augmented reality',
                        'Wearables/ Biosensors', 'DCT (Broadly)', 'Digital Health Platform')))
            df_final = df_final.withColumn('URL', dct_url_udf(f.col('NCT Number')))

            df_final = df_final.withColumn('DiME (Yes/No)', dime_udf(f.col('NCT Number')))

            col_ord_list = new_col_names
            df_final = df_final.select(col_ord_list)
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate14".format(self.destination_source))

            exclude_nct_list = self.df_exclude_list.select('NCT Number').rdd.flatMap(lambda x: x).collect()
            df_final = df_final.filter(~f.col('NCT Number').isin(exclude_nct_list))
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate15".format(self.destination_source))
            
            self.df_dct_include_list = self.df_dct_include_list.select(new_col_names)
            self.df_dct_include_list = self.df_dct_include_list.join(df_final,self.df_dct_include_list["NCT Number"] == df_final["NCT Number"],"leftanti")
            df_final = df_final.union(self.df_dct_include_list)
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate16".format(self.destination_source))
                
            pharma_sponsors_list_keywords = self.dct_parameters_dict['pharma_sponsors_list'].split("||")
            dtx_keywords = self.dct_parameters_dict['dtx_keywords'].split("||")
            
            def dtx_generator(search_text):
                for kw in dtx_keywords:
                    if kw.lower() in search_text.lower():
                        return "Yes"
                
                return ""

            dtx_udf = udf(dtx_generator, StringType())

            def ps_dct_generator(search_text):
                for kw in pharma_sponsors_list_keywords:
                    if kw.lower() in search_text.lower():
                        return "Yes"
                
                return ""

            ps_dct_udf = udf(ps_dct_generator, StringType())
            
            df_final = df_final.withColumn("DTx", dtx_udf(f.concat_ws("||", f.col("Title"), f.col("Condition"), f.col("Interventions"), f.col("Outcome Measures"), f.col("Sponsor/Collaborators"), f.col("Study Designs")))).withColumn("Pharma Sponsored DCTs", ps_dct_udf(f.col("Sponsor/Collaborators")))
            
            df_final = df_final.withColumn("Drug/ Biologic", f.when((df_final['Biological']=="Yes"),df_final['Biological']).otherwise(df_final['Drug/ Biologic']))
            
            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save(
                "{}/intermediate17".format(self.destination_source))
            
            # df_final = spark.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header","true").option("inferSchema", "true").load('{}/intermediate17/'.format(self.destination_source))

            now = datetime.now()
            process_id = now.strftime("%Y%m%d")

            def column_rename(column_mapping, df, rename_type=1):
                if rename_type==1:
                    source_col_list = list(column_mapping.keys())
                    target_col_list = tuple(list(column_mapping.values()))
                    df = df.select(source_col_list)
                    df = df.toDF(*target_col_list)
                elif rename_type==2:
                    source_col_list = list(column_mapping.values())
                    target_col_list = tuple(list(column_mapping.keys()))
                    df = df.select(source_col_list)
                    df = df.toDF(*target_col_list)
                return df

            column_mapping = {"NCT Number": "NCT_Number", "Title": "Title", "Acronym": "Acronym", "Status": "Status", "Study Results": "Study_Results", "Condition": "Condition", "Interventions": "Interventions", "Outcome Measures": "Outcome_Measures", "Sponsor/Collaborators": "Sponsor_Collaborators", "Parent Company (short name)": "Parent_Company_short_name", "Top 20 Pharma": "Top_20_Pharma", "Drug/ Biologic": "Drug_Biologic", "Device": "Device", "Behavioral": "Behavioral", "Diagnostic Test": "Diagnostic_Test", "Genetic": "Genetic", "Procedure": "Trial_Procedure", "Biological": "Biological", "Combination Product": "Combination_Product", "Radiation": "Radiation", "Dietary Supplement": "Dietary_Supplement", "Other": "Other", "Gender": "Gender", "Age": "Age", "Phases": "Phases", "Enrollment": "Enrollment", "Funded Bys": "Funded_Bys", "Study Type": "Study_Type", "Study Designs": "Study_Designs", "Other IDs": "Other_IDs", "Start Date": "Start_Date", "Year": "Year", "Primary Completion Date": "Primary_Completion_Date", "Completion Date": "Completion_Date", "First Posted": "First_Posted", "Results First Posted": "Results_First_Posted", "Last Update Posted": "Last_Update_Posted", "Locations": "Locations", "Study Documents": "Study_Documents", "Home Health": "Home_Health", "At-Home Sample Collection": "At_Home_Sample_Collection", "Medication Shipment": "Medication_Shipment", "Chatbot": "Chatbot", "ePRO": "ePRO", "eConsent": "eConsent", "Mobile Apps": "Mobile_Apps", "Telemedicine (excluding phone call)": "Telemedicine_excluding_phone_call", "Phone Calls": "Phone_Calls", "Remote Monitoring": "Remote_Monitoring", "Social Media": "Social_Media", "eCOA": "eCOA", "Augmented reality": "Augmented_reality", "Wearables/ Biosensors": "Wearables_Biosensors", "DCT (Broadly)": "DCT_Broadly", "Digital Health Platform": "Digital_Health_Platform", "ALLERGY": "ALLERGY", "AUTOIMMUNEIMMUNOLOGY": "AUTOIMMUNEIMMUNOLOGY", "CARDIOVASCULAR": "CARDIOVASCULAR", "DERMATOLOGY": "DERMATOLOGY", "ENDOCRINE": "ENDOCRINE", "ENTDENTAL": "ENTDENTAL", "GASTROENTEROLOGY NON INFLAMMATORY BOWEL DISEASE": "GASTROENTEROLOGY_NON_INFLAMMATORY_BOWEL_DISEASE", "HEMATOLOGY": "HEMATOLOGY", "INFECTIOUS DISEASE": "INFECTIOUS_DISEASE", "METABOLIC": "METABOLIC", "MUSCULOSKELETAL": "MUSCULOSKELETAL", "NEUROLOGY": "NEUROLOGY", "NOT SPECIFIED": "NOT_SPECIFIED", "OBSTETRICSGYNECOLOGY": "OBSTETRICSGYNECOLOGY", "ONCOLOGY": "ONCOLOGY", "OPHTHALMOLOGY": "OPHTHALMOLOGY", "ORTHOPEDICS": "ORTHOPEDICS", "PSYCHIATRY": "PSYCHIATRY", "RARE DISEASE": "RARE_DISEASE", "RENAL": "RENAL", "RESPIRATORY": "RESPIRATORY", "RHEUMATOLOGY NON AUTOIMMUNE": "RHEUMATOLOGY_NON_AUTOIMMUNE", "SURGERY": "SURGERY", "UROLOGY": "UROLOGY", "Total count of DCT elements per clinical trial": "Total_count_of_DCT_elements_per_clinical_trial", "URL": "URL", "DiME (Yes/No)": "DiME_Yes_No", "DTx": "DTx", "Pharma Sponsored DCTs": "Pharma_Sponsored_DCTs", "Created By": "Created_By", "Updated By": "Updated_By", "Created On": "Created_On", "Updated On": "Updated_On", "Process ID": "Process_ID", "Active Record": "Active_Record"}
            
            if "$$Prev_Data"=="Y":
                print("Executing query to get latest successful cycle_id")
                query = "select cycle_id,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = {process_id} order by cycle_start_time desc limit 1 ".format(orchestration_db_name=audit_db, process_id="5000")
                cursor.execute(query)
                fetch_enable_flag_result = cursor.fetchone()
                print("Query: "+str(query))
                print("Query Output: "+str(fetch_enable_flag_result))
                
                if len(fetch_enable_flag_result)==0:
                    raise Exception("There is no record of last successful run. Please set the '$$Prev_Data' flag to N and re-run the entire DAG.")
                if fetch_enable_flag_result['data_date']=="" or fetch_enable_flag_result['cycle_id']=="":
                    raise Exception("There is no record of last successful run. Please set the '$$Prev_Data' flag to N and re-run the entire DAG.")
                
                latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
                latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
                path = "{bucket_path}/applications/commons/dimensions/d_decentralized_clinical_trial/pt_data_dt={pt_data_dt}/pt_cycle_id={pt_cycle_id}/".format(bucket_path=bucket_path, pt_data_dt=latest_stable_data_date, pt_cycle_id=latest_stable_cycle_id)
                try:
                    df_old_output = spark.read.parquet(path)
                except Exception as E:
                    raise Exception("Could not read last run's Publish file. Please find the error caused: "+str(E))
                
                df_old_output = column_rename(column_mapping, df_old_output, 2)
                df_old_output = df_old_output.fillna('')
                df_old_output = df_old_output.withColumn("First Posted", f.col("First Posted").cast(DateType()))
                
                df_final = df_final.fillna('')
                df_final = df_final.withColumn("First Posted", f.col("First Posted").cast(DateType()))
                old_output_nct_list = df_old_output.select('NCT Number').rdd.flatMap(lambda x: x).collect()
                curr_output_nct_list = df_final.select('NCT Number').rdd.flatMap(lambda x: x).collect()
                common = list(set(curr_output_nct_list) & set(old_output_nct_list))
                new_curr = list(set(curr_output_nct_list) - set(old_output_nct_list))
                not_curr = list(set(old_output_nct_list) - set(curr_output_nct_list))
                # print("old_output_nct_list: ", len(old_output_nct_list))
                # print("curr_output_nct_list: ", len(curr_output_nct_list))
                # print("common: ", len(common))
                # print("new_curr: ", len(new_curr))
                # print("not_curr: ", len(not_curr))
                
                df = df_old_output.filter(f.col('NCT Number').isin(common))
                df1 = df_final.filter(f.col('NCT Number').isin(common))
                df = df.select(*df1.columns)
                df = df.orderBy(['NCT Number'], ascending = [True])
                df1 = df1.orderBy(['NCT Number'], ascending = [True])
                
                # print("old df: ", df.count())
                # print("new df: ", df1.count())
                # df.printSchema()
                # df1.printSchema()
                
                df2 = df.select(*[col(c).alias("x_"+c) for c in df.columns])
                df3 = df1.join(df2, col("NCT Number") == col("x_NCT Number"), "left")
                
                def CheckMatch(Column,r):
                    # check=''
                    check = []
                    ColList=Column.split(",")
                    for cc in ColList:
                        x = str(r[cc]).split('|')
                        while '' in x: x.remove('')
                        x.sort()
                        x = [i.strip() for i in x]
                        x = '|'.join(x)
                        
                        x1 = str(r["x_" + cc]).split('|')
                        while '' in x1: x1.remove('')
                        x1.sort()
                        x1 = [i.strip() for i in x1]
                        x1 = '|'.join(x1)
                        if(x != x1):
                            # check=check + "," + cc
                            temp = []
                            temp.append(cc)
                            temp.append(r[cc])
                            temp.append(r["x_" + cc])
                            temp.append(x)
                            temp.append(x1)
                            check.append(temp)
                    # return check.replace(',','',1).split(",")
                    return check
                
                CheckMatchUDF = udf(CheckMatch, ArrayType(ArrayType(StringType())))
                finalCol = df1.columns
                finalCol.insert(len(finalCol), "column_names")
                df4 = df3.withColumn("column_names", CheckMatchUDF(lit(','.join(df1.columns)),struct([df3[x] for x in df3.columns])))
                
                df4 = df4.select(*['NCT Number','column_names'])
                
                df4 = df4.withColumn("Count", f.size(f.col('column_names')))
                df4.show(truncate=False)
                df_old_output.printSchema()
                df_final.printSchema()
                comm_updated = df4.filter(f.col('Count')>0).select('NCT Number').rdd.flatMap(lambda x: x).collect()
                comm_not_updated = df4.filter(f.col('Count')<=0).select('NCT Number').rdd.flatMap(lambda x: x).collect()
                # print("comm_updated: ", len(comm_updated))
                # print("comm_not_updated: ", len(comm_not_updated))
                df_temp_comm_not_updated = df_old_output.filter(f.col('NCT Number').isin(comm_not_updated))
                df_temp_comm_updated = df_old_output.select("NCT Number", "Created By", "Updated By", "Created On", "Updated On", "Process ID", "Active Record").filter(f.col('NCT Number').isin(comm_updated))
                df_temp_comm_updated = df_temp_comm_updated.join(df_final, on=['NCT Number'], how='left')
                df_temp_comm_updated = df_temp_comm_updated.withColumn("Updated On", f.current_timestamp())
                df_temp_comm_updated = df_temp_comm_updated.withColumn("Process ID", lit(process_id))
                df_temp_comm_updated = df_temp_comm_updated.withColumn("Active Record", lit("Y"))
                
                # print("comm_updated df: ", df_temp_comm_updated.count())
                # print("comm_not_updated df: ", df_temp_comm_not_updated.count())
                
                df_temp_comm_not_updated = df_temp_comm_not_updated.withColumn("Process ID", f.col("Process ID").cast(StringType()))
                # df_temp_comm_updated.printSchema()
                # df_temp_comm_not_updated.printSchema()
                
                df_final_temp_not_curr = df_old_output.filter(f.col('NCT Number').isin(not_curr))
                # print("not curr: ",df_final_temp_not_curr.count())
                df_final_temp_not_curr = df_final_temp_not_curr.withColumn("Updated On", f.current_timestamp())
                df_final_temp_not_curr = df_final_temp_not_curr.withColumn("Process ID", lit(process_id))
                df_final_temp_not_curr = df_final_temp_not_curr.withColumn("Active Record", lit("N"))
                # df_final_temp_not_curr.printSchema()
                
                df_final_temp_new_curr = df_final.filter(f.col('NCT Number').isin(new_curr))
                # print("new curr: ",df_final_temp_new_curr.count())
                df_final_temp_new_curr = df_final_temp_new_curr.withColumn("Created By", lit("DCT"))
                df_final_temp_new_curr = df_final_temp_new_curr.withColumn("Updated By", lit("DCT"))
                df_final_temp_new_curr = df_final_temp_new_curr.withColumn("Created On", f.current_timestamp())
                df_final_temp_new_curr = df_final_temp_new_curr.withColumn("Updated On", f.current_timestamp())
                df_final_temp_new_curr = df_final_temp_new_curr.withColumn("Process ID", lit(process_id))
                df_final_temp_new_curr = df_final_temp_new_curr.withColumn("Active Record", lit("Y"))
                # df_final_temp_new_curr.printSchema()
                
                columns = df_final_temp_new_curr.columns
                # print(len(columns))
                if (df_temp_comm_not_updated.rdd.isEmpty()):
                    df_temp_comm_updated = df_temp_comm_updated.select(*columns)
                    df_final = df_temp_comm_updated
                elif (df_temp_comm_updated.rdd.isEmpty()):
                    df_temp_comm_not_updated = df_temp_comm_not_updated.select(*columns)
                    df_final = df_temp_comm_not_updated
                else:
                    df_temp_comm_not_updated = df_temp_comm_not_updated.select(*columns)
                    df_temp_comm_updated = df_temp_comm_updated.select(*columns)
                    df_final = df_temp_comm_not_updated.union(df_temp_comm_updated)
                
                if (not df_final_temp_not_curr.rdd.isEmpty()):
                    df_final_temp_not_curr = df_final_temp_not_curr.select(*columns)
                    df_final = df_final.union(df_final_temp_not_curr)
                
                if (not df_final_temp_new_curr.rdd.isEmpty()):
                    df_final_temp_new_curr = df_final_temp_new_curr.select(*columns)
                    df_final = df_final.union(df_final_temp_new_curr)
                # print("com combined: ",df_final.count())
                # df_final.printSchema()
                
            else:    
                df_final = df_final.withColumn("Created By", lit("DCT"))
                df_final = df_final.withColumn("Updated By", lit("DCT"))
                df_final = df_final.withColumn("Created On", f.current_timestamp())
                df_final = df_final.withColumn("Updated On", f.current_timestamp())
                df_final = df_final.withColumn("Process ID", lit(process_id))
                df_final = df_final.withColumn("Active Record", lit("Y"))
            
            def pivot_down_generator(col_list, key_val_dict):
                value_list = []
                col_list = col_list.split(",")
                for key in col_list:
                    if key_val_dict[key]=="Yes":
                        value_list.append(key)
                
                if len(value_list)!=0:
                    return "|".join(value_list)
                else:
                    return ""

            pivot_down_udf = udf(pivot_down_generator, StringType())

            df_final = df_final.dropDuplicates(['NCT Number'])
            df_final_reporting = df_final.withColumn("DCT Category", pivot_down_udf(f.lit(",".join(["Home Health","At-Home Sample Collection","Medication Shipment","Chatbot","ePRO","eConsent","Mobile Apps","Telemedicine (excluding phone call)","Phone Calls","Remote Monitoring","Social Media","eCOA","Augmented reality","Wearables/ Biosensors","DCT (Broadly)","Digital Health Platform"])), f.struct("Home Health","At-Home Sample Collection","Medication Shipment","Chatbot","ePRO","eConsent","Mobile Apps","Telemedicine (excluding phone call)","Phone Calls","Remote Monitoring","Social Media","eCOA","Augmented reality","Wearables/ Biosensors","DCT (Broadly)","Digital Health Platform"))).withColumn("Therapeutic Area", pivot_down_udf(f.lit(",".join(ta_columns)), f.struct(*ta_columns))).withColumn("Intervention Type", pivot_down_udf(f.lit(",".join(["Drug/ Biologic","Device","Behavioral","Diagnostic Test","Genetic","Procedure","Biological","Combination Product","Radiation","Dietary Supplement","Other"])), f.struct(f.col("Drug/ Biologic"), f.col("Device"), f.col("Behavioral"), f.col("Diagnostic Test"), f.col("Genetic"), f.col("Procedure"), f.col("Biological"), f.col("Combination Product"), f.col("Radiation"), f.col("Dietary Supplement"), f.col("Other"))))

            df_final.write.format("csv").option("delimiter", "\t").mode('overwrite').option("header", True).save("{}/dct_database_{}".format(self.destination_source,process_id))
            df_final_reporting.toPandas().to_csv("DCT_Database_{process_id}.csv".format(process_id=process_id),sep='\t',index=False,escapechar="\\",doublequote=False,encoding='utf-8', mode='w+')
            self.send_email("DCT_Database_{process_id}.csv".format(process_id=process_id))
            
            df_final = column_rename(column_mapping, df_final)
            df_final.createOrReplaceTempView('d_decentralized_clinical_trials')

            # Insert data in hdfs table

            spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.d_decentralized_clinical_trials
            partition(

            pt_data_dt='$$data_dt',

            pt_cycle_id='$$cycle_id')

            select * from d_decentralized_clinical_trials

            """)
            CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.d_decentralized_clinical_trials')

        except Exception as e:
            print(" Error in main_matcher is", str(e), traceback.format_exc())
            raise e

os.system('hdfs dfs -mkdir /user/hadoop/dct_output')
key_obj = DCTMatcher()
key_obj.main_matcher()
