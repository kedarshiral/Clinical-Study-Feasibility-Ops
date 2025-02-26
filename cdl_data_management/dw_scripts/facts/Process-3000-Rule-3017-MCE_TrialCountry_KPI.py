################################# Module Information #####################################################
#  Module Name         : Metric Engine Country Grain                                                     
#  Purpose             : To create the KPIs of Inv grain                                                 
#  Output              : country_metric_engine_KPI_final                                                 
#  Last changed on     : 18-01-2022                                                                      
#  Last changed by     : Kashish Mogha                                                                   
#  Reason for change   : Initial Code $db_envelopment                                                    
##########################################################################################################


import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from CommonUtils import CommonUtils
from pyspark.sql.functions import lower, col
import os
import pandas as pd
from Randomization_rate import *
from Screen_failure_rate import *
import json
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])


path = bucket_path + "/applications/commons/mce/mce_kpi/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"
path_coalesced = bucket_path + "/applications/commons/mce/mce_kpi_coalesced/table_name/" \
                     "pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"
                     
spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

########## Status Variablization ###################

trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

trial_status_mapping = spark.sql(""" select distinct * from (select raw_status, status from trial_status_mapping_temp 
union select status, status from trial_status_mapping_temp )  """)
trial_status_mapping.registerTempTable('trial_status_mapping')

#Values in Variables

Ongoing = trial_status_mapping.filter(trial_status_mapping.status == "Ongoing").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Completed = trial_status_mapping.filter(trial_status_mapping.status == "Completed").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Planned = trial_status_mapping.filter(trial_status_mapping.status == "Planned").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Others = trial_status_mapping.filter(trial_status_mapping.status == "Others").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()


with open("Status_variablization.json", "r") as jsonFile:
    data = json.load(jsonFile)

#Save Values in Json
Ongoing = data["Ongoing"]
Completed = data["Completed"]
Planned = data["Planned"]
Others = data["Others"]

#JSON WRITE
with open("Status_variablization.json", "w") as jsonFile:
    json.dump(data, jsonFile)

Ongoing_variable = tuple(Ongoing)
Completed_variable = tuple(Completed)
Planned_variable = tuple(Planned)
Others_variable = tuple(Others)

Ongoing_Completed = tuple(Ongoing)+tuple(Completed)
Ongoing_Planned = tuple(Ongoing)+tuple(Planned)
Ongoing_Completed_Planned = tuple(Ongoing)+tuple(Completed)+tuple(Planned)



os.system("aws s3 cp {bucket_path}/uploads/Metric_Engine/metric_engine_config.xlsx {code_path}/".format(bucket_path=bucket_path,code_path=CommonConstants.AIRFLOW_CODE_PATH))

config = pd.read_excel("{code_path}/metric_engine_config.xlsx".format(code_path=CommonConstants.AIRFLOW_CODE_PATH),
                       sheet_name='Metric Engine Config', engine='openpyxl')
                       
trial_country_df = config[(config['Grain'] == 'Trial_Country')]
counter_df = trial_country_df.index
counter = len(counter_df)
trial_country_df = trial_country_df.astype(str)
j = 1
final_query = ""
final_sub_query = ""
KPI_List = []
for index, row in trial_country_df.iterrows():
    if row["AGGREGATE"] == 'N':
        if row["FILTERS_APPLIED"] == 'Y':
            print("Where Condition Present")
            sub_query = """ (select {aggregate} , {logic} as {metric} from {src_table} where lower(trim(source))='{src}' and {filter} ) {alias} """.format(
                logic=row["LOGIC"], src_table=row["SOURCE_TABLE"], filter=row["FILTERS"],
                aggregate=row["SUPPORTING_METRIC"],
                metric=row["METRIC"], alias=row["METRIC_ALIAS"], src=row["SOURCE"])
        else:
            sub_query = """ (select {aggregate} , {logic} as {metric} from {src_table} where lower(trim(source))='{src}') {alias} """.format(
                logic=row["LOGIC"], src_table=row["SOURCE_TABLE"], filter=row["FILTERS"],
                aggregate=row["SUPPORTING_METRIC"],
                metric=row["METRIC"], alias=row["METRIC_ALIAS"], src=row["SOURCE"])
    else:
        print("Aggregate Present")
        if row["FILTERS_APPLIED"] == 'Y':
            print("Where Condition Present")
            sub_query = """ (select {aggregate} , {logic} as {metric} from {src_table} where lower(trim(source))='{src}' and {filter} group by {aggregation} ) {alias} """.format(
                logic=row["LOGIC"], src_table=row["SOURCE_TABLE"], filter=row["FILTERS"],
                aggregate=row["SUPPORTING_METRIC"],
                metric=row["METRIC"], alias=row["METRIC_ALIAS"], aggregation=row["AGGREGATE_ON"], src=row["SOURCE"])
        else:
            sub_query = """ (select {aggregate} , {logic} as {metric} from {src_table} where lower(trim(source))='{src}' group by {aggregation}) {alias} """.format(
                logic=row["LOGIC"], src_table=row["SOURCE_TABLE"], filter=row["FILTERS"],
                aggregate=row["SUPPORTING_METRIC"],
                metric=row["METRIC"], alias=row["METRIC_ALIAS"], aggregation=row["AGGREGATE_ON"], src=row["SOURCE"])
    print("sub_query ---------------->>>>>>>>",sub_query)
    if counter == 1:
        final_query = sub_query
    else:
        condition = row["SUPPORTING_METRIC"]
        my_list = condition.split(",")
        on_condition = " on "
        for i in range(len(my_list)):
            if i == len(my_list) - 1:
                on_condition += " {src_table}.{i}={alias}.{i} ".format(src_table=row["SOURCE_TABLE"].strip(),
                                                                       alias=row["METRIC_ALIAS"].strip(),
                                                                       i=my_list[i].strip())
            else:
                on_condition += " {src_table}.{i}={alias}.{i} and ".format(src_table=row["SOURCE_TABLE"].strip(),
                                                                           alias=row["METRIC_ALIAS"].strip(),
                                                                           i=my_list[i].strip())
        if j == counter:
            print("j==counter:")
            final_sub_query += " left join " + sub_query + "   " + on_condition

        else:
            final_sub_query += " left join " + sub_query + on_condition
            print("final_sub_query ---------------->>>>>>>>",final_sub_query)
        KPI_List.append(row["METRIC"])
        print("kpi_list--->",*KPI_List,sep = ",")
        s = 'select '
        for i in KPI_List:
            s += ' ' + str(i) + ','   
        # One row per Grain
        print("s----->",s)
        a = row["SUPPORTING_METRIC"] 
        my_list = a.split(",")
        my_variable = row["SOURCE_TABLE"]
        a = ",".join([my_variable + '.' + e.strip() for e in my_list])
        final_query = s + ' {aggregation} from  {base_table}  {final_sub_query}'.format(
            base_table=row["SOURCE_TABLE"], final_sub_query=final_sub_query, aggregation=a)
        j = j + 1

final_query=final_query.format(on_com=Ongoing_Completed,on_status=Ongoing_variable, com_status=Completed_variable)
country_metric_engine_KPI_1=spark.sql(final_query)
country_metric_engine_KPI_1 = country_metric_engine_KPI_1.dropDuplicates()
country_metric_engine_KPI_1.registerTempTable('country_metric_engine_KPI_1')
#country_metric_engine_KPI_1.write.mode("overwrite").saveAsTable('country_metric_engine_KPI_1')

#############################################
Randomization_rate("Trial_Country")
Screen_failure_rate("Trial_Country")


country_metric_engine_KPI = spark.sql("""select a.*,b.ctms_sfr as ctms_screen_fail_rate ,c.randomization_rate as ctms_randomization_rate,
e.enrollment_duration as ctms_enrollment_duration,
d.dqs_randomization_rate,
g.dqs_sfr as dqs_screen_fail_rate,
f.enrollment_duration as dqs_enrollment_duration
from country_metric_engine_KPI_1 a
left join ctms_screen_fail_rate_country b
on a.ctfo_trial_id=b.ctfo_trial_id and lower(trim(a.country))=lower(trim(b.country)) 
left join country_ctms_randomization_rate c
on a.ctfo_trial_id=c.ctfo_trial_id and lower(trim(a.country))=lower(trim(c.country))
left join country_ctms_enrollment_duration e
on a.ctfo_trial_id=e.ctfo_trial_id and lower(trim(a.country))=lower(trim(e.country))
left join country_dqs_randomization_rate d
on a.ctfo_trial_id=d.ctfo_trial_id and lower(trim(a.country))=lower(trim(d.country))
left join country_dqs_enrollment_duration f
on a.ctfo_trial_id=f.ctfo_trial_id and lower(trim(a.country))=lower(trim(f.country))
left join dqs_screen_failure_calc_country g
on a.ctfo_trial_id=g.ctfo_trial_id and lower(trim(a.country))=lower(trim(g.country))
""")
country_metric_engine_KPI = country_metric_engine_KPI.dropDuplicates()
country_metric_engine_KPI.registerTempTable('country_metric_engine_KPI')
country_metric_engine_KPI.write.mode("overwrite").saveAsTable('country_metric_engine_KPI')

#path=path.replace('table_name','country')
#country_metric_engine_KPI.coalesce(1).write.mode('overwrite').parquet(path)

##################################################
config_2 = pd.read_excel("{code_path}/metric_engine_config.xlsx".format(code_path=CommonConstants.AIRFLOW_CODE_PATH),
                         sheet_name='Final_Config', engine='openpyxl')
trial_country_final_df = config_2[(config_2['Grain'] == 'Trial_Country')]
final_counter_df = trial_country_final_df.index
final_counter = len(final_counter_df)
trial_country_final_df = trial_country_final_df.astype(str)
j = 1
final_coalesce_query = ""
final_coalesce_sub_query = ""
for index, row in trial_country_final_df.iterrows():
    coalesce_sub_query = """ {logic} as {metric} """.format(logic=row["LOGIC"], metric=row["METRIC"])
    if final_counter == 1:
        final_coalesce_query = ('(select {aggregate}, ' + coalesce_sub_query + ' from {src_table})').format(
            aggregate=row["SUPPORTING_METRIC"], src_table=row["SOURCE_TABLE"])
    else:
        if j == final_counter:
            final_coalesce_sub_query += coalesce_sub_query
        else:
            final_coalesce_sub_query += coalesce_sub_query + ','

        final_coalesce_query = ('(select {aggregate}, ' + final_coalesce_sub_query + ' from {src_table})').format(
            aggregate=row["SUPPORTING_METRIC"], src_table=row["SOURCE_TABLE"])
        j = j + 1

country_metric_engine_KPI_final = spark.sql(final_coalesce_query)
country_metric_engine_KPI_final.registerTempTable('country_metric_engine_KPI_final')
country_metric_engine_KPI_final.write.mode("overwrite").saveAsTable('country_metric_engine_KPI_final')

#path_coalesced = path_coalesced.replace('table_name', 'country')
#country_metric_engine_KPI_final.repartition(10).write.mode('overwrite').parquet(path_coalesced)


spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_country_kpi partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   country_metric_engine_KPI_final
""")

if country_metric_engine_KPI_final.count() == 0:
    print("Skipping copy_hdfs_to_s3 for mce_trial_country_kpi as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_country_kpi")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")