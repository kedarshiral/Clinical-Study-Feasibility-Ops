################################# Module Information #####################################################
#  Module Name         : Metric Engine Inv Grain
#  Purpose             : To create the KPIs of Inv grain
#  Output              : inv_metric_engine_KPI_final
#  Last changed on     : 18-01-2022
#  Last changed by     : Kashish Mogha
#  Reason for change   : Initial Code $db_envelopment
##########################################################################################################



import os
import pandas as pd
from Randomization_rate import *
from Screen_failure_rate import *
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

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


# Open JSOn
import json
with open("Status_variablization.json", "r") as jsonFile:
    data = json.load(jsonFile)

#Save Values in Json
data["Ongoing"] = Ongoing
data["Completed"] = Completed
data["Planned"] = Planned
data["Others"] = Others

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
trial_inv_df = config[(config['Grain'] == 'Trial_Inv')]
counter_df = trial_inv_df.index
counter = len(counter_df)
trial_inv_df = trial_inv_df.astype(str)
j = 1
final_query = ""
final_sub_query = ""
KPI_List = []
for index, row in trial_inv_df.iterrows():
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
            # print("final_sub_query ---------------->>>>>>>>",final_sub_query)
        KPI_List.append(row["METRIC"])
        s = 'select '
        for i in KPI_List:
            s += ' ' + str(i) + ','
        # One row per Grain
        a = row["SUPPORTING_METRIC"]
        my_list = a.split(",")
        my_variable = row["SOURCE_TABLE"]
        a = ",".join([my_variable + '.' + e.strip() for e in my_list])
        final_query = s + ' {aggregation} from  {base_table}  {final_sub_query}'.format(
            base_table=row["SOURCE_TABLE"], final_sub_query=final_sub_query, aggregation=a)
        j = j + 1

final_query = final_query.format(on_status=Ongoing_variable, com_status=Completed_variable,on_com=Ongoing_Completed)
inv_metric_engine_KPI_1 = spark.sql(final_query)
inv_metric_engine_KPI_1 = inv_metric_engine_KPI_1.dropDuplicates()
inv_metric_engine_KPI_1.registerTempTable('inv_metric_engine_KPI_1')
#inv_metric_engine_KPI_1.write.mode("overwrite").saveAsTable('inv_metric_engine_KPI_1')

#############################################
Randomization_rate("Trial_Inv")
Screen_failure_rate("Trial_Inv")

inv_metric_engine_KPI = spark.sql("""select a.*,
case when  b.ctms_sfr <=100 and b.ctms_sfr >=0 then b.ctms_sfr
                  when  b.ctms_sfr > 100  then 100
                  when  b.ctms_sfr < 0.0  then null   
    end as ctms_screen_fail_rate,
    c.ctms_randomization_rate,
    e.total_recruitment_months as ctms_total_recruitment_months, 
    I.duration as ctms_enrollment_duration, 
    d.dqs_randomization_rate,
    f.total_recruitment_months as dqs_total_recruitment_months,
    d.recruitment_duration as dqs_enrollment_duration    
from inv_metric_engine_KPI_1 a
left join ctms_sfr_inv b
on lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id)) and lower(trim(a.ctfo_investigator_id)) = lower(trim(b.ctfo_investigator_id)) and lower(trim(a.ctfo_site_id))=lower(trim(b.ctfo_site_id))
left join ctms_randomization_rate_inv c
on a.ctfo_trial_id=c.ctfo_trial_id and a.ctfo_site_id=c.ctfo_site_id and a.ctfo_investigator_id= c.ctfo_investigator_id
left join dqs_randomization_rate_inv d
on a.ctfo_trial_id=d.ctfo_trial_id and a.ctfo_site_id=d.ctfo_site_id and a.ctfo_investigator_id= d.ctfo_investigator_id
left join ctms_randomiztion_duration_inv I
on a.ctfo_trial_id=I.ctfo_trial_id and a.ctfo_site_id=I.ctfo_site_id and a.ctfo_investigator_id= I.ctfo_investigator_id 
left join ctms_total_recruitment_months e 
on a.ctfo_trial_id=e.ctfo_trial_id and a.ctfo_site_id=e.ctfo_site_id and a.ctfo_investigator_id= e.ctfo_investigator_id
left join dqs_total_recruitment_months f 
on a.ctfo_trial_id=f.ctfo_trial_id and a.ctfo_site_id=f.ctfo_site_id and a.ctfo_investigator_id= f.ctfo_investigator_id
""")
inv_metric_engine_KPI = inv_metric_engine_KPI.dropDuplicates()
inv_metric_engine_KPI.registerTempTable('inv_metric_engine_KPI')
inv_metric_engine_KPI.write.mode("overwrite").saveAsTable('inv_metric_engine_KPI')
#path=path.replace('table_name','investigator')
#inv_metric_engine_KPI.repartition(10).write.mode('overwrite').parquet(path)

#############################################

config_2 = pd.read_excel("{code_path}/metric_engine_config.xlsx".format(code_path=CommonConstants.AIRFLOW_CODE_PATH),
                         sheet_name='Final_Config', engine='openpyxl')
trial_inv_final_df = config_2[(config_2['Grain'] == 'Trial_Inv')]
final_counter_df = trial_inv_final_df.index
final_counter = len(final_counter_df)
trial_inv_final_df = trial_inv_final_df.astype(str)
j = 1
final_coalesce_query = ""
final_coalesce_sub_query = ""
for index, row in trial_inv_final_df.iterrows():
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

inv_metric_engine_KPI_final = spark.sql(final_coalesce_query)
inv_metric_engine_KPI_final = inv_metric_engine_KPI_final.dropDuplicates()
inv_metric_engine_KPI_final.registerTempTable('inv_metric_engine_KPI_final')
inv_metric_engine_KPI_final.write.mode("overwrite").saveAsTable('inv_metric_engine_KPI_final')

#path_coalesced=path_coalesced.replace('table_name','investigator')
#inv_metric_engine_KPI_final.repartition(10).write.mode('overwrite').parquet(path_coalesced)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_inv_kpi partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   inv_metric_engine_KPI_final
""")
if inv_metric_engine_KPI_final.count() == 0:
    print("Skipping copy_hdfs_to_s3 for mce_trial_inv_kpi as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_inv_kpi")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")