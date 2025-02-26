# importing libraries
from pyspark.sql.functions import *
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
import MySQLdb
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pandas as pd
import json
import unidecode


def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


sqlContext.udf.register("get_ascii_value", get_ascii_value)


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])


# Reading data given by API_Team
tag_data_temp = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/past_bridging_file/input_files/site_tag_bridging_data.xlsx".format(bucket_path=bucket_path))
tag_data_temp.registerTempTable('tag_data_temp')

api_data_temp = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/past_bridging_file/input_files/add_sites_bridging_data.xlsx".format(bucket_path=bucket_path))
api_data_temp.registerTempTable('api_data_temp')

# Building mysql connection
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/'
                                  + CommonConstants.ENVIRONMENT_CONFIG_FILE)
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, 'mysql_db'])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

cursor.execute("""select a.process_id,a.dataset_id,a.dependency_value ,a.write_dependency_value,a.data_date,b.dataset_name from {audit_db}.ctl_process_dependency_details a left join {audit_db}.ctl_dataset_master b
on a.dataset_id=b.dataset_id""".format(audit_db = audit_db))
table_rows = cursor.fetchall()

# converting mysql data into pandas dataframe
df = pd.DataFrame(table_rows)

# converting from pandas to spark dataframe
schema = StructType([StructField("process_id", StringType(), True),
                     StructField("dataset_id", StringType(), True),
                     StructField("dependency_value", StringType(), True),
                     StructField("write_dependency_value", StringType(), True),
                     StructField("data_date", StringType(), True),
                     StructField("dataset_name", StringType(), True)])

spark_df = spark.createDataFrame(df, schema=schema)
spark_df.registerTempTable('spark_df')


tag_data_temp_2 = spark.sql("""select distinct write_dependency_value as reporting,dependency_value from spark_df where dependency_value in (SELECT distinct pt_cycle_id from tag_data_temp ) and dependency_value!='-1'""")
tag_data_temp_2.registerTempTable('tag_data_temp_2')

final_tag_data=spark.sql("""select reporting as pt_cycle_id,b.ctfo_site_id from tag_data_temp_2 a left join tag_data_temp b on a.dependency_value=b.pt_cycle_id""")
final_tag_data.registerTempTable('final_tag_data')

api_data=spark.sql("""select pt_cycle_id,ctfo_site_id from api_data_temp union
select pt_cycle_id,ctfo_site_id from final_tag_data""")
api_data.registerTempTable('api_data')



# Generating process_run column
api_data_final = spark.sql(
    """select distinct pt_cycle_id as reporting, concat_ws('','run_',DENSE_RANK() Over(Order by pt_cycle_id Desc)) as process_run from api_data""")
api_data_final.write.mode('overwrite').saveAsTable('api_data_final')


# Reading json file containg process names
with open("process_list_bridging.json", "r") as jsonFile:
    data = json.load(jsonFile)

# Save Values in Json
process_name_list = data["process_name_list"]
dataset_name_list = data["dataset_name_list"]

# Building queries with loop
for (i, j) in zip(process_name_list, dataset_name_list):
    if i in ('dedupe'):
        query_1 = """select distinct dependency_value as {process_name},write_dependency_value from spark_df where write_dependency_value in (SELECT distinct post_dedupe from post_dedupe ) and lower(trim(dataset_name))='{dataset_name}' and dependency_value!='-1'""".format(
            process_name=i, dataset_name=j)
        print("For Dedupe ----- ", query_1)
        temp = spark.sql(query_1)
        temp.registerTempTable(i)
    else:
        query_1 = """select distinct dependency_value as {process_name},write_dependency_value from spark_df where write_dependency_value in (SELECT distinct pt_cycle_id from api_data ) and lower(trim(dataset_name))='{dataset_name}' and dependency_value!='-1'""".format(
            process_name=i, dataset_name=j)
        print(query_1)
        temp = spark.sql(query_1)
        temp.registerTempTable(i)

# Building final table via loop
original_process_name_list = process_name_list
process_name_list.remove('dedupe')
process_name_list_main_array = ','.join(str(e) for e in process_name_list)
length = (len(process_name_list))
print(length)
query = "select " + process_name_list_main_array + " , reporting, process_run from api_data_final api_data_alias"
for i in range(0, length):
    # print(process_name_list[i])
    join_query = " left join " + process_name_list[i] + ' ' + process_name_list[
        i] + "_alias on api_data_alias.reporting = " + process_name_list[i] + "_alias.write_dependency_value"
    # print(join_query)
    query += join_query

print(query)
batch_cycle_mapping_temp1 = spark.sql(query)
batch_cycle_mapping_temp1.registerTempTable('batch_cycle_mapping_temp1')

# final query of dedupe
final_query = "select process_run,dedupe,post_dedupe,usl_ctms,usl_ir,citeline_investigator,citeline_organization,citeline_trialtrove,citeline_organization_hierarchy,aact_studies,aact_facility_investigators,aact_facilities,reporting from  batch_cycle_mapping_temp1 a left join dedupe dedupe_alias ON a.post_dedupe = dedupe_alias.write_dependency_value"
batch_cycle_mapping = spark.sql(final_query)
# batch_cycle_mapping.registerTempTable("batch_cycle_mapping")
batch_cycle_mapping.write.mode('overwrite').saveAsTable('batch_cycle_mapping')
batch_cycle_mapping.repartition(100).write.mode('overwrite').parquet(
    '{bucket_path}/uploads/past_bridging_file/output_tables/batch_cycle_mapping/'.format(bucket_path=bucket_path))


############ MAIN TABLE ############################


site_data_prep = spark.read.format("parquet").option("header", "true").load(
    "{bucket_path}/applications/commons/temp/mastering/site/temp_all_site_data_prep/".format(bucket_path=bucket_path))
site_data_prep.registerTempTable('site_data_prep')

distinct_dates_prep = spark.sql(
    """select distinct pt_data_dt from site_data_prep where pt_cycle_id in (select distinct dedupe from batch_cycle_mapping)""")
distinct_dates_prep.registerTempTable('distinct_dates_prep')
list_dates_prep = distinct_dates_prep.select('pt_data_dt').rdd.flatMap(lambda x: x).collect()

for i in list_dates_prep:
    site_xref = spark.read.format("parquet").option("header", "true").load(
        "{bucket_path}/applications/commons/temp/mastering/site/temp_all_site_data_prep/pt_data_dt={i}".format(
            i=i, bucket_path=bucket_path))
    site_xref.createOrReplaceTempView('pt_data_dt' + str(i))

length = (len(list_dates_prep))
print(length)
query = "select * from pt_data_dt" + str(list_dates_prep[0])
for i in range(1, length):
    # print(process_name_list[i])
    join_query = " union select * from pt_data_dt" + str(list_dates_prep[i])
    query += join_query

site_data_prep_final = spark.sql(query)
site_data_prep_final.registerTempTable('site_data_prep_final')

site_xref = spark.read.format("parquet").option("header", "true").load(
    "{bucket_path}/applications/commons/dimensions/xref_src_site/".format(bucket_path=bucket_path))
site_xref.registerTempTable('site_xref')

distinct_dates = spark.sql(
    """select distinct pt_data_dt from site_xref where pt_cycle_id in (select distinct post_dedupe from batch_cycle_mapping)""")
distinct_dates.registerTempTable('distinct_dates')
list_dates = distinct_dates.select('pt_data_dt').rdd.flatMap(lambda x: x).collect()

for i in list_dates:
    site_xref = spark.read.format("parquet").option("header", "true").load(
        "{bucket_path}/applications/commons/dimensions/xref_src_site/pt_data_dt={i}".format(
            i=i, bucket_path=bucket_path))
    site_xref.createOrReplaceTempView('pt_data_dt' + str(i))

length = (len(list_dates))
print(length)
query = "select * from pt_data_dt" + str(list_dates[0])
for i in range(1, length):
    # print(process_name_list[i])
    join_query = " union select * from pt_data_dt" + str(list_dates[i])
    query += join_query

site_xref_final = spark.sql(query)
site_xref_final.registerTempTable('site_xref_final')

temp_maintable_1 = spark.sql("""select concat_ws('_',data_src_nm,src_site_id,therapeutic_area) as key,
ctfo_site_id as golden_ids,pt_cycle_id from site_xref_final where ctfo_site_id in (select distinct ctfo_site_id from api_data)
and pt_cycle_id in (select distinct post_dedupe from batch_cycle_mapping)
and data_src_nm in ('citeline','ir','ctms')
""")

temp_maintable_1.write.mode('overwrite').saveAsTable('temp_maintable_1')

temp_maintable_2 = spark.sql("""
select distinct concat_ws('_',data_src_nm,hash_uid,therapeutic_area) as key,golden_ids,t1.pt_cycle_id from
(select ctfo_site_id as golden_ids,pt_cycle_id ,src_site_id,data_src_nm,c.reporting from site_xref_final a
left join batch_cycle_mapping c on a.pt_cycle_id=c.post_dedupe)t1

left join 

(select hash_uid,ds_site_id1,pt_cycle_id,c.reporting,therapeutic_area from (select hash_uid,ds_site_id1,pt_cycle_id ,therapeutic_area from site_data_prep_final 
 lateral view outer explode (split(ds_site_id,"\;"))one as ds_site_id1) b
 left join batch_cycle_mapping c on b.pt_cycle_id=c.dedupe
 )t2

 on concat_ws('_',data_src_nm,t1.src_site_id)=t2.ds_site_id1 and t1.reporting=t2.reporting
 where golden_ids in (select distinct ctfo_site_id from api_data)
and t1.pt_cycle_id in (select distinct post_dedupe from batch_cycle_mapping)
and t1.data_src_nm='aact' """)

temp_maintable_2.write.mode('overwrite').saveAsTable('temp_maintable_2')
temp_maintable = spark.sql("""select * from temp_maintable_1 union select * from temp_maintable_2""")
temp_maintable.write.mode('overwrite').saveAsTable('temp_maintable')

temp_maintable_final = spark.sql("""select a.*,b.process_run,concat_ws('_',a.key,a.golden_ids) as concat_key from temp_maintable a left join batch_cycle_mapping b on a.pt_cycle_id=b.post_dedupe""")
temp_maintable_final.registerTempTable('temp_maintable_final')
transposedDf =temp_maintable_final.groupBy("concat_key").pivot("process_run").agg(first("golden_ids"))
transposedDf.write.mode('overwrite').saveAsTable('transposedDf')

temp_all_site_data = spark.sql("""
select distinct data_src_nm, 
case when name in (' ','','null','NA', NULL) then null 
else lower(trim(name)) end as name ,
case when city in (' ','','null','NA', NULL) then null
else lower(trim(regexp_replace(get_ascii_value(city),"[^0-9A-Za-z ]",''))) end as city,
case when lower(trim(state)) in (' ','','null','na','not known','not know') then null
else lower(trim(state)) end as state ,
case when zip in (' ','','null','NA', NULL) then null
else lower(trim(zip)) end as zip,
case when address in (' ','','null','NA', NULL) then null
else lower(trim(address)) end as address,
case when country in (' ','','null','NA', NULL) then null
else lower(trim(country)) end as country,hash_uid
from site_data_prep_final""")
temp_all_site_data.registerTempTable('temp_all_site_data')

key_without_ta = spark.sql("""select concat_ws('_',data_src_nm,key_without_ta) as key_without_ta,hash_uid from (
select 
sha2(concat_ws('',lower(trim(coalesce(name,'NA'))) ,lower(trim(coalesce(city,'NA'))), lower(trim(coalesce(zip,'NA'))), lower(trim(coalesce(country,'NA')))),256) as key_without_ta,hash_uid,data_src_nm
    from temp_all_site_data where data_src_nm='aact')
    """)

key_without_ta.registerTempTable('key_without_ta')

distinct_process_run = spark.sql(
    """select distinct process_run,reporting from batch_cycle_mapping order by reporting desc""")
distinct_process_run.registerTempTable('distinct_process_run')
list_process_run = distinct_process_run.select('process_run').rdd.flatMap(lambda x: x).collect()
process_run_array = ','.join(str(e) for e in list_process_run)

query = "select distinct coalesce(b.key_without_ta,concat(split(a.concat_key, '_')[0],'_', split(a.concat_key, '_')[1])) as key_without_ta,concat(split(concat_key, '_')[0],'_', split(concat_key, '_')[1],'_',split(concat_key, '_')[2]) as key, " + process_run_array + " from transposedDf a left join key_without_ta b on split(a.concat_key,'_')[1]=b.hash_uid"

main_temp1 = spark.sql(query)
main_temp1.registerTempTable('main_temp1')

df_for_latest = spark.sql("""select key,
max(run_1) as run_latest from main_temp1 
group by key""")
df_for_latest.registerTempTable('df_for_latest')

run_list = []
for i in range(1, 101):
    runs = 'run_' + str(i)
    run_list.append(runs)

final_run_list = []
for element in run_list:
    if element not in list_process_run:
        final_run_list.append(element)

list_process_run.remove('run_1')
process_run_array_temp = ','.join(str(e) for e in list_process_run)

final_query = "select key_without_ta,a.key,coalesce(a.run_1,b.run_latest," + process_run_array_temp + ") as run_latest, " + process_run_array + " from main_temp1 a left join df_for_latest b on a.key=b.key"
print(final_query)
main_table = spark.sql(final_query)
main_table.write.mode('overwrite').saveAsTable('main_table')

length = (len(final_run_list))
query_for_main = "select a.*"

for i in range(0, length):
    join_query = "Cast(NULL as string) as " + final_run_list[i]
    query_for_main += "," + join_query


query_for_main += " from main_table a"
main_table_past = spark.sql(query_for_main)
main_table_past.write.mode('overwrite').saveAsTable('main_table_past')
main_table_past.repartition(100).write.mode('overwrite').parquet(
    '{bucket_path}/uploads/past_bridging_file/output_tables/main_table_with_tag/'.format(bucket_path=bucket_path))

