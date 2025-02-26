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

process_id_reporting = '3000'
process_id_postdedupe = '2000'

# Building mysql connection
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/'
                                  + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, 'mysql_db'])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

# to fetch the current reporting cycle id
cursor.execute(
    """select cycle_id,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'in progress' and process_id = {process_id} order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db, process_id=process_id_reporting))
fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date_reporting = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id_reporting = str(fetch_enable_flag_result['cycle_id'])

cursor.execute("""select a.process_id,a.dataset_id,a.dependency_value ,a.write_dependency_value,a.data_date,b.dataset_name from {audit_db}.ctl_process_dependency_details a left join {audit_db}.ctl_dataset_master b
on lower(trim(a.dataset_id))=lower(trim(b.dataset_id)) where a.write_dependency_value={latest_stable_cycle_id}""".format(
    latest_stable_cycle_id=latest_stable_cycle_id_reporting, audit_db=audit_db))
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

spark_df_reporting = spark.createDataFrame(df, schema=schema)
spark_df_reporting.registerTempTable('spark_df_reporting')

# fetching  post ded id for latest reporting run
cursor.execute("""select distinct a.dependency_value from {audit_db}.ctl_process_dependency_details a left join {audit_db}.ctl_dataset_master b
on lower(trim(a.dataset_id))=lower(trim(b.dataset_id)) where a.write_dependency_value={latest_stable_cycle_id} and a.dataset_id ='20001'""".format(
    latest_stable_cycle_id=latest_stable_cycle_id_reporting, audit_db=audit_db))
fetch_post_dedupe_id = cursor.fetchone()
latest_stable_cycle_id_postdedupe = str(fetch_post_dedupe_id['dependency_value'])

cursor.execute("""select a.process_id,a.dataset_id,a.dependency_value ,a.write_dependency_value,a.data_date,b.dataset_name from {audit_db}.ctl_process_dependency_details a left join {audit_db}.ctl_dataset_master b
on lower(trim(a.dataset_id))=lower(trim(b.dataset_id)) where a.write_dependency_value ={latest_stable_cycle_id}""".format(
    latest_stable_cycle_id=latest_stable_cycle_id_postdedupe, audit_db=audit_db))
table_rows = cursor.fetchall()

df = pd.DataFrame(table_rows)

# converting from pandas to spark dataframe
schema = StructType([StructField("process_id", StringType(), True),
                     StructField("dataset_id", StringType(), True),
                     StructField("dependency_value", StringType(), True),
                     StructField("write_dependency_value", StringType(), True),
                     StructField("data_date", StringType(), True),
                     StructField("dataset_name", StringType(), True)])

spark_df_postdedupe = spark.createDataFrame(df, schema=schema)
spark_df_postdedupe.registerTempTable('spark_df_postdedupe')

# Reading json file containg process names
with open("process_list_bridging.json", "r") as jsonFile:
    data = json.load(jsonFile)

# Save Values in Json
process_name_list = data["process_name_list"]
dataset_name_list = data["dataset_name_list"]

# Building queries with loop
for (i, j) in zip(process_name_list, dataset_name_list):
    if i in ('dedupe'):
        query_1 = """select distinct dependency_value as {process_name},write_dependency_value from spark_df_postdedupe where lower(trim(dataset_name))='{dataset_name}'""".format(
            process_name=i, dataset_name=j)
        print("For Dedupe ----- ", query_1)
        temp = spark.sql(query_1)
        temp.registerTempTable(i)
    else:
        query_1 = """select distinct dependency_value as {process_name},write_dependency_value from spark_df_reporting where lower(trim(dataset_name))='{dataset_name}'""".format(
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
query = "select distinct " + process_name_list_main_array + " , reporting_data_alias.write_dependency_value as reporting from spark_df_reporting reporting_data_alias"
print(query)
for i in range(0, length):
    # print(process_name_list[i])
    join_query = " left join " + process_name_list[i] + ' ' + process_name_list[
        i] + "_alias on reporting_data_alias.write_dependency_value = " + process_name_list[
                     i] + "_alias.write_dependency_value"
    # print(join_query)
    query += join_query

print(query)
batch_cycle_mapping_temp1 = spark.sql(query)
batch_cycle_mapping_temp1.registerTempTable('batch_cycle_mapping_temp1')

# final query of dedupe
final_query = "select distinct dedupe,a.* from  batch_cycle_mapping_temp1 a left join dedupe dedupe_alias ON a.post_dedupe = dedupe_alias.write_dependency_value"
batch_cycle_mapping = spark.sql(final_query)
batch_cycle_mapping.registerTempTable("batch_cycle_mapping")
batch_cycle_mapping.write.mode('overwrite').saveAsTable('batch_cycle_mapping')

if (CommonConstants.BRIDGING_RUN_FIRST == 'Y'):
    final_df = spark.sql(
        """ select distinct concat('run_',(rank() over (order by reporting desc))) as process_run ,* from batch_cycle_mapping """)
    final_df.registerTempTable('final_df')

else:
    cursor.execute(
        "select cycle_id,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = {process_id}  order by cycle_start_time desc limit 1 ".format(
            orchestration_db_name=audit_db, process_id=process_id_reporting,
            latest_stable_cycle_id_reporting=latest_stable_cycle_id_reporting))
    fetch_enable_flag_result = cursor.fetchone()
    latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
    latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
    path = "{}/applications/commons/temp/kpi_output_dimension/batch_id_maintain_future/pt_data_dt={}/pt_cycle_id={}/".format(
        bucket_path, latest_stable_data_date, latest_stable_cycle_id)
    prev_batch_id_maintain = spark.read.parquet(path)
    prev_batch_id_maintain.registerTempTable('prev_batch_id_maintain')
    prev_batch_id_maintain=prev_batch_id_maintain.drop('process_run')
    prev_batch_id_maintain.registerTempTable('prev_batch_id_maintain')
    final_df_temp = prev_batch_id_maintain.union(batch_cycle_mapping)
    final_df_temp.registerTempTable('final_df_temp')
    final_df = spark.sql(
        """ select distinct concat('run_',(rank() over (order by reporting desc))) as process_run,* from final_df_temp """)
    final_df.registerTempTable('final_df')

'''
final_df = final_df.withColumn('process_run',
                               when(final_df.process_run == 'run_1', 'run_latest').otherwise(final_df.process_run))
final_df.registerTempTable('final_df')
'''

spark.sql("""
   INSERT OVERWRITE TABLE
   $$client_name_ctfo_datastore_app_fa_$$db_env.batch_id_maintain_future PARTITION (
           pt_data_dt='$$data_dt',
           pt_cycle_id='$$cycle_id'
           )
   SELECT *
   FROM final_df
   """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_fa_$$db_env.batch_id_maintain_future')

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")