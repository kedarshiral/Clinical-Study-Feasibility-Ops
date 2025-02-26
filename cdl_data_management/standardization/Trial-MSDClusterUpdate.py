import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
import MySQLdb
import os

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

##Reading file from s3 to read the input file

trial_file = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/applications/commons/temp/km_validation/identified_records/".format(bucket_path=bucket_path))
trial_file.registerTempTable('trial_file')

Trial_records = spark.sql("""
select * from trial_file where lower(trim(module)) = 'trial'""")
Trial_records.registerTempTable('Trial_records')

Trial_records_new_id = spark.sql("""
select * , row_number() over( order by null) as new_golden_id from Trial_records where lower(trim(type)) = 'c'
union 
select *  from Trial_records where lower(trim(type)) = 'd'""")
Trial_records_new_id.registerTempTable('Trial_records_new_id')

Trial_records_explode = spark.sql("""
select exp_golden_id, type, new_golden_id  from Trial_records_new_id 
lateral view outer explode(split(golden_id, '\\\,'))one as exp_golden_id """)
Trial_records_explode.registerTempTable('Trial_records_explode')

# mapping the golden id with hash_uid

cursor.execute(
    """select cycle_id ,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = 2000 order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db))

fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
print(latest_stable_data_date, latest_stable_cycle_id)
xref_src_trial_precedence_int = spark.read.parquet(
    "{bucket_path}/applications/commons/dimensions/xref_src_trial_precedence_int/pt_data_dt={}/pt_cycle_id={}/".format(
        latest_stable_data_date, latest_stable_cycle_id, bucket_path=bucket_path))
xref_src_trial_precedence_int.write.mode('overwrite').saveAsTable('xref_src_trial_precedence_int')

final_trial_output = spark.sql("""
select 
b.data_src_nm, 
b.hash_uid,
a.exp_golden_id as golden_id, 
a.type, 
case when lower(trim(a.type)) ='c' then a.new_golden_id
else null end as new_cluster_id ,
case when lower(trim(a.type)) ='c' then '0.99'
else null end as new_score,
'UPDATED' as comment
from
Trial_records_explode a
inner join xref_src_trial_precedence_int1 b on trim(a.exp_golden_id) = trim(b.ctfo_trial_id)

""")
final_trial_output.write.mode('overwrite').saveAsTable('final_trial_output')

# move input file to archieve
# move input file to archieve
command = "aws s3 mv {}/applications/commons/temp/km_validation/identified_records/{}/ {}/applications/commons/temp/km_validation/identified_records/archieve/{}/{}/{}/ --recursive".format(
    bucket_path, bucket_path)
os.system(command)

csv_path = bucket_path + '/applications/commons/temp/km_validation/identified_records/archieve/output/pt_batch_dt=$$batch_ID'
csv_write_path = csv_path.replace('table_name', 'final_trial_output')
final_trial_output.repartition(1).write.mode('overwrite').format('csv').option(
    'header', 'true').option('delimiter', ',').option('quoteAll', 'true').save(csv_write_path)

