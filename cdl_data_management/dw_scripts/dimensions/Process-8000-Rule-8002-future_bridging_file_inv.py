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

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

##Reading file from s3 to get the structure(to be used in case of first run)
bridging_file = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(
    '{bucket_path}/uploads/bridging_main_table_structure/bridging_table_structure.xlsx'.format(
        bucket_path=bucket_path))
bridging_file.registerTempTable('bridging_file')

process_id='8000'


# dictionary to change the column name,ex:- run_2-> run_3,......run_99->run_100
def Convert(lst):
    res_dct = {lst[i]: lst[i + 1] for i in range(1, len(lst) - 1)}
    return res_dct


# To check whether its first run or not, in case of first run empty structure will be used otherwise previous succeded file will be used
if (CommonConstants.BRIDGING_RUN_FIRST == 'Y'):
    main_table_df = spark.sql(""" select * from bridging_file limit 0 """)
    main_table_df.registerTempTable('main_table_df')

    final_df = spark.sql(
        """ select distinct case when lower(trim(data_src_nm))='aact' then concat(data_src_nm,'_',hash_uid) else concat(data_src_nm,'_',src_investigator_id) end  as key, ctfo_investigator_id as run_latest,ctfo_investigator_id as run_1 from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_inv_precedence_int """)
    final_df.registerTempTable('final_df')

    # Add other columns-> run_2.....run_100
    for column in [column for column in main_table_df.columns if column not in final_df.columns]:
        final_df = final_df.withColumn(column, lit(''))
        final_df.registerTempTable('final_df')


else:
    ##fetching latest successful id
    cursor.execute(
        "select cycle_id,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = {process_id} order by cycle_start_time desc limit 1 ".format(
            orchestration_db_name=audit_db, process_id=process_id))
    fetch_enable_flag_result = cursor.fetchone()
    latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
    latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
    path = "{}/applications/commons/dimensions/future_bridging_file_inv/pt_data_dt={}/pt_cycle_id={}/".format(
        bucket_path, latest_stable_data_date, latest_stable_cycle_id)

    main_table_df = spark.read.parquet(path)
    main_table_df.registerTempTable('main_table_df')

    current_data = spark.sql(
        """ select distinct case when lower(trim(data_src_nm))='aact' then concat(data_src_nm,'_',hash_uid) else concat(data_src_nm,'_',src_investigator_id) end as key, ctfo_investigator_id as run_latest,ctfo_investigator_id as run_1 from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_inv_precedence_int group by 1,2 """)
    current_data.registerTempTable('current_data')

    ##Logic for dropping the run_100 column and rename the other columns,ex:- run_2-> run_,......run_99->run_100
    main_table_df = main_table_df.drop('run_100')
    #main_table_df.registerTempTable('main_table_df')
    #main_table_df = main_table_df.withColumnRenamed('run_latest', 'run_1')
    main_table_df.registerTempTable('main_table_df')
    Columns = main_table_df.columns
    rename_dict = Convert(Columns)
    rename_dict.update({"run_99": "run_100", "run_latest": "run_latest"})

    main_table_df = main_table_df.select([col(c).alias(rename_dict.get(c, c)) for c in main_table_df.columns])
    main_table_df.registerTempTable('main_table_df')

    ###fetching the extra keys and golden id came in this run and need to be appended
    extra_src_keys = spark.sql(""" select distinct a.key as key from current_data a left join 
    main_table_df  b on lower(trim(a.key))=lower(trim(b.key)) where b.key is null """)
    extra_src_keys.registerTempTable('extra_src_keys')

    # Adding other columns run_2...run_100 to merge with previous data
    for column in [column for column in main_table_df.columns if column not in extra_src_keys.columns]:
        extra_src_keys = extra_src_keys.withColumn(column, lit(''))
        extra_src_keys.registerTempTable('extra_src_keys')

    ##Appending new keys df into prev data in maintained order
    merged_df = main_table_df.union(extra_src_keys)
    merged_df.registerTempTable('merged_df')

    ##logic to populate the golden ids corresponding to this run
    Columns = merged_df.columns
    Columns.remove('key')
    Columns.remove('run_latest')
    rest_columns = ''
    for i in range(0, len(Columns)):
        rest_columns = rest_columns + 'a.' + Columns[i] + ','

    rest_columns = rest_columns[0:len(rest_columns) - 1]
    query = " select distinct a.key,coalesce(b.run_latest,a.run_latest) as run_latest,b.run_latest as run_1 ,{rest_columns} from merged_df a left join current_data b on lower(trim(a.key))=lower(trim(b.key)) ".format(
        rest_columns=rest_columns)
    final_df = spark.sql(query)
    final_df.registerTempTable('final_df')

spark.sql("""
   INSERT OVERWRITE TABLE
   $$client_name_ctfo_datastore_app_commons_$$db_env.future_bridging_file_inv PARTITION (
           pt_data_dt='$$data_dt',
           pt_cycle_id='$$cycle_id'
           )
   SELECT *
   FROM final_df
   """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.future_bridging_file_inv')



try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")






