
################################# Module Information ######################################
#  Module Name         : All Trial Precedence
#  Purpose             : This will execute queries to apply precedence rule on data coming
#                                                from xref_src_trial and to prepare data for final_precedence_trial
#                                                table
#  Pre-requisites      : Source table required: xref_src_trial
#  Last changed on     : 20-01-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# Prepare data for final_precedence_trial table
############################################################################################
import json

import pyspark
from pyspark.sql.types import *

spark = pyspark.sql.SparkSession.builder.appName(
    'AllTrialPrecedence').enableHiveSupport().getOrCreate()
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")

# Reading config file.

json_file = open(
    '/app/clinical_design_center/data_management/sanofi_ctfo/configs/job_executor/precedence_params.json',
    'r').read()
config_data = json.loads(json_file)
print(config_data)

# Ordering data source order based on value.
data_source_order_dict = config_data['data_source_ordering']
sorted_data_source_order_dict = sorted(data_source_order_dict.items(), key=lambda x: x[1])

# Accessing the data source column & respected module ID columns
data_source_name_column = config_data["modules"]["trials_dedupe"]["data_source_name_column"]
source_id_column_name = config_data["modules"]["trials_dedupe"]["source_id_column_name"]
score_column_name = config_data["modules"]["trials_dedupe"]["score_column_name"]


# Reading XREF Table
xref_table_name = config_data["modules"]["trials_dedupe"]["xref_table_name"]
temp_df = spark.sql("""select * from {}""".format(xref_table_name))
temp_df = temp_df.withColumn(source_id_column_name, temp_df[source_id_column_name].cast(LongType()))
temp_df.createOrReplaceTempView('temp')
columns_list = temp_df.columns

# Sorting table based on given data source ordering and respective module's id
query_for_sorting = ""
count = 1
for elem in sorted_data_source_order_dict:
    query_for_sorting = query_for_sorting + """ When {col_name} == '{data_source_name}'
    THEN {order_no}""".format(
        col_name=data_source_name_column, data_source_name=elem[0], order_no=count)
    count += 1

query_for_sorting = """SELECT *, CASE""" + query_for_sorting + """ END AS rank_data_src from temp order by rank_data_src asc,{source_id_column_name} desc, {score_column_name} desc""".format(score_column_name =score_column_name,source_id_column_name=source_id_column_name)
print("query_for_sorting --> " + query_for_sorting)
temp_df_sort_by_data_src_nm = spark.sql(query_for_sorting)
temp_df_sort_by_data_src_nm.createOrReplaceTempView('temp_df_sort_by_data_src_nm')
# temp_df_sort_by_data_src_nm.write.mode("overwrite").saveAsTable("sorted_table")

# Getting golden_id_column
golden_id_column = config_data["modules"]["trials_dedupe"]["golden_id"]

# Preparing list for subsequent operations. Keeping only necessary columns
columns_list.remove(data_source_name_column)
columns_list.remove(source_id_column_name)
columns_list.remove(score_column_name)
columns_list.remove(golden_id_column)

# Perform collect_list over each window partition to get the desired list for all required columns.
#  Reason to do is to maintain ordering for collect_list.
query_for_partitioning = """SELECT *,"""
for i in range(len(columns_list)):
	if i == len(columns_list) - 1:
		query_for_partitioning = query_for_partitioning + """ collect_list(""" + columns_list[i] + """) over (PARTITION BY {golden_id} ORDER BY rank_data_src asc,  {source_id_column_name} desc,{score_column_name} desc) as""".format(golden_id=golden_id_column,score_column_name=score_column_name, source_id_column_name=source_id_column_name) + """ """ + columns_list[i] + """_list"""
	else:
		query_for_partitioning = query_for_partitioning + """ collect_list(""" + columns_list[i] + """) over (PARTITION BY {golden_id} ORDER BY rank_data_src asc, {source_id_column_name} desc, {score_column_name} desc) as""".format(golden_id=golden_id_column,score_column_name=score_column_name, source_id_column_name=source_id_column_name) + """ """ + columns_list[i] + """_list,"""
query_for_partitioning = query_for_partitioning + """ FROM temp_df_sort_by_data_src_nm"""
print("query_for_partitioning --> " + query_for_partitioning)
partitioned_1_df = spark.sql(query_for_partitioning)
partitioned_1_df.createOrReplaceTempView('partitioned_1')

# Preparing a temporary table for final precedence table.
# This is an additional transformation required for final_precedence.
temp_table_1_query = """SELECT """ + golden_id_column + ""","""
for i in range(len(columns_list)):
    if i == len(columns_list) - 1:
        temp_table_1_query = temp_table_1_query + """ """ + columns_list[i] \
                             + """_list[0]""" + " " + \
                             columns_list[i]
    else:
        temp_table_1_query = temp_table_1_query + """ """ + columns_list[
            i] + """_list[0]""" + """ """ + columns_list[i] + ""","""
temp_table_1_query = temp_table_1_query + """ FROM partitioned_1"""
print("temp_table_1_query --> " + temp_table_1_query)
temp_table_1_df = spark.sql(temp_table_1_query)
temp_table_1_df.createOrReplaceTempView('temp_table_1')

# Generating final precedence table
final_precedence_query = """SELECT """ + golden_id_column + ""","""
for i in range(len(columns_list)):
    if i == len(columns_list) - 1:
        final_precedence_query = final_precedence_query + """ collect_list(""" + columns_list[
            i] + """)[0] """ + columns_list[i]
    else:
        final_precedence_query = final_precedence_query + """ collect_list(""" + columns_list[
            i] + """)[0] """ + columns_list[i] + ""","""
final_precedence_query = final_precedence_query + """ FROM temp_table_1 GROUP BY """ \
                         + golden_id_column
print("final_precedence_query --> " + final_precedence_query)
final_precedence_df = spark.sql(final_precedence_query)
final_precedence_df = final_precedence_df.drop("pt_data_dt").drop("pt_cycle_id")

# Finally writing to hive.
final_precedence_df.write.mode('overwrite').saveAsTable('final_precedence_trial')





