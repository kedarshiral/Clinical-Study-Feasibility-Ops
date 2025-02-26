################################# Module Information ######################################
#  Module Name         : D_SPONSOR
#  Purpose             : This will create target table d_sponsor
#  Pre-requisites      : L1 source table required: aact_sponsors, citeline_trialtrove , ctms_organizations, dqs_study
#  Last changed on     : 06-01-2023
#  Last changed by     : Dhananjay|Ujjwal
#  Reason for change   : Standard Sponsor Type
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from L1 layer table
# 2. Pass through the L1 table on key columns to create final target table
############################################################################################
import re
import pandas as pd
import pyspark.sql.functions as f
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, when
import os
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from NotificationUtility import NotificationUtility
import traceback

sys.path.insert(0, os.getcwd())

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = "{bucket_path}/applications/commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id".format(
    bucket_path=bucket_path)

sponsor_type_mapping = spark.read.format('csv').option('header', 'true') \
    .load("{bucket_path}/uploads/SPONSOR/Sponsor_Type_Mapping/sponsor_type_mapping.csv".format(bucket_path=bucket_path))
sponsor_type_mapping.registerTempTable('sponsor_type_mapping')

# Some sponsor names contains | in them. Identify such sponsors and replace | with ^
# eg. In NCT02295176: The sponsor name "Viatris {Mylan/Meda {Rottapharm|Madaus/Rottapharm}}| Xytis" contains "Rottapharm|Madaus" seperated by |
# for above case this function finds such cases and replace | in value (ie. {Rottapharm|Madaus/Rottapharm})with ^
# Expected Output: "Viatris {Mylan/Meda {Rottapharm^Madaus/Rottapharm}}| Xytis"
def regex_handle_pipe(x):
    result = re.search("({[a-zA-Z\/\-\s]*\|[a-zA-Z\/\-\s]*})", x)
    if result:
        after_match = result.group()
        after_replace = re.sub("\|", "^", after_match)
        final = x.replace(after_match, after_replace)
        return final
    else:
        return x

handle_pipe_UDF = udf(lambda x: regex_handle_pipe(x))

citeline_initial_sponsor_data = spark.sql("""
select distinct 
       trial_id,
       regexp_replace(trim(sponsor_name), '\\\"', '') as sponsor_name,
       sponsor_type,
       coalesce(actual_trial_start_date,start_date) as trial_start_dt
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove
where sponsor_name is not null or sponsor_name != ''
""")
citeline_initial_sponsor_data.createOrReplaceTempView('citeline_initial_sponsor_data')

citeline_sponsor_data_df = citeline_initial_sponsor_data.withColumn("sponsor_name", handle_pipe_UDF("sponsor_name"))
citeline_sponsor_data_df.createOrReplaceTempView('citeline_sponsor_data_df')

# The citeline_sponsor_exploded df sponsor name and type are exploded on pipe |
citeline_sponsor_exploded = spark.sql("""
select 
  trial_id, trim(sn) as sponsor_name, trim(st) as sponsor_type, trial_start_dt 
from citeline_sponsor_data_df 
lateral view outer posexplode(split(sponsor_name, '\\\|')) one AS pos1, sn 
lateral view outer posexplode(split(sponsor_type, '\\\|')) two AS pos2, st 
WHERE pos1 = pos2 """)
citeline_sponsor_exploded.createOrReplaceTempView('citeline_sponsor_exploded')

citeline_sponsor_case_wise_sorted = spark.sql("""
select distinct case when rlike(trim(lower(sponsor_type)), 'industry') then "case1"
                     when rlike(trim(lower(sponsor_type)), 'academic') then "case1"
                     when rlike(trim(lower(sponsor_name)), 'university') then "case1"
                     when rlike(trim(lower(sponsor_name)), 'universidad') then "case1"
                else "case2" end as case_type,
        'citeline' as source, trial_id, regexp_replace(lower(trim(sponsor_name)), 'astellas pharma', 'astellas') as sponsor_name, sponsor_type, trial_start_dt
from citeline_sponsor_exploded
""")
citeline_sponsor_case_wise_sorted = citeline_sponsor_case_wise_sorted.dropDuplicates()
citeline_sponsor_case_wise_sorted.createOrReplaceTempView('citeline_sponsor_case_wise_sorted')

citeline_sponsor_df = citeline_sponsor_case_wise_sorted \
    .withColumn("raw_sponsor_name", when(citeline_sponsor_case_wise_sorted.case_type == "case2", "Other").otherwise(citeline_sponsor_case_wise_sorted.sponsor_name)) \
    .withColumn("sponsor_type", when(citeline_sponsor_case_wise_sorted.case_type == "case2", "Other").otherwise(citeline_sponsor_case_wise_sorted.sponsor_type))
citeline_sponsor_df = citeline_sponsor_df.dropDuplicates()
citeline_sponsor_df.createOrReplaceTempView('citeline_sponsor_df')

##AACT
aact_sponsor = spark.sql(""" select distinct 'aact' as source, aact_sp.nct_id as trial_id,
regexp_replace(trim(aact_sp.name),'\\\"','') as raw_sponsor_name, trim(aact_sp.agency_class) as sponsor_type,
study.start_date as trial_start_dt
from $$client_name_ctfo_datastore_staging_$$db_env.aact_sponsors aact_sp  
left join $$client_name_ctfo_datastore_staging_$$db_env.aact_studies study 
on lower(trim(aact_sp.nct_id)) = lower(trim(study.nct_id)) 
where lower(trim(aact_sp.agency_class)) in ("nih", "industry") """)
aact_sponsor.registerTempTable("aact_sponsor")

##CTMS
ctms_sponsor = spark.sql(""" select distinct 'ctms' as source, src_trial_id as trial_id, trim(sponsor) as raw_sponsor_name, 
trim(sponsor_type) as sponsor_type ,null as trial_start_dt
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study """)
ctms_sponsor.registerTempTable("ctms_sponsor")

tascan_sponsor = spark.sql(""" 
select distinct 'tascan' as source, trial_id, trim(sponsor_name) as raw_sponsor_name, 
trim(sponsor_type) as sponsor_type ,trial_start_date as trial_start_dt
from $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial
 """)
tascan_sponsor.registerTempTable("tascan_sponsor")

##DQS
dqs_sponsor = spark.sql("""select distinct 'ir' as source,member_study_id  as trial_id, trim(sponsor) as raw_sponsor_name,
'Unknown' as sponsor_type ,null as trial_start_dt
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir """)
dqs_sponsor.registerTempTable("dqs_sponsor")

other_sources_union = spark.sql("""
select distinct source, trial_id, raw_sponsor_name, sponsor_type ,trial_start_dt from  aact_sponsor
union                                               
select distinct source, trial_id, raw_sponsor_name, sponsor_type ,trial_start_dt from  ctms_sponsor
union                                               
select distinct source, trial_id, raw_sponsor_name, sponsor_type ,trial_start_dt from dqs_sponsor
union                                               
select distinct source, trial_id, raw_sponsor_name, sponsor_type ,trial_start_dt from tascan_sponsor
""")
other_sources_union = other_sources_union.dropDuplicates()
other_sources_union.write.mode("overwrite").saveAsTable('other_sources_union')

sponsor_mapping_file = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/SPONSOR/Sponsor_Mapping_File/Sponsor_Mapping.xlsx".format(bucket_path=bucket_path))
sponsor_mapping_file = sponsor_mapping_file.dropDuplicates()
sponsor_mapping_file.createOrReplaceTempView('sponsor_mapping_file')

citeline_sponsor = spark.sql("""
select 
  distinct 'citeline' as source, A.trial_id, A.raw_sponsor_name, A.sponsor_type, B.parent_sponsor, 
  coalesce(NULLIF(B.exploded_sponsor, ''), B.parent_sponsor) as subsidiary_sponsor, A.trial_start_dt 
from citeline_sponsor_df A left join sponsor_mapping_file B on lower(trim(A.raw_sponsor_name)) = lower(trim(B.raw_sponsor_name))
""")
citeline_sponsor = citeline_sponsor.dropDuplicates()
citeline_sponsor.write.mode("overwrite").saveAsTable('citeline_sponsor')

# joining the other_sources and sponsor_mapping_file on source sponsor_name
other_sources_sponsor = spark.sql(""" 
select distinct A.trial_id, A.raw_sponsor_name , coalesce(A.sponsor_type, 'Other') as sponsor_type , 
coalesce(B.parent_sponsor, 'Other') as parent_sponsor , coalesce(B.exploded_sponsor, 'Other') as subsidiary_sponsor , A.trial_start_dt
from other_sources_union A left join sponsor_mapping_file B on lower(trim(A.raw_sponsor_name)) = lower(trim(B.raw_sponsor_name))
""")
other_sources_sponsor.dropDuplicates()
# other_sources_sponsor.createOrReplaceTempView('other_sources_sponsor')
other_sources_sponsor.write.mode("overwrite").saveAsTable('other_sources_sponsor')

temp_citeline_d_sponsor = spark.sql("""select distinct A.trial_id, A.raw_sponsor_name, A.parent_sponsor, A.subsidiary_sponsor,
coalesce(B.standard_sponsor_type,'Unassigned') as sponsor_type, A.trial_start_dt
from citeline_sponsor A left join sponsor_type_mapping B on lower(trim(A.sponsor_type)) = lower(trim(B.sponsor_type))
""")
temp_citeline_d_sponsor = temp_citeline_d_sponsor.dropDuplicates()
temp_citeline_d_sponsor.write.mode("overwrite").saveAsTable('temp_citeline_d_sponsor')

temp_others_d_sponsor = spark.sql("""select distinct A.trial_id, A.raw_sponsor_name, A.parent_sponsor, A.subsidiary_sponsor,
coalesce(C.sponsor_type,B.standard_sponsor_type) as sponsor_type, A.trial_start_dt
from other_sources_sponsor A 
left join sponsor_type_mapping B on lower(trim(A.sponsor_type)) = lower(trim(B.sponsor_type))
left join temp_citeline_d_sponsor C on lower(trim(A.parent_sponsor)) = lower(trim(C.parent_sponsor)) 
and lower(trim(A.subsidiary_sponsor)) = lower(trim(C.subsidiary_sponsor))""")
temp_others_d_sponsor = temp_others_d_sponsor.dropDuplicates()
temp_others_d_sponsor.write.mode("overwrite").saveAsTable('temp_others_d_sponsor')

# To update sponsor_type, parent_sponsor, subsidiary_sponsor on the raw_sponsor_name
master_File = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/SPONSOR/Master_File/Sponsor_Master_File.xlsx".format(bucket_path=bucket_path))
master_File.dropDuplicates()
master_File.createOrReplaceTempView('master_File')

temp_d_sponsor = spark.sql(""" 
select distinct A.trial_id, A.raw_sponsor_name, 
coalesce(B.sponsor_type,A.sponsor_type) as sponsor_type, coalesce(B.parent_sponsor,A.parent_sponsor) as parent_sponsor,
coalesce(B.subsidiary_sponsor,A.subsidiary_sponsor) as subsidiary_sponsor, trial_start_dt 
from (select * from temp_citeline_d_sponsor
union
select * from temp_others_d_sponsor)A
left join master_File B on lower(trim(A.raw_sponsor_name)) = lower(trim(B.raw_sponsor_name))
""")
temp_d_sponsor = temp_d_sponsor.dropDuplicates()
# temp_d_sponsor.createOrReplaceTempView('temp_d_sponsor')
temp_d_sponsor.write.mode("overwrite").saveAsTable('temp_d_sponsor')
write_path = path.replace('table_name', 'temp_d_sponsor')
temp_d_sponsor.repartition(100).write.mode('overwrite').parquet(write_path)

std_sponsor_type_df_ranked = spark.sql("""
select trial_id,parent_sponsor,sponsor_type ,trial_start_dt 
from 
(SELECT dsst.* ,RANK() OVER(PARTITION BY lower(trim(parent_sponsor)) ORDER BY trial_start_dt DESC) AS rank
FROM temp_d_sponsor dsst) dsst_final
where rank=1
""")
std_sponsor_type_df_ranked = std_sponsor_type_df_ranked.dropDuplicates()
std_sponsor_type_df_ranked.write.mode('overwrite').saveAsTable('std_sponsor_type_df_ranked')

std_sponsor_type_df_concat = spark.sql("""
select 
lower(trim(parent_sponsor)) as parent_sponsor,
concat_ws(';',collect_set(initcap(sponsor_type))) as sponsor_type
from std_sponsor_type_df_ranked
group by 1
""")
std_sponsor_type_df_concat = std_sponsor_type_df_concat.dropDuplicates()
std_sponsor_type_df_concat.write.mode('overwrite').saveAsTable('std_sponsor_type_df_concat')

d_sponsor = spark.sql("""
select distinct sponsor_id,aa.parent_sponsor as sponsor_name,b.sponsor_type as sponsor_type
from
(select dense_rank() over(order by lower(trim(a.parent_sponsor))) as sponsor_id,a.parent_sponsor  
  from
   (select distinct parent_sponsor from temp_d_sponsor group by 1)a )aa
left join std_sponsor_type_df_concat b
on lower(trim(aa.parent_sponsor))=lower(trim(b.parent_sponsor))
group by 1,2,3
""")
# d_sponsor.registerTempTable('d_sponsor')
d_sponsor.write.mode('overwrite').saveAsTable('d_sponsor')

# Insert data in hdfs table
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.d_sponsor
 partition(
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id')
select *
from d_sponsor
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.d_sponsor')