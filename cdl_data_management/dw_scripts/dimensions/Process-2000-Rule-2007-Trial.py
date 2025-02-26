
################################# Module Information ######################################
#  Module Name         : D_TRIAL
#  Purpose             : This will create target table d_trial
#  Pre-requisites      : Source table required: xref_src_trial_precedence_int, final_precedence_trial
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# Prepare data for xref tables
############################################################################################
import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf, col, lit,regexp_replace
from pyspark.sql.functions import initcap

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

sqlContext = SQLContext(spark)


def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


sqlContext.udf.register("get_ascii_value", get_ascii_value)

# getting data from final precedence table and adding patient segment

df_xref_temp = spark.sql("""
select
    coalesce(protocol.ctfo_trial_id, segment.ctfo_trial_id) as ctfo_trial_id,
    trim(concat_ws('\,',collect_set(protocol_id_1))) protocol_ids,
    trim(concat_ws('#',collect_set(patient_segment_1))) patient_segments
from
    (select a.ctfo_trial_id ,  
    case when a.protocol_id_1 like "%," then LEFT(a.protocol_id_1, length(a.protocol_id_1)-1) else a.protocol_id_1 end as protocol_id_1
    from
    (select ctfo_trial_id, protocol_id_1 from xref_src_trial_precedence_int
    lateral view outer explode(split(protocol_id, '\\\|')) as protocol_id_1
    group by 1,2) a ) protocol
full outer join
    (select ctfo_trial_id, patient_segment_1 from xref_src_trial_precedence_int
    lateral view outer explode(split(patient_segment, '\\\#')) as patient_segment_1
    group by 1,2) segment
on lower(trim(protocol.ctfo_trial_id)) = lower(trim(segment.ctfo_trial_id))
group by coalesce(protocol.ctfo_trial_id, segment.ctfo_trial_id)
""")
df_xref_temp.createOrReplaceTempView("xref_temp")

D_TRIAL = spark.sql("""
select
    final_precedence_trial.ctfo_trial_id,
    temp.protocol_ids as protocol_id,
    protocol_type,
    trim(regexp_replace(get_ascii_value(trial_title),"[^0-9A-Za-z, ^\/()_-]","")) as trial_title,
    trial_phase,
    trial_status,
    trial_type,
    trim(trial_design) as trial_design,
    trial_start_dt,
    trial_start_dt_type,
    trial_end_dt,
    trial_end_dt_type,
    cast(trial_age_min as BigInt) as trial_age_min,
    cast(trial_age_max as BigInt) as trial_age_max,
    patient_gender,
    temp.patient_segments as patient_segment,
    mesh_terms,
    trim(results_date) as results_date,
    primary_endpoints_reported_date,
    enrollment_close_date
from final_precedence_trial
left outer join xref_temp temp
on lower(trim(final_precedence_trial.ctfo_trial_id)) = lower(trim(temp.ctfo_trial_id))
""")
D_TRIAL = D_TRIAL.dropDuplicates()
#Removing extra space in trial_design , trial_title and results_date
D_TRIAL=D_TRIAL.withColumn("trial_title", regexp_replace("trial_title"," +"," ")) 
D_TRIAL=D_TRIAL.withColumn("trial_design", regexp_replace("trial_design"," +"," ")) 
D_TRIAL=D_TRIAL.withColumn("results_date", regexp_replace("results_date"," +"," "))
D_TRIAL.createOrReplaceTempView('d_trial')

# inserting data into d_trial table
spark.sql("""
INSERT OVERWRITE TABLE $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial PARTITION (
        pt_data_dt='$$data_dt',
        pt_cycle_id='$$cycle_id'
        )
SELECT *
FROM d_trial
""")

# copy to s3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.d_trial')