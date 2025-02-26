################################# Module Information #####################################################
#  Module Name         : Trial Universe Metric Calculation Data
#  Purpose             : To create the source data for the Trial Universe
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : f_rpt_trial_universe
#  Last changed on     : 23-01-2025
#  Last changed by     : Kedar
#  Reason for change   : TASCAN
##########################################################################################################

# Importing python modules
import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf
from CommonUtils import *
from pyspark.sql.functions import initcap

import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

data_dt = "$$data_dt"
cycle_id = "$$cycle_id"

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/commons/temp/" \
                         "kpi_output_dimension/table_name"

trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

trial_status_mapping = spark.sql(""" select distinct * from (select raw_status, status from trial_status_mapping_temp 
union select status, status from trial_status_mapping_temp )  """)
trial_status_mapping.registerTempTable('trial_status_mapping')

Ongoing = trial_status_mapping.filter(trial_status_mapping.status == "Ongoing").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Completed = trial_status_mapping.filter(trial_status_mapping.status == "Completed").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Planned = trial_status_mapping.filter(trial_status_mapping.status == "Planned").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Others = trial_status_mapping.filter(trial_status_mapping.status == "Others").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()

# Open JSOn
import json

with open("Status_variablization.json", "r") as jsonFile:
    data = json.load(jsonFile)

# Save Values in Json
data["Ongoing"] = Ongoing
data["Completed"] = Completed
data["Planned"] = Planned
data["Others"] = Others

# JSON WRITE
with open("Status_variablization.json", "w") as jsonFile:
    json.dump(data, jsonFile)

Ongoing_variable = tuple(Ongoing)
Completed_variable = tuple(Completed)
Planned_variable = tuple(Planned)
Others_variable = tuple(Others)

Ongoing_Completed = tuple(Ongoing) + tuple(Completed)
Ongoing_Planned = tuple(Ongoing) + tuple(Planned)
Ongoing_Completed_Planned = tuple(Ongoing) + tuple(Completed) + tuple(Planned)

##status standardisation file
status_mapping = spark.read.format("csv").option("header", "true").load(
    "{bucket_path}/uploads/trial_status.csv".format(bucket_path=bucket_path))
status_mapping.createOrReplaceTempView("status_mapping")
# status_mapping.write.mode('overwrite').saveAsTable('status_mapping')

xref_src_trial = spark.sql("""
SELECT
    data_src_nm,
    src_trial_id,
    ctfo_trial_id
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
""")
xref_src_trial = xref_src_trial.dropDuplicates()
xref_src_trial.registerTempTable('xref_src_trial')
xref_src_trial.write.mode('overwrite').saveAsTable('xref_src_trial')

xref_src_trial_citeline = spark.sql("""
SELECT
    data_src_nm,
    src_trial_id,
    ctfo_trial_id
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
WHERE data_src_nm = 'citeline'
""")
xref_src_trial_citeline = xref_src_trial_citeline.dropDuplicates()
xref_src_trial_citeline.registerTempTable('xref_src_trial_citeline')
xref_src_trial_citeline.write.mode('overwrite').saveAsTable('xref_src_trial_citeline')

citeline_trailtrove_temp = spark.sql("""
SELECT
    trial_id,
	trial_title AS title,
	study_keywords,
    study_design,
    inclusion_criteria,
    exclusion_criteria
FROM $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove
""")
citeline_trailtrove_temp = citeline_trailtrove_temp.dropDuplicates()
citeline_trailtrove_temp.registerTempTable('citeline_trailtrove_temp')
# citeline_trailtrove_temp.write.mode('overwrite').saveAsTable('citeline_trailtrove_temp')


citeline_exclude_label_temp = spark.sql("""
SELECT
    trial_id,
  concat_ws('',NULLIF(trim(title),''),NULLIF(trim(study_design),''),NULLIF(trim(study_keywords),'') ) as search_col
from citeline_trailtrove_temp
""")

citeline_exclude_label_temp.registerTempTable('citeline_exclude_label_temp')
# citeline_exclude_label_temp.write.mode('overwrite').saveAsTable('citeline_exclude_label_temp')

citeline_exclude_label_temp_1 = spark.sql("""
select trial_id,
CASE WHEN lower(search_col) like '%open-label%' OR lower(search_col) like '%open label%' then 'OL' else '' end as excl_label_ol,
CASE WHEN lower(search_col) like '%long term%' OR lower(search_col) like '%extension%' then 'LTE' else ''  end as excl_label_lte,
CASE WHEN lower(search_col) like '%juvenile%' OR lower(search_col) like '%pediatric%' then 'JT' else '' end as excl_label_jt
from citeline_exclude_label_temp
""")

citeline_exclude_label_temp_1.registerTempTable('citeline_exclude_label_temp_1')
citeline_exclude_label_temp_1.write.mode('overwrite').saveAsTable('citeline_exclude_label_temp_1')

citeline_exclude_label = spark.sql("""
select trial_id, concat_ws('|', NULLIF(trim(excl_label_ol),''), NULLIF(trim(excl_label_lte),''), NULLIF(trim(excl_label_jt),'')) as exclude_label
FROM citeline_exclude_label_temp_1
""")
citeline_exclude_label.registerTempTable('citeline_exclude_label')
# citeline_exclude_label.write.mode('overwrite').saveAsTable('citeline_exclude_label')

citeline_join = spark.sql("""
SELECT
    t.trial_id,
    e.exclude_label,
    t.study_design,
    t.inclusion_criteria as inclusion_criteria,
    t.exclusion_criteria as exclusion_criteria
FROM citeline_trailtrove_temp t
left join citeline_exclude_label e ON lower(trim(t.trial_id)) = lower(trim(e.trial_id))
group by 1,2,3,4,5""")

citeline_join = citeline_join.dropDuplicates()
citeline_join.registerTempTable('citeline_join')
citeline_join.write.mode('overwrite').saveAsTable('citeline_join')

citeline_df_temp = spark.sql("""
SELECT
s.ctfo_trial_id AS   ctfo_trial_id,
concat_ws('|',sort_array(collect_set(NULLIF(trim(t.exclude_label),'')),true)) as exclude_label ,
concat_ws('|',sort_array(collect_set(NULLIF(trim(t.study_design),'')),true)) as study_design ,
concat_ws('|',sort_array(collect_set(NULLIF(trim(t.inclusion_criteria),'')),true)) as inclusion_criteria ,
concat_ws('|',sort_array(collect_set(NULLIF(trim(t.exclusion_criteria),'')),true)) as exclusion_criteria,
CAST(null as string) as ct_gov_url
FROM xref_src_trial_citeline s
left join citeline_join t
    ON lower(trim(t.trial_id)) = lower(trim(s.src_trial_id))
  group by 1
""")
citeline_df_temp = citeline_df_temp.dropDuplicates()
citeline_df_temp.registerTempTable('citeline_df_temp')
citeline_df_temp.write.mode('overwrite').saveAsTable('citeline_df_temp')

ctms_final_df_test = spark.sql("""
SELECT           
A.ctfo_trial_id,
CAST(null as string) as exclude_label,
CAST(null as string) as study_design,
CAST(null as string) as inclusion_criteria,
CAST(null as string) as exclusion_criteria,
CAST(null as string) as ct_gov_url
from
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial A
where lower(trim(A.data_src_nm) ) = 'ctms'
group by 1,2,3,4,5
""")
ctms_final_df_test = ctms_final_df_test.dropDuplicates()
ctms_final_df_test.registerTempTable('ctms_final_df_test')
ctms_final_df_test.write.mode('overwrite').saveAsTable('ctms_final_df_test')

aact_study_df_temp = spark.sql("""select
A.ctfo_trial_id,
CAST(null as string) as study_design,
CAST(null as string) as exclude_label,
CAST(null as string) as exclusion_criteria,
CAST(null as string) as inclusion_criteria,ct_gov_url
from (
select
f.data_src_nm,
h.nct_id,
f.ctfo_trial_id, concat('https://clinicaltrials.gov/study/', trim(h.nct_id)) as ct_gov_url
from
(select nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies b ) h
left join
(select nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_facilities group by nct_id) d on lower(trim(h.nct_id)) = lower(trim(d.nct_id))
left join
xref_src_trial f on lower(trim(h.nct_id))=lower(trim(f.src_trial_id)) where lower(trim(f.data_src_nm))='aact'
group by 1,2,3) A group by 1,2,3,4,5,6
""")
aact_study_df_temp = aact_study_df_temp.dropDuplicates()
aact_study_df_temp.registerTempTable('aact_study_df_temp')
aact_study_df_temp.write.mode('overwrite').saveAsTable('aact_study_df_temp')

dqs_study_temp = spark.sql("""
SELECT
    a.nct_number AS nct_id,
    b.ctfo_trial_id,
    a.member_study_id,
    a.facility_golden_id
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir a
left join xref_src_trial b on lower(trim(a.member_study_id))=lower(trim(b.src_trial_id)) where lower(trim(b.data_src_nm))='ir'
group by 1,2,3,4
""")
dqs_study_temp = dqs_study_temp.dropDuplicates()
dqs_study_temp.registerTempTable('dqs_study_temp')
dqs_study_temp.write.mode('overwrite').saveAsTable('dqs_study_temp')

dqs_study_df = spark.sql("""
SELECT
    t.ctfo_trial_id AS ctfo_trial_id,
    CAST(null as string) as exclude_label,
    CAST(null as string) as study_design,
    CAST(null as string) as inclusion_criteria,
    CAST(null as string) as exclusion_criteria,CAST(null as string) as ct_gov_url
FROM dqs_study_temp t
group by 1,2,3,4,5,6
""")
dqs_study_df = dqs_study_df.dropDuplicates()
dqs_study_df.registerTempTable('dqs_study_df')
dqs_study_df.write.mode('overwrite').saveAsTable('dqs_study_df')

#################################TASCAN#############################################

xref_src_trial_tascan = spark.sql("""
SELECT
    data_src_nm,
    src_trial_id,
    ctfo_trial_id
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
WHERE data_src_nm = 'tascan'
""")
xref_src_trial_tascan = xref_src_trial_tascan.dropDuplicates()
xref_src_trial_tascan.registerTempTable('xref_src_trial_tascan')
xref_src_trial_tascan.write.mode('overwrite').saveAsTable('xref_src_trial_tascan')

tascan_trial_temp = spark.sql("""
SELECT
    trial_id,
	trial_title AS title,
	'' as study_keywords,
    trial_design AS study_design,
    inclusion_criteria,
    exclusion_criteria
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial
""")
tascan_trial_temp = tascan_trial_temp.dropDuplicates()
tascan_trial_temp.registerTempTable('tascan_trial_temp')
# tascan_trial_temp.write.mode('overwrite').saveAsTable('tascan_trial_temp')


tascan_exclude_label_temp = spark.sql("""
SELECT
    trial_id,
  concat_ws('',NULLIF(trim(title),''),NULLIF(trim(study_design),''),NULLIF(trim(study_keywords),'') ) as search_col
from tascan_trial_temp
""")

tascan_exclude_label_temp.registerTempTable('tascan_exclude_label_temp')

tascan_exclude_label_temp_1 = spark.sql("""
select trial_id,
CASE WHEN lower(search_col) like '%open-label%' OR lower(search_col) like '%open label%' then 'OL' else '' end as excl_label_ol,
CASE WHEN lower(search_col) like '%long term%' OR lower(search_col) like '%extension%' then 'LTE' else ''  end as excl_label_lte,
CASE WHEN lower(search_col) like '%juvenile%' OR lower(search_col) like '%pediatric%' then 'JT' else '' end as excl_label_jt
from tascan_exclude_label_temp
""")

tascan_exclude_label_temp_1.registerTempTable('tascan_exclude_label_temp_1')
tascan_exclude_label_temp_1.write.mode('overwrite').saveAsTable('tascan_exclude_label_temp_1')

tascan_exclude_label = spark.sql("""
select trial_id, concat_ws('|', NULLIF(trim(excl_label_ol),''), NULLIF(trim(excl_label_lte),''), NULLIF(trim(excl_label_jt),'')) as exclude_label
FROM tascan_exclude_label_temp_1
""")
tascan_exclude_label.registerTempTable('tascan_exclude_label')

tascan_join = spark.sql("""
SELECT
    t.trial_id,
    e.exclude_label,
    t.study_design,
    t.inclusion_criteria as inclusion_criteria,
    t.exclusion_criteria as exclusion_criteria
FROM tascan_trial_temp t
left join tascan_exclude_label e ON lower(trim(t.trial_id)) = lower(trim(e.trial_id))
group by 1,2,3,4,5""")

tascan_join = tascan_join.dropDuplicates()
tascan_join.registerTempTable('tascan_join')
tascan_join.write.mode('overwrite').saveAsTable('tascan_join')

tascan_df_temp = spark.sql("""
SELECT
s.ctfo_trial_id AS   ctfo_trial_id,
concat_ws('|',sort_array(collect_set(NULLIF(trim(t.exclude_label),'')),true)) as exclude_label ,
concat_ws('|',sort_array(collect_set(NULLIF(trim(t.study_design),'')),true)) as study_design ,
concat_ws('|',sort_array(collect_set(NULLIF(trim(t.inclusion_criteria),'')),true)) as inclusion_criteria ,
concat_ws('|',sort_array(collect_set(NULLIF(trim(t.exclusion_criteria),'')),true)) as exclusion_criteria,
CAST(null as string) as ct_gov_url
FROM xref_src_trial_tascan s
left join tascan_join t
    ON lower(trim(t.trial_id)) = lower(trim(s.src_trial_id))
  group by 1
""")
tascan_df_temp = tascan_df_temp.dropDuplicates()
tascan_df_temp.registerTempTable('tascan_df_temp')
tascan_df_temp.write.mode('overwrite').saveAsTable('tascan_df_temp')

####################################################################################


df_union = spark.sql("""
select * from citeline_df_temp
UNION
select * from ctms_final_df_test
UNION 
select * from aact_study_df_temp
UNION 
select * from dqs_study_df
UNION
select * from tascan_df_temp
""")
df_union = df_union.dropDuplicates()
# df_union.registerTempTable('df_union')
df_union.write.mode('overwrite').saveAsTable('df_union')

df_criteria = spark.sql("""
select ctfo_trial_id,
 concat_ws(',',sort_array(collect_set(NULLIF(trim(inclusion_criteria),'')),true)) as inclusion_criteria,
 concat_ws(',',sort_array(collect_set(NULLIF(trim(exclusion_criteria),'')),true)) as exclusion_criteria,
 concat_ws(',',sort_array(collect_set(NULLIF(trim(study_design),'')),true)) as study_design,
 concat_ws(',',sort_array(collect_set(NULLIF(trim(exclude_label),'')),true)) as exclude_label,
 concat_ws(',',sort_array(collect_set(NULLIF(trim(ct_gov_url),'')),true)) as ct_gov_url
 from df_union group by 1
""")
df_criteria = df_criteria.dropDuplicates()
df_criteria.registerTempTable('df_criteria')

## handled for disease,title,phase,data_source,sponsor,sponsor_type,patient_segment,therapy_area,trial_start_dt
df_disease = spark.sql("""
select
    b.ctfo_trial_id,
    concat_ws('||',sort_array(collect_set(NULLIF(trim(a.disease_nm),'')),true)) as disease_nm,
    concat_ws('||',sort_array(collect_set(NULLIF(trim(a.therapeutic_area),'')),true)) as therapeutic_area
from
$$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_disease b left join $$client_name_ctfo_datastore_app_commons_$$db_env.d_disease a
on lower(trim(a.disease_id))=lower(trim(b.disease_id))
group by 1
""")
df_disease = df_disease.dropDuplicates()
df_disease.registerTempTable('df_disease')
# df_disease.write.mode('overwrite').saveAsTable('df_disease')

df_trial = spark.sql("""
select
    b.ctfo_trial_id,
	(case when trim(a.disease_nm) is null or trim(a.disease_nm) = '' then 'Other' else
    a.disease_nm end) as disease_nm,
	(case when trim(a.therapeutic_area) is null or trim(a.therapeutic_area) = '' then 'Other' else
    a.therapeutic_area end) as therapeutic_area,
    b.protocol_id,
    b.trial_title,
    b.trial_phase,
    b.trial_status,
    b.trial_start_dt,
    b.patient_segment
from
$$client_name_ctfo_datastore_app_commons_$$db_env.d_trial b left join df_disease a
on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id))
""")
df_trial = df_trial.dropDuplicates()
# df_trial.registerTempTable('df_trial')
df_trial.write.mode('overwrite').saveAsTable('df_trial')

sponsor_df = spark.sql("""
SELECT
    d.ctfo_trial_id,
    concat_ws("\||",sort_array(collect_set(distinct NULLIF(trim(s.sponsor_nm),'')),true)) as sponsor, 
    concat_ws("\||",sort_array(collect_set(distinct NULLIF(trim(s.subsidiary_sponsors),'')),true)) as subsidiary_sponsors,
    concat_ws("\||",sort_array(collect_set(distinct NULLIF(trim(s.sponsor_type),'')),true)) as sponsor_type
FROM df_trial d
left JOIN default.temp_sponsor_info s ON lower(trim(d.ctfo_trial_id)) = lower(trim(s.s_ctfo_trial_id))
group by 1
""")
sponsor_df.registerTempTable('sponsor_df')

temp_mce_kpi = spark.sql("""
SELECT distinct
ctfo_trial_id, trim(nct_id_exp) as nct_id  from
(select ctfo_trial_id, replace(nct_id,'\,','|') as nct_id from trial_uni_metric_engine_KPI_final)
LATERAL VIEW outer explode(split(nct_id, "\\\|"))
as nct_id_exp group by 1,2 """)
temp_mce_kpi.createOrReplaceTempView('temp_mce_kpi')

f_rpt_trial_universe = spark.sql("""
select 
a.ctfo_trial_id,
case 
when (c.protocol_id IS NOT NULL and trim(c.protocol_id) <> '') then c.protocol_id 
ELSE null end as protocol_ids,
concat_ws('|', collect_set(case when trim(n.nct_id) is not null and trim(n.nct_id) <> '' then n.nct_id else null end)) as nct_id,
c.therapeutic_area as therapy_area,
c.disease_nm as disease,
c.trial_title as title,
c.trial_phase as phase,
case when a.trial_status is null or trim(a.trial_status)='' then 'Others' else a.trial_status end as trial_status ,
coalesce(NULLIF(b.sponsor, ''), 'Other') as sponsor,
coalesce(NULLIF(b.subsidiary_sponsors, ''), 'Other') as  subsidiary_sponsors,
coalesce(NULLIF(b.sponsor_type, ''), 'Other') as  sponsor_type,
case when lower(trim(a.trial_status)) in {on_com} then a.no_of_countries else null end as no_of_countries,
case when lower(trim(a.trial_status)) in {on_com} then a.no_of_sites else null end as no_of_sites,
cast(c.trial_start_dt as date) as trial_start_dt,
CASE WHEN lower(trim(a.trial_status)) in {on_com}  THEN
CASE
  WHEN cast(a.enrollment_duration as double) < 0 THEN 0
  ELSE cast(a.enrollment_duration as double)
END 
ELSE NULL END AS enrollment_duration,
CASE when lower(trim(a.trial_status)) in {on_com_plan} THEN  CAST(a.patients_enrolled_planned as bigint) ELSE null END as patients_enrolled_planned,
CASE WHEN lower(trim(a.trial_status)) in {on_com}  
    THEN CAST(a.patients_enrolled_actual as bigint) 
ELSE null END as patients_enrolled_actual,
CASE when lower(trim(a.trial_status)) in {on_com}  THEN
CASE
  WHEN cast(a.enrollment_rate as double) < 0 THEN 0
  ELSE cast(a.enrollment_rate as double)
END       
ELSE NULL END AS enrollment_rate,
CASE when (lower(trim(a.trial_status)) in {on_com}) THEN
CASE
  WHEN cast(a.screen_failure_rate as double) < 0 THEN 0
  WHEN cast(a.screen_failure_rate as double)>100 THEN 100
  ELSE cast(a.screen_failure_rate as double)
END        
ELSE NULL END AS screen_failure_rate,
CASE WHEN lower(trim(a.trial_status)) in {on_com} THEN
CASE
  WHEN cast(a.patient_dropout_rate as double) < 0 THEN 0
  WHEN cast(a.patient_dropout_rate as double)>100 THEN 100
  ELSE cast(a.patient_dropout_rate as double)
END       
ELSE NULL END AS  patient_dropout_rate,
case when trim(d.exclude_label) = '' then 'Other' else d.exclude_label end as exclude_label,
c.patient_segment,
case when trim(d.study_design) = '' then 'Other' else d.study_design end as study_design,
case when trim(d.inclusion_criteria) = '' then 'Other' else d.inclusion_criteria end as inclusion_criteria,
case when trim(d.exclusion_criteria) = '' then 'Other' else d.exclusion_criteria end as exclusion_criteria,
case when lower(trim(a.trial_status)) in {on_com_plan} then  cast(a.patients_screened_planned as bigint) else null end as patients_screened_planned,
case when lower(trim(a.trial_status)) in {on_com}  then cast(a.patients_screened_actual as bigint) else null end as patients_screened_actual,
trim(d.ct_gov_url) as ct_gov_url
from 
trial_uni_metric_engine_KPI_final a
left join temp_mce_kpi n on lower(trim(a.ctfo_trial_id)) = lower(trim(n.ctfo_trial_id))
left join sponsor_df b on lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id))
left join df_trial c  on lower(trim(a.ctfo_trial_id)) = lower(trim(c.ctfo_trial_id))
left join df_criteria d on lower(trim(a.ctfo_trial_id)) = lower(trim(d.ctfo_trial_id))
group by 
a.ctfo_trial_id, c.protocol_id, c.therapeutic_area, c.disease_nm, c.trial_title, c.trial_phase, 
a.trial_status,b.sponsor, b.subsidiary_sponsors,b.sponsor_type,a.no_of_countries, a.no_of_sites, 
c.trial_start_dt, a.enrollment_duration, a.patients_enrolled_planned, a.patients_enrolled_actual, 
a.enrollment_rate, a.screen_failure_rate, a.patient_dropout_rate, d.exclude_label, c.patient_segment, 
d.study_design, d.inclusion_criteria, d.exclusion_criteria, a.patients_screened_planned, a.patients_screened_actual,
d.ct_gov_url
""".format(on_com=Ongoing_Completed, on_com_plan=Ongoing_Completed_Planned))
f_rpt_trial_universe = f_rpt_trial_universe.dropDuplicates()
f_rpt_trial_universe.createOrReplaceTempView('f_rpt_trial_universe')
f_rpt_trial_universe.write.mode("overwrite").saveAsTable("f_rpt_trial_universe")

# Write Final Table f_rpt_trial_universe to S3
# write_path = path.replace("table_name", "f_rpt_trial_universe")
# f_rpt_trial_universe.registerTempTable("f_rpt_trial_universe")
# f_rpt_trial_universe.write.format("parquet").mode("overwrite").save(path=write_path)


write_path = path.replace("table_name", "f_rpt_trial_universe")
# write_path_csv = path_csv.replace("table_name", "f_rpt_trial_universe")
# write_path_csv = write_path_csv + "_csv/"

# f_rpt_patient_details_df.write.format("parquet").mode("overwrite").save(path=write_path)


# final_trial_df.coalesce(1).write.option("header", "false").option("sep", "|").option('quote', '"') \
#     .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)


f_rpt_trial_universe.coalesce(1).write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_trial_universe partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   f_rpt_trial_universe
""")

if f_rpt_trial_universe.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_trial_universe as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_trial_universe")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")

