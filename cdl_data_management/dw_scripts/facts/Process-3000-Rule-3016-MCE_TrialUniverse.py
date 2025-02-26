################################# Module Information #####################################################
#  Module Name         : Trial Universe Metric Engine Calculation Data
#  Purpose             : To create the Trial Universe KPI
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : Trial Universe KPIs
#  Last changed on     : 23-01-2025
#  Last changed by     : YASH
#  Reason for change   : TASCAN
##########################################################################################################


import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lower, col
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + "/applications/commons/mce/mce_src/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

########## Status Variablization ###################
trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')

# Values in Variables

Ongoing = trial_status_mapping.filter(trial_status_mapping.br_status == "Ongoing").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Completed = trial_status_mapping.filter(trial_status_mapping.br_status == "Completed").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Planned = trial_status_mapping.filter(trial_status_mapping.br_status == "Planned").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Others = trial_status_mapping.filter(trial_status_mapping.br_status == "Others").select(
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

trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

status_mapping = spark.sql(""" select distinct * from (select data_source,raw_status, status from trial_status_mapping_temp 
union select data_source,status, status from trial_status_mapping_temp )  """)
status_mapping.registerTempTable('status_mapping')
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
# xref_src_trial.write.mode('overwrite').saveAsTable('xref_src_trial')

######################ct############################

aact_trial_status = spark.sql("""
select ctfo_trial_id,
trial_status
from
(select ctfo_trial_id,trial_status,row_number() over(partition by ctfo_trial_id order by prec) as rnk
from
(select ctfo_trial_id,trial_status, case when lower(trim(trial_status))='ongoing' then 1 when lower(trim(trial_status))='completed' then 2 when lower(trim(trial_status))='planned' then 3
 when lower(trim(trial_status))='others' then 4 else 5 end as prec
from xref_src_trial f 
left join 
(select nct_id,g.status as trial_status,overall_status from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies b 
left join
(select raw_status,status from status_mapping where lower(trim(data_source))='aact') g on lower(trim(b.overall_status))=lower(trim(g.raw_status))) h 
on h.nct_id=f.src_trial_id where lower(trim(f.data_src_nm))='aact'
) a) b where b.rnk=1 group by 1,2""")

aact_trial_status = aact_trial_status.dropDuplicates()
aact_trial_status.registerTempTable('aact_trial_status')

countries_sites_actual = spark.sql("""select s.ctfo_trial_id,sum(s.no_of_sites) as no_of_sites ,sum(s.no_of_countries) as  no_of_countries from(
(select * from xref_src_trial) a
left join
(select nct_id,count(distinct name) as no_of_sites,count(distinct country) as no_of_countries  from $$client_name_ctfo_datastore_staging_$$db_env.aact_facilities where lower(trim(status)) in {on_com} group by nct_id) b
on a.src_trial_id = b.nct_id)  as s
group by 1
""".format(on_com=Ongoing_Completed))
countries_sites_actual.registerTempTable("countries_sites_actual")

actual_kpi = spark.sql(""" select s.ctfo_trial_id,sum(s.patients_enrolled) as patients_enrolled_actual from(
(select * from xref_src_trial) a
left join 
(select nct_id,max(enrollment) as patients_enrolled from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies where lower(trim(overall_status)) in {on_com} group by 1) b on a.src_trial_id = b.nct_id) as s group by 1
""".format(on_com=Ongoing_Completed))
actual_kpi.registerTempTable("actual_kpi")
aact_study_df = spark.sql("""select 
A.data_src_nm,
A.ctfo_trial_id,
A.temp_precedence,
A.trial_status,
A.no_of_countries,
A.no_of_sites,
min(A.trial_start_dt) as trial_start_dt,
A.patients_enrolled_actual,
A.patients_screened_actual
from (select 
f.data_src_nm,
f.ctfo_trial_id,
'4' as temp_precedence,
b.no_of_countries as no_of_countries,
b.no_of_sites as no_of_sites,
h.start_date as trial_start_dt,
c.patients_enrolled_actual as patients_enrolled_actual,
'' as patients_screened_actual,
case when st.trial_status is null then 'Others' else st.trial_status end as trial_status 
from 
(select * from xref_src_trial) f
left outer join
(select start_date,nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies) h on f.src_trial_id = h.nct_id
left outer join
(select * from countries_sites_actual) b on f.ctfo_trial_id = b.ctfo_trial_id
left outer join 
(select * from actual_kpi) c on f.ctfo_trial_id = c.ctfo_trial_id
left outer join 
(select * from aact_trial_status) st on lower(trim(f.ctfo_trial_id))=lower(trim(st.ctfo_trial_id)) where lower(trim(f.data_src_nm))='aact'
group by 1,2,3,4,5,6,7,8,9) A group by 1,2,3,4,5,6,8,9
""")
aact_study_df = aact_study_df.dropDuplicates()
aact_study_df.registerTempTable('aact_study_df')

aact_nct = spark.sql("""
select ctfo_trial_id, concat_ws('|',collect_set(c.nct_id)) as nct_id
 from 
xref_src_trial f
left outer join 
(select nct_id from  $$client_name_ctfo_datastore_staging_$$db_env.aact_studies) c on 
lower(trim(c.nct_id))=lower(trim(f.src_trial_id)) where lower(trim(f.data_src_nm))='aact' group by 1
""")
aact_nct.registerTempTable('aact_nct')

aact_trial_uni_mce = spark.sql("""SELECT
a.ctfo_trial_id,
d.nct_id,
'' as protocol_type,
'' as trial_type,
'' as trial_design,
a.trial_start_dt,
'' as trial_end_dt,
'' as patient_segment,
'' as mesh_terms,
'' as results_date,
'' as primary_endpoints_details,
'' as primary_endpoints_reported_date,
'' as secondary_endpoints_details,
'' as secondary_endpoints_reported_date,
'' as number_of_sites_planned,
'' as number_of_sites_activated,
case when lower(trim(b.trial_status)) in {on_com} then a.no_of_sites else null end as number_of_sites_actual,
'' as number_of_countries_planned,
'' as number_of_countries_activated,
case when lower(trim(b.trial_status)) in {on_com} then a.no_of_countries else null end as number_of_countries_actual,
'' as number_of_investigators,
'' as inclusion_criteria,
'' as exclusion_criteria,
''  AS exclude_label,
'' AS study_design,
'' as patient_dropout_rate,
'' as study_objective,
'' as patients_consented_planned,
'' as patients_consented_actual,
'' as patients_screened_planned,
case when lower(trim(b.trial_status)) in {on_com} then a.patients_screened_actual else null end as patients_screened_actual,
'' as dropped_at_screening_actual,
'' as patients_randomized_planned,
case when lower(trim(b.trial_status)) in {on_com} then a.patients_enrolled_actual else null end as patients_randomized_actual,
'' as patients_completed_actual,
'' as patients_dropped_actual,
'' as sponsor,
'' as sponsor_type,
'' as first_site_initiation_visit_bsln_dt,
'' as first_site_initiation_visit_planned_dt,
'' as first_site_initiation_visit_actual_dt,
'' as first_subject_first_treatment_bsln_dt,
'' as first_subject_first_treatment_planned_dt,
'' as first_subject_first_treatment_actual_dt,
'' as last_subject_first_visit_bsln_dt,
'' as last_subject_first_visit_planned_dt,
'' as last_subject_first_visit_actual_dt,
'' as last_subject_first_treatment_bsln_dt,
'' as last_subject_first_treatment_planned_dt,
'' as last_subject_first_treatment_actual_dt,
a.data_src_nm as source,
'' as fsiv_fsfr_delta,
'' as enrollment_rate,
'' as screen_failure_rate,
'' as lost_opp_time,
'' as enrollment_duration,
'' as cntry_lsfr_dt,
case when b.trial_status is NULL then 'Others ' else b.trial_status end  as standard_status,
'' as protocol_ids,
'' as therapy_area,
'' as disease,
'' as title,
'' as phase,
'' as last_subject_consented_dt,
'' as trial_start_dt_rd
from 
aact_study_df a
left join
aact_trial_status b on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id))
left join aact_nct d on lower(trim(a.ctfo_trial_id))=lower(trim(d.ctfo_trial_id))

""".format(on_com=Ongoing_Completed, com_status=Completed_variable,
           status=Ongoing_Completed_Planned))
aact_trial_uni_mce = aact_trial_uni_mce.dropDuplicates()
aact_trial_uni_mce.registerTempTable('aact_trial_uni_mce')
# aact_trial_uni_mce.write.mode('overwrite').saveAsTable('aact_trial_uni_mce')


############################################## CITELINE ############################################

# added status as Ongoing,Completed
# patient_dropout_rate,patients_screened_planned,patients_screened_actual #-> added in above df as there are flowing as null for citleine

citeline_trailtrove_temp = spark.sql("""
SELECT
    trial_id,
    nct_id,
    trial_therapeutic_areas_name AS therapy_area,
    coalesce(trial_mesh_term_name,disease_name) AS disease,
    trial_title AS title,
    trial_phase AS phase,
    trial_status,
    study_design,
    inclusion_criteria,
    exclusion_criteria,
   study_keywords,
    sponsor_name AS sponsor,
    sponsor_type,
    trial_countries AS country,
    number_of_sites AS sites,
   actual_trial_start_date AS trial_start_dt,
    enrollment_duration,
    target_accrual AS patients_enrolled_planned,
    trial_actual_accrual AS patients_enrolled_actual,
	trial_patients_per_site_per_month AS enrollment_rate,
	protocol_ids,
    '' As patient_dropout_rate,
    '' As patients_screened_planned,
    '' As patients_screened_actual
FROM $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove where lower(trim(trial_status)) in {on_com}
""".format(on_com=Ongoing_Completed, on_com_plan=Ongoing_Completed_Planned))
citeline_trailtrove_temp = citeline_trailtrove_temp.dropDuplicates()
citeline_trailtrove_temp.registerTempTable('citeline_trailtrove_temp')

# Fetch patients_enrolled_planned ,patients_screened_planned from citeline_trailtrove_temp2 in final citeline df

citeline_trailtrove_temp2 = spark.sql("""
SELECT
    trial_id,
    nct_id,
    trial_therapeutic_areas_name AS therapy_area,
    coalesce(trial_mesh_term_name,disease_name) AS disease,
    trial_title AS title,
    trial_phase AS phase,
    trial_status,
    study_design,
    inclusion_criteria,
    exclusion_criteria,
   study_keywords,
    sponsor_name AS sponsor,
    sponsor_type,
    trial_countries AS country,
    number_of_sites AS sites,
   actual_trial_start_date AS trial_start_dt,
    enrollment_duration,
    target_accrual AS patients_enrolled_planned,
    trial_actual_accrual AS patients_enrolled_actual,
	trial_patients_per_site_per_month AS enrollment_rate,
	protocol_ids,
    '' As patient_dropout_rate,
    '' As patients_screened_planned,
    '' As patients_screened_actual
FROM $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove where lower(trim(trial_status)) in {on_com_plan}
""".format(on_com=Ongoing_Completed, on_com_plan=Ongoing_Completed_Planned))
citeline_trailtrove_temp2 = citeline_trailtrove_temp2.dropDuplicates()
citeline_trailtrove_temp2.registerTempTable('citeline_trailtrove_temp2')

citeline_sites_country = spark.sql("""
SELECT s.* from (select distinct
 site_id ,trim(site_trial_id_exp) as trial_id, site_location_country as country,trim(status) as status
FROM $$client_name_ctfo_datastore_staging_$$db_env.citeline_organization 
LATERAL VIEW outer posexplode(split(site_trial_id, "\\\;"))
as pos1,site_trial_id_exp
LATERAL VIEW outer posexplode(split(site_trial_status, "\\\;")) as pos2,status where pos1=pos2
  group by 1,2,3,4) s where lower(trim(status))in {on_com} """.format(on_com=Ongoing_Completed))

citeline_sites_country = citeline_sites_country.dropDuplicates()
citeline_sites_country.registerTempTable('citeline_sites_country')
# citeline_sites_country.write.mode('overwrite').saveAsTable('citeline_sites_country')


citeline_kpii = spark.sql("""
SELECT distinct
trial_id, country_exp as country,sites,patients_enrolled_planned,patients_enrolled_actual,enrollment_duration
from citeline_trailtrove_temp
LATERAL VIEW outer explode(split(country, "\\\|"))
as country_exp group by 1,2,3,4,5,6 """)

citeline_kpii = citeline_kpii.dropDuplicates()
citeline_kpii.registerTempTable('citeline_kpii')

citeline_kpii_plan = spark.sql("""
SELECT distinct
trial_id, country_exp as country,sites,patients_enrolled_planned,patients_enrolled_actual,enrollment_duration
from citeline_trailtrove_temp2
LATERAL VIEW outer explode(split(country, "\\\|"))
as country_exp group by 1,2,3,4,5,6 """)

citeline_kpii_plan = citeline_kpii_plan.dropDuplicates()
citeline_kpii_plan.registerTempTable('citeline_kpii_plan')
#citeline_kpii_plan.write.mode('overwrite').saveAsTable('citeline_kpii_plan')
# fetch no_of_countries from df 'citeline_kpi' and left join with final table
citeline_kpi = spark.sql("""
SELECT
ckp.trial_id,
    COUNT(DISTINCT sc.country) AS no_of_countries,
    COUNT (DISTINCT sc.site_id) AS no_of_sites,
    MAX(ckp.patients_enrolled_planned) AS patients_enrolled_planned,
    MAX(CAST(ck.patients_enrolled_actual AS INT)) AS patients_enrolled_actual,
    MAX(ck.enrollment_duration) AS enrollment_duration,
    '' AS patient_dropout_rate
FROM citeline_kpii_plan ckp   
left join citeline_kpii ck ON lower(trim(ckp.trial_id)) = lower(trim(ck.trial_id))
left join citeline_sites_country sc ON lower(trim(ckp.trial_id)) = lower(trim(sc.trial_id))
GROUP BY ckp.trial_id
""")

citeline_kpi = citeline_kpi.dropDuplicates()
citeline_kpi.registerTempTable('citeline_kpi')
#citeline_kpi.write.mode('overwrite').saveAsTable('citeline_kpi')
# below tables could be exculded for citeline source except citeline_trial_uni_mce

citeline_exclude_label_temp = spark.sql("""

(SELECT
    trial_id,
  concat(title,study_design,study_keywords) as search_col
from citeline_trailtrove_temp)
""")

citeline_exclude_label_temp.registerTempTable('citeline_exclude_label_temp')

citeline_exclude_label_temp_1 = spark.sql("""
select trial_id,
CASE WHEN lower(search_col) like '%open-label%' OR lower(search_col) like '%open label%' then 'OL' else '' end as excl_label_ol,
CASE WHEN lower(search_col) like '%long term%' OR lower(search_col) like '%extension%' then 'LTE' else ''  end as excl_label_lte,
CASE WHEN lower(search_col) like '%juvenile%' OR lower(search_col) like '%pediatric%' then 'JT' else '' end as excl_label_jt
from citeline_exclude_label_temp
""")

citeline_exclude_label_temp_1.registerTempTable('citeline_exclude_label_temp_1')

citeline_exclude_label = spark.sql("""
select trial_id, concat_ws('|', excl_label_ol, excl_label_lte, excl_label_jt) as exclude_label
FROM citeline_exclude_label_temp_1
""")
citeline_exclude_label.registerTempTable('citeline_exclude_label')

citeline_join = spark.sql("""
SELECT
    k.trial_id, 
    t.nct_id,
    t.therapy_area,
    t.disease,
    t.title,
    t.phase,
    t.trial_status,
    t.sponsor,
    t.sponsor_type,
    case when (k.no_of_countries>0 and k.no_of_countries is not null) then k.no_of_countries else null end as no_of_countries,
    case when (k.no_of_sites>0 and k.no_of_sites is not null) then k.no_of_sites else null end as no_of_sites,
    t.trial_start_dt,
    t.enrollment_duration,
    k.patients_enrolled_planned,
    k.patients_enrolled_actual,
    t.enrollment_rate,
    e.exclude_label,
    t.study_design,
    t.inclusion_criteria as inclusion_criteria,
    t.exclusion_criteria as exclusion_criteria,
    t.protocol_ids
FROM citeline_kpi k
LEFT JOIN citeline_trailtrove_temp t ON lower(trim(t.trial_id)) = lower(trim(k.trial_id))
LEFT JOIN citeline_exclude_label e ON lower(trim(k.trial_id)) = lower(trim(e.trial_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21""")

citeline_join = citeline_join.dropDuplicates()
citeline_join.registerTempTable('citeline_join')
#citeline_join.write.mode('overwrite').saveAsTable('citeline_join')

xref_src_trial_citeline = spark.sql("""
SELECT
    data_src_nm,
    src_trial_id,
    ctfo_trial_id,
    b.trial_status,
	b.protocol_ids,
	replace(b.nct_id,'\|',',') as nct_id,
	b.trial_therapeutic_areas_name AS therapy_area,
	coalesce(b.trial_mesh_term_name,b.disease_name) AS disease,
    b.trial_title AS title,
    b.trial_phase AS phase,
	b.sponsor_name as sponsor,
	b.sponsor_type,
	coalesce(b.actual_trial_start_date,b.start_date)  as trial_start_dt,
	c.exclude_label,
	b.study_design,
	b.inclusion_criteria,
	b.exclusion_criteria
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial a 
left join $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove b
on lower(trim(a.src_trial_id)) = lower(trim(b.trial_id)) 
left join citeline_exclude_label c
on lower(trim(a.src_trial_id)) = lower(trim(c.trial_id)) 
WHERE data_src_nm = 'citeline'
""")

xref_src_trial_citeline = xref_src_trial_citeline.dropDuplicates()
xref_src_trial_citeline.registerTempTable('xref_src_trial_citeline')
#xref_src_trial_citeline.write.mode('overwrite').saveAsTable('xref_src_trial_citeline')

citeline_df_temp = spark.sql("""
SELECT 
	distinct s.data_src_nm AS data_src_nm,
	s.ctfo_trial_id AS ctfo_trial_id,
    concat_ws('|',collect_set(s.protocol_ids)) as protocol_ids ,
    concat_ws('|',collect_set(CASE WHEN s.nct_id<>'' THEN s.nct_id end)) as nct_id ,
    "3" AS temp_precedence,
    concat_ws('|',collect_set(s.therapy_area)) as therapy_area ,
    concat_ws('|',collect_set(s.disease)) as disease ,
    concat_ws('|',collect_set(s.title)) as title ,
    concat_ws('|',collect_set(s.phase)) as phase ,
    case when h.status is null then 'Others' else h.status end  as trial_status, 
    concat_ws('|',collect_set(s.sponsor)) as sponsor,
    concat_ws('|',collect_set(s.sponsor_type)) as sponsor_type, 
    sum(t.no_of_countries) as no_of_countries,
    sum(t.no_of_sites) as no_of_sites,
    min(s.trial_start_dt) as trial_start_dt ,
    percentile(t.enrollment_duration,0.5,10000) as randomization_duration,
    sum(t.patients_enrolled_planned) as patients_enrolled_planned,
    '' as patients_screened,
    sum(t.patients_enrolled_actual) as patients_enrolled_actual,
    percentile(t.enrollment_rate,0.5,10000) as randomization_rate,
    '' as screen_failure_rate,
    '' AS patient_dropout_rate,
	concat_ws('|',collect_set(s.exclude_label)) as exclude_label ,
	concat_ws('|',collect_set(s.study_design)) as study_design ,
	concat_ws('|',collect_set(s.inclusion_criteria )) as inclusion_criteria ,
    concat_ws('|',collect_set(s.exclusion_criteria )) as exclusion_criteria 

FROM xref_src_trial_citeline s
left join citeline_join t
    ON lower(trim(t.trial_id)) = lower(trim(s.src_trial_id))
    left join
    (select raw_status,status from status_mapping where lower(trim(data_source))='citeline') h on lower(trim(s.trial_status))=lower(trim(h.raw_status))
  group by 1,2,5,10,18,21,22
""")

citeline_df_temp = citeline_df_temp.dropDuplicates()
citeline_df_temp.registerTempTable('citeline_df_temp')
#citeline_df_temp.write.mode('overwrite').saveAsTable('citeline_df_temp')

citeline_trial_status = spark.sql("""
select ctfo_trial_id,
trial_status
from
(select ctfo_trial_id,trial_status,row_number() over(partition by ctfo_trial_id order by prec) as rnk
from
(select ctfo_trial_id,trial_status, case when lower(trim(trial_status))='ongoing' then 1 when lower(trim(trial_status))='completed' then 2 
when lower(trim(trial_status))='planned' then 3
 when lower(trim(trial_status))='others' then 4 else 5 end as prec
from citeline_df_temp) a) b where b.rnk=1 group by 1,2""")

citeline_trial_status = citeline_trial_status.dropDuplicates()
citeline_trial_status.registerTempTable('citeline_trial_status')

citeline_trial_uni_mce = spark.sql("""SELECT
a.ctfo_trial_id,
concat_ws('|',collect_set(CASE WHEN nct_id<>'' THEN nct_id end)) as nct_id ,
'' as protocol_type,
'' as trial_type,
'' as trial_design,
case when b.trial_status in ('Ongoing','Completed') then min(a.trial_start_dt) else null end as trial_start_dt,
'' as trial_end_dt,
'' as patient_segment,
'' as mesh_terms,
'' as results_date,
'' as primary_endpoints_details,
'' as primary_endpoints_reported_date,
'' as secondary_endpoints_details,
'' as secondary_endpoints_reported_date,
'' as number_of_sites_planned,
'' as number_of_sites_activated,
case when b.trial_status in ('Ongoing','Completed') then sum(a.no_of_sites) else null end  as number_of_sites_actual,
'' as number_of_countries_planned,
'' as number_of_countries_activated,
case when b.trial_status in ('Ongoing','Completed') then sum(a.no_of_countries) else null end  as number_of_countries_actual ,
'' as number_of_investigators,
concat_ws('|',collect_set(inclusion_criteria)) as inclusion_criteria ,
concat_ws('|',collect_set(exclusion_criteria)) as exclusion_criteria ,
concat_ws('|',collect_set(exclude_label)) as exclude_label ,
concat_ws('|',collect_set(study_design)) as study_design ,
'' as patient_dropout_rate,
'' as study_objective,
'' as patients_consented_planned,
'' as patients_consented_actual,
'' as patients_screened_planned,
case when lower(trim(b.trial_status)) in {on_com}  then sum(a.patients_screened) else null end as patients_screened_actual,
'' as dropped_at_screening_actual,
case when lower(trim(b.trial_status)) in {on_com_plan} then sum(a.patients_enrolled_planned) else null end as patients_randomized_planned,
case when lower(trim(b.trial_status)) in {on_com}  then sum(a.patients_enrolled_actual) else null end as patients_randomized_actual,
'' as patients_completed_actual,
'' as patients_dropped_actual,
concat_ws('|',collect_set(a.sponsor)) as sponsor,
concat_ws('|',collect_set(sponsor_type)) as sponsor_type,
'' as first_site_initiation_visit_bsln_dt,
'' as first_site_initiation_visit_planned_dt,
'' as first_site_initiation_visit_actual_dt,
'' as first_subject_first_treatment_bsln_dt,
'' as first_subject_first_treatment_planned_dt,
'' as first_subject_first_treatment_actual_dt,
'' as last_subject_first_visit_bsln_dt,
'' as last_subject_first_visit_planned_dt,
'' as last_subject_first_visit_actual_dt,
'' as last_subject_first_treatment_bsln_dt,
'' as last_subject_first_treatment_planned_dt,
'' as last_subject_first_treatment_actual_dt,
a.data_src_nm as source,
'' as fsiv_fsfr_delta,
case when b.trial_status in ('Ongoing','Completed') then percentile(randomization_rate,0.5,10000) else null end as enrollment_rate,
'' as screen_failure_rate,
'' as lost_opp_time,
case when b.trial_status in ('Ongoing','Completed') then percentile(randomization_duration,0.5,10000) else null end as enrollment_duration,
'' as cntry_lsfr_dt,
b.trial_status as standard_status,
concat_ws('|',collect_set(protocol_ids)) as protocol_ids ,
concat_ws('|',collect_set(therapy_area)) as therapy_area ,
concat_ws('|',collect_set(disease)) as disease ,
concat_ws('|',collect_set(title)) as title ,
concat_ws('|',collect_set(phase)) as phase ,
'' as last_subject_consented_dt,
'' as trial_start_dt_rd
FROM citeline_df_temp a
left join 
citeline_trial_status b on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id))
group by
1,3,4,5,7,8,9,10,11,12,13,14,15,16,18,19,21,26,27,28,29,32,35,36,39,40,41,42,43,44,45,46,47,48,49,50,51,52,54,55,57,58,64,65

""".format(on_status=Ongoing_variable, com_status=Completed_variable, on_com=Ongoing_Completed,
           on_com_plan=Ongoing_Completed_Planned))
citeline_trial_uni_mce = citeline_trial_uni_mce.dropDuplicates()
citeline_trial_uni_mce.registerTempTable('citeline_trial_uni_mce')
#citeline_trial_uni_mce.write.mode('overwrite').saveAsTable('citeline_trial_uni_mce')

############################################## TASCAN ############################################

tascan_trail_temp = spark.sql("""
SELECT
    tt.trial_id,
    tt.nct_id,
    tt.trial_therapeutic_area_name AS therapy_area,
    tt.mesh_term_names AS disease,
    tt.trial_title AS title,
    tt.trial_phase AS phase,
    tt.state as trial_status,
    tt.trial_design as study_design,
    tt.inclusion_criteria,
    tt.exclusion_criteria,
    '' as study_keywords,
    tt.sponsor_name AS sponsor,
    tt.sponsor_type,
    ts.site_location_country AS country,
    tt.number_of_sites AS sites,
    tt.trial_actual_start_date AS trial_start_dt,
    tt.trial_duration_in_months as enrollment_duration,
    '' AS patients_enrolled_planned,
    tt.enrollment AS patients_enrolled_actual,
	tt.trial_patients_per_site_per_month AS enrollment_rate,
	'' As protocol_ids,
    '' As patient_dropout_rate,
    '' As patients_screened_planned,
    '' As patients_screened_actual
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial tt
JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_site ts
on tt.trial_id = ts.site_trial_id
where lower(trim(tt.state)) in {on_com}
""".format(on_com=Ongoing_Completed, on_com_plan=Ongoing_Completed_Planned))
tascan_trail_temp = tascan_trail_temp.dropDuplicates()
tascan_trail_temp.registerTempTable('tascan_trail_temp')



tascan_trail_temp2 = spark.sql("""
SELECT
    tt.trial_id,
    tt.nct_id,
    tt.trial_therapeutic_area_name AS therapy_area,
    tt.mesh_term_names AS disease,
    tt.trial_title AS title,
    tt.trial_phase AS phase,
    tt.state as trial_status,
    tt.trial_design as study_design,
    tt.inclusion_criteria,
    tt.exclusion_criteria,
    '' as study_keywords,
    tt.sponsor_name AS sponsor,
    tt.sponsor_type,
    ts.site_location_country AS country,
    tt.number_of_sites AS sites,
    tt.trial_actual_start_date AS trial_start_dt,
    tt.trial_duration_in_months as enrollment_duration,
    '' AS patients_enrolled_planned,
    tt.enrollment AS patients_enrolled_actual,
	tt.trial_patients_per_site_per_month AS enrollment_rate,
	'' As protocol_ids,
    '' As patient_dropout_rate,
    '' As patients_screened_planned,
    '' As patients_screened_actual
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial tt
JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_site ts
on tt.trial_id = ts.site_trial_id
where lower(trim(tt.state)) in {on_com_plan}
""".format(on_com=Ongoing_Completed, on_com_plan=Ongoing_Completed_Planned))
tascan_trail_temp2 = tascan_trail_temp2.dropDuplicates()
tascan_trail_temp2.registerTempTable('tascan_trail_temp2')

tascan_sites_country = spark.sql("""
SELECT s.* from (select distinct
 site_id ,trim(site_trial_id_exp) as trial_id, site_location_country as country,trim(status) as status
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_site ts
join $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial tt
on tt.trial_id = ts.site_trial_id
LATERAL VIEW outer posexplode(split(site_trial_id, "\\\;"))
as pos1,site_trial_id_exp
LATERAL VIEW outer posexplode(split(tt.state, "\\\;")) as pos2,status where pos1=pos2
  group by 1,2,3,4) s where lower(trim(status))in {on_com} """.format(on_com=Ongoing_Completed))


tascan_sites_country = tascan_sites_country.dropDuplicates()
tascan_sites_country.registerTempTable('tascan_sites_country')
#tascan_sites_country.write.mode('overwrite').saveAsTable('tascan_sites_country')

tascan_kpii = spark.sql("""
SELECT distinct
trial_id, country_exp as country,sites,patients_enrolled_planned,patients_enrolled_actual,enrollment_duration
from tascan_trail_temp
LATERAL VIEW outer explode(split(country, "\\\|"))
as country_exp group by 1,2,3,4,5,6 """)


tascan_kpii = tascan_kpii.dropDuplicates()
tascan_kpii.registerTempTable('tascan_kpii')



tascan_kpii_plan = spark.sql("""
SELECT distinct
trial_id, country_exp as country,sites,patients_enrolled_planned,patients_enrolled_actual,enrollment_duration
from tascan_trail_temp2
LATERAL VIEW outer explode(split(country, "\\\|"))
as country_exp group by 1,2,3,4,5,6 """)


tascan_kpii_plan = tascan_kpii_plan.dropDuplicates()
tascan_kpii_plan.registerTempTable('tascan_kpii_plan')
tascan_kpii_plan.write.mode('overwrite').saveAsTable('tascan_kpii_plan')

#fetch no_of_countries from df 'tascan_kpi' and left join with final table
tascan_kpi = spark.sql("""
SELECT
tkp.trial_id,
    COUNT(DISTINCT sc.country) AS no_of_countries,
    COUNT (DISTINCT sc.site_id) AS no_of_sites,
    MAX(tkp.patients_enrolled_planned) AS patients_enrolled_planned,
    MAX(CAST(tk.patients_enrolled_actual AS INT)) AS patients_enrolled_actual,
    MAX(tk.enrollment_duration) AS enrollment_duration,
    '' AS patient_dropout_rate
FROM tascan_kpii_plan tkp   
left join tascan_kpii tk ON lower(trim(tkp.trial_id)) = lower(trim(tk.trial_id))
left join tascan_sites_country sc ON lower(trim(tkp.trial_id)) = lower(trim(sc.trial_id))
GROUP BY tkp.trial_id
""")

tascan_kpi = tascan_kpi.dropDuplicates()
tascan_kpi.registerTempTable('tascan_kpi')
#tascan_kpi.write.mode('overwrite').saveAsTable('tascan_kpi')


tascan_exclude_label_temp = spark.sql("""

(SELECT
    trial_id,
  concat(title,study_design,study_keywords) as search_col
from tascan_trail_temp)
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

tascan_exclude_label = spark.sql("""
select trial_id, concat_ws('|', excl_label_ol, excl_label_lte, excl_label_jt) as exclude_label
FROM tascan_exclude_label_temp_1
""")
tascan_exclude_label.registerTempTable('tascan_exclude_label')

tascan_join = spark.sql("""
SELECT
    k.trial_id, 
    t.nct_id,
    t.therapy_area,
    t.disease,
    t.title,
    t.phase,
    t.trial_status,
    t.sponsor,
    t.sponsor_type,
    case when (k.no_of_countries>0 and k.no_of_countries is not null) then k.no_of_countries else null end as no_of_countries,
    case when (k.no_of_sites>0 and k.no_of_sites is not null) then k.no_of_sites else null end as no_of_sites,
    t.trial_start_dt,
    t.enrollment_duration,
    k.patients_enrolled_planned,
    k.patients_enrolled_actual,
    t.enrollment_rate,
    e.exclude_label,
    t.study_design,
    t.inclusion_criteria as inclusion_criteria,
    t.exclusion_criteria as exclusion_criteria,
    t.protocol_ids
FROM tascan_kpi k
LEFT JOIN tascan_trail_temp t ON lower(trim(t.trial_id)) = lower(trim(k.trial_id))
LEFT JOIN tascan_exclude_label e ON lower(trim(k.trial_id)) = lower(trim(e.trial_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21""")

tascan_join = tascan_join.dropDuplicates()
tascan_join.registerTempTable('tascan_join')
#tascan_join.write.mode('overwrite').saveAsTable('tascan_join')

xref_src_trial_tascan = spark.sql("""
SELECT
    data_src_nm,
    src_trial_id,
    ctfo_trial_id,
    b.state as trial_status,
	'' as protocol_ids,
	replace(b.nct_id,'\|',',') as nct_id,
	b.trial_therapeutic_area_name AS therapy_area,
	coalesce(b.mesh_term_names,b.disease_name) AS disease,
    b.trial_title AS title,
    b.trial_phase AS phase,
	b.sponsor_name as sponsor,
	b.sponsor_type,
	b.trial_actual_start_date  as trial_start_dt,
	c.exclude_label,
	b.trial_design,
	b.inclusion_criteria,
	b.exclusion_criteria,
    b.trial_design as study_design
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial a 
left join $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial b
on lower(trim(a.src_trial_id)) = lower(trim(b.trial_id)) 
left join tascan_exclude_label c
on lower(trim(a.src_trial_id)) = lower(trim(c.trial_id)) 
WHERE data_src_nm = 'tascan'
""")

xref_src_trial_tascan = xref_src_trial_tascan.dropDuplicates()
xref_src_trial_tascan.registerTempTable('xref_src_trial_tascan')
#xref_src_trial_tascan.write.mode('overwrite').saveAsTable('xref_src_trial_tascan')

tascan_df_temp = spark.sql("""
SELECT 
	distinct s.data_src_nm AS data_src_nm,
	s.ctfo_trial_id AS ctfo_trial_id,
    concat_ws('|',collect_set(s.protocol_ids)) as protocol_ids ,
    concat_ws('|',collect_set(CASE WHEN s.nct_id<>'' THEN s.nct_id end)) as nct_id ,
    "3" AS temp_precedence,
    concat_ws('|',collect_set(s.therapy_area)) as therapy_area ,
    concat_ws('|',collect_set(s.disease)) as disease ,
    concat_ws('|',collect_set(s.title)) as title ,
    concat_ws('|',collect_set(s.phase)) as phase ,
    case when h.status is null then 'Others' else h.status end  as trial_status, 
    concat_ws('|',collect_set(s.sponsor)) as sponsor,
    concat_ws('|',collect_set(s.sponsor_type)) as sponsor_type, 
    sum(t.no_of_countries) as no_of_countries,
    sum(t.no_of_sites) as no_of_sites,
    min(s.trial_start_dt) as trial_start_dt ,
    percentile(t.enrollment_duration,0.5,10000) as randomization_duration,
    sum(t.patients_enrolled_planned) as patients_enrolled_planned,
    '' as patients_screened,
    sum(t.patients_enrolled_actual) as patients_enrolled_actual,
    percentile(t.enrollment_rate,0.5,10000) as randomization_rate,
    '' as screen_failure_rate,
    '' AS patient_dropout_rate,
	concat_ws('|',collect_set(s.exclude_label)) as exclude_label ,
	concat_ws('|',collect_set(s.study_design)) as study_design ,
	concat_ws('|',collect_set(s.inclusion_criteria )) as inclusion_criteria ,
    concat_ws('|',collect_set(s.exclusion_criteria )) as exclusion_criteria 

FROM xref_src_trial_tascan s
left join tascan_join t
    ON lower(trim(t.trial_id)) = lower(trim(s.src_trial_id))
    left join
    (select raw_status,status from status_mapping where lower(trim(data_source))='tascan') h on lower(trim(s.trial_status))=lower(trim(h.raw_status))
  group by 1,2,5,10,18,21,22
""")

tascan_df_temp = tascan_df_temp.dropDuplicates()
tascan_df_temp.registerTempTable('tascan_df_temp')
#tascan_df_temp.write.mode('overwrite').saveAsTable('tascan_df_temp')

tascan_trial_status =spark.sql("""
select ctfo_trial_id,
trial_status
from
(select ctfo_trial_id,trial_status,row_number() over(partition by ctfo_trial_id order by prec) as rnk
from
(select ctfo_trial_id,trial_status, case when lower(trim(trial_status))='ongoing' then 1 when lower(trim(trial_status))='completed' then 2 
when lower(trim(trial_status))='planned' then 3
 when lower(trim(trial_status))='others' then 4 else 5 end as prec
from tascan_df_temp) a) b where b.rnk=1 group by 1,2""")

tascan_trial_status = tascan_trial_status.dropDuplicates()
tascan_trial_status.registerTempTable('tascan_trial_status')


tascan_trial_uni_mce= spark.sql("""SELECT
a.ctfo_trial_id,
concat_ws('|',collect_set(CASE WHEN nct_id<>'' THEN nct_id end)) as nct_id ,
'' as protocol_type,
'' as trial_type,
'' as trial_design,
case when b.trial_status in ('Ongoing','Completed') then min(a.trial_start_dt) else null end as trial_start_dt,
'' as trial_end_dt,
'' as patient_segment,
'' as mesh_terms,
'' as results_date,
'' as primary_endpoints_details,
'' as primary_endpoints_reported_date,
'' as secondary_endpoints_details,
'' as secondary_endpoints_reported_date,
'' as number_of_sites_planned,
'' as number_of_sites_activated,
case when b.trial_status in ('Ongoing','Completed') then sum(a.no_of_sites) else null end  as number_of_sites_actual,
'' as number_of_countries_planned,
'' as number_of_countries_activated,
case when b.trial_status in ('Ongoing','Completed') then sum(a.no_of_countries) else null end  as number_of_countries_actual ,
'' as number_of_investigators,
concat_ws('|',collect_set(inclusion_criteria)) as inclusion_criteria ,
concat_ws('|',collect_set(exclusion_criteria)) as exclusion_criteria ,
concat_ws('|',collect_set(exclude_label)) as exclude_label ,
concat_ws('|',collect_set(study_design)) as study_design ,
'' as patient_dropout_rate,
'' as study_objective,
'' as patients_consented_planned,
'' as patients_consented_actual,
'' as patients_screened_planned,
case when lower(trim(b.trial_status)) in {on_com}  then sum(a.patients_screened) else null end as patients_screened_actual,
'' as dropped_at_screening_actual,
case when lower(trim(b.trial_status)) in {on_com_plan} then sum(a.patients_enrolled_planned) else null end as patients_randomized_planned,
case when lower(trim(b.trial_status)) in {on_com}  then sum(a.patients_enrolled_actual) else null end as patients_randomized_actual,
'' as patients_completed_actual,
'' as patients_dropped_actual,
concat_ws('|',collect_set(a.sponsor)) as sponsor,
concat_ws('|',collect_set(sponsor_type)) as sponsor_type,
'' as first_site_initiation_visit_bsln_dt,
'' as first_site_initiation_visit_planned_dt,
'' as first_site_initiation_visit_actual_dt,
'' as first_subject_first_treatment_bsln_dt,
'' as first_subject_first_treatment_planned_dt,
'' as first_subject_first_treatment_actual_dt,
'' as last_subject_first_visit_bsln_dt,
'' as last_subject_first_visit_planned_dt,
'' as last_subject_first_visit_actual_dt,
'' as last_subject_first_treatment_bsln_dt,
'' as last_subject_first_treatment_planned_dt,
'' as last_subject_first_treatment_actual_dt,
a.data_src_nm as source,
'' as fsiv_fsfr_delta,
case when b.trial_status in ('Ongoing','Completed') then percentile(randomization_rate,0.5,10000) else null end as enrollment_rate,
'' as screen_failure_rate,
'' as lost_opp_time,
case when b.trial_status in ('Ongoing','Completed') then percentile(randomization_duration,0.5,10000) else null end as enrollment_duration,
'' as cntry_lsfr_dt,
b.trial_status as standard_status,
concat_ws('|',collect_set(protocol_ids)) as protocol_ids ,
concat_ws('|',collect_set(therapy_area)) as therapy_area ,
concat_ws('|',collect_set(disease)) as disease ,
concat_ws('|',collect_set(title)) as title ,
concat_ws('|',collect_set(phase)) as phase ,
'' as last_subject_consented_dt,
'' as trial_start_dt_rd
FROM tascan_df_temp a
left join 
tascan_trial_status b on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id))
group by
1,3,4,5,7,8,9,10,11,12,13,14,15,16,18,19,21,26,27,28,29,32,35,36,39,40,41,42,43,44,45,46,47,48,49,50,51,52,54,55,57,58,64,65

""".format(on_status=Ongoing_variable,com_status=Completed_variable,on_com=Ongoing_Completed,on_com_plan = Ongoing_Completed_Planned))
tascan_trial_uni_mce = tascan_trial_uni_mce.dropDuplicates()
tascan_trial_uni_mce.registerTempTable('tascan_trial_uni_mce')
#tascan_trial_uni_mce.write.mode('overwrite').saveAsTable('tascan_trial_uni_mce')

############################################### DQS #############################################

dqs_status_mapping = spark.sql("""select raw_status,status from status_mapping where lower(trim(data_source))='dqs' """)
dqs_status_mapping.createOrReplaceTempView("dqs_status_mapping")

dqs_trial_status = spark.sql("""select final.ctfo_trial_id,final.trial_status from (select inq.ctfo_trial_id,inq.site_status as trial_status, row_number()
over(partition by inq.ctfo_trial_id order by inq.precedence) as rnk from (select temp.ctfo_trial_id,temp.site_status,
case when lower(trim(temp.site_status))='ongoing' then 1
     when lower(trim(temp.site_status))='completed' then 2
         when lower(trim(temp.site_status))='planned' then 3
         when lower(trim(temp.site_status))='other' then 4 else 5 end as precedence
    from (select distinct c.ctfo_trial_id,b.status as site_status from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir a left join dqs_status_mapping b on
        lower(trim(a.site_status))=lower(trim(b.raw_status)) left join xref_src_trial c on lower(trim(a.member_study_id))=lower(trim(c.src_trial_id)) where lower(trim(c.data_src_nm))='ir') temp) inq ) final where final.rnk=1 """)

dqs_trial_status = dqs_trial_status.dropDuplicates()
dqs_trial_status.createOrReplaceTempView("dqs_trial_status")
# dqs_trial_status.write.mode('overwrite').saveAsTable('dqs_trial_status')

# To get the data from DQS study from L1 layer
# Added person_golden_id grain to this
actual_kpi = spark.sql("""select 
s.ctfo_trial_id as ctfo_trial_id,
SUM(s.country) as no_of_countries,
SUM(s.sites) as no_of_sites,
MIN(s.first_subject_enrolled_dt) as first_subject_enrolled_dt,
MAX(s.last_subject_enrolled_dt) as last_subject_enrolled_dt,
MIN(s.first_subject_consented_dt) as first_subject_consented_dt,
MAX(s.last_subject_consented_dt) as last_subject_consented_dt,
MIN(s.site_open_dt) as trial_start_dt_rd
from(
(select * from xref_src_trial where lower(trim(xref_src_trial.data_src_nm))='ir') as  a
left join 
(select
    member_study_id,
    MIN(site_open_dt) as site_open_dt,
    COUNT(DISTINCT(facility_country)) as country,
    COUNT(DISTINCT(facility_golden_id)) as sites,
    MIN(first_subject_enrolled_dt) as first_subject_enrolled_dt,
    MAX(last_subject_enrolled_dt) as last_subject_enrolled_dt,
    MIN(first_subject_consented_dt) as first_subject_consented_dt,
    MAX(last_subject_consented_dt) as last_subject_consented_dt
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir a where lower(trim(site_status)) in {on_com} group by 1 ) as b
on a.src_trial_id = b.member_study_id
) as s group by  1
""".format(on_com=Ongoing_Completed))
actual_kpi.registerTempTable("actual_kpi")
dqs_study_temp = spark.sql("""
SELECT
    a.nct_number AS nct_id,
    b.ctfo_trial_id,
    a.member_study_id,
    a.facility_golden_id,
    coalesce(a.mesh_heading,a.primary_indication) AS disease,
    a.study_title AS title,
    a.phase AS phase,
    a.site_status,
    a.sponsor,
    a.facility_country as country,
    a.total_sites AS sites,
    a.site_open_dt,
    a.first_subject_enrolled_dt,
    a.last_subject_enrolled_dt as last_subject_enrolled_dt,
    a.first_subject_consented_dt as first_subject_consented_dt,
    a.last_subject_consented_dt as last_subject_consented_dt,
    a.total_enrolled,
    a.consented as consented,
    a.enrolled as enrolled,
    a.completed as completed
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir a 
left join xref_src_trial b on lower(trim(a.member_study_id))=lower(trim(b.src_trial_id)) where lower(trim(b.data_src_nm))='ir'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
""")
dqs_study_temp = dqs_study_temp.dropDuplicates()
dqs_study_temp.registerTempTable('dqs_study_temp')
# dqs_study_temp.write.mode('overwrite').saveAsTable('dqs_study_temp')

dqs_study_temp_metric = spark.sql("""
select a.ctfo_trial_id,member_study_id,facility_golden_id,max(consented) as consented ,max(enrolled) as enrolled ,max(completed) as completed 
from dqs_study_temp a where lower(trim(site_status)) in {on_com}
group by 1,2,3
""".format(on_com=Ongoing_Completed))
dqs_study_temp_metric = dqs_study_temp_metric.dropDuplicates()
dqs_study_temp_metric.registerTempTable('dqs_study_temp_metric')

dqs_study_temp_metric_sum = spark.sql(""" select ctfo_trial_id,SUM(consented) AS consented,sum(completed) as completed,
     SUM(enrolled) AS enrolled from dqs_study_temp_metric group by 1 """)
dqs_study_temp_metric_sum = dqs_study_temp_metric_sum.dropDuplicates()
dqs_study_temp_metric_sum.registerTempTable('dqs_study_temp_metric_sum')

##handled for null values of enrolled,completed,randomised

##handled for null values of enrolled,completed,randomised


dqs_kpi_01 = spark.sql("""
SELECT a.ctfo_trial_id,
b.trial_status,
    c.no_of_countries,
    c.no_of_sites,
    bb.consented AS patients_enrolled_actual,
    bb.enrolled AS patients_randomized_actual,
    bb.completed AS patients_completed_actual,   
    MIN(a.site_open_dt) as trial_start_dt,
    c.first_subject_enrolled_dt,
    c.last_subject_enrolled_dt,
    c.last_subject_consented_dt,
    c.trial_start_dt_rd
FROM (select ctfo_trial_id,country,facility_golden_id,site_open_dt 
from dqs_study_temp) a 
left join dqs_study_temp_metric_sum bb on lower(trim(a.ctfo_trial_id))=lower(trim(bb.ctfo_trial_id))
left join actual_kpi c on lower(trim(c.ctfo_trial_id))=lower(trim(a.ctfo_trial_id))
left join (select * from dqs_trial_status)b 
on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id))
GROUP BY 1,2,3,4,5,6,7,9,10,11,12
""".format(on_com=Ongoing_Completed))
dqs_kpi_01 = dqs_kpi_01.dropDuplicates()
dqs_kpi_01.registerTempTable('dqs_kpi_01')
# dqs_kpi_01.write.mode('overwrite').saveAsTable('dqs_kpi_01')


dqs_trial_uni_mce = spark.sql("""SELECT
t.ctfo_trial_id,
concat_ws(',',collect_set(t.nct_id)) as nct_id,
'' as protocol_type,
'' as trial_type,
'' as trial_design,
k.trial_start_dt,
'' as trial_end_dt,
'' as patient_segment,
'' as mesh_terms,
'' as results_date,
'' as primary_endpoints_details,
'' as primary_endpoints_reported_date,
'' as secondary_endpoints_details,
'' as secondary_endpoints_reported_date,
'' as number_of_sites_planned,
'' as number_of_sites_activated,
k.no_of_sites as number_of_sites_actual,
'' as number_of_countries_planned,
'' as number_of_countries_activated,
k.no_of_countries as number_of_countries_actual,
'' as number_of_investigators,
'' as inclusion_criteria,
'' as exclusion_criteria,
''  AS exclude_label,
'' AS study_design,
'' as patient_dropout_rate,
'' as study_objective,
'' as patients_consented_planned,
'' as patients_consented_actual,
'' as patients_screened_planned,
k.patients_enrolled_actual as patients_screened_actual,
'' as dropped_at_screening_actual,
'' as patients_randomized_planned,
k.patients_randomized_actual as patients_randomized_actual,
k.patients_completed_actual as patients_completed_actual,
'' as patients_dropped_actual,
concat_ws('|',collect_set(t.sponsor)) as sponsor,
'' as sponsor_type,
'' as first_site_initiation_visit_bsln_dt,
'' as first_site_initiation_visit_planned_dt,
'' as first_site_initiation_visit_actual_dt,
'' as first_subject_first_treatment_bsln_dt,
'' as first_subject_first_treatment_planned_dt,
k.first_subject_enrolled_dt as first_subject_first_treatment_actual_dt,
'' as last_subject_first_visit_bsln_dt,
'' as last_subject_first_visit_planned_dt,
'' as last_subject_first_visit_actual_dt,
'' as last_subject_first_treatment_bsln_dt,
'' as last_subject_first_treatment_planned_dt,
k.last_subject_enrolled_dt as last_subject_first_treatment_actual_dt,
'ir' as source,
'' as fsiv_fsfr_delta,
'' as enrollment_rate,
'' as screen_failure_rate,
'' as lost_opp_time,
'' as enrollment_duration,
'' as cntry_lsfr_dt,
case when c.trial_status is null then 'Others' else c.trial_status end as standard_status,
'' as protocol_ids,
'' as therapy_area,
    concat_ws('||',collect_set(t.disease)) as disease,
    concat_ws('|',collect_set(t.title)) as title,
    concat_ws('||',collect_set(t.phase)) as phase,
k.last_subject_consented_dt as last_subject_consented_dt,
k.trial_start_dt_rd
FROM dqs_study_temp t
LEFT JOIN dqs_kpi_01 k ON lower(trim(k.ctfo_trial_id)) = lower(trim(t.ctfo_trial_id))
left join dqs_trial_status c on lower(trim(c.ctfo_trial_id))=lower(trim(t.ctfo_trial_id))
group by
1,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,	21,	22,	23,	24,	25,	26,	27,	28,	29,	30,	31,	32,	33,34,35,36,38,	39,	41,	42,	43,	44,	45,	46,	47,	48,	49,	50,	51,	52,	53,54,
55,56,57,58,59,60,64,65
""")
dqs_trial_uni_mce = dqs_trial_uni_mce.dropDuplicates()
dqs_trial_uni_mce.registerTempTable('dqs_trial_uni_mce')
# dqs_trial_uni_mce.write.mode('overwrite').saveAsTable('dqs_trial_uni_mce')


##################################################### CTMS #########################################################


ctms_status_mapping = spark.sql \
    ("""select raw_status,status from status_mapping  where lower(trim(data_source))='ctms'""")

ctms_status_mapping = ctms_status_mapping.dropDuplicates()
ctms_status_mapping.registerTempTable('ctms_status_mapping')

ctms_status = spark.sql("""select ctfo_trial_id,
case when standard_status is null then 'Others' else standard_status end as standard_status from (select aa.*,row_number() over(partition by aa.ctfo_trial_id order by precedence asc) as rnk from
(select b.ctfo_trial_id,b.trial_status,t.status as standard_status,case when lower(trim(t.status))='ongoing' then 1
     when lower(trim(t.status))='completed' then 2
	 when lower(trim(t.status))='planned' then 3
	 when lower(trim(t.status))='others' then 4 else 5 end as precedence  
from (( select ctfo_trial_id,  src_trial_id from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial where lower(trim(data_src_nm))='ctms' group by 1,2 ) xref
left join
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study a on lower(trim(xref.src_trial_id)) = lower(trim(a.src_trial_id))) b
left join ctms_status_mapping t on  lower(trim(b.trial_status))=lower(trim(t.raw_status))) aa) where rnk=1
""")
ctms_status = ctms_status.dropDuplicates()
ctms_status.registerTempTable('ctms_status')

screen_fail_rate_trial_temp_test = spark.sql("""
select 
b.ctfo_trial_id,
c.standard_status as trial_status,
max(a.last_subject_first_visit_actual_dt) as last_subject_first_visit_actual_dt ,
sum(a.dropped_at_screening_actual) as dropped_at_screening_actual,
sum(a.patients_screened_actual) as patients_screened_actual

from 

$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial b  
left join (select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study 
where lower(trim(trial_status)) in {on_com_status})  a 
     on  lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id)) 
left join ctms_status c 
     on lower(trim(b.ctfo_trial_id))=lower(trim(c.ctfo_trial_id)) 
where lower(trim(b.data_src_nm))='ctms' 
group by 1,2
""".format(on_status=Ongoing_variable, com_status=Completed_variable, on_com_status=Ongoing_Completed))
screen_fail_rate_trial_temp_test = screen_fail_rate_trial_temp_test.dropDuplicates()
screen_fail_rate_trial_temp_test.registerTempTable('screen_fail_rate_trial_temp_test')

num_site_countries_ctms_df_test = spark.sql(""" select b.data_src_nm,b.ctfo_trial_id, 
sum(number_of_countries_actual) as number_of_countries_actual,sum(number_of_sites_actual) as number_of_sites_actual  from 

( select trial_status,src_trial_id,number_of_sites_actual,number_of_countries_actual from
 $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study) a

left join 
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial b 
on  lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id)) 
 left join  ctms_status_mapping c on lower(trim(a.trial_status))=lower(trim(c.raw_status)) 
 where lower(trim(c.status)) in {on_com_status} and lower(trim(b.data_src_nm))='ctms' 
 group by 1,2""".format(on_status=Ongoing_variable, com_status=Completed_variable, on_com_status=Ongoing_Completed))
num_site_countries_ctms_df_test = num_site_countries_ctms_df_test.dropDuplicates()
num_site_countries_ctms_df_test.registerTempTable('num_site_countries_ctms_df_test')

##added
ctms_dates_test = spark.sql("""select xref.ctfo_trial_id,
min(a.first_site_initiation_visit_actual_dt) as first_site_initiation_visit_actual_dt,
max(b.last_subject_first_treatment_actual_dt) as cntry_last_subject_first_treatment_actual_dt,
max(a.last_subject_first_treatment_actual_dt) as last_subject_first_treatment_actual_dt,
min(a.first_subject_first_treatment_actual_dt) as first_subject_first_treatment_actual_dt,
max(a.last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt,
max(a.fsiv_fsfr_delta) as fsiv_fsfr_delta
from
(select ctfo_trial_id, src_trial_id from  $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial where lower(trim(data_src_nm))='ctms' group by 1,2) xref
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study where lower(trim(trial_status)) in {on_com_status} ) a
   on lower(trim(xref.src_trial_id)) = lower(trim(a.src_trial_id))
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country where lower(trim(country_trial_status)) in {on_com_status} ) b
    on lower(trim(xref.src_trial_id)) = lower(trim(b.src_trial_id))
group by 1""".format(on_com_status=Ongoing_Completed)).registerTempTable("ctms_dates_test")

ctms_trial_status_temp = spark.sql("""
select  ctfo_trial_id, trial_status from
(select ctfo_trial_id,  src_trial_id from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial where lower(trim(data_src_nm))='ctms' group by 1,2) xref
left join
(
select c.status as trial_status,a.src_trial_id from
(select src_trial_id,trial_status from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study) a
left join ctms_status_mapping c on  lower(trim(a.trial_status))=lower(trim(c.raw_status))

) b
on lower(trim(b.src_trial_id)) = lower(trim(xref.src_trial_id))

""".format(status=Ongoing_Completed))

ctms_trial_status_temp = ctms_trial_status_temp.dropDuplicates()
ctms_trial_status_temp.registerTempTable('ctms_trial_status_temp')
# ctms_trial_status_temp.write.mode("overwrite").saveAsTable("ctms_trial_status_temp")

ctms_trial_status_test = spark.sql("""
select ctfo_trial_id,
case when trial_status is null then 'Others' else trial_status end as trial_status
from
(select ctfo_trial_id,trial_status,row_number() over(partition by ctfo_trial_id order by prec) as rnk
from
(select ctfo_trial_id,trial_status, case when lower(trim(trial_status))='ongoing' then 1 when lower(trim(trial_status))='completed' then 2 when lower(trim(trial_status))='planned' then 3
 when lower(trim(trial_status))='others' then 4 else 5 end as prec
from ctms_trial_status_temp) a) b where b.rnk=1 """)

ctms_trial_status_test = ctms_trial_status_test.dropDuplicates()
ctms_trial_status_test.registerTempTable('ctms_trial_status_test')
# ctms_trial_status_test.write.mode("overwrite").saveAsTable("ctms_trial_status_test")
##added
plan_ctms_df_test = spark.sql("""select 
b.data_src_nm,
b.ctfo_trial_id,
c.trial_status,
sum(patients_randomized_planned) as patients_randomized_planned ,
sum(patients_screened_planned) as patients_screened_planned,
max(A.last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt,
min(A.first_subject_first_treatment_planned_dt) as first_subject_first_treatment_planned_dt,
min(A.first_site_initiation_visit_planned_dt) as first_site_initiation_visit_planned_dt

from
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial b
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study where lower(trim(trial_status)) in {on_com_plan_status} ) a 
     on  lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id)) 
left join ctms_trial_status_test c 
     on lower(trim(b.ctfo_trial_id))=lower(trim(c.ctfo_trial_id)) 
where lower(trim(b.data_src_nm))='ctms' group by 1,2,3""".format(on_status=Ongoing_variable
                                                                 , com_status=Completed_variable
                                                                 , on_com_status=Ongoing_Completed,
                                                                 on_com_plan_status=Ongoing_Completed_Planned))
plan_ctms_df_test = plan_ctms_df_test.dropDuplicates()
plan_ctms_df_test.registerTempTable("plan_ctms_df_test")

patient_dropout_rand_enrol_act_ctms_df_test = spark.sql("""select 
b.data_src_nm,
b.ctfo_trial_id,
c.trial_status,
sum(patients_randomized_actual) as patients_randomized_actual ,
sum(patients_screened_actual) as patients_screened_actual,
sum(patients_dropped_actual) as patients_dropped_actual
from
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial b
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study where lower(trim(trial_status)) in {on_com_status} ) a 
     on  lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id)) 
left join ctms_trial_status_test c 
      on lower(trim(b.ctfo_trial_id))=lower(trim(c.ctfo_trial_id)) 
where lower(trim(b.data_src_nm))='ctms' group by 1,2,3""".format(on_status=Ongoing_variable
                                                                 , com_status=Completed_variable
                                                                 , on_com_status=Ongoing_Completed))
patient_dropout_rand_enrol_act_ctms_df_test = patient_dropout_rand_enrol_act_ctms_df_test.dropDuplicates()
patient_dropout_rand_enrol_act_ctms_df_test.registerTempTable("patient_dropout_rand_enrol_act_ctms_df_test")

ctms_trial_uni_mce = spark.sql("""SELECT
xref.ctfo_trial_id,
concat_ws('|',collect_set(xref.nct_id)) as nct_id,
a.protocol_type,
a.trial_type,
a.trial_design,
min(a.trial_start_dt) as trial_start_dt,
max(a.trial_end_dt) as trial_end_dt,
a.patient_segment,
a.mesh_terms,
max(a.results_date) as results_date,
a.primary_endpoints_details,
max(a.primary_endpoints_reported_date) as primary_endpoints_reported_date,
a.secondary_endpoints_details,
max(a.secondary_endpoints_reported_date) as secondary_endpoints_reported_date,
sum(a_on_com_plan.number_of_sites_planned) as number_of_sites_planned,
sum(a.number_of_sites_activated) as number_of_sites_activated,
P.number_of_sites_actual,
sum(a_on_com_plan.number_of_countries_planned) as number_of_countries_planned,
sum(a.number_of_countries_activated) as number_of_countries_activated,
P.number_of_countries_actual,
sum(a.number_of_investigators) as number_of_investigators,
a.inclusion_criteria,
a.exclusion_criteria,
''  AS exclude_label,
'' AS study_design,
'' as patient_dropout_rate,
a.study_objective,
sum(a_on_com_plan.patients_consented_planned) as patients_consented_planned,
sum(a.patients_consented_actual) as patients_consented_actual,
plan_ctms_df_test.patients_screened_planned,
patient_dropout_rand_enrol_act_ctms_df_test.patients_screened_actual,
sfr.dropped_at_screening_actual,
plan_ctms_df_test.patients_randomized_planned,
patient_dropout_rand_enrol_act_ctms_df_test.patients_randomized_actual,
sum(a.patients_completed_actual) as patients_completed_actual,
patient_dropout_rand_enrol_act_ctms_df_test.patients_dropped_actual,
a.sponsor,
a.sponsor_type,
min(a.first_site_initiation_visit_bsln_dt) as first_site_initiation_visit_bsln_dt,
min(plan_ctms_df_test.first_site_initiation_visit_planned_dt) as first_site_initiation_visit_planned_dt,
min(ctms_dates_test.first_site_initiation_visit_actual_dt) as first_site_initiation_visit_actual_dt,
min(a.first_subject_first_treatment_bsln_dt) as first_subject_first_treatment_bsln_dt,
min(plan_ctms_df_test.first_subject_first_treatment_planned_dt) as first_subject_first_treatment_planned_dt,
min(ctms_dates_test.first_subject_first_treatment_actual_dt) as first_subject_first_treatment_actual_dt,
max(a.last_subject_first_visit_bsln_dt) as last_subject_first_visit_bsln_dt,
max(a_on_com_plan.last_subject_first_visit_planned_dt) as last_subject_first_visit_planned_dt,
sfr.last_subject_first_visit_actual_dt,
max(a.last_subject_first_treatment_bsln_dt) as last_subject_first_treatment_bsln_dt,
max(plan_ctms_df_test.last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt,
max(ctms_dates_test.last_subject_first_treatment_actual_dt) as last_subject_first_treatment_actual_dt,
a.source,
ctms_dates_test.fsiv_fsfr_delta,
a.enrollment_rate,
a.screen_failure_rate,
a.lost_opp_time,
a.enrollment_duration,
max(ctms_dates_test.cntry_last_subject_first_treatment_actual_dt) as cntry_lsfr_dt
from 
(select ctfo_trial_id,  src_trial_id,nct_id from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial where lower(trim(data_src_nm))='ctms' group by 1,2,3) xref
left join
( select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study) a  
on lower(trim(xref.src_trial_id)) = lower(trim(a.src_trial_id))
left join
( select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study where lower(trim(trial_status)) in {on_com_plan_status} ) a_on_com_plan  
on lower(trim(xref.src_trial_id)) = lower(trim(a_on_com_plan.src_trial_id))
left join 
ctms_dates_test
on lower(trim(xref.ctfo_trial_id)) = lower(trim(ctms_dates_test.ctfo_trial_id))
left join 
plan_ctms_df_test
on lower(trim(xref.ctfo_trial_id)) = lower(trim(plan_ctms_df_test.ctfo_trial_id))
left join
patient_dropout_rand_enrol_act_ctms_df_test
on lower(trim(xref.ctfo_trial_id)) = lower(trim(patient_dropout_rand_enrol_act_ctms_df_test.ctfo_trial_id))
left join num_site_countries_ctms_df_test P on lower(trim(xref.ctfo_trial_id)) = lower(trim(P.ctfo_trial_id))
left join screen_fail_rate_trial_temp_test sfr on lower(trim(xref.ctfo_trial_id)) = lower(trim(sfr.ctfo_trial_id))
left join ctms_status_mapping c on  lower(trim(a.trial_status))=lower(trim(c.raw_status))
group by
1,	3,	4,	5,8,	9,	11,	13,17,20,	22,	23,	24,25,26,27,30,31,32,33,34,36,37,38,47,51,52,53,54,55,56
                         """.format(on_com_plan_status=Ongoing_Completed_Planned))

ctms_trial_uni_mce = ctms_trial_uni_mce.dropDuplicates()
ctms_trial_uni_mce.registerTempTable('ctms_trial_uni_mce')
# ctms_trial_uni_mce.write.mode('overwrite').saveAsTable('ctms_trial_uni_mce')


ctms_trial_uni_mce_2 = spark.sql("""select a.* ,b.standard_status,'' as protocol_ids,
'' as therapy_area,
'' as disease,
'' as title,
'' as phase,
'' as last_subject_consented_dt,
'' as trial_start_dt_rd
 from ctms_trial_uni_mce a left join ctms_status b on 
a.ctfo_trial_id=b.ctfo_trial_id """)
ctms_trial_uni_mce_2 = ctms_trial_uni_mce_2.dropDuplicates()
ctms_trial_uni_mce_2.registerTempTable('ctms_trial_uni_mce_2')
# ctms_trial_uni_mce_2.write.mode('overwrite').saveAsTable('ctms_trial_uni_mce_2')


trial_uni_mce = spark.sql("""select * from ctms_trial_uni_mce_2
union
select * from dqs_trial_uni_mce
union 
select * from citeline_trial_uni_mce
union
select * from tascan_trial_uni_mce
union
select * from aact_trial_uni_mce  """)
trial_uni_mce = trial_uni_mce.dropDuplicates()
# trial_uni_mce.registerTempTable('trial_uni_mce')
trial_uni_mce.write.mode('overwrite').saveAsTable('trial_uni_mce')

# path = path.replace('table_name', 'Universe')
# trial_uni_mce.repartition(10).write.mode('overwrite').parquet(path)


spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_universe partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   trial_uni_mce
""")

if trial_uni_mce.count() == 0:
    print("Skipping copy_hdfs_to_s3 for trial_uni_mce as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_universe")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")

