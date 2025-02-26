
################################# Module Information ######################################
#  Module Name         : All Trial Unique ID Generation
#  Purpose             : This will execute queries to prepare data for xref tables to create
#                        cross reference mapping of source IDs with newly generated
#                        golden IDs based on dedupe clustering
#  Pre-requisites      : Source table required: temp_all_trial_dedupe_results_union_final
#  Last changed on     : 16-06-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# Prepare data for xref tables
############################################################################################
import sys

sys.path.insert(1, '/clinical_design_center/data_management/sanofi_ctfo/code')
from DedupeUtility import maintainGoldenID
from DedupeUtility import KMValidate
from pyspark.sql.functions import *
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
import MySQLdb

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/mastering/trial/' \
       'table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/'
                                  + CommonConstants.ENVIRONMENT_CONFIG_FILE)
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, 'mysql_db'])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

age_temp_path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/' \
                'uploads/age_mapping_latest.csv'
age_standardization = spark.read.csv(age_temp_path, sep=',', header=True, inferSchema=True)
age_standardization.createOrReplaceTempView('age_standardization')

gender_temp_path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/' \
                   'gender_mapping.csv'
gender_standardization = spark.read.csv(gender_temp_path, sep=',', header=True, inferSchema=True)
gender_standardization.createOrReplaceTempView('gender_standardization')

trial_phase_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/trial_phase.csv')
trial_phase_mapping.createOrReplaceTempView('trial_phase_mapping')

trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/'
          'uploads/trial_status.csv')
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')


# Preparing union of all source data
# Get trial information from AACT
temp_aact_trial_data = spark.sql("""
select
    'aact' as data_src_nm,
    null as patient_segment,
    study.nct_id as nct_id,
    coalesce(study.brief_title, study.official_title) as trial_title,
    study.phase as trial_phase,
    drug.name as trial_drug,
    disease.source_condition as trial_indication,
    study.nct_id as src_trial_id,
    study.nct_id as protocol_id,
    'clinicaltrials.gov' as protocol_type,
    study.overall_status as trial_status,
    study.study_type as trial_type,
    design.description as trial_design,
    study.start_date as trial_start_dt,
    study.start_date_type as trial_start_dt_type,
    coalesce(study.completion_date,study.primary_completion_date) as trial_end_dt,
    coalesce(study.completion_date_type, study.primary_completion_date_type) as trial_end_dt_type,
    trim(concat_ws('\|',collect_set(aact_facilities.country))) as trial_country,
    null as trial_endpoint,
    null as trial_inclusion_criteria,
    null as trial_exclusion_criteria,
    eligibilities.minimum_age as trial_age_min,
    eligibilities.maximum_age as trial_age_max,
    eligibilities.gender as patient_gender,
    eligibilities.criteria as criteria,
    mesh.mesh_terms,
    study.results_first_submitted_date as results_date,
    null as primary_endpoints_reported_date,
    null as enrollment_close_date
from sanofi_ctfo_datastore_staging_$$db_env.aact_studies study
left outer join
(select
    d.nct_id,
    concat_ws('\;',collect_set(d.name)) as name
from sanofi_ctfo_datastore_staging_$$db_env.aact_interventions d
where lower(trim(d.intervention_type))='drug'
group by d.nct_id) drug
on study.nct_id = drug.nct_id
left outer join
(select
    e.nct_id,
    trim(concat_ws('\;',collect_set(e.name))) as condition,
    trim(concat_ws('\;',collect_set(e.name))) as source_condition
from sanofi_ctfo_datastore_staging_$$db_env.aact_conditions e
group by e.nct_id ) disease
on study.nct_id = disease.nct_id
left outer join
    (select nct_id,trim(concat_ws('\;',collect_set(description))) as description
    from sanofi_ctfo_datastore_staging_$$db_env.aact_design_groups group by nct_id) design
on study.nct_id=design.nct_id
left outer join
    (select nct_id,trim(concat_ws('\;',collect_set(mesh_term))) as mesh_terms
    from  sanofi_ctfo_datastore_staging_$$db_env.aact_browse_conditions group by nct_id) mesh
on study.nct_id=mesh.nct_id
left outer join
(select * from sanofi_ctfo_datastore_staging_$$db_env.aact_eligibilities) eligibilities
on study.nct_id=eligibilities.nct_id
left outer join
(select nct_id,trim(concat_ws('\;',collect_set(country))) as country from
 sanofi_ctfo_datastore_staging_$$db_env.aact_facilities group by 1) aact_facilities
on study.nct_id=aact_facilities.nct_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,19,20,21,22,23,24,25,26,27,28,29
""")

temp_aact_trial_data.createOrReplaceTempView('temp_aact_trial_data')

# Get trial information from CITELINE
temp_citeline_trial_data_exploaded = spark.sql("""
select
    'citeline' as data_src_nm,
    trial_therapeutic_areas_name as therapeutic_area,
    ct.disease_name as disease_name,
    ct.patient_segment as patient_segment,
    case when trim((src_protocol_ids)) like 'NCT%' and trim((src_protocol_ids)) not like 'NCT#%'
        then src_protocol_ids else null end as nct_id,
    ct.trial_title as trial_title ,
    ct.trial_phase as trial_phase,
    ct.drug_name as trial_drug,
    ct.disease_name as trial_indication,
    ct.trial_id as src_trial_id,
    src_protocol_ids as protocol_ids,
    ct.trial_source as protocol_type,
    ct.trial_status as trial_status,
    null as trial_type,
    ct.study_design as trial_design,
    ct.start_date as trial_start_dt,
    null as trial_start_dt_type,
    ct.primary_completion_date as trial_end_dt,
    null as trial_end_dt_type,
    ct.trial_countries as trial_country,
    concat(ct.primary_endpoint,ct.other_endpoint) as trial_endpoint,
    ct.inclusion_criteria as trial_inclusion_criteria,
    ct.exclusion_criteria as trial_exclusion_criteria,
    concat(ct.min_patient_age, " ", ct.min_age_units) as trial_age_min,
    concat(ct.max_patient_age, " ", ct.max_age_units) as trial_age_max,
    ct.patient_gender as patient_gender,
    concat(ct.inclusion_criteria, " ", ct.exclusion_criteria) as criteria,
    trial_mesh_term_name as mesh_terms,
    ct.results_date,
    ct.trial_primary_endpoints_reported as primary_endpoints_reported_date,
    ct.enrollment_close_date as enrollment_close_date
from sanofi_ctfo_datastore_staging_$$db_env.citeline_trialtrove ct
lateral view explode(split(protocol_ids,'\\\|'))one as src_protocol_ids
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
""")

temp_citeline_trial_data_exploaded.createOrReplaceTempView('temp_citeline_trial_data_exploaded')

temp_citeline_trial_data = spark.sql("""
select
    data_src_nm,
    trim(ct.patient_segment) as patient_segment,
    ct.nct_id as nct_id,
    ct.trial_title,
    ct.trial_phase,
    ct.trial_drug,
    case when ct.trial_indication in (' ','','null','na') then null else ct.trial_indication end
    as trial_indication,
    ct.src_trial_id,
    ct.protocol_ids as protocol_id,
    ct.protocol_type,
    ct.trial_status,
    ct.trial_type,
    ct.trial_design,
    ct.trial_start_dt,
    ct.trial_start_dt_type,
    ct.trial_end_dt,
    ct.trial_end_dt_type,
    ct.trial_country,
    ct.trial_endpoint,
    ct.trial_inclusion_criteria,
    ct.trial_exclusion_criteria,
    ct.trial_age_min,
    ct.trial_age_max,
    ct.patient_gender,
    ct.criteria,
    ct.mesh_terms,
    ct.results_date,
    ct.primary_endpoints_reported_date,
    ct.enrollment_close_date
from
(
select
    data_src_nm,
    concat_ws('\|',collect_set(trial_data_exploaded.patient_segment)) as patient_segment,
    concat_ws('\|',collect_set(trial_data_exploaded.nct_id)) as nct_id,
    concat_ws('\|',collect_set(trial_data_exploaded.protocol_ids)) as protocol_ids,
    trial_data_exploaded.trial_title as trial_title,
    trial_data_exploaded.trial_phase as trial_phase,
    trial_data_exploaded.trial_drug as trial_drug,
    concat_ws('\|',collect_set(trial_data_exploaded.disease_name)) as trial_indication,
    trial_data_exploaded.src_trial_id as src_trial_id,
    trial_data_exploaded.protocol_type,
    trial_data_exploaded.trial_status,
    trial_data_exploaded.trial_type,
    trial_data_exploaded.trial_design,
    trial_data_exploaded.trial_start_dt,
    trial_data_exploaded.trial_start_dt_type,
    trial_data_exploaded.trial_end_dt,
    trial_data_exploaded.trial_end_dt_type,
    trim(concat_ws('\|',collect_set(trial_data_exploaded.trial_country))) as trial_country,
    trial_data_exploaded.trial_endpoint,
    trial_data_exploaded.trial_inclusion_criteria,
    trial_data_exploaded.trial_exclusion_criteria,
    trial_data_exploaded.trial_age_min,
    trial_data_exploaded.trial_age_max,
    trial_data_exploaded.patient_gender,
    trial_data_exploaded.criteria,
    trial_data_exploaded.mesh_terms,
    trial_data_exploaded.results_date,
    trial_data_exploaded.primary_endpoints_reported_date,
    trial_data_exploaded.enrollment_close_date
from temp_citeline_trial_data_exploaded trial_data_exploaded
group by 1,5,6,7,9,10,11,12,13,14,15,16,17,19,20,21,22,23,24,25,26,27,28,29) ct
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
""")

temp_citeline_trial_data.createOrReplaceTempView('temp_citeline_trial_data')

# Get trial information for WHO
#temp_who_trial_data = spark.sql("""
#select
#    'who' as data_src_nm,
#    null as patient_segment,
#    base.nct_id as nct_id,
 #   base.trial_title as trial_title ,
 #   base.trial_phase as trial_phase,
#    base.trial_drug as trial_drug,
#    case when trim(lower(base.trial_indication)) in (' ','','null','na') then null
#     else base.trial_indication end as trial_indication,
#    base.src_trial_id as src_trial_id,
 #   base.protocol_id,
#    base.protocol_type,
#    base.trial_status,
#    base.trial_type,
#    base.trial_design,
#    base.trial_start_dt,
#    base.trial_start_dt_type,
  #  base.trial_end_dt,
 #   base.trial_end_dt_type,
#    base.trial_country,
#    base.trial_endpoint,
#    base.trial_inclusion_criteria,
#   base.trial_exclusion_criteria,
 #   base.trial_age_min,
 #   base.trial_age_max,
#    base.patient_gender,
#    base.criteria,
#    base.mesh_terms,
#    base.results_date,
#    base.primary_endpoints_reported_date,
#    base.enrollment_close_date
#from
#(select
 #   trim(concat_ws('\|',collect_set(trials.condition)))  as trial_indication,
 #   case when trials.trialid like 'NCT%' then trials.trialid else null end as nct_id,
#   coalesce(trials.public_title) as trial_title,
#    trials.phase as trial_phase,
#    trials.intervention as trial_drug,
#    trials.trialid as src_trial_id,
#    trials.trialid as protocol_id,
#    trials.source_register as protocol_type,
#   trials.recruitment_status as trial_status,
#    trials.study_type as trial_type,
#    trials.study_design as trial_design,
#    null as trial_start_dt,
#    null as trial_start_dt_type,
#   null as trial_end_dt,
#   null as trial_end_dt_type,
#    trim(concat_ws('\|',collect_set(trials.countries))) as trial_country,
#    null as trial_endpoint,
#    trials.inclusion_criteria as trial_inclusion_criteria,
#    trials.exclusion_criteria as trial_exclusion_criteria,
#    trials.inclusion_agemin as trial_age_min,
#    trials.inclusion_agemax as trial_age_max,
#    trials.inclusion_gender as patient_gender,
#    concat(trials.inclusion_criteria, " ", trials.exclusion_criteria) as criteria,
#    null as mesh_terms,
#    null as results_date,
#    null as primary_endpoints_reported_date,
#    null as enrollment_close_date
#from sanofi_ctfo_datastore_staging_$$db_env.who_trials trials
#group by 2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,18,19,20,21,22,23,24,25,26,27) base
#group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
#""")

#temp_who_trial_data.createOrReplaceTempView('temp_who_trial_data')

# Get trial information for DQS

dqs_status = spark.sql("""
select nct_number,
case when trial_status_collected like '%ongoing%' then 'Ongoing'
        when trial_status_collected = 'completed' then 'Completed'
        else trial_status_collected
        end as trial_status
from (select nct_number, (concat_ws('\|',collect_set(lower(site_status)))) trial_status_collected
        from sanofi_ctfo_datastore_staging_$$db_env.dqs_study group by 1)
group by 1,2
""")
dqs_status.createOrReplaceTempView("dqs_status")

temp_dqs_trial_data = spark.sql("""
select
    trials.data_src_nm,
    trials.patient_segment,
    trials.nct_id,
    trials.trial_title ,
    trials.trial_phase,
    trials.trial_drug,
    case when trim(lower(trials.trial_indication)) in (' ','','null','na') then null
    else trials.trial_indication end as trial_indication,
    src_trial_id,
    trials.protocol_id,
    trials.protocol_type,
    trials.trial_status,
    trials.trial_type,
    trials.trial_design,
    trials.trial_start_dt,
    trials.trial_start_dt_type,
    trials.trial_end_dt,
    trials.trial_end_dt_type,
    trials.trial_country,
    trials.trial_endpoint,
    trials.trial_inclusion_criteria,
    trials.trial_exclusion_criteria,
    trials.trial_age_min,
    trials.trial_age_max,
    trials.patient_gender,
    trials.criteria,
    trials.mesh_terms,
    trials.results_date,
    trials.primary_endpoints_reported_date,
    trials.enrollment_close_date
from
(select
    'ir' as data_src_nm,
    null as patient_segment,
    case when base.nct_number like 'NCT%' then base.nct_number else null end as nct_id,
    base.study_title as trial_title ,
    base.phase as trial_phase,
    null as trial_drug,
    base.primary_indication as trial_indication,
    base.member_study_id as src_trial_id,
    base.nct_number as protocol_id,
    null as protocol_type,
    st.trial_status,
    null as trial_type,
    null as trial_design,
    min(base.site_open_dt) as trial_start_dt,
    null as trial_start_dt_type,
    max(base.last_subject_enrolled_dt) as trial_end_dt,
    null as trial_end_dt_type,
    trim(concat_ws('\|',collect_set(base.country))) as trial_country,
    null as trial_endpoint,
    null as trial_inclusion_criteria,
    null as trial_exclusion_criteria,
    null as trial_age_min,
    null as trial_age_max,
    null as patient_gender,
    null as criteria,
    base.mesh_heading as mesh_terms,
    null as results_date,
    null as primary_endpoints_reported_date,
    max(last_subject_enrolled_dt) as enrollment_close_date
from
sanofi_ctfo_datastore_staging_$$db_env.dqs_study base
left join dqs_status st
on base.nct_number = st.nct_number
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,15,17,20,21,22,23,24,25,26,27,28) trials
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29
""")

temp_dqs_trial_data.createOrReplaceTempView('temp_dqs_trial_data')

# Get trial information from CTMS
temp_ctms_trial_data = spark.sql("""
select
    'ctms' as data_src_nm,
    '' as patient_segment,
    '' as nct_id,
    a.PROTOCOL_TITLE as trial_title,
    a.PHASE_CODE as trial_phase,
    trim(concat_ws('\|',collect_set(b.PRODUCT_NAME))) as trial_drug,
    trim(concat_ws('\|',collect_set(c.INDICATION))) as trial_indication,
    a.STUDY_CODE as src_trial_id,
    '' as protocol_id,
    '' as protocol_type,
    a.status as trial_status,
    '' as trial_type,
    '' as trial_design,
    min(case when lower(trim(cssm.milestone)) = 'site initiation visit' then cssm.actual_date end) as trial_start_dt,
    '' as trial_start_dt_type,
    '' as trial_end_dt,
    '' as trial_end_dt_type,
    '' as trial_country,
    '' as trial_endpoint,
    '' as trial_inclusion_criteria,
    '' as trial_exclusion_criteria,
    '' as trial_age_min,
    '' as trial_age_max,
    '' as patient_gender,
    '' as criteria,
    '' as mesh_terms,
    '' as results_date,
    '' as primary_endpoints_reported_date,
    min(case when lower(trim(cssm.milestone)) = 'last subject first rando/treat' then cssm.actual_date end) as  enrollment_close_date
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study as a
left outer join sanofi_ctfo_datastore_staging_$$db_env.ctms_product b
on lower(trim(a.INVESTIGATIONAL_PRODUCT)) = lower(trim(b.PRODUCT_CODE))
left outer join sanofi_ctfo_datastore_staging_$$db_env.ctms_study_indication c
on lower(trim(a.STUDY_CODE)) = lower(trim(c.STUDY_CODE))
left join (select milestone,actual_date,study_code from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones group by 1,2,3) cssm
on lower(trim(a.STUDY_CODE)) = lower(trim(cssm.STUDY_CODE))
group by 1,2,3,4,5,8,9,10,11,12,13,15,16,17,19,20,21,22,23,24,25,26,27,28
""")
temp_ctms_trial_data.createOrReplaceTempView('temp_ctms_trial_data')
temp_ctms_trial_data.write.mode("overwrite").saveAsTable("temp_ctms_trial_data")

# Union all data sources
'''
temp_all_trial_data_final = spark.sql("""
select * from temp_aact_trial_data
union
select * from temp_citeline_trial_data
#union
#select * from temp_who_trial_data
union
select * from temp_dqs_trial_data
""")
'''

temp_all_trial_data_final = spark.sql("""
select * from temp_aact_trial_data
union
select * from temp_citeline_trial_data
union
select * from temp_dqs_trial_data
union
select * from temp_ctms_trial_data
""")

# Save on HDFS
temp_all_trial_data_final.write.mode('overwrite').saveAsTable('temp_all_trial_data_final')

# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_final')
temp_all_trial_data_final.repartition(100).write.mode('overwrite').parquet(write_path)

# Modified code to use KMValidate function for incorporating KM input

# Writing base table on HDFS to make it available for the function to fetch data
temp_all_trial_dedupe_results_final = spark.sql("""
select *
from
sanofi_ctfo_datastore_app_commons_$$db_env.temp_all_trial_dedupe_results_final
""")

temp_all_trial_dedupe_results_final.write.mode('overwrite'). \
    saveAsTable('temp_all_trial_dedupe_results_final')

# final table contains the name of the final table [temp_all_<trial>_dedupe_results_km_opt_union]
# having columns final_cluster_id, final_score and final_KM_comment to be used for
#  further processing
final_table = KMValidate('temp_all_trial_dedupe_results_final', 'trial', '$$data_dt', '$$cycle_id', '$$s3_env')

# Assiging Golden ID for records with score greater than $$threshold
temp_trial_id_clusterid = spark.sql("""
select
    concat('ctfo_trial_',row_number() over(order by null)) as trial_id,
    temp.final_cluster_id as cluster_id
from
    (select distinct final_cluster_id from """ + final_table + """ where
    final_cluster_id is not null
    and final_cluster_id <> '' and round(cast(final_score as double),2) >=$$threshold) temp
""")

# Save on HDFS
temp_trial_id_clusterid.write.mode('overwrite').saveAsTable('temp_trial_id_clusterid')

# Push to S3
write_path = path.replace('table_name', 'temp_trial_id_clusterid')
temp_trial_id_clusterid.repartition(100).write.mode('overwrite').parquet(write_path)

# Generating Cross reference for records with score greater than $$threshold
temp_xref_trial_1 = spark.sql("""
select
    res.uid,
    res.hash_uid,
    res.data_src_nm,
    res.ds_trial_id,
    ctoid.trial_id as trial_id,
    res.trial_title,
    res.trial_phase,
    res.trial_drug,
    res.trial_indication,
    res.score
from
    (select distinct uid,final_cluster_id as cluster_id,hash_uid,final_score as score,
    trial_title, trial_phase,
    trial_drug, trial_indication, ds_trial_id, data_src_nm
    from """ + final_table + """ where round(coalesce(final_score,0),2) >= $$threshold) res
left outer join temp_trial_id_clusterid ctoid
on ctoid.cluster_id=res.cluster_id
""")


# Save on HDFS
temp_xref_trial_1.write.mode('overwrite').saveAsTable('temp_xref_trial_1')

# Push to S3
write_path = path.replace('table_name', 'temp_xref_trial_1')
temp_xref_trial_1.repartition(100).write.mode('overwrite').parquet(write_path)

# for some records whose score>=$$threshold and no cluster id after post dedupe logic
# assign them unique golden_id=max(site_id)+row_number
temp_xref_trial_2 = spark.sql("""
select /*+ broadcast(temp) */
    res.uid,
    res.hash_uid,
    res.data_src_nm,
    res.ds_trial_id,
    concat('ctfo_trial_', row_number() over(order by null) + coalesce(temp.max_id,0)) as trial_id,
    res.trial_title,
    res.trial_phase,
    res.trial_drug,
    res.trial_indication,
    res.score
from
    (select distinct uid,final_cluster_id as cluster_id,hash_uid,final_score as score,
     trial_title, trial_phase,
    trial_drug, trial_indication, ds_trial_id, data_src_nm
    from """ + final_table + """ where round(coalesce(final_score,0),2) < $$threshold) res
cross join
    (select max(cast(split(trial_id,'trial_')[1] as bigint)) as max_id from temp_xref_trial_1) temp
""")

# Save on HDFS
temp_xref_trial_2.write.mode('overwrite').saveAsTable('temp_xref_trial_2')

# Push to S3
write_path = path.replace('table_name', 'temp_xref_trial_2')
temp_xref_trial_2.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_trial_final_0 = spark.sql("""
select *
from temp_xref_trial_1
union
select *
from temp_xref_trial_2
""")

temp_xref_trial_final_0.write.mode('overwrite').saveAsTable('temp_xref_trial_final_0')

# Calling maintainGoldenID function to preserve IDs across runs
final_table_1 = maintainGoldenID('temp_xref_trial_final_0', 'trial', '$$process_id', '$$Prev_Data', '$$s3_env')

if final_table_1 == 'temp_xref_trial_final_0':
    temp_xref_trial = spark.sql("""
    select *
    from temp_xref_trial_final_0
    """)
else:
    temp_xref_trial = spark.sql("""
    select
        base.uid,
        base.hash_uid,
        base.data_src_nm,
        base.ds_trial_id,
        func_opt.ctfo_trial_id as trial_id,
        base.trial_title,
        base.trial_phase,
        base.trial_drug,
        base.trial_indication,
        base.score
    from
    temp_xref_trial_final_0 base
    left outer join
    """ + final_table_1 + """ func_opt
    on base.hash_uid = func_opt.hash_uid
    """)

temp_xref_trial.write.mode('overwrite').saveAsTable('temp_xref_trial')
write_path = path.replace('table_name', 'temp_xref_trial')
temp_xref_trial.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_trial_intd = spark.sql("""
select
    split(ds_trial_ids,'_')[0] AS data_src_nm,
        SUBSTRING(ds_trial_ids,(instr(ds_trial_ids, '_')+1)) as src_trial_id,
    trial_id,
    uid,
    hash_uid,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication,
    score
from temp_xref_trial
lateral view explode(split(ds_trial_id,'\\s*[;|]\\s*'))one as ds_trial_ids
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd = temp_xref_trial_intd.dropDuplicates()
temp_xref_trial_intd.write.mode('overwrite').saveAsTable('temp_xref_trial_intd')
write_path = path.replace('table_name', 'temp_xref_trial_intd')
temp_xref_trial_intd.repartition(100).write.mode('overwrite').parquet(write_path)


#temp_xref_trial_intd.registerTempTable("temp_xref_trial_intd")





temp_xref_trial_intd_temp = spark.sql("""
select
b.data_src_nm,
b.src_trial_id,
a.trial_id,
a.uid,
a.hash_uid,
a.trial_title,
a.trial_phase,
a.trial_drug,
a.trial_indication,
a.score
from temp_xref_trial_intd a
inner join
(select
src_trial_id,data_src_nm
from  temp_xref_trial_intd group by 1,2 having count(distinct uid)<2) b on a.src_trial_id=b.src_trial_id AND a.data_src_nm=b.data_src_nm
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd_temp = temp_xref_trial_intd_temp.dropDuplicates()
temp_xref_trial_intd_temp.write.mode('overwrite').\
    saveAsTable('temp_xref_trial_intd_temp')
#temp_xref_trial_intd_temp.registerTempTable("temp_xref_trial_intd_temp")



#partitoning the records having count distinct uid 1
temp_xref_trial_intd_0 = spark.sql("""
select data_src_nm,
src_trial_id,
trial_id,
uid,
hash_uid,
trial_title,
trial_phase,
trial_drug,
trial_indication,
score
from
temp_xref_trial_intd_temp
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd_0 = temp_xref_trial_intd_0.dropDuplicates()
#temp_xref_trial_intd_0.registerTempTable("temp_xref_trial_intd_0")
temp_xref_trial_intd_0.write.mode('overwrite').\
    saveAsTable('temp_xref_trial_intd_0')




temp_xref_trial_intd_data_src_rnk = spark.sql ("""
select temp_xref_trial_intd.*,
        case when data_src_nm like '%ctms%' then 1
        when data_src_nm like '%ir%' then 2
        when data_src_nm like '%citeline%' then 3
    when data_src_nm like '%aact%' then 4
else 5 end as datasource_rnk from  temp_xref_trial_intd""")
temp_xref_trial_intd_data_src_rnk.write.mode('overwrite').\
    saveAsTable('temp_xref_trial_intd_data_src_rnk')

#temp_xref_trial_intd_data_src_rnk.registerTempTable("temp_xref_trial_intd_data_src_rnk")


# Giving rank to records having multiple distinct UID
temp_xref_trial_intd_1= spark.sql("""
select data_src_nm,
src_trial_id,
trial_id,
uid,
hash_uid,
trial_title,
trial_phase,
trial_drug,
trial_indication,
score,
datasource_rnk,
ROW_NUMBER() over (partition by src_trial_id order by datasource_rnk asc) as rnk
from
temp_xref_trial_intd_data_src_rnk where src_trial_id in
(select src_trial_id from (select
src_trial_id,data_src_nm
from  temp_xref_trial_intd_data_src_rnk group by 1,2 having count(distinct uid)>1))
group by 1,2,3,4,5,6,7,8,9,10,11
""")
temp_xref_trial_intd_1 = temp_xref_trial_intd_1.dropDuplicates()
temp_xref_trial_intd_1.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_1')
write_path = path.replace('table_name', 'temp_xref_trial_intd_1')
temp_xref_trial_intd_1.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_trial_intd_1.registerTempTable("temp_xref_trial_intd_1")



temp_xref_trial_intd_2 = spark.sql("""
select
data_src_nm,
src_trial_id,
trial_id,
uid,
hash_uid,
trial_title,
trial_phase,
trial_drug,
trial_indication,
score
from
temp_xref_trial_intd_1  where rnk = 1
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd_2 = temp_xref_trial_intd_2.dropDuplicates()
temp_xref_trial_intd_2.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_2')
write_path = path.replace('table_name', 'temp_xref_trial_intd_2')
temp_xref_trial_intd_2.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_trial_intd_2.registerTempTable("temp_xref_trial_intd_2")


# Union of records with unique UID and prec records having distinct multi. UID

temp_xref_trial_intd_3 = spark.sql("""
select
* from temp_xref_trial_intd_2
union
select * from temp_xref_trial_intd_0
""")
temp_xref_trial_intd_3 = temp_xref_trial_intd_3.dropDuplicates()
temp_xref_trial_intd_3.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_3')
write_path = path.replace('table_name', 'temp_xref_trial_intd_3')
temp_xref_trial_intd_3.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_trial_intd_3.registerTempTable("temp_xref_trial_intd_3")





'''
###############
spark.sql("""select src_trial_id,count(*) from temp_xref_trial_intd_3 group by 1 having count(distinct uid)>1""").show()

temp_xref_trial_intd_1= spark.sql("""
select data_src_nm,
src_trial_id,
trial_id,
uid,
hash_uid,
trial_title,
trial_phase,
trial_drug,
trial_indication,
score,
ROW_NUMBER() over (partition by trial_id,data_src_nm order by score desc) as rnk
from temp_xref_trial_intd
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd_1 = temp_xref_trial_intd_1.dropDuplicates()
temp_xref_trial_intd_1.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_1')
write_path = path.replace('table_name', 'temp_xref_trial_intd_1')
temp_xref_trial_intd_1.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_trial_intd_2 = spark.sql("""
select
trim(concat_ws('\|',collect_set(src_trial_id))) as collect_src_trial_id,
trial_id
from temp_xref_trial_intd_1
group by 2
""")
temp_xref_trial_intd_2 = temp_xref_trial_intd_2.dropDuplicates()
temp_xref_trial_intd_2.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_2')
write_path = path.replace('table_name', 'temp_xref_trial_intd_2')
temp_xref_trial_intd_2.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_trial_intd_3 = spark.sql("""
select
a.data_src_nm,
a.src_trial_id,
b.collect_src_trial_id,
a.trial_id,
a.uid,
a.hash_uid,
a.trial_title,
a.trial_phase,
a.trial_drug,
a.trial_indication,
a.score
from
temp_xref_trial_intd_1 a
inner join
temp_xref_trial_intd_2 b
on a.trial_id = b.trial_id where a.rnk = 1
group by 1,2,3,4,5,6,7,8,9,10,11
""")
temp_xref_trial_intd_3 = temp_xref_trial_intd_3.dropDuplicates()
temp_xref_trial_intd_3.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_3')
write_path = path.replace('table_name', 'temp_xref_trial_intd_3')
temp_xref_trial_intd_3.repartition(100).write.mode('overwrite').parquet(write_path)
##############3
'''




# joining golden ids with trial data
xref_src_trial = spark.sql("""
select
    base.data_src_nm,
    base.src_trial_id,
    base.trial_id as ctfo_trial_id,
    case when src_data.nct_id in (null, '', 'NA', 'na')
    then null else src_data.nct_id end as nct_id,
    trim(src_data.trial_title) as trial_title,
    coalesce(B.standard_value, 'Other') as trial_phase,
    src_data.trial_drug,
    src_data.trial_indication
from   temp_xref_trial_intd_3 base
left outer join temp_all_trial_data_final src_data
on base.data_src_nm = src_data.data_src_nm and base.src_trial_id = src_data.src_trial_id
left outer join trial_phase_mapping B on trim(lower(src_data.trial_phase)) =
trim(lower(B.trial_phase))
group by 1,2,3,4,5,6,7,8
""")

xref_src_trial = xref_src_trial.dropDuplicates()
xref_src_trial.write.mode('overwrite').saveAsTable('xref_src_trial_test')

xref_src_trial_precedence_int = spark.sql("""
select
    base.data_src_nm,
    base.src_trial_id,
    base.trial_id as ctfo_trial_id,
    base.score,
    case when src_data.protocol_id in (null, '', 'NA', 'na') then null else
    src_data.protocol_id end as protocol_id,
    src_data.protocol_type,
    trim(src_data.trial_title) as trial_title,
    coalesce(B.standard_value, 'Other') as trial_phase,
    coalesce(C.status, 'Others') as trial_status,
    src_data.trial_type,
    trim(src_data.trial_design) as trial_design,
    src_data.trial_start_dt,
    src_data.trial_start_dt_type,
    src_data.trial_end_dt,
    src_data.trial_end_dt_type,
    coalesce(case when age_std.standard_min_age = 'N/A' then '0' else
    age_std.standard_min_age end, '0') as trial_age_min,
    coalesce(case when age_std.standard_max_age = 'N/A' then '100' else
    age_std.standard_max_age end, '100') as trial_age_max,
    trim(coalesce(gender_std.std_gender, 'Others')) as patient_gender,
    src_data.patient_segment,
    src_data.mesh_terms,
    trim(src_data.results_date) as results_date,
    src_data.primary_endpoints_reported_date,
    src_data.enrollment_close_date
from temp_xref_trial_intd_3 base
left outer join temp_all_trial_data_final src_data
on base.data_src_nm = src_data.data_src_nm and base.src_trial_id = src_data.src_trial_id
left outer join age_standardization age_std
on regexp_replace(lower(trim(src_data.trial_age_min)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(age_std.minimum_age)),"[^0-9A-Za-z_ -,']",'')
and regexp_replace(lower(trim(src_data.trial_age_max)),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(lower(trim(age_std.maximum_age)),"[^0-9A-Za-z_ -,']",'')
left outer join gender_standardization gender_std
on regexp_replace(trim(src_data.patient_gender),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(trim(gender_std.gender),"[^0-9A-Za-z_ -,']",'')
left outer join trial_phase_mapping B on trim(lower(src_data.trial_phase)) =
trim(lower(B.trial_phase))
left outer join (select distinct raw_status,status from trial_status_mapping) C
on trim(lower(src_data.trial_status)) = trim(lower(C.raw_status))
""")
xref_src_trial_precedence_int = xref_src_trial_precedence_int.dropDuplicates()
xref_src_trial_precedence_int.write.mode('overwrite').saveAsTable('xref_src_trial_precedence_int')

for col in xref_src_trial.columns:
    xref_src_trial = xref_src_trial.withColumn(col, regexp_replace(col, ' ', '<>')) \
        .withColumn(col, regexp_replace(col, '><', '')) \
        .withColumn(col, regexp_replace(col, '<>', ' ')) \
        .withColumn(col, regexp_replace(col, '[|]+', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@ ', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@', '|')) \
        .withColumn(col, regexp_replace(col, '[|]+', '|')) \
        .withColumn(col, regexp_replace(col, '[|]$', '')) \
        .withColumn(col, regexp_replace(col, '^[|]', ''))

xref_src_trial = xref_src_trial.dropDuplicates()
xref_src_trial.write.mode('overwrite').saveAsTable('xref_src_trial')

spark.sql("""
insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial partition (
        pt_data_dt='$$data_dt',
        pt_cycle_id='$$cycle_id'
        )
select *
from xref_src_trial
""")

CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial')



