################################# Module Information #####################################################
#  Module Name         : Trial Universe Metric Calculation Data
#  Purpose             : To create the source data for the CTMS Refactored COUNTRY
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : CTMS Refactored COUNTRY
#  Last changed on     : 10-12-2024
#  Last changed by     : Kedar
#  Reason for change   : Status Update
##########################################################################################################

from CommonUtils import *
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(
    CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"


trials_final_0 = spark.sql("""
select t.*, ts.to_id as site_id
from $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials t
inner join $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials_sites ts on t.id = ts.from_id
""")

trials_final_0.write.mode('overwrite').saveAsTable('trials_final_0')

# Join trials with drugs using trials_drugs table
trials_final_1 = spark.sql("""
WITH trials_drugs AS (
    SELECT 
        td.from_id AS trial_id, 
        CONCAT_WS('\;', ARRAY_DISTINCT(SORT_ARRAY(COLLECT_LIST(d.first_approved_brand)))) as drug_names
    FROM $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials_drugs td
    LEFT JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_drugs d ON td.to_id = d.id
    GROUP BY td.from_id
),
trials_mesh_terms AS (
    SELECT 
        tmt.from_id AS trial_id, 
        CONCAT_WS('\;', ARRAY_DISTINCT(SORT_ARRAY(COLLECT_LIST(m.mesh_term_name)))) as trial_mesh_term_names
    FROM $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials_mesh_terms tmt
    LEFT JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_mesh_terms m ON tmt.to_id = m.id
    GROUP BY tmt.from_id
),
trial_sponsors AS (
    SELECT 
        ts.from_id AS trial_id,
        org.organization_name as sponsor_name,ts.sponsor_type  as sponsor_type
    FROM $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials_sponsors ts
    LEFT JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_organizations org ON org.id = ts.to_id
)
SELECT
    t.id AS trial_id,
    t.trial_title,
    t.phase AS trial_phase,
    t.study_start AS trial_start_date,
    t.primary_completion_date AS trial_end_date,
    t.inclusion_criteria,
    t.exclusion_criteria,
    t.minimum_age AS min_patient_age,
    t.maximum_age AS max_patient_age,
    t.trial_participant_gender_restriction AS patient_gender,
    t.enrollment,
    t.trial_purpose,
    coalesce(agg_drugs.drug_names, 'Other') AS drug_names,
    coalesce(agg_mesh_terms.trial_mesh_term_names, 'Other') AS mesh_term_names,
    coalesce(agg_sponsor.sponsor_name, 'Other') AS sponsor_name,
    coalesce(agg_sponsor.sponsor_type, 'Other') AS sponsor_type,
    '' AS trial_therapeutic_area_name,
    t.trial_recruitment_period_start_date AS trial_actual_start_date,
    t.trial_recruitment_period_end_date AS enrollment_close_date,
    t.trial_study_design AS trial_design,
    t.state,
    t.trial_url,
    t.trial_id as nct_id,
    t.primary_outcomes,
    t.secondary_outcomes,
    t.study_end AS trial_end_date_combined,
    t.landmark_trials_tag,
    t.number_of_sites, 
    t.trial_duration_in_months AS enrollment_duration,
    t.patients_per_site_per_month AS trial_patients_per_site_per_month,    
    '' AS patient_segment,
    '' AS disease_name
FROM 
    trials_final_0 t
LEFT JOIN trials_drugs agg_drugs ON t.id = agg_drugs.trial_id
LEFT JOIN trials_mesh_terms agg_mesh_terms ON t.id = agg_mesh_terms.trial_id
LEFT JOIN trial_sponsors agg_sponsor ON t.id = agg_sponsor.trial_id
""")

trials_final_1 = trials_final_1.dropDuplicates()


write_path = path.replace("table_name", "tascan_trial")
trials_final_1.write.mode("overwrite").parquet(write_path)

trials_final_1.write.mode('overwrite').saveAsTable('tascan_trial')

# write_path = path.replace("table_name", "ctms_refactored_country")
# ctms_refactoring_country_final_df.write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   tascan_trial where trial_id is not null
""")

if trials_final_1.count() == 0:
    print("Skipping copy_hdfs_to_s3 for tascan_trial as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3(
        "$$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
