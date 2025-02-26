
#QUERIES FOR LOG_SURVEY TABLES

#SQL for insert a record in log_survey_scheduled_job_smry table
QUERY_INSERT_LOG_SMRY = """
INSERT INTO {schema_name}.LOG_SURVEY_RESPONSE_DATA_SYNC_SMRY VALUES (
'{job_id}',
'{start_time}',
{end_time},
'{requested_by}',
'{status}'
)
"""

#SQL for updating a record in log_survey_scheduled_job_smry table
QUERY_UPDATE_LOG_SMRY ="""
UPDATE {schema_name}.LOG_SURVEY_RESPONSE_DATA_SYNC_SMRY VALUES SET
end_time = '{end_time}',
status = '{status}'
WHERE job_id = '{job_id}'
"""

#SQL for inserting a record in the log_survey_scheduled_job_dtl table
QUERY_INSERT_LOG_DTL = """
INSERT INTO {schema_name}.LOG_SURVEY_RESPONSE_DATA_SYNC_DTL VALUES (
'{job_id}',
'{survey_id}',
'{survey_type}',
'{level}',
'{country_name}',
'{ctfo_site_id}',
'{investigator_id}',
'{start_time}',
{end_time},
'{requested_by}',
'{status}'
)
"""


#SQL query to update a record in the log_survey_scheduled_job_dtl table
QUERY_UPDATE_LOG_DTL = """
UPDATE {schema_name}.LOG_SURVEY_RESPONSE_DATA_SYNC_DTL VALUES SET
country_name = '{country_name}',
ctfo_site_id = '{ctfo_site_id}',
investigator_id = '{investigator_id}',
end_time = '{end_time}',
status = '{status}'
WHERE job_id = '{job_id}' AND
survey_id = '{survey_id}' AND
survey_type = '{survey_type}' AND
level = '{level}'
"""

#Query to get the latest (max) job_id from log_survey_scheduled_job_smry table for retry failed job logic
QUERY_JOB_ID_LOG_SMRY = """
SELECT MAX(job_id) from {schema_name}.LOG_SURVEY_RESPONSE_DATA_SYNC_SMRY
"""

#Query to get the failed job from log_survey_scheduled_job_dlt table for retry failed job logic
QUERY_GET_FAILED_DTL_LOG = """
SELECT count(distinct survey_id) as count from {schema_name}.LOG_SURVEY_RESPONSE_DATA_SYNC_DTL
WHERE job_id = '{job_id}' and status = 'Failed'
"""

#Query to to check if an active survey scheduled job is running
QUERY_CHECK_ACTIVE_JOB = """
SELECT * FROM {schema_name}.LOG_SURVEY_RESPONSE_DATA_SYNC_SMRY
where job_id in
(select max(job_id) from {schema_name}.LOG_SURVEY_RESPONSE_DATA_SYNC_SMRY)
"""

#Query to fetch the active survey_id
QUERY_TO_GET_SURVEYID = """
select survey.theme_id, survey.survey_id, 
survey.survey_type, survey.survey_version, 
survey.survey_published_date, survey.survey_status, 
survey.qualtrics_user, 
key_store.key from {schema_name}.f_rpt_api_key_store key_store 
FULL OUTER JOIN {schema_name}.f_rpt_user_scenario_survey_dtl survey 
ON key_store.user_id = survey.qualtrics_user 
WHERE is_active = 'true'
"""

#Query to get the records in f_rpt_user_scenario_survey_country_dtl which is going to update
GET_RECORD_COUNTRY_DTL = """
SELECT * FROM {schema_name}.f_rpt_user_scenario_survey_country_dtl
where survey_id = '{survey_id}'
and theme_id= '{theme_id}'
and qualtrics_contact_id = '{qualtrics_contact_id}'
and is_active = 'true';
"""

#Query to get the records in f_rpt_user_scenario_survey_site_investigator_dtl which is going to update
GET_RECORD_INVESTIGATOR_DTL = """
SELECT * FROM {schema_name}.f_rpt_user_scenario_survey_site_investigator_dtl
where survey_id = '{survey_id}'
and theme_id= '{theme_id}'
and qualtrics_contact_id = '{qualtrics_contact_id}'
and is_active = 'true';
"""

# Query to get the failed transactions from previous run
GET_PREVIOUS_FAILED = """
SELECT survey_id, survey_type, level FROM {schema_name}.log_survey_response_data_sync_dtl WHERE job_id IN (
SELECT job_id FROM {schema_name}.log_survey_response_data_sync_smry WHERE job_id IN (
SELECT MAX(job_id) FROM {schema_name}.log_survey_response_data_sync_smry WHERE status <> 'In-Progess') AND status = 'Failed')
AND status = 'Failed' group by 1,2,3
"""

#Query to get the record from survey_dtl table given survey_id
GET_FAILED_SURVEY_DTL = """
SELECT survey.theme_id, survey.survey_id, survey.survey_type, 
survey.survey_version, survey.survey_published_date, 
survey.survey_status, survey.qualtrics_user, 
key_store.key FROM {schema_name}.f_rpt_user_scenario_survey_dtl survey 
INNER JOIN {schema_name}.f_rpt_api_key_store key_store 
ON key_store.user_id = survey.qualtrics_user 
WHERE is_active = 'true' AND survey_id = '{survey_id}'
"""



#Query to get the records in f_rpt_user_scenario_survey_country_dtl which is going to update
UPDATE_RESPONSE_LINK_SHARED_COUNTRY = """
UPDATE {schema_name}.f_rpt_user_scenario_survey_country_dtl SET response_link_shared = 'true' 
where survey_id = '{survey_id}'
and theme_id= '{theme_id}'
and qualtrics_contact_id = '{qualtrics_contact_id}'
and is_active = 'true';
"""

UPDATE_RESPONSE_LINK_SHARED_SITE = """
UPDATE {schema_name}.f_rpt_user_scenario_survey_site_investigator_dtl SET response_link_shared = 'true' 
where survey_id = '{survey_id}'
and theme_id= '{theme_id}'
and qualtrics_contact_id = '{qualtrics_contact_id}'
and is_active = 'true';
"""

GET_COUNTRY_HEAD_DETAILS = """
SELECT * FROM {schema_name}.f_rpt_user_scenario_survey_country_dtl where lower(country_name) = lower('{country_name}') and 
scenario_id = '{scenario_id}' and is_active"""

GET_SMRY_DETAILS = """SELECT * FROM {schema_name}.f_rpt_scenario_stdy_sumry where scenario_id = '{scenario_id}'
 AND active_flag = 'Y'"""

GET_REPORTING_COUNTRY_HEAD_DETAILS = """
SELECT * FROM {schema_name}.f_rpt_study_country_details where 
lower(standard_country) = lower('{country_name}') AND study_code = '{study_code}'"""

COHORT_DETAILS_QUERY = """
SELECT * FROM {schema_name}.f_rpt_cohort_scenario_stdy_sumry where
scenario_id = '{scenario_id}' AND active_flag = 'Y'
"""
ENROLLMENT_DETAILS_QUERY = """
SELECT * FROM {schema_name}.f_rpt_user_scenario_survey_site_investigator_dtl WHERE 
theme_id = '{theme_id}' AND survey_id = '{survey_id}' AND 
qualtrics_contact_id = '{contact_id}' AND is_active = 'true'
"""

ENROLLMENT_DETAILS_QUERY_COUNTRY = """
SELECT * FROM {schema_name}.f_rpt_user_scenario_survey_country_dtl WHERE 
theme_id = '{theme_id}' AND survey_id = '{survey_id}' AND 
qualtrics_contact_id = '{contact_id}' AND is_active = 'true'
"""
