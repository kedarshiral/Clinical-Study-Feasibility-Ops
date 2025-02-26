import os
import logging
from utilities_postgres.utils import Utils
from ConfigUtility import JsonConfigUtility
import CommonConstants
from utilities_postgres import CommonServicesConstants
from datetime import datetime


logger = logging.getLogger('__name__')

UTILES_OBJ = Utils()
app_config_dict = UTILES_OBJ.get_application_config_json()
secret_name = \
    app_config_dict[CommonServicesConstants.CTFO_SERVICES]["db_connection_details"].get(
        "secret_name", None)
region_name = \
    app_config_dict[CommonServicesConstants.CTFO_SERVICES]["db_connection_details"].get(
        "db_region_name", None)

get_secret_dict = UTILES_OBJ.get_secret(secret_name, region_name)
os.environ['DB_PASSWORD'] = get_secret_dict["password"]
UTILES_OBJ.initialize_variable(app_config_dict)


configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "utilities_postgres/environment_params.json"))
flask_schema = configuration.get_configuration(["schema"])
logger.info('flask schema %s', flask_schema)


def insert_scenario_data():
    current_timestamp = datetime.utcnow()
    current_time = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

    survey_mapping_country = [{'question_id': 'QID1', 'application_mapping_key': 'number_of_sites_contacted_diverse_backgrounds', 'ui_display_key': 'Number of Sites Contacted Diverse Backgrounds', 'is_mandatory': False }, { 'question_id': 'QID2', 'application_mapping_key': 'precision_sites', 'ui_display_key': 'Precision Sites', 'is_mandatory': False }, { 'question_id': 'QID4', 'application_mapping_key': 'covid_impact', 'ui_display_key': 'Covid Impact', 'is_mandatory': False }, { 'question_id': 'QID6', 'application_mapping_key': 'investigators_declining_operational_feasibility', 'ui_display_key': 'Investigators Declining Operational Feasibility', 'is_mandatory': False }, { 'question_id': 'QID8', 'application_mapping_key': 'feedback_from_the_contacted_investigators', 'ui_display_key': 'Feedback from the Contacted Investigators', 'is_mandatory': False }, { 'question_id': 'QID11', 'application_mapping_key': 'inv_interested_in_study', 'ui_display_key': 'Inv Interested in Study', 'is_mandatory': False }, { 'question_id': 'QID15', 'application_mapping_key': 'standard_of_care', 'ui_display_key': 'Standard of Care', 'is_mandatory': False }, { 'question_id': 'QID17', 'application_mapping_key': 'degree_of_burden_patient', 'ui_display_key': 'Degree of Burden Patient', 'is_mandatory': False }, { 'question_id': 'QID19', 'application_mapping_key': 'concern_on_include_exclude_criteria', 'ui_display_key': 'Concern on Include Exclude Criteria', 'is_mandatory': False }, { 'question_id': 'QID21', 'application_mapping_key': 'screen_failure_rate', 'ui_display_key': 'Screen Failure Rate', 'is_mandatory': False }, { 'question_id': 'QID23', 'application_mapping_key': 'dropout_rate', 'ui_display_key': 'Dropout Rate', 'is_mandatory': False }, { 'question_id': 'QID25', 'application_mapping_key': 'operational_capacity', 'ui_display_key': 'Operational Capacity', 'is_mandatory': False }, { 'question_id': 'QID28', 'application_mapping_key': 'ethics_committee_delay', 'ui_display_key': 'Ethics Committee Delay', 'is_mandatory': False }, { 'question_id': 'QID30', 'application_mapping_key': 'feasibility_or_protocol_fit', 'ui_display_key': 'Feasibility or Protocol Fit', 'is_mandatory': False }, { 'question_id': 'QID32', 'application_mapping_key': 'any_competing_ongoing_planned_trials_patients', 'ui_display_key': 'Any Competing Ongoing Planned Trials Patients', 'is_mandatory': False }, { 'question_id': 'QID35', 'application_mapping_key': 'regulatory_fit', 'ui_display_key': 'Regulatory Fit', 'is_mandatory': False }, { 'question_id': 'QID37', 'application_mapping_key': 'patient_advocacy_group', 'ui_display_key': 'Patient Advocacy Group', 'is_mandatory': False }, { 'question_id': 'QID45', 'application_mapping_key': 'first_site_initiated_response', 'ui_display_key': 'First Site Initiated Response', 'is_mandatory': False }, { 'question_id': 'QID46', 'application_mapping_key': 'fifty_percent_sites_initiated_response', 'ui_display_key': 'Fifty Percent Sites Initiated Response', 'is_mandatory': False }, { 'question_id': 'QID47', 'application_mapping_key': 'all_sites_initiated_response', 'ui_display_key': 'All Sites Initiated Response', 'is_mandatory': False }, { 'question_id': 'QID48', 'application_mapping_key': 'avg_time_for_site_initiation_response', 'ui_display_key': 'Avg Time for Site Initiation Response', 'is_mandatory': False }, { 'question_id': 'QID49', 'application_mapping_key': 'last_patient_in_response', 'ui_display_key': 'Last Patient in Response', 'is_mandatory': False }, { 'question_id': 'QID50', 'application_mapping_key': 'last_patient_visit', 'ui_display_key': 'Last Patient Visit', 'is_mandatory': False }, { 'question_id': 'QID51', 'application_mapping_key': 'first_patient_in_date', 'ui_display_key': 'First Patient in Date', 'is_mandatory': False }, { 'question_id': 'QID52', 'application_mapping_key': 'recruitment_duration', 'ui_display_key': 'Recruitment Duration', 'is_mandatory': False }, { 'question_id': 'QID53', 'application_mapping_key': 'no_of_sites', 'ui_display_key': 'No of Sites', 'is_mandatory': False }, { 'question_id': 'QID54', 'application_mapping_key': 'no_of_patients', 'ui_display_key': 'No of Patients', 'is_mandatory': False }, { 'question_id': 'QID57', 'application_mapping_key': 'interested_in_study', 'ui_display_key': 'Interested in Study', 'is_mandatory': True }, { 'question_id': 'QID58', 'application_mapping_key': 'reason_not_interested_in_study', 'ui_display_key': 'Reason not Interested in Study', 'is_mandatory': True }]

    get_country_outreach_data_query = """select scenario_id, survey_id, theme_id from $$schema$$.f_rpt_user_scenario_survey_country_dtl where is_active and survey_sent_date is not null group by scenario_id, survey_id, theme_id"""

    get_country_outreach_data_query = get_country_outreach_data_query.replace("$$schema$$",
                                                                              flask_schema)
    print(f"get_country_outreach_data_query: {get_country_outreach_data_query}")
    get_country_outreach_data_query_output = UTILES_OBJ.execute_query_without_g(
        get_country_outreach_data_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
        UTILES_OBJ.password, UTILES_OBJ.database_name)
    print(f"get_country_outreach_data_query_output: {get_country_outreach_data_query_output}")

    for each_survey in get_country_outreach_data_query_output['result']:
        insert_country_outreach_data_query = """INSERT INTO $$schema$$.scenario_qualtrics_mapping ( scenario_id ,survey_id , qual_survey_version , survey_type ,created_by ,created_time ,last_updated_by ,last_updated_time ,is_active ,theme_id ,qual_json_mapping ,screen_name ) VALUES(%(scenario_id)s, %(survey_id)s, %(qual_survey_version)s, %(survey_type)s, %(created_by)s, %(created_time)s, %(last_updated_by)s, %(last_updated_time)s, %(is_active)s, %(theme_id)s, %(qual_json_mapping)s, %(screen_name)s)"""
        insert_country_outreach_data_query_param = {"theme_id": each_survey["theme_id"],
                                                    "scenario_id": each_survey["scenario_id"],
                                                    "survey_id": each_survey["survey_id"],
                                                    "survey_type": "country_outreach",
                                                    "qual_survey_version": 1,
                                                    "created_by": "gururaj.teli@zs.com",
                                                    "created_time": current_time,
                                                    "last_updated_by": "gururaj.teli@zs.com",
                                                    "last_updated_time": current_time,
                                                    "is_active": True,
                                                    "qual_json_mapping": str(
                                                        survey_mapping_country),
                                                    "screen_name": "country_outreach"}

        insert_country_outreach_data_query = \
            insert_country_outreach_data_query.replace("$$schema$$", flask_schema)
        insert_country_outreach_data_query_output = UTILES_OBJ.execute_query_without_g(
            insert_country_outreach_data_query, UTILES_OBJ.host, UTILES_OBJ.port,
            UTILES_OBJ.username, UTILES_OBJ.password, UTILES_OBJ.database_name,
            insert_country_outreach_data_query_param)
        print(f"insert_country_outreach_data_query_output for {each_survey['scenario_id']}:"
              f" {insert_country_outreach_data_query_output}")

    survey_mapping_site = [{ 'question_id': 'QID1', 'application_mapping_key': 'interested_in_study', 'ui_display_key': 'Interested in Study', 'is_mandatory': True }, { 'question_id': 'QID2', 'application_mapping_key': 'reason_not_interested_in_study', 'ui_display_key': 'Reason not Interested in Study', 'is_mandatory': True }, { 'question_id': 'QID28', 'application_mapping_key': 'patients_committed', 'ui_display_key': 'Patients Committed', 'is_mandatory': False }, { 'question_id': 'QID8', 'application_mapping_key': 'degree_of_interest_scientific', 'ui_display_key': 'Degree of Interest Scientific', 'is_mandatory': False }, { 'question_id': 'QID11', 'application_mapping_key': 'degree_of_interest_medical', 'ui_display_key': 'Degree of Interest Medical', 'is_mandatory': False }, { 'question_id': 'QID9', 'application_mapping_key': 'degree_of_burden_operation', 'ui_display_key': 'Degree of Burden Operation', 'is_mandatory': False }, { 'question_id': 'QID12', 'application_mapping_key': 'degree_of_burden_patient', 'ui_display_key': 'Degree of Burden Patient', 'is_mandatory': False }, { 'question_id': 'QID4', 'application_mapping_key': 'covid_impact', 'ui_display_key': 'Covid Impact', 'is_mandatory': False }, { 'question_id': 'QID7', 'application_mapping_key': 'principal_investigator_specialty', 'ui_display_key': 'Principal Investigator Specialty', 'is_mandatory': False }, { 'question_id': 'QID15', 'application_mapping_key': 'exp_in_comparable_complex_study', 'ui_display_key': 'Exp in Comparable Complex Study', 'is_mandatory': False }, { 'question_id': 'QID17', 'application_mapping_key': 'study_procedure_availability', 'ui_display_key': 'Study Procedure Availability', 'is_mandatory': False }, { 'question_id': 'QID19', 'application_mapping_key': 'competition_for_patient_recruitment', 'ui_display_key': 'Competition for Patient Recruitment', 'is_mandatory': False }, { 'question_id': 'QID22', 'application_mapping_key': 'patient_followed_last_year', 'ui_display_key': 'Patient Followed Last Year', 'is_mandatory': False }, { 'question_id': 'QID23', 'application_mapping_key': 'patients_allowed_on_criteria', 'ui_display_key': 'Patients Allowed on Criteria', 'is_mandatory': False }, { 'question_id': 'QID24', 'application_mapping_key': 'screen_failure_rate', 'ui_display_key': 'Screen Failure Rate', 'is_mandatory': False }, { 'question_id': 'QID26', 'application_mapping_key': 'dropout_rate', 'ui_display_key': 'Dropout Rate', 'is_mandatory': False }, { 'question_id': 'QID30', 'application_mapping_key': 'ethics_committee_delay', 'ui_display_key': 'Ethics Committee Delay', 'is_mandatory': False }, { 'question_id': 'QID33', 'application_mapping_key': 'percentage_patients_enrolled', 'ui_display_key': 'Percentage Patients Enrolled', 'is_mandatory': False }, { 'question_id': 'QID34', 'application_mapping_key': 'female_participated_on_similar_study', 'ui_display_key': 'Female Participated on Similar Study', 'is_mandatory': False }, { 'question_id': 'QID35', 'application_mapping_key': 'patients_above_64_participated_on_similar_study', 'ui_display_key': 'Patients Above 64 Participated on Similar Study', 'is_mandatory': False }, { 'question_id': 'QID36', 'application_mapping_key': 'count_of_patients_enrolled', 'ui_display_key': 'Count of Patients Enrolled', 'is_mandatory': False }, { 'question_id': 'QID37', 'application_mapping_key': 'female_patient_followed_last_year', 'ui_display_key': 'Female Patient Followed Last Year', 'is_mandatory': False }, { 'question_id': 'QID38', 'application_mapping_key': 'patient_above_64_followed_last_year', 'ui_display_key': 'Patient Above 64 Followed Last Year', 'is_mandatory': False }]

    get_site_outreach_data_query = """select scenario_id, survey_id, theme_id from $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl where is_active and survey_sent_date is not null group by scenario_id, survey_id, theme_id"""

    get_site_outreach_data_query = \
        get_site_outreach_data_query.replace("$$schema$$", flask_schema)
    print(f"get_site_outreach_data_query: {get_site_outreach_data_query}")
    get_site_outreach_data_query_output = UTILES_OBJ.execute_query_without_g(
        get_site_outreach_data_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
        UTILES_OBJ.password, UTILES_OBJ.database_name)
    print(f"get_site_outreach_data_query_output: {get_site_outreach_data_query_output}")

    for each_survey in get_site_outreach_data_query_output['result']:
        insert_site_outreach_data_query = """INSERT INTO $$schema$$.scenario_qualtrics_mapping ( scenario_id ,survey_id , qual_survey_version ,survey_type ,created_by ,created_time ,last_updated_by ,last_updated_time ,is_active ,theme_id ,qual_json_mapping ,screen_name ) VALUES(%(scenario_id)s, %(survey_id)s, %(qual_survey_version)s, %(survey_type)s, %(created_by)s, %(created_time)s, %(last_updated_by)s, %(last_updated_time)s, %(is_active)s, %(theme_id)s, %(qual_json_mapping)s, %(screen_name)s)"""
        insert_site_outreach_data_query_param = {"theme_id": each_survey["theme_id"],
                                                 "scenario_id": each_survey["scenario_id"],
                                                 "survey_id": each_survey["survey_id"],
                                                 "survey_type": "site_outreach",
                                                 "qual_survey_version": 1,
                                                 "created_by": "gururaj.teli@zs.com",
                                                 "created_time": current_time,
                                                 "last_updated_by": "gururaj.teli@zs.com",
                                                 "last_updated_time": current_time,
                                                 "is_active": True,
                                                 "qual_json_mapping": str(
                                                     survey_mapping_site),
                                                 "screen_name": "site_outreach"}

        insert_site_outreach_data_query = \
            insert_site_outreach_data_query.replace("$$schema$$", flask_schema)
        insert_site_outreach_data_query_output = UTILES_OBJ.execute_query_without_g(
            insert_site_outreach_data_query, UTILES_OBJ.host, UTILES_OBJ.port,
            UTILES_OBJ.username, UTILES_OBJ.password, UTILES_OBJ.database_name,
            insert_site_outreach_data_query_param)
        print(f"insert_site_outreach_data_query_output for {each_survey['scenario_id']}:"
              f" {insert_site_outreach_data_query_output}")


insert_scenario_data()
