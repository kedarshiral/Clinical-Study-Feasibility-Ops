import os
import re
import traceback
import sys
import pandas as pd
from datetime import datetime
from ConfigUtility import JsonConfigUtility
import CommonConstants
import logging

SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
UTILITIES_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "utilities/"))
sys.path.insert(0, UTILITIES_DIR_PATH)

from utilities.utils import Utils

from utilities import CommonServicesConstants

UTILES_OBJ = Utils()
app_config_dict = UTILES_OBJ.get_application_config_json()
secret_name = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["db_connection_details"].get("secret_name", None)
region_name = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["db_connection_details"].get("db_region_name",
                                                                                                  None)
get_secret_dict = UTILES_OBJ.get_secret(secret_name, region_name)
os.environ['DB_PASSWORD'] = get_secret_dict["password"]
UTILES_OBJ.initialize_variable(app_config_dict)
logger = logging.getLogger('__name__')

SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))

configuration = JsonConfigUtility(
    os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "utilities_postgres/environment_params.json"))
flask_schema = configuration.get_configuration(["schema"])
logger.info('flask schema %s', flask_schema)
backend_schema = configuration.get_configuration(["schema_reporting"])
logger.info('backend schema %s', backend_schema)
foreign_server = configuration.get_configuration(["foreign_server"])


def retaining_TU():
    try:

        current_timestamp = datetime.utcnow()
        print("current_timestamp - %s", str(current_timestamp))
        current_time_string = datetime.strftime(current_timestamp, "%Y-%m-%d %H:%M:%S")

        df = pd.read_csv("")
        scenario_ids = tuple(df['scenario_id'].tolist())

        update_active_flag = """UPDATE $$schema$$.f_rpt_user_trial_universe SET active_flag = 'N' where scenario_id IN $$scenario_ids$$ and active_flag = 'Y'"""

        update_active_flag = update_active_flag.replace("$$schema$$", flask_schema).replace("$$scenario_ids$$",
                                                                                            scenario_ids)
        print('update_active_flag %s', update_active_flag)

        # Executing update_active_flag_to_n_query
        update_active_flag_output = UTILES_OBJ.execute_query_without_g(
            update_active_flag, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        print("update_active_flag_output --> %s", str(update_active_flag_output))

        for i in df['scenario_id']:
            ctfo_trial_ids = tuple(df['new_trial_id'].tolist())

            all_data_query = """select user_id, theme_id, scenario_id, trial_universe_start_date, trial_universe_end_date from $$schema$$.f_rpt_scenario_stdy_sumry where scenario_id  = $$scenario_id$$ and housekeeping_status IN ('active', 'archive')"""

            all_data_query = all_data_query.replace("$$scenario_id", i)

            all_data_query_output = UTILES_OBJ.execute_query_without_g(
                all_data_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
                UTILES_OBJ.password, UTILES_OBJ.database_name)
            print("all_data_query_output --> %s", str(all_data_query_output))

            scenario_id = all_data_query_output['result']['scenario_id']
            theme_id = all_data_query_output['result']['theme_id']
            user_id = all_data_query_output['result']['user_id']
            trial_start_dt = all_data_query_output['result']['trial_universe_start_date']
            trial_end_dt = all_data_query_output['result']['trial_universe_end_date']

            insert_trial_universe_query = """WITH c1 AS( SELECT * FROM dblink('$$foreign_server$$',$redshift$ SELECT ctfo_trial_id, protocol_ids, nct_id, therapy_area, disease, title, phase, trial_status, sponsor, sponsor_type, subsidiary_sponsors AS child_sponsor, no_of_countries, no_of_sites, trial_start_dt, CASE WHEN enrollment_duration IS NULL THEN NULL ELSE enrollment_duration END AS enrollment_duration, CASE WHEN patients_enrolled_planned IS NULL THEN NULL ELSE patients_enrolled_planned END AS patients_enrolled_planned, CASE WHEN patients_enrolled_actual IS NULL THEN NULL ELSE patients_enrolled_actual END AS patients_enrolled_actual, CASE WHEN regexp_count(enrollment_rate, '^[0-9.]+$') >= 1 THEN cast(enrollment_rate AS DOUBLE PRECISION) ELSE NULL END AS enrollment_rate, CASE WHEN regexp_count(screen_failure_rate, '^[0-9.]+$') >= 1 THEN cast(screen_failure_rate AS DOUBLE PRECISION) ELSE NULL END AS screen_failure_rate, CASE WHEN regexp_count(patient_dropout_rate, '^[0-9.]+$') >= 1 THEN cast(patient_dropout_rate AS DOUBLE PRECISION) ELSE NULL END AS patient_dropout_rate, exclude_label FROM $$schema_reporting$$.f_rpt_trial_universe WHERE ctfo_trial_id IN $$ctfo_trial_ids$$ $redshift$) AS t1 ( ctfo_trial_id text, protocol_ids text, nct_id text, therapy_area text, disease text, title text, phase text, trial_status text, sponsor text, sponsor_type text, child_sponsor text, no_of_countries bigint, no_of_sites bigint, trial_start_dt text, enrollment_duration DOUBLE PRECISION, patients_enrolled_planned DOUBLE PRECISION, patients_enrolled_actual DOUBLE PRECISION, enrollment_rate DOUBLE PRECISION, screen_failure_rate DOUBLE PRECISION, patient_dropout_rate DOUBLE PRECISION, exclude_label text)) INSERT INTO $$schema$$.f_rpt_user_trial_universe ( ctfo_trial_id, protocol_ids, nct_id, therapy_area, disease, title, phase, trial_status, sponsor, sponsor_type, child_sponsor, no_of_countries, no_of_sites, trial_start_dt, enrollment_duration, patients_enrolled_planned, patients_enrolled_actual, enrollment_rate, screen_failure_rate, patient_dropout_rate, scenario_id, theme_id, include_exclude_flag, exclude_label, user_added_flag, active_flag, created_by, created_timestamp, last_updated_by, last_updated_timestamp) SELECT ctfo_trial_id, CASE WHEN protocol_ids = '' THEN NULL ELSE protocol_ids END AS protocol_ids, CASE WHEN nct_id = '' THEN NULL ELSE nct_id END AS nct_id, therapy_area, disease, title, phase, trial_status, sponsor, sponsor_type, child_sponsor, no_of_countries, no_of_sites, trial_start_dt, enrollment_duration, patients_enrolled_planned, patients_enrolled_actual, enrollment_rate, screen_failure_rate, patient_dropout_rate, %(scenario_id)s, %(theme_id) s, '$$include_exclude_flag$$', exclude_label, '$$user_added_flag$$', '$$active_flag$$', %(created_by)s, '$$created_timestamp$$', %(last_updated_by)s, '$$last_updated_timestamp$$' FROM c1"""

            insert_trial_universe_query = insert_trial_universe_query.replace("$$foreign_server$$",
                                                                              foreign_server).replace(
                '$$schema_reporting$$', backend_schema).replace('$$schema$$', flask_schema).replace(
                '$$ctfo_trial_ids$$', ctfo_trial_ids)

            insert_trial_universe_query_params = {"user_id": user_id, "theme_id": theme_id,
                                                  "scenario_id": scenario_id,
                                                  "trial_universe_start_date": trial_start_dt,
                                                  "trial_universe_end_date": trial_end_dt,
                                                  "created_by": user_id,
                                                  "last_updated_by": user_id}

            insert_trial_universe_query = re.sub(r"\s+", " ", insert_trial_universe_query)
            insert_trial_universe_query = re.sub(r"WHERE\s+and", " where ",
                                                 insert_trial_universe_query, flags=re.I)
            insert_trial_universe_query = re.sub(r"and\s+and", " and ", insert_trial_universe_query,
                                                 flags=re.I)
            insert_trial_universe_query = re.sub(r'WHERE\s+\) ', ') ', insert_trial_universe_query,
                                                 flags=re.I)
            insert_trial_universe_query = re.sub(r"WHERE\s+GROUP", " GROUP ",
                                                 insert_trial_universe_query, flags=re.I)
            insert_trial_universe_query = re.sub("and\s+group", " group ",
                                                 insert_trial_universe_query, flags=re.I)
            insert_trial_universe_query = re.sub("\)\s+\(", ") and ( ", insert_trial_universe_query,
                                                 flags=re.I)
            insert_trial_universe_query = re.sub(r'true\s+\(', ' (', insert_trial_universe_query,
                                                 flags=re.I)
            insert_trial_universe_query = re.sub(r'and\s+\(\s+and', ' and ( ',
                                                 insert_trial_universe_query, flags=re.I)
            insert_trial_universe_query = re.sub(r'where\s+\(\s+and', ' where ( ',
                                                 insert_trial_universe_query,
                                                 flags=re.I)

            print("%s insert_trial_universe_query is : {}".format(
                UTILES_OBJ.get_replaced_query(insert_trial_universe_query, insert_trial_universe_query_params)))
            insert_trial_universe_output = UTILES_OBJ.execute_query_without_g(insert_trial_universe_query,
                                                                              UTILES_OBJ.host,
                                                                              UTILES_OBJ.port,
                                                                              UTILES_OBJ.username,
                                                                              UTILES_OBJ.password,
                                                                              UTILES_OBJ.database_name,
                                                                              insert_trial_universe_query_params)
    except Exception as ex:
        print("Error in display_email_details. ERROR - " + str(traceback.format_exc()))
        return {"status": "Failed", "error": str(ex)}
