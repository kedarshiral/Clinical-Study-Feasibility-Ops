import os
import traceback
import sys

import logging
from utilities_postgres.utils import Utils
from ConfigUtility import JsonConfigUtility
import CommonConstants

from utilities_postgres import CommonServicesConstants

SERVICE_DIRECTORY_PATH = os.path.dirname(os.path.abspath(__file__))
UTILITIES_DIR_PATH = os.path.abspath(os.path.join(SERVICE_DIRECTORY_PATH, "utilities/"))
sys.path.insert(0, UTILITIES_DIR_PATH)

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


def get_inv_backend_data(vlist):
    try:
        norm_metric_update = """UPDATE $$schema$$.f_rpt_scenario_site_ranking_final SET ctfo_investigator_id= %(ctfo_investigator_id)s,investigator_full_nm= %(investigator_full_nm)s,investigator_details=%(investigator_details)s WHERE scenario_id = %(scenario_id)s AND theme_id = %(theme_id)s AND active_flag = 'Y' AND ctfo_site_id= %(ctfo_site_id)s"""

        norm_metric_update = norm_metric_update.replace("$$schema$$", flask_schema)

        print("norm_metric_update  %s", norm_metric_update)
        norm_metric_update_output = UTILES_OBJ.execute_many_query_without_g(
            norm_metric_update, vlist, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        print(" norm_metric_update_output  %s", norm_metric_update_output)
    except Exception as ex:
        print("Error in display_email_details. ERROR - " + str(traceback.format_exc()))
        return {"status": "Failed", "error": str(ex)}


def get_inv_scenario_data():
    try:
        all_data_query = """select distinct scenario_id,theme_id  from $$schema$$.f_rpt_scenario_site_ranking_final where active_flag='Y'  """
        get_all_data_query = all_data_query.replace("$$schema$$", flask_schema)
        get_all_data_query_output = UTILES_OBJ.execute_query_without_g(
            get_all_data_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        vlist = []
        slist = []

        for data in get_all_data_query_output['result']:
            scenario_id = data['scenario_id']
            slist.append(scenario_id)
            theme_id = data['theme_id']

            get_inv_norm_query = """select ctfo_site_id from $$schema$$.f_rpt_scenario_site_ranking_final where  active_flag='Y' and scenario_id= %(scenario_id)s and theme_id= %(theme_id)s"""
            get_inv_norm_params = {"scenario_id": scenario_id,
                                   "theme_id": theme_id
                                   }
            get_site_norm_query_rep = get_inv_norm_query.replace("$$schema$$", flask_schema)
            print('Scenario_data %s', get_site_norm_query_rep)

            get_site_norm_query_output = UTILES_OBJ.execute_query_without_g(
                get_site_norm_query_rep, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
                UTILES_OBJ.password, UTILES_OBJ.database_name, get_inv_norm_params)
            print('get_site_norm_query_output %s', get_site_norm_query_output)
            for item in get_site_norm_query_output['result']:
                ctfo_site_id = item['ctfo_site_id']
                get_site_norm_value_query = """select scenario_id,theme_id,ctfo_site_id,ctfo_investigator_id,investigator_full_nm,investigator_details from $$schema$$.f_rpt_scenario_site_ranking where  active_flag='Y' and scenario_id= %(scenario_id)s and theme_id= %(theme_id)s and ctfo_site_id='$$ctfo_site_id$$' """

                get_inv_query_params_1 = {"scenario_id": scenario_id,
                                          "theme_id": theme_id
                                          }
                get_inv_norm_value_query_rep = get_site_norm_value_query.replace("$$schema$$", flask_schema).replace(
                    "$$ctfo_site_id$$", ctfo_site_id)
                print('Scenario_data %s', get_inv_norm_value_query_rep)

                get_site_norm_value_query_output = UTILES_OBJ.execute_query_without_g(
                    get_inv_norm_value_query_rep, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
                    UTILES_OBJ.password, UTILES_OBJ.database_name, get_inv_query_params_1)
                print('get_inv_norm_value_query_output %s', get_site_norm_value_query_output)

                get_inv_backend_data(get_site_norm_value_query_output['result'])

        print("slist : %s", slist)
    except Exception as ex:
        print("Error in display_email_details. ERROR - " + str(traceback.format_exc()))
        return {"status": "Failed", "error": str(ex)}


#############################################################
def get_inv_opt_backend_data(vlist):
    try:
        norm_metric_update = """UPDATE $$schema$$.f_rpt_scenario_site_ranking_optimized SET ctfo_investigator_id= %(ctfo_investigator_id)s,investigator_full_nm= %(investigator_full_nm)s,investigator_details=%(investigator_details)s WHERE scenario_id = %(scenario_id)s AND theme_id = %(theme_id)s AND active_flag = 'Y' AND ctfo_site_id= %(ctfo_site_id)s"""

        norm_metric_update = norm_metric_update.replace("$$schema$$", flask_schema)

        print("norm_metric_update  %s", norm_metric_update)
        norm_metric_update_output = UTILES_OBJ.execute_many_query_without_g(
            norm_metric_update, vlist, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        print(" norm_metric_update_output  %s", norm_metric_update_output)
    except Exception as ex:
        print("Error in display_email_details. ERROR - " + str(traceback.format_exc()))
        return {"status": "Failed", "error": str(ex)}


def get_inv_opt_scenario_data():
    try:
        all_data_query = """select distinct scenario_id,theme_id  from $$schema$$.f_rpt_scenario_site_ranking_optimized where active_flag='Y'  """
        get_all_data_query = all_data_query.replace("$$schema$$", flask_schema)
        get_all_data_query_output = UTILES_OBJ.execute_query_without_g(
            get_all_data_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        vlist = []
        slist_opt = []

        for data in get_all_data_query_output['result']:
            scenario_id = data['scenario_id']
            slist_opt.append(scenario_id)
            theme_id = data['theme_id']

            get_inv_norm_query = """select ctfo_site_id from $$schema$$.f_rpt_scenario_site_ranking_optimized where  active_flag='Y' and scenario_id= %(scenario_id)s and theme_id= %(theme_id)s"""
            get_inv_norm_params = {"scenario_id": scenario_id,
                                   "theme_id": theme_id
                                   }
            get_site_norm_query_rep = get_inv_norm_query.replace("$$schema$$", flask_schema)
            print('Scenario_data %s', get_site_norm_query_rep)

            get_site_norm_query_output = UTILES_OBJ.execute_query_without_g(
                get_site_norm_query_rep, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
                UTILES_OBJ.password, UTILES_OBJ.database_name, get_inv_norm_params)
            print('get_site_norm_query_output %s', get_site_norm_query_output)
            for item in get_site_norm_query_output['result']:
                ctfo_site_id = item['ctfo_site_id']
                get_site_norm_value_query = """select scenario_id,theme_id, ctfo_site_id,ctfo_investigator_id,investigator_full_nm ,investigator_details from $$schema$$.f_rpt_scenario_site_ranking where  active_flag='Y' and scenario_id= %(scenario_id)s and theme_id= %(theme_id)s and ctfo_site_id='$$ctfo_site_id$$' """

                get_inv_query_params_1 = {"scenario_id": scenario_id,
                                          "theme_id": theme_id
                                          }
                get_inv_norm_value_query_rep = get_site_norm_value_query.replace("$$schema$$", flask_schema).replace(
                    "$$ctfo_site_id$$", ctfo_site_id)
                print('Scenario_data %s', get_inv_norm_value_query_rep)

                get_site_norm_value_query_output = UTILES_OBJ.execute_query_without_g(
                    get_inv_norm_value_query_rep, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
                    UTILES_OBJ.password, UTILES_OBJ.database_name, get_inv_query_params_1)
                print('get_inv_norm_value_query_output %s', get_site_norm_value_query_output)

                get_inv_opt_backend_data(get_site_norm_value_query_output['result'])

        print("slist_opt : %s", slist_opt)
    except Exception as ex:
        print("Error in display_email_details. ERROR - " + str(traceback.format_exc()))
        return {"status": "Failed", "error": str(ex)}


get_inv_scenario_data()
get_inv_opt_scenario_data()


