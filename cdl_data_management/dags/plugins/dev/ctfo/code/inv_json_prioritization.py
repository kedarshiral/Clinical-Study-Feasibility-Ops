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
logger.info('foreign server %s', foreign_server)


def update_wt_flag_data(vlist):

    try:
        scenario_id = vlist[0]['scenario_id']
        theme_id = vlist[0]['theme_id']

        outreach_inv_update = """WITH main AS(WITH abc AS( SELECT investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, investigator_email, similar_trials_experience, Row_number() OVER ( partition BY ctfo_site_id, scenario_id ORDER BY similar_trials_experience DESC) rank, 'a' AS email_flag FROM $$schema$$.f_rpt_scenario_highest_investigator WHERE scenario_id = %(scenario_id)s AND theme_id = %(theme_id)s AND active_flag = 'Y' AND investigator_email IS NOT NULL AND investigator_email != '' AND ctfo_site_id IN ( SELECT DISTINCT ctfo_site_id FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl WHERE scenario_id='ba07fe30' AND theme_id= %(theme_id)s AND is_active) GROUP BY investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, similar_trials_experience, investigator_email UNION SELECT investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, investigator_email, similar_trials_experience, row_number() OVER ( partition BY ctfo_site_id, scenario_id ORDER BY similar_trials_experience DESC) rank, 'd' AS email_flag FROM $$schema$$.f_rpt_scenario_highest_investigator WHERE scenario_id = %(scenario_id)s AND theme_id = %(theme_id)s AND active_flag = 'Y' AND ( investigator_email IS NULL OR investigator_email = '') AND ctfo_investigator_id IS NOT NULL AND ctfo_site_id IN ( SELECT DISTINCT ctfo_site_id FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl WHERE scenario_id='ba07fe30' AND theme_id= %(theme_id)s AND is_active) GROUP BY investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, similar_trials_experience, investigator_email UNION SELECT investigator_name AS investigator_full_nm, investigator_id AS ctfo_investigator_id, ctfo_site_id, scenario_id, investigator_email, 0 AS similar_trials_experience, 0 AS rank, 'b' AS email_flag FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl WHERE scenario_id = %(scenario_id)s AND theme_id = %(theme_id)s AND is_manual = true AND is_active = true UNION SELECT investigator_name AS investigator_full_nm, investigator_id AS ctfo_investigator_id, ctfo_site_id, scenario_id, investigator_email, 0 AS similar_trials_experience, 0 AS rank, 'c' AS email_flag FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl WHERE scenario_id = %(scenario_id)s AND theme_id = %(theme_id)s AND is_manual = true AND is_active=false ORDER BY ctfo_site_id, email_flag , rank ) SELECT investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, investigator_email, email_flag, similar_trials_experience, row_number() OVER ( partition BY ctfo_site_id, scenario_id ORDER BY ctfo_site_id, email_flag , rank ) rank FROM abc WHERE ctfo_site_id IN ( SELECT DISTINCT ctfo_site_id FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl WHERE scenario_id= %(scenario_id)s AND theme_id= %(theme_id)s ) ) SELECT main.ctfo_site_id, main.scenario_id, json_agg(jsonb_build_object('inv_email', main.investigator_email, 'inv_id' , ctfo_investigator_id, 'inv_name', investigator_full_nm , 'selected_flag' , CASE WHEN ot.scenario_id IS NOT NULL THEN 'true' ELSE 'false' END )) AS jsn_col FROM main LEFT JOIN $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl ot ON main.ctfo_site_id=ot.ctfo_site_id AND main.scenario_id=ot.scenario_id AND main.ctfo_investigator_id=ot.investigator_id AND ot.created_time=(Select max(created_time) from $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl where scenario_id = %(scenario_id)s AND theme_id = %(theme_id)s) GROUP BY main.ctfo_site_id, main.scenario_id ORDER BY ctfo_site_id"""
        outreach_inv_update = outreach_inv_update.replace("$$schema$$", flask_schema)

        outreach_inv_update_parms = {"scenario_id": scenario_id, "theme_id": theme_id}
        print("%s outreach_inv_update is : {}".format(
            UTILES_OBJ.get_replaced_query(outreach_inv_update,
                                          outreach_inv_update_parms)))

        ranking_inv_update = """WITH all_ct AS(WITH abc AS( SELECT investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, investigator_email, similar_trials_experience, Row_number() OVER ( partition BY ctfo_site_id, scenario_id ORDER BY similar_trials_experience DESC,investigator_full_nm) rank, 'yes' AS email_flag FROM $$schema$$.f_rpt_scenario_highest_investigator WHERE scenario_id = %(scenario_id)s AND theme_id=%(theme_id)s AND active_flag = 'Y' AND investigator_email IS NOT NULL AND investigator_email != '' GROUP BY investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, similar_trials_experience, investigator_email UNION SELECT investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, investigator_email, similar_trials_experience, row_number() OVER ( partition BY ctfo_site_id, scenario_id ORDER BY similar_trials_experience DESC,investigator_full_nm) rank, 'no' AS email_flag FROM $$schema$$.f_rpt_scenario_highest_investigator WHERE scenario_id = %(scenario_id)s AND theme_id=%(theme_id)s AND active_flag = 'Y' AND ( investigator_email IS NULL OR investigator_email='') AND ctfo_investigator_id IS NOT NULL GROUP BY investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, similar_trials_experience, investigator_email ORDER BY ctfo_site_id, email_flag DESC, rank) SELECT investigator_full_nm, ctfo_investigator_id, ctfo_site_id, scenario_id, case when investigator_email='' then null else investigator_email end as investigator_email , similar_trials_experience, row_number() OVER ( partition BY ctfo_site_id, scenario_id ORDER BY ctfo_site_id, email_flag DESC, rank) rank FROM abc), hinv AS ( SELECT ctfo_site_id, scenario_id, json_agg(jsonb_build_object('inv_email', investigator_email, 'inv_id', ctfo_investigator_id, 'inv_name', investigator_full_nm, 'selected_flag' , CASE WHEN rank = 1 THEN 'true' ELSE 'false' END)) AS jsn_col FROM all_ct GROUP BY ctfo_site_id, scenario_id ORDER BY ctfo_site_id) UPDATE $$schema$$.f_rpt_scenario_site_ranking AS rnk SET investigator_details = hinv.jsn_col FROM hinv WHERE rnk.ctfo_site_id = hinv.ctfo_site_id AND rnk.scenario_id = hinv.scenario_id AND rnk.active_flag = 'Y'"""

        ranking_inv_update = ranking_inv_update.replace("$$schema$$", flask_schema)

        print("%s ranking_inv_update is : {}".format(
            UTILES_OBJ.get_replaced_query(ranking_inv_update,
                                          outreach_inv_update_parms)))
        ranking_inv_update_op = UTILES_OBJ.execute_many_query_without_g(
            ranking_inv_update, vlist, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)

        print("ranking_inv_update_op : %s", ranking_inv_update_op)

        outreach_inv_update_op = UTILES_OBJ.execute_many_query_without_g(
            outreach_inv_update, vlist, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        print("outreach_inv_update_op : %s", outreach_inv_update_op)

        concat_inv_name_query = """WITH hinv AS( SELECT scenario_id, ctfo_site_id, String_agg(dat.inv_id,';') AS ctfo_investigator_id, String_agg(dat.inv_name,';') AS investigator_full_nm FROM $$schema$$.f_rpt_scenario_site_ranking CROSS JOIN json_to_recordset(investigator_details::json) AS dat("inv_id" text,"inv_name" text,"inv_email" text,"selected_flag" text) WHERE scenario_id= %(scenario_id)s GROUP BY 1 , 2) UPDATE $$schema$$.f_rpt_scenario_site_ranking AS rnk SET ctfo_investigator_id = hinv.ctfo_investigator_id, investigator_full_nm=hinv.investigator_full_nm FROM hinv WHERE rnk.ctfo_site_id = hinv.ctfo_site_id AND rnk.scenario_id = hinv.scenario_id AND rnk.active_flag = 'Y' AND rnk.ctfo_site_id IN ( SELECT DISTINCT ctfo_site_id FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl WHERE scenario_id= %(scenario_id)s AND theme_id= %(theme_id)s)"""

        concat_inv_name_query = concat_inv_name_query.replace("$$schema$$", flask_schema)
        print("%s concat_inv_name_query is : {}".format(
            UTILES_OBJ.get_replaced_query(concat_inv_name_query,
                                          outreach_inv_update_parms)))
        concat_inv_name_query_update = UTILES_OBJ.execute_many_query_without_g(
            concat_inv_name_query, vlist, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)

        print("concat_inv_name_query_update : %s", concat_inv_name_query_update)
    except Exception as ex:
        print("Error  ERROR - " + str(traceback.format_exc()))
        return {"status":"Failed","error": str(ex)}


def update_inv():

    try:
        all_data_query = """select scenario_id,theme_id from $$schema$$.f_rpt_scenario_stdy_sumry where housekeeping_status in ('active','archive') and scenario_status != 'in progress' and active_flag = 'Y'  group by 1,2"""

        get_all_data_query = all_data_query.replace("$$schema$$", flask_schema)
        get_all_data_query_output = UTILES_OBJ.execute_query_without_g(
            get_all_data_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        data_replace_query = """WITH hinv AS(SELECT ctfo_site_id, scenario_id, ctfo_investigator_id, split_part(Replace(Replace(Replace(Replace(Replace(Replace(Replace( Replace( Replace ( investigator_email, '''', ''), '"', '') , ']', ''), '*' , ''), ')', ''), '}', ''), ':', ''), '?', ''), '=', ''), '|', 1) AS investigator_email FROM $$schema$$.f_rpt_scenario_highest_investigator WHERE active_flag = 'Y' AND investigator_email IS NOT NULL AND investigator_email != '' AND investigator_email LIKE '%%''%%' OR investigator_email LIKE '%%"%%' OR investigator_email LIKE '%%*%%' OR investigator_email LIKE '%%}%%' OR investigator_email LIKE '%%?%%' OR investigator_email LIKE '%%=%%' OR investigator_email LIKE '%%|%%' ORDER BY ctfo_site_id) UPDATE $$schema$$.f_rpt_scenario_highest_investigator AS rnk SET investigator_email = hinv.investigator_email FROM hinv WHERE rnk.ctfo_investigator_id = hinv.ctfo_investigator_id AND rnk.ctfo_site_id = hinv.ctfo_site_id AND rnk.scenario_id = hinv.scenario_id AND rnk.active_flag = 'Y' """

        data_replace_query = data_replace_query.replace("$$schema$$", flask_schema)
        data_query_output = UTILES_OBJ.execute_query_without_g(
            data_replace_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        update_wt_flag_data(get_all_data_query_output['result'])

        print("FINal REUSLT : %s", str(get_all_data_query_output['result']))
    except Exception as ex:
        print("Error in display_email_details. ERROR - " + str(traceback.format_exc()))
        return {"status":"Failed","error": str(ex)}


update_inv()

