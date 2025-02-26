import os
import traceback
import sys
import json
import time
import logging
from utilities_postgres.utils import Utils
from ConfigUtility import JsonConfigUtility
import CommonConstants



from utilities_postgres import CommonServicesConstants

logger = logging.getLogger('__name__')

UTILES_OBJ = Utils()
app_config_dict = UTILES_OBJ.get_application_config_json()
secret_name = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["db_connection_details"].get("secret_name", None)
region_name = app_config_dict[CommonServicesConstants.CTFO_SERVICES]["db_connection_details"].get("db_region_name",
                                                                                                  None)
get_secret_dict = UTILES_OBJ.get_secret(secret_name, region_name)
os.environ['DB_PASSWORD'] = get_secret_dict["password"]
UTILES_OBJ.initialize_variable(app_config_dict)


configuration = JsonConfigUtility(
        os.path.join(CommonConstants.AIRFLOW_CODE_PATH, "utilities_postgres/environment_params.json"))
flask_schema = configuration.get_configuration(["schema"])
logger.info('flask schema %s', flask_schema)
backend_schema = configuration.get_configuration(["schema_reporting"])
logger.info('backend schema %s', backend_schema)
foreign_server = configuration.get_configuration(["foreign_server"])
logger.info('foreign server %s', foreign_server)




failed_list = []

def fetch_details (data_json):
    try:
        email_json_data = {}
        result_json = {}
        investigator_details = []
        get_study_code = """Select study_code from $$schema$$.f_rpt_scenario_stdy_sumry where theme_id = %(theme_id)s and scenario_id = %(scenario_id)s"""
        get_study_code = get_study_code.replace('$$schema$$', flask_schema)
        get_study_code_params = {"scenario_id": data_json["scenario_id"], "theme_id": data_json["theme_id"]}

        print("%s get_study_code is : {}".format(
            UTILES_OBJ.get_replaced_query(get_study_code,
                                         get_study_code_params)))

        get_study_code_response = UTILES_OBJ.execute_query_without_g(get_study_code,
                                                                UTILES_OBJ.host, UTILES_OBJ.port,
                                                                UTILES_OBJ.username, UTILES_OBJ.password,
                                                                UTILES_OBJ.database_name,
                                                                get_study_code_params)

        print("get_study_code_response Output --> %s", str(get_study_code_response))
        if get_study_code_response["result"]:
            study_code = get_study_code_response["result"][0]["study_code"]
            data_json["study_code"] = study_code
        else:
            raise Exception("Unable to fetch study code")
        get_site_investigator_list_query = """SELECT site_country AS country_name ,ctfo_site_id ,site_nm, investigator_id ,investigator_first_name AS first_name ,investigator_last_name AS last_name ,SPLIT_PART(investigator_email, '|', 1) AS email ,SPLIT_PART(investigator_phone, '|', 1) AS phone ,CONCAT( ctfo_site_id ,investigator_id) AS extref ,ctfo_inv_email_secondary AS secondary_email ,is_ecda ,is_manual FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl WHERE scenario_id = %(scenario_id)s AND survey_id = %(survey_id)s AND theme_id = %(theme_id)s AND investigator_id = %(ctfo_inv_id)s AND ctfo_site_id = %(ctfo_site_id)s  LIMIT 1 """
        get_site_investigator_list_query = get_site_investigator_list_query.replace('$$schema$$', flask_schema)
        get_site_investigator_list_query_params = {"scenario_id": data_json["scenario_id"], "theme_id": data_json["theme_id"],
                                                   "survey_id": data_json["survey_id"], "ctfo_site_id": data_json["ctfo_site_id"],
                                                   "ctfo_inv_id": data_json["ctfo_inv_id"]}
        print("%s get_site_investigator_list_query is : {}".format(
            UTILES_OBJ.get_replaced_query(get_site_investigator_list_query,
                                         get_site_investigator_list_query_params)))

        investigator_details_response = UTILES_OBJ.execute_query_without_g(get_site_investigator_list_query,
                                                                UTILES_OBJ.host, UTILES_OBJ.port,
                                                                UTILES_OBJ.username, UTILES_OBJ.password,
                                                                UTILES_OBJ.database_name,
                                                                get_site_investigator_list_query_params)

        print("Investigator Information Query Output --> %s", str(investigator_details_response))

        if investigator_details_response["status"].lower() != "success" or not \
                investigator_details_response["result"]:
            failed_list.append({"scenario_id": data_json["scenario_id"], "theme_id": data_json["theme_id"],
                                                   "survey_id": data_json["survey_id"], "ctfo_site_id": data_json["ctfo_site_id"],
                                                   "ctfo_inv_id": data_json["ctfo_inv_id"],"reason":"Failed to fetch investigator_details_response"})


        investigator_details.append(investigator_details_response["result"][0])
        for details in investigator_details:
            ctfo_site_id = details["ctfo_site_id"]
            ctfo_inv_id = details["investigator_id"]
            get_inv_email_details_query ="""SELECT smry.scenario_id, smry.study_code, regexp_replace( smry.description, Chr(39), '' ) as study_name, smry.therapeutic_area as ta_name, site.site_country AS country_name, site.ctfo_site_id, site.investigator_id, site.recipient_personalized_link, site.investigator_last_name AS investigator_name, site.investigator_email, COALESCE(site.site_alias_primary, site.site_nm) as site_nm, con.country_head_email AS cc_email_id, con.country_head_email_secondary AS country_secondary_email, site.ctfo_inv_email_secondary as secondary_email, con.country_head_name AS country_head_name, site.salutation, smry.manual_mail_subject FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl site INNER JOIN $$schema$$.f_rpt_user_scenario_survey_country_dtl con ON lower(site.site_country) = lower(con.country_name) AND site.scenario_id = con.scenario_id INNER JOIN $$schema$$.f_rpt_scenario_stdy_sumry smry on site.scenario_id = smry.scenario_id where smry.study_code = %(study_code)s and smry.active_flag = 'Y' and site.scenario_id = %(scenario_id)s AND site.survey_id = %(survey_id)s AND site.theme_id = %(theme_id)s AND site.investigator_id = %(ctfo_inv_id)s AND site.ctfo_site_id = %(ctfo_site_id)s  limit 1"""
            get_inv_email_details_query = get_inv_email_details_query.replace('$$schema$$', flask_schema)


            get_inv_email_details_query_params = {"scenario_id": data_json["scenario_id"], "theme_id": data_json["theme_id"],
             "survey_id": data_json["survey_id"], "ctfo_site_id": data_json["ctfo_site_id"],
             "ctfo_inv_id": data_json["ctfo_inv_id"],"study_code":data_json["study_code"]}

            print("%s get_inv_email_details_query is : {}".format(
                UTILES_OBJ.get_replaced_query(get_inv_email_details_query,
                                             get_inv_email_details_query_params)))

            get_inv_email_details_query_output = UTILES_OBJ.execute_query_without_g(get_inv_email_details_query,
                                                                         UTILES_OBJ.host,
                                                                         UTILES_OBJ.port,
                                                                         UTILES_OBJ.username,
                                                                         UTILES_OBJ.password,
                                                                         UTILES_OBJ.database_name,
                                                                         get_inv_email_details_query_params)

            print("Fetch Email Detail Query Output --> %s",
                         str(get_inv_email_details_query_output))

            if get_inv_email_details_query_output["status"].lower() != "success" or \
                    not len(get_inv_email_details_query_output["result"]) or \
                    get_inv_email_details_query_output["result"][0]["cc_email_id"] is None or \
                    get_inv_email_details_query_output["result"][0]["country_head_name"] is None:
                print(
                    "Country head email not present in outreach table checking in country table")

                get_inv_email_details_query = """WITH fs1 AS ( SELECT * FROM dblink( '$$foreign_server$$', $REDSHIFT$ Select country_head_email, country_head_name, standard_country, study_code FROM $$schema_reporting$$.f_rpt_study_country_details WHERE study_code = %(study_code)s $REDSHIFT$ ) AS t( country_head_email text, country_head_name text, standard_country text, study_code text ) ) SELECT smry.scenario_id as scenario_id, smry.study_code, regexp_replace( smry.description, Chr(39), '' ) as study_name, smry.therapeutic_area as ta_name, site.site_country AS country_name, site.ctfo_site_id, site.investigator_id, site.recipient_personalized_link, site.investigator_last_name AS investigator_name, site.investigator_email, COALESCE( site.site_alias_primary, site.site_nm ) as site_nm, site.ctfo_inv_email_secondary as secondary_email, con.country_head_email AS cc_email_id, con.country_head_name AS country_head_name, smry.manual_mail_subject, site.salutation FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl site LEFT JOIN fs1 con ON lower(site.site_country) = lower(con.standard_country) LEFT JOIN $$schema$$.f_rpt_scenario_stdy_sumry smry on site.scenario_id = smry.scenario_id and con.study_code = smry.study_code where smry.study_code = %(study_code)s and smry.active_flag = 'Y' and site.scenario_id = %(scenario_id)s AND site.survey_id = %(survey_id)s AND site.theme_id = %(theme_id)s AND site.investigator_id = %(ctfo_inv_id)s AND site.ctfo_site_id = %(ctfo_site_id)s  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16"""
                get_inv_email_details_query = get_inv_email_details_query.replace('$$schema$$', flask_schema).replace(
                    '$$foreign_server$$', foreign_server).replace('$$schema_reporting$$', backend_schema)
                get_inv_email_details_query_params =  {"scenario_id": data_json["scenario_id"], "theme_id": data_json["theme_id"],
                 "survey_id": data_json["survey_id"], "ctfo_site_id": data_json["ctfo_site_id"],
                 "ctfo_inv_id": data_json["ctfo_inv_id"], "study_code": data_json["study_code"]}

                print("%s get_inv_email_details_query is : {}".format(
                    UTILES_OBJ.get_replaced_query(get_inv_email_details_query,
                                                 get_inv_email_details_query_params)))

                get_inv_email_details_query_output = UTILES_OBJ.execute_query_without_g(
                    get_inv_email_details_query,
                    UTILES_OBJ.host,
                    UTILES_OBJ.port,
                    UTILES_OBJ.username,
                    UTILES_OBJ.password,
                    UTILES_OBJ.database_name, get_inv_email_details_query_params)
                print("Fetch Email Detail Query Output --> %s",
                             str(get_inv_email_details_query_output))
                if get_inv_email_details_query_output["status"].lower() != "success":
                    raise Exception('Failed to fetch Email Details')

                if not get_inv_email_details_query_output["result"]:
                    get_site_inv_details_if_no_country_detail_query  = """SELECT smry.scenario_id as scenario_id, smry.study_code, regexp_replace( smry.description, Chr(39), '' ) as study_name, smry.therapeutic_area as ta_name, site.site_country AS country_name, site.ctfo_site_id, site.investigator_id, site.recipient_personalized_link, site.investigator_last_name AS investigator_name, site.investigator_email, COALESCE(site.site_alias_primary, site.site_nm) as site_nm, site.ctfo_inv_email_secondary as secondary_email, '' AS cc_email_id, '' AS country_head_name, smry.manual_mail_subject, site.salutation FROM $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl site INNER JOIN $$schema$$.f_rpt_scenario_stdy_sumry smry on site.scenario_id = smry.scenario_id where smry.study_code = %(study_code)s and smry.active_flag = 'Y' and site.scenario_id = %(scenario_id)s AND site.survey_id = %(survey_id)s AND site.theme_id = %(theme_id)s AND site.investigator_id = %(ctfo_inv_id)s AND site.ctfo_site_id = %(ctfo_site_id)s """
                    get_site_inv_details_if_no_country_detail_query = get_site_inv_details_if_no_country_detail_query.replace(
                        '$$schema$$', flask_schema)

                    get_site_inv_details_if_no_country_detail_query_params =  {"scenario_id": data_json["scenario_id"], "theme_id": data_json["theme_id"],
                 "survey_id": data_json["survey_id"], "ctfo_site_id": data_json["ctfo_site_id"],
                 "ctfo_inv_id": data_json["ctfo_inv_id"], "study_code": data_json["study_code"]}
                    print("%s get_site_inv_details_if_no_country_detail_query is : {}".format(
                        UTILES_OBJ.get_replaced_query(get_site_inv_details_if_no_country_detail_query,
                                                     get_site_inv_details_if_no_country_detail_query_params)))

                    get_site_inv_details_if_no_country_detail_query_output = UTILES_OBJ.execute_query_without_g(
                        get_site_inv_details_if_no_country_detail_query,
                        UTILES_OBJ.host,
                        UTILES_OBJ.port,
                        UTILES_OBJ.username,
                        UTILES_OBJ.password,
                        UTILES_OBJ.database_name, get_site_inv_details_if_no_country_detail_query_params)

                    print(
                        "Fetch Email Detail (No Country Head Details Found) Query Output --> %s",
                        str(get_site_inv_details_if_no_country_detail_query_output))
                    if get_site_inv_details_if_no_country_detail_query_output[
                        "status"].lower() != "success":
                        raise Exception('Failed to fetch Email Details')

                    get_inv_email_details_query_output["result"] = \
                        get_site_inv_details_if_no_country_detail_query_output["result"]

            for each_contact in get_inv_email_details_query_output["result"]:
                email_subject = data_json["subject"]
                client_name = app_config_dict["ctfo_services"]["client_name"].capitalize()

                if each_contact["manual_mail_subject"] == "" or each_contact[
                    "manual_mail_subject"] is None:
                    manual_mail_subject = each_contact["ta_name"].title()
                else:
                    manual_mail_subject = each_contact["manual_mail_subject"].title()
                print("data_json --> %s",{str(data_json)})
                email_str = data_json["email_body"]
                new_str = data_json["email_body"]
                email_body_str = new_str.split("Please click on this link to the questionnaire")[0].split("\n")
                email_body = email_body_str[1] + email_body_str[2]

                email_body = email_body. \
                    replace("$$study_code$$", each_contact["study_code"]). \
                    replace("$$ta_name$$", manual_mail_subject).replace("$$client_name$$", client_name). \
                    replace("$$study_tittle$$", each_contact["study_name"]). \
                    replace("$$site_name$$", each_contact["site_nm"].replace('"', "'"))
                email_subject = email_subject. \
                    replace("$$study_code$$", each_contact["study_code"]). \
                    replace("$$ta_name$$", manual_mail_subject).replace("$$client_name$$", client_name)

                email_json_data["subject"] = email_subject
                email_json_data["email_body"] = email_body
                email_json_data["salutation"] = email_str.split(",")[0]
                final_str = email_str.split("your efforts")[1].lstrip(",").split("You are informed")[0]
                email_json_data["signature_name"] = final_str.split("\n")[0].lstrip(" ")
                email_json_data["signature_designation"] = final_str.split("\n")[1]
        result_json["status"] = 'success'
        result_json["email_template_type"] = 'default'
        result_json["email_json"] = json.dumps(email_json_data)
        return result_json
    except Exception as ex:
        print("Error in display_email_details. ERROR - " + str(traceback.format_exc()))
        return {"status":"Failed","error": str(ex)}

def data_list():
        try:
            logging.info("check")
            print("olol")
            data_list_query = """select * from $$schema$$.log_scenario_survey_email_dtl where  investigator_id != ''
            and email_type = 'survey_mail' and email_json is null and  ctfo_site_id != ' '"""
            data_list_query = data_list_query.replace('$$schema$$', flask_schema)
            data_list_query_output = UTILES_OBJ.execute_query_without_g(
                data_list_query,
                UTILES_OBJ.host,
                UTILES_OBJ.port,
                UTILES_OBJ.username,
                UTILES_OBJ.password,
                UTILES_OBJ.database_name, {})
            #print("data_list_query_output --> %s",
            #      str(data_list_query_output))
            data_list = data_list_query_output["result"]
            for items in data_list:
                items["ctfo_inv_id"] = items["investigator_id"]
                update_json = fetch_details(items)
                if update_json["status"] == 'success':
                    update_json["scenario_id"] = items["scenario_id"]
                    update_json["ctfo_site_id"] = items["ctfo_site_id"]
                    update_json["theme_id"] = items["theme_id"]
                    update_json["country_name"] = items["country_name"]
                    update_json["investigator_email"] = items["recipient_list"]
                    update_json["investigator_id"] = items["investigator_id"]
                    update_json_condition = "(%(email_template_type)s,%(email_json)s,%(scenario_id)s,%(ctfo_site_id)s,%(theme_id)s,%(country_name)s,%(investigator_email)s,%(investigator_id)s)"

                    update_query = """UPDATE $$schema$$.log_scenario_survey_email_dtl as t SET email_template_type = e.email_template_type, email_json = e.email_json::jsonb from(values $$value_list$$)as e(email_template_type,email_json,scenario_id,ctfo_site_id,theme_id,country_name,investigator_email,investigator_id) where  e.scenario_id = t.scenario_id and e.ctfo_site_id = t.ctfo_site_id and e.theme_id = t.theme_id and e.country_name = t.country_name and e.investigator_email = t.recipient_list and e.investigator_id = t.investigator_id and t.email_type = 'survey_mail'"""
                    update_query = update_query.replace('$$schema$$', flask_schema)
                    update_query = update_query.replace(
                        "$$value_list$$",update_json_condition)
                    print("%s update_site_email_data_query is : {}".format(
                        UTILES_OBJ.get_replaced_query(update_query,
                                                     update_json)))
                    update_site_email_data_query_output = UTILES_OBJ.execute_query_without_g(
                        update_query, UTILES_OBJ.host,
                        UTILES_OBJ.port, UTILES_OBJ.username,
                        UTILES_OBJ.password,
                        UTILES_OBJ.database_name, update_json)
                    print("%s  Ran update query successfully")
                else:
                    print("Something went wrong in fetch details function")

        except Exception as ex:
            print("Error in data_list. ERROR - " + str(traceback.format_exc()))




start = time.time()
data_list()
print("Failed list")
print(failed_list)
end = time.time()
print("The time of execution of above program is :",
      (end-start) * 10**3, "ms")
seconds, milliseconds = divmod((end-start) * 10**3, 1000)
minutes, seconds = divmod(seconds, 60)
print("The time of execution of above program is :",
      minutes, "mimutes")
print("The time of execution of above program is :",
      seconds, "seconds")


