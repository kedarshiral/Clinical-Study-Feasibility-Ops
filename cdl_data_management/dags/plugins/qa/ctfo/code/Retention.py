import os
import sys
import json
import logging
from utilities_postgres.utils import Utils
from ConfigUtility import JsonConfigUtility
import CommonConstants

UTILS_OBJ = Utils()

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
print('flask schema %s', flask_schema)
backend_schema = configuration.get_configuration(["schema_reporting"])
print('backend schema %s', backend_schema)
foreign_server = configuration.get_configuration(["foreign_server"])
print('foreign server %s', foreign_server)





def get_scenario_data(batch_id_maintain,bridging_file_site):

        rep_id_run_query = """SELECT * from dblink('$$foreign_server$$',$redshift$ with abc as (select process_run,reporting from $$backend_schema$$.$$batch_id_maintain$$ )  SELECT * FROM abc $redshift$) AS t1 (process_run text, reporting bigint) """

        rep_id_run_query = rep_id_run_query.replace("$$schema$$", flask_schema).replace("$$backend_schema$$", backend_schema)\
            .replace("$$batch_id_maintain$$",batch_id_maintain).replace('$$foreign_server$$',foreign_server)
        print(logger.debug('rep_id_run_query %s', rep_id_run_query))
        print(logger.debug("host %s", UTILES_OBJ.host))
        print(logger.debug("port %s", UTILES_OBJ.port))
        print(logger.debug("usernames %s", UTILES_OBJ.username))
        print(logger.debug("password %s", UTILES_OBJ.password))
        print(logger.debug("database_name %s", UTILES_OBJ.database_name))

        rep_id_run_query_output = UTILES_OBJ.execute_query_without_g(
            rep_id_run_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        rep_id_run_query_output = rep_id_run_query_output['result']
        print('rep_id_run_query_output %s', rep_id_run_query_output)

        for i in rep_id_run_query_output:
            run = i['process_run']
            rep = i['reporting']
            print("run =  %s,rep = %s", run, rep)
            update_site_id_query = """ WITH cte AS
                    (
                           SELECT *
                           FROM   dblink('$$foreign_server$$',$redshift$
                           select $$process_run$$,'ctfo_site_'||max(split_part(run_latest,'_',3)::bigint) as run_latest from (SELECT $$process_run$$, run_latest,key,
                            rank() OVER(
                            PARTITION BY $$process_run$$
                            order by
                              CASE WHEN key like 'ctms%' and run_latest is not null THEN '1'
                                            WHEN key like 'ir%' and run_latest is not null THEN '2'
                                            WHEN key like 'citeline%' and run_latest is not null THEN '3'
                                            WHEN key like 'aact%' and run_latest is not null THEN '4'
                                            WHEN run_latest is not null THEN '5'
                                            ELSE '6'  END ASC)abc FROM $$backend_schema$$.$$bridging_file_site$$   GROUP BY 1, 2,3) where abc =1
                                            group by 1 $redshift$) AS t1 (process_run text, run_latest text))
                    UPDATE $$schema$$.f_rpt_scenario_site_ranking t1
                    SET    latest_ctfo_site_id = run_latest
                    FROM   cte AS t2
                    WHERE  t1.ctfo_site_id = t2.process_run
                    AND    pt_cycle_id = '$$reporting$$'
                    AND    active_flag = 'Y'
                    and t2.run_latest is not null"""

            update_site_id_query = update_site_id_query.replace("$$schema$$", flask_schema).\
                replace("$$process_run$$",  run).replace("$$backend_schema$$",backend_schema).replace(
                "$$reporting$$", rep).replace("$$bridging_file_site$$",bridging_file_site).replace('$$foreign_server$$',foreign_server)
            print('update_site_id_query %s', update_site_id_query)
            update_site_id_query_output = UTILES_OBJ.execute_query_without_g(
                update_site_id_query, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
                UTILES_OBJ.password, UTILES_OBJ.database_name)

        update_outreach_table = """update $$schema$$.f_rpt_user_scenario_survey_site_investigator_dtl a
                    set latest_ctfo_site_id = b.latest_ctfo_site_id
                    from $$schema$$.f_rpt_scenario_site_ranking b
                    where a.ctfo_site_id= b.ctfo_site_id
                    and b.active_flag = 'Y'
                    and a.scenario_id = b.scenario_id
                    """

        update_outreach_table = update_outreach_table.replace("$$schema$$", flask_schema).replace("$$backend_schema$$",
                                                                                        backend_schema)
        print('update_outreach_table %s', update_outreach_table)
        update_outreach_table_output = UTILES_OBJ.execute_query_without_g(
            update_outreach_table, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        update_outreach_table_output = update_outreach_table_output['result']
        print('update_outreach_table_output %s', update_outreach_table_output)

def update_past_site_table_view_add_site():

        delete_insert_table = """DROP TABLE $$schema$$.past_bridging_file_site_table;
            CREATE TABLE $$schema$$.past_bridging_file_site_table AS
            SELECT * FROM dblink( '$$foreign_server$$', $redshift$
            select run_1,'ctfo_site_'||max(split_part(run_latest,'_',3)::bigint) as run_latest from (SELECT run_1, run_latest,key,
            rank() OVER(
            PARTITION BY run_1
            order by
              CASE WHEN key like 'ctms%' and run_latest is not null THEN '1'
                                            WHEN key like 'ir%' and run_latest is not null THEN '2'
                                            WHEN key like 'citeline%' and run_latest is not null THEN '3'
                                            WHEN key like 'aact%' and run_latest is not null THEN '4'
                                            WHEN run_latest is not null THEN '5'
                            ELSE '6'  END ASC)abc FROM $$schema_reporting$$.past_bridging_file_site   GROUP BY 1, 2,3) where abc =1
                            group by 1 $redshift$ ) as t1(run_1 text,run_latest text);
            CREATE UNIQUE INDEX past_bridging_file_site_index
            ON $$schema$$.past_bridging_file_site_table (run_1, run_latest);

            """

        delete_insert_table = delete_insert_table.replace("$$schema$$", flask_schema).replace("$$schema_reporting$$",
                                                                                                  backend_schema).replace('$$foreign_server$$',foreign_server)
        print('delete_innsert_table %s', delete_insert_table)
        delete_insert_table_output = UTILES_OBJ.execute_query_without_g(
            delete_insert_table, UTILES_OBJ.host, UTILES_OBJ.port, UTILES_OBJ.username,
            UTILES_OBJ.password, UTILES_OBJ.database_name)
        delete_insert_table_output = delete_insert_table_output['result']
        print('delete_insert_table_output %s', delete_insert_table_output)

get_scenario_data('batch_id_maintain_past','past_bridging_file_site')
get_scenario_data('batch_id_maintain_future','future_bridging_file_site')
update_past_site_table_view_add_site()
