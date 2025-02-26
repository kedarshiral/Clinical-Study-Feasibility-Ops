# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

import boto3
import time
from ConfigUtility import JsonConfigUtility
from MySQLConnectionManager import MySQLConnectionManager
import CommonConstants as CommonConstants
from ExecutionContext import ExecutionContext
from LogSetup import logger
from PySparkUtility import PySparkUtility

MODULE_NAME = "AthenaUtils"


class AthenaUtils(object):
    def __init__(self, execution_context=None):

        if execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

        self.configuration = JsonConfigUtility(
            CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
        self.audit_db = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
        self.hive_port = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "hive_port"])
        self.private_key_location = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "private_key_loaction"])
        self.cluster_mode = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "cluster_mode"])



    def query_results(self, dataset_id):
        ## This function executes the query and returns the query execution ID
        module_name_for_spark = "Athena Utility"
        spark_context = PySparkUtility(self.execution_context).get_spark_context(module_name_for_spark,
                                                                                 CommonConstants.HADOOP_CONF_PROPERTY_DICT)
        try:

            query = "select table_name, table_s3_path from {audit_db}." \
                    "{ctl_dataset_information} where dataset_id = {dataset_id}".format(
                audit_db=self.audit_db,
                ctl_dataset_information=CommonConstants.DATASOURCE_INFORMATION_TABLE_NAME,
                dataset_id=dataset_id)

            result_set = MySQLConnectionManager().execute_query_mysql(query, False)
            if len(result_set) != 1:
                raise Exception("Improper result ontained from mysql")
            table_s3_path = str(result_set[0]['table_s3_path'])
            table_name = str(result_set[0]['table_name'])

            client = boto3.client('athena')

            params = {
                'region': self.configuration.get_configuration(
                    [CommonConstants.ENVIRONMENT_PARAMS_KEY,
                     "s3_region"]),
                'database': table_name.split(".")[0],
                'output_location': table_s3_path,
                'query': 'SELECT * FROM ' + table_name
            }
            response_query_execution_id = client.start_query_execution(
                QueryString=params['query'],
                QueryExecutionContext={
                    'Database': params['database']
                },
                ResultConfiguration={
                    'OutputLocation': params['output_location']
                }
            )

            ## This function takes query execution id as input and returns the details of the query executed
            response_get_query_details = client.get_query_execution(
                QueryExecutionId=response_query_execution_id['QueryExecutionId']
            )

            print(response_get_query_details)

            status = 'RUNNING'
            iterations = 5

            while (iterations > 0):
                iterations = iterations - 1
                response_get_query_details = client.get_query_execution(
                    QueryExecutionId=response_query_execution_id['QueryExecutionId']
                )
                status = response_get_query_details['QueryExecution']['Status']['State']
                print(status)
                if (status == 'FAILED') or (status == 'CANCELLED'):
                    return False, False

                elif status == 'SUCCEEDED':
                    location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
                    athena_table = spark_context.read.format("csv").option("header", "true").load(location)
                    athena_table.write.mode("overwrite").saveAsTable(table_name.split(".")[1])
                    return location
                else:
                    time.sleep(5)
            return False
        except Exception as e:
            logger.info("Failed to create athena tables")
            raise e
        finally:
            spark_context.stop()
