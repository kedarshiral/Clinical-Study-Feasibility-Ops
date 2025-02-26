# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import datetime
import json
from pyspark.sql.session import SparkSession
from pyspark import SparkContext
import pymysql
import base64
import logging
import os
import argparse
from logging.handlers import RotatingFileHandler
from datetime import timezone
import numpy as np
import boto3
import getpass
from ConfigUtility import JsonConfigUtility
from multiprocessing.pool import ThreadPool
import subprocess
import CommonConstants
# --------------------------------------Constants-----------------------------------------------------------------

CONFIGURATION_FILE = "" #Path of the rdbms_application_conf.json file
with open(CONFIGURATION_FILE, "r") as file:
    app_config = file.read()

configuration = JsonConfigUtility(
os.path.join(CommonConstants.AIRFLOW_CODE_PATH,CommonConstants.ENVIRONMENT_CONFIG_FILE))
rdbms_connection_details=configuration.get_configuration(["rdbms_configs"])


for key in rdbms_connection_details:
    app_config = app_config.replace(key + "$$", rdbms_connection_details[key])
app_config = json.loads(app_config)


_log_file_name__ = "Data extractor"
__log_path__ = "logs/"
__log_level__ = "DEBUG"
__handler_type__ = "rotating_file_handler"
__max_bytes__ = 50000000
__backup_count__ = 2
STATUS_FAILED = 'FAILED'
STATUS_SUCCEEDED = 'SUCCEEDED'
complete_log_path = os.path.join(__log_path__, _log_file_name__)
if not os.path.isdir(__log_path__):
    os.makedirs(__log_path__)

spark = SparkSession \
        .builder \
        .enableHiveSupport()\
        .appName("database extraction") \
        .getOrCreate()


# --------------------------------------------------------------------------------------------------------------------------------

def file_reader(directory_path, file_name):
    """
    Purpose:   To read the query from local EMR and make a list
    Input: Path of directory contains the query
    Output: Query list
    """
    if "s3" in file_name:
        read_s3_path= file_name.split("/")
        read_file_name=read_s3_path[-1]
        os.system("aws s3 cp " + file_name + " " +directory_path)
        file_name=read_file_name
    with open(directory_path + file_name, 'r') as stream:
        query_data = stream.read()
    return query_data


def decode_password(password):
    region_name = app_config['generic_config']['aws_region']
    try:
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        logger.info("Fetching the details for the secret name %s", password)
        get_secret_value_response = client.get_secret_value(
            SecretId=password
        )
        logger.info(
            "Fetched the Encrypted Secret from Secrets Manager for %s", password)
    except Exception as error:
        raise Exception("Unable to fetch secrets.")
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            logger.info("Decrypted the Secret")
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            logger.info("Decrypted the Secret")
        get_secret = json.loads(secret)
        return get_secret['password']


def get_logger(log_file_name=complete_log_path, log_level=__log_level__, time_format="%Y-%m-%d %H:%M:%S",
               handler_type=__handler_type__, max_bytes=__max_bytes__, backup_count=__backup_count__):
    """
    Purpose: To create the logger file
    Input: logging parameters
    Output: logger
    """
    log_file = os.path.join(log_file_name + '.log')
    __logger__ = logging.getLogger(log_file_name)
    __logger__.setLevel(log_level.strip().upper())
    debug_formatter = '%(asctime)s - %(levelname)-6s - [%(threadName)5s:%(filename)5s:%(funcName)5s():''%(lineno)s] - %(message)s'
    formatter_string = '%(asctime)s - %(levelname)-6s- - %(message)s'

    if log_level.strip().upper() == log_level:
        formatter_string = debug_formatter

    formatter = logging.Formatter(formatter_string, time_format)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    if __logger__.hasHandlers():
        __logger__.handlers.clear()

    if str(handler_type).lower() == "rotating_file_handler":
        handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        handler.setFormatter(formatter)
        if __logger__.hasHandlers():
            __logger__.handlers.clear()
        __logger__.addHandler(handler)

    else:
        hdlr_service = logging.FileHandler(log_file)
        hdlr_service.setFormatter(formatter)
        if __logger__.hasHandlers():
            __logger__.handlers.clear()
        __logger__.addHandler(hdlr_service)
    return __logger__


logger = get_logger()

cycle_id = str(datetime.datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f'))
def read_from_database(each_data):
    """
    Purpose: Main function for connecting to databases and saving the file to s3
    Input: Connection list
    """
    get_prev_max_time = ""
    get_current_max_time = ""
    overide_flag = "N"
    try:
        try:
            src_system = each_data['src_system'].lower()
            db_obj=Database_extractor()

            rdbms_host, rdbms_port, rdbms_username, rdbms_password, rdbms_database, rdbms_service_name, date_format, timestamp_format=db_obj.rdbms_connection_details (src_system)
            if each_data['active_flag'].lower() == "y" and src_system in app_config['datasource_details']:
                start_time = str(datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
                database_list = str(each_data['database_name']).split(",")
                table_list = str(each_data['object_name']).split(",")
                target_directory = str(datetime.datetime.now(timezone.utc).strftime(
                    str(app_config["datasource_details"][src_system]['misc_configs']['file_format'])))
                target_location = each_data['target_location'] + str(each_data['object_name']) + "_" + target_directory
                changeDateproperty = app_config["datasource_details"][src_system]['misc_configs'][
                    'changeDatetoTimestamp']
                if each_data['load_type'].lower() == "incremental" or each_data['load_type'].lower() == "delta":
                    # check the proper conf for incremental load
                    if each_data["query_file_name"] is None or each_data["query_file_name"] == "" or len(
                            database_list) > 1 or len(table_list) > 1:
                        raise Exception("please provide proper information for incremental load")
                    # Might need to put parameterized database_name and log extractor detail table name
                    query_to_fetch_latest_load_time = "select max(load_start_time) as ls from log_db_extractor_dtl where status ='SUCCEEDED'"
                    get_load_time = utility_obj.mysql_utilty(query_to_fetch_latest_load_time,
                                                             dict_flag="Y", insert_update_key="N")
                    load_time = get_load_time['ls']
                    query = file_reader(app_config['datasource_details'][src_system]['misc_configs']['path'],
                                        each_data['query_file_name'])
                    multiple_increment_column = each_data["incremental_load_column_name"].split(",")
                    current_time_max = dict()
                    flag_to_get_currn_date = ""
                    for each_increment_col in multiple_increment_column:
                        temp_dict_curr = dict()
                        table_name = each_data['object_name']
                        query_to_fetch_latest_time_from_rdbms = "select max({incremental_load_column}) as d from {db_name}.{table_name}".format(
                            incremental_load_column=each_increment_col, db_name=each_data['database_name'],
                            table_name=table_name)
                        df_last_refrsh_time = Utility().spark_oracle(
                            query=query_to_fetch_latest_time_from_rdbms,
                            host=rdbms_host,
                            port=rdbms_port,
                            service_name=rdbms_service_name,
                            username=rdbms_username,
                            password=decode_password(rdbms_password),
                            database_name=rdbms_database, changeDateproperty=changeDateproperty)
                        data = df_last_refrsh_time.collect()[0]['D']
                        temp_dict_curr[each_increment_col] = str(data)
                        current_time_max.update(temp_dict_curr)
                        if each_data['override_last_refresh_date'] == "" or each_data[
                            'override_last_refresh_date'] is None:
                            # Might need to put parameterized database_name and log extractor detail table name
                            query_to_fetch_latest_time_from_log = "select max(curr_max_refresh_timestamp) as prevtime  from log_db_extractor_dtl  where cycle_id = (select max(cycle_id) from log_db_extractor_dtl where object_name='{table_name}' and status in ('SUCCEEDED','SKIPPED') and src_system='{src_system}') and object_name='{table_name}'".format(
                                table_name=each_data['object_name'],src_system=each_data['src_system'])
                            get_prev_max_time = utility_obj.mysql_utilty(query_to_fetch_latest_time_from_log,
                                                                         dict_flag="Y", insert_update_key="N")
                            get_prev_max_time = json.loads(get_prev_max_time['prevtime'])
                            query = query.replace("$$load_date", str(load_time))
                            if get_prev_max_time[each_increment_col] == 'None':
                                query = query.replace("'$$" + each_increment_col + "'", "null").replace(
                                    "$$database_name", str(each_data['database_name']))
                            else:
                                get_timestamp_to_date = str(get_prev_max_time[each_increment_col])
                                date_to_fetch_increment = get_timestamp_to_date.split(" ")
                                query = query.replace("$$" + each_increment_col, date_to_fetch_increment[0]).replace(
                                    "$$database_name", str(
                                        each_data['database_name']))
                        else:

                            get_current_max_time = json.loads(each_data['override_last_refresh_date'])
                            query = query.replace("$$load_date", str(load_time))
                            if get_current_max_time[each_increment_col] == 'None':
                                query = query.replace("'$$" + each_increment_col + "'", "null").replace(
                                    "$$database_name", str(each_data['database_name']))
                            else:
                                query = query.replace("$$" + each_increment_col,
                                                      str(get_current_max_time[each_increment_col])).replace(
                                    "$$database_name", str(each_data['database_name']))
                            get_prev_max_time = dict()
                            overide_flag = "Y"
                    get_current_max_time = current_time_max
                    query_to_update = "update {table_name} set override_last_refresh_date = Null where object_name='{object_name}';".format(
                        table_name="ctl_db_extractor_dtl", object_name=each_data['object_name'])
                    utility_obj.mysql_utilty(query_to_update, insert_update_key="y", dict_flag="N")

                # condition for full load
                else:

                    if len(database_list) > 1 or len(table_list) > 1 and each_data["query_file_name"] is None:
                        raise Exception("please provide proper information for full load")
                    if each_data["query_file_name"] is None or each_data["query_file_name"] == '':
                        query = "select * from {database_name}.{table_name}".format(
                            database_name=each_data['database_name'], table_name=each_data['object_name'])
                    else:
                        query = file_reader(app_config['datasource_details'][src_system]['misc_configs']['path'],
                                            each_data['query_file_name'])

                # execute the query through spark jdbc
                print("inside the main call")
                df = Utility().spark_oracle(query=query,
                                            host=rdbms_host,
                                            port=rdbms_port,
                                            service_name=rdbms_service_name,
                                            username=rdbms_username,
                                            password=decode_password(rdbms_password),
                                            database_name=rdbms_database, changeDateproperty=changeDateproperty)
                query_lower=query.lower()
                query_to_get_count = query_lower.replace("select *", "select cast(count(*) as INT) as c ")
                df_count = Utility().spark_oracle(query=query_to_get_count,
                                            host=rdbms_host,
                                            port=rdbms_port,
                                            service_name=rdbms_service_name,
                                            username=rdbms_username,
                                            password=decode_password(rdbms_password),
                                            database_name=rdbms_database, changeDateproperty=changeDateproperty)
                record_count = df_count.collect()[0]['C']
                load_method=CommonConstants.SQOOP_ENABLE_FLAG.lower()
                hdfs_path = "/data/"+ each_data['object_name'] + "/"+str(cycle_id) + "/"
                if load_method == "y":
                    Utility().sqoop_utility(query=query,
                                            host=rdbms_host,
                                            port=rdbms_port,
                                            service_name=rdbms_service_name,
                                            username=rdbms_username,
                                            password=decode_password(rdbms_password),
                                            database_name=rdbms_database, df=df,table_name=each_data['object_name'],hdfs_path=hdfs_path)
                db_obj.save_to_s3(cycle_id=cycle_id, dataframe=df, file_name=each_data['query_file_name'],
                                query_file_name=str(each_data['query_file_name']).split(".")[0],
                                status=STATUS_SUCCEEDED, step_name="making connection with s3",
                                start_time=start_time, target_location=target_location,
                                src_system=each_data['src_system'],
                                query_description=query,
                                object_name=each_data['object_name'], load_type=each_data['load_type'],
                                incremental_load_column_name=each_data['incremental_load_column_name'],
                                curr_max_refresh_timestamp=get_current_max_time,
                                prev_max_refresh_timestamp=get_prev_max_time,
                                database_name=each_data['database_name'],
                                output_file_format=each_data['output_file_format'], overide_flag=overide_flag,
                                date_format=date_format, timestamp_format=date_format,
                                geography_id=each_data['geography_id'],record_count=record_count,load_method=load_method,hdfs_path=hdfs_path)
        except Exception as e:
            logger.exception("Error in connecting to oracle db:" + str(rdbms_host))
            logger.exception(e)
            end_time = str(datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
            Utility().rds_execute_query(cycle_id=cycle_id,
                                        query_file_name=str(each_data['query_file_name']).split(".")[0],
                                        status=STATUS_FAILED,
                                        step_name="Failed to extract the data due to {}".format(e),
                                        start_time=start_time, end_time=end_time,
                                        target_location=target_location,
                                        src_system=each_data['src_system'],
                                        query_description=query,
                                        object_name=each_data['object_name'], load_type=each_data['load_type'],
                                        incremental_load_column_name=each_data[
                                            'incremental_load_column_name'],
                                        curr_max_refresh_timestamp=get_current_max_time,
                                        prev_max_refresh_timestamp=get_prev_max_time,
                                        database_name=each_data['database_name'],
                                        output_file_format=each_data['output_file_format'],
                                        overide_flag=overide_flag, record_count="",
                                        geography_id=each_data['geography_id'])

    except Exception as e:
        logger.exception("Error in connecting to mysql")
        logger.exception(e)
        raise Exception


class Utility(object):

    def __init__(self):
        """
        Purpose: *Utility for creating the connection for oracle and teradata through spark
                 *Utility for create log entry in RDS
                 *Utility for mysql
        Input: Conneection parameters
        Output: Connection object
        """

    def mysql_utilty(self, query, dict_flag, insert_update_key):
        try:
            spark_obj = Database_extractor()
            mysql_client = pymysql.connect(host=spark_obj.mysql_host, user=spark_obj.mysql_username,
                                           password=spark_obj.mysql_password, db=spark_obj.mysql_database)
            cursor = mysql_client.cursor()
            if insert_update_key.lower() == "y":
                query_ouput = cursor.execute(query)
                mysql_client.commit()
                return query_ouput
            cursor.execute(query)
            resp = cursor.fetchall()
            desc = cursor.description
            final_list = []
            if resp is not None:
                for each_version in resp:
                    temp_dict = dict()
                    for index, item in enumerate(desc):
                        if isinstance(each_version[index], bytes):
                            temp_dict[item[0].lower()] = each_version[index].decode()
                        else:
                            temp_dict[item[0].lower()] = each_version[index]
                    final_list.append(temp_dict)
            if dict_flag.lower() == "y":
                return temp_dict
            else:
                return final_list
        except Exception as e:
            logger.error(str(e))
            raise Exception

    def rds_execute_query(self, cycle_id, src_system, status, step_name, database_name, object_name,
                          incremental_load_column_name, target_location, load_type, output_file_format,
                          query_file_name, query_description, prev_max_refresh_timestamp, curr_max_refresh_timestamp,
                          start_time, end_time, overide_flag,record_count,geography_id):
        """
        Purpose: Function for creating the log entry in RDS
        Input: Connection parameters
        Output: Connection object
        """
        try:
            # Might need to put parameterize log extractor detail table name
            table = "log_db_extractor_dtl"
            current_user = getpass.getuser()
            curr_max_refresh_timestamp = json.dumps(curr_max_refresh_timestamp)
            prev_max_refresh_timestamp = json.dumps(prev_max_refresh_timestamp)
            query = 'INSERT INTO `' + table + '` (`cycle_id`,`geography_id`, `src_system`, `status`, ' \
                                              '`comments`, `database_name`, `object_name`, `incremental_load_column_name`,`target_location`,`output_file_format`,`load_type`,`query_file_name`,`query_description`,`prev_max_refresh_timestamp`,`curr_max_refresh_timestamp`,`load_start_time`,`load_end_time`,`overide_flag`,`record_count`,`insert_by`,`update_by`,`insert_date`,`update_date`) ' \
                                              'VALUES (' + '"' + str(
                cycle_id) + '"' + ', ' + '"'+ str(geography_id) + '"' +' , ' + '"' + str(
                src_system) + '"' + ' , ' + '"' + str(status) + '"' + ' , ' + '"' + str(
                step_name) + '"' + ' , ' + '"' + str(database_name) + '"' + ' , ' + '"' + str(
                object_name) + '"' + ' , ' + '"' + str(incremental_load_column_name) + '"' + ' , ' + '"' + str(
                target_location) + '"' + ' , ' + '"' + str(
                output_file_format) + '"' + ' , ' + '"' + str(
                load_type) + '"' + ' , ' + '"' + str(
                query_file_name) + '"' + ' , ' + '"' + str(
                query_description) + '"' + ' , ' + "'" + prev_max_refresh_timestamp + "'" + ' , ' + "'" + curr_max_refresh_timestamp + "'" + ' , ' + '"' + str(
                start_time) + '"' + ' , ' + '"' + end_time + '"' + ' , ' + '"' + str(
                overide_flag) + '"' +' , ' + '"' + str(
                record_count) + '"' + ' , ' + '"' + str(
                current_user) + '"' +' , ' + '"' + str(
                current_user) + '"' + ' , ' + "NOW()" + ' , ' + "NOW()" + ');'
            self.mysql_utilty(query, insert_update_key="y", dict_flag="N")
        except Exception as exception:
            logger.error("Error in executing mqsql database query")
            logger.error(exception)
            raise Exception

    def spark_oracle(self, query, host, port, service_name,changeDateproperty, username=None, password=None, database_name=None):
        """
        Purpose: Function for creating the connection for oracle through spark
        Input: Connection parameters
        Output: Connection object
        """
        try:
            _query_ = "(" + query.replace(';', '') + ") abc"
            df_oracle = spark.read.format("jdbc").options(
                url="jdbc:oracle:thin:@" + host + ":" + port + "/" + service_name,
                user=username, dsn="dns_nm",
                password=password,
                driver="oracle.jdbc.driver.OracleDriver", dbtable=_query_).option("oracle.jdbc.mapDateToTimestamp", str(changeDateproperty)).load()
            table_dtype=df_oracle.dtypes
            dtype_dict = dict()
            for each_value in table_dtype:
                temp_dict = dict()
                temp_dict[each_value[0]] = each_value[1]
                dtype_dict.update(temp_dict)
            for column_name, data_type in dtype_dict.items():
                if data_type.lower() == "decimal(38,10)":
                    df_oracle = df_oracle.withColumn(column_name, df_oracle[column_name].cast('Decimal(38,6)'))
            return df_oracle
        except Exception as e:
            logger.error("Error in connection")
            logger.error(e)
            raise Exception

    def sqoop_utility(self, query, host, port, service_name, df,table_name,hdfs_path, username=None, password=None,
                     database_name=None):
        """
        Purpose: Function for creating the connection for oracle through spark
        Input: Connection parameters
        Output: Connection object
        """
        try:
            logger.info("inside the sqoop utility")
            os.environ['HADOOP_CLASSPATH'] = '/apps/code/ojdbc7.jar'
            os.system("export HADOOP_CLASSPATH=$HADOOP_CLASSPATH")
            url = "jdbc:oracle:thin:@" + host + ":" + port + "/" + service_name
            sqoop_command=r"""sqoop import  --libjars /apps/code/ojdbc7.jar --driver oracle.jdbc.driver.OracleDriver --connect {url} --username {username} --password Vj753uL  --query "{query} and \$CONDITIONS" --target-dir '{hdfs_path}'  -m 1 --fields-terminated-by "|" --null-string '\\N' --null-non-string '\\N'"""
            query = query.replace(";", "")
            sqoop_command_formated = sqoop_command.format(url=url, username=username, password=password, query=query,hdfs_path=hdfs_path)
            process = subprocess.Popen(sqoop_command_formated,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            logger.info("inside creation of ddl and import get successfully")
            object_schema = df.schema
            types = [f.dataType for f in df.schema.fields]
            columns = (','.join([field.simpleString() for field in object_schema])).replace(':', ' ')
            for each_data_type in types:
                lower_data_value = str(each_data_type).lower()
                split_data = str(each_data_type).lower().split(",")
                if "decimal" in lower_data_value:
                    check_value = split_data[0].split("(")
                    changed_type = "decimal(" + check_value[1] + "," + split_data[1]

                    if "0" in split_data[1]:
                        if int(check_value[1]) > 8:
                            columns = columns.replace(changed_type, "bigint")
                        else:
                            columns = columns.replace(changed_type, "int")
                    else:
                        columns = columns.replace(changed_type, "double")
            spark.sql("drop table if exists {table_name}".format(table_name=table_name))
            ddl = """
                            CREATE EXTERNAL TABLE {table_name} ({columns})
                            ROW FORMAT DELIMITED 
                              FIELDS TERMINATED BY '|' 
                            STORED AS INPUTFORMAT 
                              'org.apache.hadoop.mapred.TextInputFormat' 
                            OUTPUTFORMAT 
                              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                            LOCATION
                              '{location}'
                            """
            ddl_to_run = ddl.format(table_name=table_name, columns=columns, location=hdfs_path)
            spark.sql(ddl_to_run)
            logger.info("ddl creation done")

        except Exception as e:
            logger.error("Error in connection")
            logger.error(e)
            raise Exception(str(e))


class Database_extractor(object):
    def __init__(self):
        """
        Purpose: Main handler for saving the file in S3 by fetching the data from oracle and teradata through spark
        """
        self.src_system = src_system
        self.database_name = database_name
        self.table_name = table_name
        self.mysql_host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_host"])
        self.mysql_username = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_username"])
        self.mysql_password = decode_password(
            configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "rds_password_secret_name"]))
        self.mysql_port = str(configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_port"]))
        self.mysql_database = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "mysql_db"])
    def rdbms_connection_details(self,src_system):
        src_system=src_system.lower()
        rdbms_host = str(app_config['datasource_details'][src_system]['datasource_connection_details']["host"])
        rdbms_port = str(app_config['datasource_details'][src_system]['datasource_connection_details']["port"])
        rdbms_username = str(
            app_config['datasource_details'][src_system]['datasource_connection_details']["username"])
        rdbms_password = str(
            app_config['datasource_details'][src_system]['datasource_connection_details']["password"])
        rdbms_database = str(
            app_config['datasource_details'][src_system]['datasource_connection_details']["database"])
        rdbms_service_name = str(
            app_config['datasource_details'][src_system]['datasource_connection_details']["service_name"])
        date_format = str(app_config['datasource_details'][src_system]['misc_configs']['date_format'])
        timestamp_format = str(app_config['datasource_details'][src_system]['misc_configs']['timestamp_format'])
        return rdbms_host,rdbms_port,rdbms_username,rdbms_password,rdbms_database,rdbms_service_name,date_format,timestamp_format


    def save_to_s3(self, cycle_id, dataframe, file_name, src_system, status, step_name, database_name, object_name,
                   incremental_load_column_name, target_location, load_type, output_file_format, query_file_name,
                   query_description, prev_max_refresh_timestamp, curr_max_refresh_timestamp, start_time, overide_flag,date_format,timestamp_format,geography_id,record_count,load_method,hdfs_path):
        """
        Purpose: Function for saving the files to S3
        Input: cycle_id, start_time, data_frame, target_location, file_name
        """
        try:
            logger.info("inside save to s3 ")
            file_name_to_save="." + str(output_file_format.lower())
            step_name="file does not save to s3 due to no record"
            status = "SKIPPED"
            if record_count:
                logger.info("record count is non zero")
                step_name = "file successfully saved to s3"
                status = STATUS_SUCCEEDED
                if load_method =="y":
                    logger.info("inside sqoop save to s3")
                    df_read_hdfs = spark.sql("select * from {}".format(object_name))
                    df_read_hdfs.coalesce(1).write.mode('overwrite').format(output_file_format.lower()).option('header',
                                                                                                            'true').option(
                        'delimiter', '|').option("dateFormat", date_format).option("timestampFormat",
                                                                                   timestamp_format).save(
                        target_location + "temp/")
                    file_list = os.popen("aws s3 ls " + target_location + "temp/").read().replace("\n", "").split(" ")
                    for filename in file_list:
                        if filename.endswith(output_file_format.lower()):
                            os.system(
                                "aws s3 mv " + target_location + "temp/" + filename + " " + target_location + file_name_to_save)
                            os.system("aws s3 rm " + target_location + "temp/" + " --recursive")
                    os.system("hdfs dfs -rm -r "+hdfs_path)
                else:
                    logger.info("inside spark save to s3")
                    dataframe.coalesce(1).write.mode('overwrite').format(output_file_format.lower()).option('header',
                                                                                                               'true').option(
                        'delimiter', '|').option("dateFormat",date_format).option("timestampFormat",timestamp_format).save(target_location + "temp/")
                    file_list = os.popen("aws s3 ls " + target_location + "temp/").read().replace("\n", "").split(" ")
                    for filename in file_list:
                        if filename.endswith(output_file_format.lower()):
                            os.system(
                                "aws s3 mv " + target_location + "temp/" + filename + " " + target_location + file_name_to_save)
                            os.system("aws s3 rm " + target_location + "temp/" + " --recursive")
            end_time = str(datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
            Utility().rds_execute_query(cycle_id=cycle_id,
                                        query_file_name=query_file_name,
                                        status=status,
                                        step_name=step_name,
                                        start_time=start_time, end_time=end_time,
                                        target_location=target_location,
                                        src_system=src_system,
                                        query_description=query_description,
                                        object_name=object_name, load_type=load_type,
                                        incremental_load_column_name=incremental_load_column_name,
                                        curr_max_refresh_timestamp=curr_max_refresh_timestamp,
                                        prev_max_refresh_timestamp=prev_max_refresh_timestamp,
                                        database_name=database_name,
                                        output_file_format=output_file_format, overide_flag=overide_flag,record_count=record_count,geography_id=geography_id)
        except Exception as e:
            logger.exception("Error in saving the " + file_name + "to s3")
            logger.exception(e)
            end_time = str(datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
            Utility().rds_execute_query(cycle_id=cycle_id,
                                        query_file_name=query_file_name,
                                        status=STATUS_FAILED,
                                        step_name="error while saving the fiile in s3 " + str(e),
                                        start_time=start_time, end_time=end_time,
                                        target_location=target_location,
                                        src_system=src_system,
                                        query_description=query_description,
                                        object_name=object_name, load_type=load_type,
                                        incremental_load_column_name=incremental_load_column_name,
                                        curr_max_refresh_timestamp=curr_max_refresh_timestamp,
                                        prev_max_refresh_timestamp=prev_max_refresh_timestamp,
                                        database_name=database_name,
                                        output_file_format=output_file_format, overide_flag=overide_flag,record_count="",geography_id=geography_id)
            raise Exception



if __name__ == '__main__':
    try:
        PARSER = argparse.ArgumentParser(description="PUDB EXTRACTOR")
        PARSER.add_argument("-sc", "--src_system",
                            help="Data Source system is required to know from which data source we have to extract data",
                            required=False)
        PARSER.add_argument("-db", "--database", help="Database name is required", required=False)
        PARSER.add_argument("-tbl", "--table", help="Table name is required", required=False)
        PARSER.add_argument("-th", "--threads", help="No of threads to run", required=False)
        ARGS = vars(PARSER.parse_args())
        # Might need to put parameterize ctl extractor detail table name
        query = "select * from ctl_db_extractor_dtl"
        query_to_append = ""
        process_limit=5
        try:
            if ARGS['src_system'] is not None:
                src_system = ARGS['src_system']
                src_system_list = src_system.split(",")
                if len(src_system_list) > 1:
                    # Might need to put parameterized database_name and ctl extractor detail table name
                    query = "select * from ctl_db_extractor_dtl where src_system in ({src_system})"
                    join_src_system = "'" + "','".join(src_system_list) + "'"
                    query_to_append = query.format(src_system=join_src_system.lower())
                else:
                    query_to_append = query + " where src_system = '{}'".format(src_system.lower())
            else:
                src_system = None
            if ARGS['database'] is not None:
                database_name = ARGS['database']
                database_list = str(database_name).split(",")
                if len(database_list) > 1:
                    raise Exception("Please provide single database name")
                if "where" in query_to_append:
                    query_to_append = query_to_append + " and database_name='{}'".format(database_name)
                else:
                    query_to_append = query_to_append + " where database_name='{}'".format(database_name)
            else:
                database_name = None
            if ARGS['table'] is not None:
                table_name = ARGS['table']
                table_list = str(table_name).split(",")
                if len(table_list) > 1:
                    raise Exception("Please provide single table name")
                if "where" in query_to_append:
                    query_to_append = query_to_append + " and object_name='{}'".format(table_name)
                else:
                    query_to_append = query_to_append + " where object_name='{}'".format(table_name)
            else:
                table_name = None
            if ARGS['threads'] is not None:
                no_of_threads = int(ARGS['threads'])
            else:
                no_of_threads = CommonConstants.rdbms_THREADS
        except Exception as e:
            logger.exception("provide the proper args")
            logger.exception(e)
            raise Exception
        if query_to_append != "":
            query = query_to_append
        print("original query", query)

        utility_obj = Utility()
        final_list = utility_obj.mysql_utilty(query=query, dict_flag="N", insert_update_key="N")
        if final_list:
            pool = ThreadPool(no_of_threads)
            try:
                results = pool.map(read_from_database,final_list)
                pool.close()
                pool.join()
            except Exception as e:
                logger.exception("Error in connecting datasource")
                logger.exception(e)
                raise Exception

    except Exception as e:
        logger.exception("Error in connecting datasource")
        logger.exception(e)
        raise Exception