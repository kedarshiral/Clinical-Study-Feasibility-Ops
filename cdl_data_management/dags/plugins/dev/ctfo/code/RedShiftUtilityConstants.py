#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module Name     :   Tech Products
Purpose         :   This utility contains the constants for RedShiftUtility
                    and RedShiftWrapper
Pre-requisites  :
Input           :
Ouput           :
Configs         :
"""
__author__ = 'ZS Associates'

STATUS_KEY = "status"
ERROR_KEY = "error"
RESULT_KEY = "result"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"
TARGET_TABLE_KEY = "target_table"
TARGET_SCHEMA_NAME = "target_schema_name"
SOURCE_S3_PATH_KEY = "source_s3_path"
CSV_HEADER_ROW_COUNT_KEY = "csv_header_row_count"
CSV_HEADER_ROW_COUNT_DEFAULT_KEY = 1
COPY_TYPE_KEY = "copy_type"
VACUUM_KEY = "VACUUM"
ANALYSE_KEY = "analyse"
COMPRESSION_KEY = "compression"
COPY_KEY = "copy"
DEEP_COPY_KEY = "deep_copy"
ACCESS_KEY_ID_KEY = "access_key_id"
SECRET_KEY_ID_KEY = "secret_access_key"
FILE_LOAD_TABLE = "REDSHIFT_FILE_LOAD_LOG"
STARTED_KEY = "started"
WITHDRAW_KEY = "WITHDRAW"
WITHDRAWN_KEY = "WITHDRAWN"
JSON_FILE_PATH_KEY = "json_file_path"
ENVIRONMENT_KEY = "environment"
CDL_RUN_IDENTIFIER_KEY = "cdl_run_identifier"
CDL_FREQUENCY_KEY = "cdl_frequency"
SERVICE_NAME_KEY = "service_name"
BUSINESS_UNIT_CODE_KEY = "business_unit_code"
DB_HOST_KEY = "db_host"
DB_PORT_KEY = "db_port"
DB_NAME_KEY = "db_name"
DB_USERNAME_KEY = "db_username"
DB_PASSWORD_KEY = "db_password"
ACCESS_KEY_KEY = "access_key"
SECRET_KEY_KEY = "secret_key"
DB_USERNAME_HIERARCHY = "PlatformEnvCredentials.database_username"
DB_PASSWORD_HIERARCHY = "PlatformEnvCredentials.database_password"
SES_SECRET_KEY_HIERARCHY = "PlatformEnvCredentials.ses_aws_secret_key"
SES_ACCESS_KEY_HIERARCHY = "PlatformEnvCredentials.ses_aws_access_key"
SES_REGION_HIERARCHY = "PlatformEnvCredentials.ses_aws_region"
NOTIFICATION_SENDER = "PlatformEnvParams.notification_sender"
RECIPIENT_HIERARCHY = "PlatformEnvParams.notification_recipients"
RECIPIENT_GROUP = "ktlo"
AWS_ACCESS_KEY = "PlatformEnvCredentials.database_password"
FILE_LIST_PATH = "RedShiftConfig.json"
CREDENTIALS_FILE_PATH = "../configs/EnvCredentials.json"
PLATFORM_FILE_PATH = "../configs/PlatformEnvParams.json"
SPLIT_TAG = 'tech_products'
NO_KEY = 'N'
YES_KEY = 'Y'
IN_PROGRESS_KEY = 'IN_PROGRESS'
LOG_UPDATE_KEY = 'UPDATE'
LOG_INSERT_KEY = 'INSERT'
EMPTY_KEY = 'EMPTY'
LOAD_KEY = 'LOAD'
REDSHIFT_CONNECTION_INFO_KEY = 'redshift_connection_info'
REDSHIFT_TABLE_DETAILS_KEY = "redshift_table_details"
TARGET_DATABASE_NAME_KEY = "target_database_name"
TABLE_FORMAT_KEY = "table_format"
TABLE_DELIMITER_KEY = "table_delimiter"
DEFAULT_TABLE_DELIMITER = ","
TIMEOUT_PERIOD_KEY = "timeout_period"
SOURCE_S3_LOCATION_KEY = "source_s3_location"
OVERWRITE_FLAG_KEY = "overwrite_flag"
TRUNCATE_FLAG_KEY = "truncate_flag"
OPERATION_KEY = "operation"
S3_CONNECTION_INFO_KEY = "s3_connection_info"
AUTOMATED_RELEASE_KEY = 'automated_release'
DEFAULT_AUTOMATED_RELEASE_KEY = False
VACUUM_THRESHOLD_KEY = 'vaccuum_threshold'
ANALYZE_THRESHOLD_KEY = "analyze_threshold"
DEFAULT_ANALYZE_THRESHOLD = 10
# the max number of columns for which vacuum will be attempted. post this deep copy will be performed
VACUUM_COLUMN_LIMIT_KEY = "vacuum_column_limit"
ANALYZE_THRESHOLD_PARAMETER = "$$analyze_threshold_parameter$$"
REDSHIFT_LOGS_AND_RELEASE_STATUS = 'redshift_logs_and_release_status'
HOST_KEY = 'host'
PORT_KEY = 'port'
DATABASE_NAME_KEY = 'database_name'
USERNAME_KEY = 'username'
PASSWORD_KEY = 'password'
TRANSACTION_STATUS_KEY = 'transaction_status'
TABLE_DELIMITER = "table_delimiter"
REDSHIFT_LOG_TABLE_KEY = "redshift_logs_and_release_status"
PRIMARY_KEY_VALUE_KEY = "primary_key_value"
EFFECTIVE_DATE_KEY = "effective_date"
MANDATORY_KEYS = [TARGET_DATABASE_NAME_KEY, TIMEOUT_PERIOD_KEY, SOURCE_S3_LOCATION_KEY, TABLE_DELIMITER,
                  S3_CONNECTION_INFO_KEY, ACCESS_KEY_KEY, SECRET_KEY_KEY, REDSHIFT_CONNECTION_INFO_KEY, USERNAME_KEY,
                  PASSWORD_KEY, HOST_KEY, PORT_KEY, DATABASE_NAME_KEY, PRIMARY_KEY_VALUE_KEY]
ACCESS_KEY_KEY = "access_key"
SECRET_KEY_KEY = "secret_key"
TARGET_DATABASE_NAME_KEY = "target_database_name"
TIMEOUT_PERIOD_KEY = "timeout_period"
SOURCE_S3_LOCATION_KEY = "source_s3_location"
TABLE_DELIMITER = "table_delimiter"
S3_CONNECTION_INFO_KEY = "s3_connection_info"
REDSHIFT_CONNECTION_INFO_KEY = "redshift_connection_info"
USERNAME_KEY = "username"
PASSWORD_KEY = "password"
HOST_KEY = "host"
PORT_KEY = "port"
DATABASE_NAME_KEY = "database_name"
PRIMARY_KEY_VALUE_KEY = "primary_key_value"
RETRY_LIMIT_KEY = "retry_limit"
COUNT_KEY = "count"
MESSAGE_KEY = "message"
ROW_COUNT_KEY = "row_count"
BU_KEY = 'bu'
FREQUENCY_KEY = 'frequency'
PARQUET_EXTENSION = ".parquet"
PARQUET_FORMAT_KEY = "parquet"
EXTERNAL_SCHEMA_KEY = "external_schema"
EXTERNAL_DATABASE_NAME_KEY = "external_database_name"
CSV_FORMAT_KEY = "csv"
EXTERNAL_SCHEMA_PARAMETER = "$$external_schema$$"
EXTERNAL_DATABASE_NAME_PARAMETER = "$$database_name$$"
BYTE_ARRAY_KEY = "BYTE_ARRAY"
VARCHAR_KEY = "VARCHAR"
COLUMN_NAME_KEY = "column_name"
DATA_TYPE_KEY = "data_type"
EXTERNAL_SCHEMA = "spectrum6"
EXTERNAL_DATABASE = "default"
DEFAULT_RETRY_VALUE = "3"
DEFAULT_TIMEOUT = "1200000000"
# Following constants will be used to map the datatypes across multiple formats
INT_KEY = "INT"
INT2_KEY = "INT2"
INT4_KEY = "INT4"
INT8_KEY = "INT8"
INT32_KEY = "INT32"
INT64_KEY = "INT64"
FIXED_LEN_BYTE_ARRAY = "FIXED_LEN_BYTE_ARRAY"
INT96_KEY = "INT96"
SMALL_INT_KEY = "SMALLINT"
BIG_INT_KEY = "BIGINT"
INTEGER_KEY = "INTEGER"
VAR_BINARY_KEY = "VARBINARY"
NUMERIC_KEY = "NUMERIC"
DECIMAL_KEY = "DECIMAL"
BOOL_KEY = "BOOL"
BOOLEAN_KEY = "BOOLEAN"
FLOAT_KEY = "FLOAT"
FLOAT4_KEY = "FLOAT4"
FLOAT8_KEY = "FLOAT8"
REAL_KEY = "REAL"
DOUBLE_PRECISION_KEY = "DOUBLE PRECISION"
DOUBLE_KEY = "DOUBLE"
CHAR_KEY = "CHAR"
NCHAR_KEY = "NCHAR"
BPCHAR_KEY = "BPCHAR"
CHARACTER_KEY = "CHARACTER"
NVARCHAR_KEY = "NVARCHAR"
CHARACTER_VARYING_KEY = "CHARACTER VARYING"
TEXT_KEY = "TEXT"
TIME_STAMP_KEY = "TIMESTAMP"
STRING_KEY = "STRING"
LONG_KEY = "LONG"
SOURCE_FILE_SCHEMA_RESPONSE_KEY = "org.apache.spark.sql.parquet.row.metadata"
FIELDS_KEY = "fields"
NAME_KEY = "name"
TYPE_KEY = "type"
PARQUET_TO_REDSHIFT_DATA_TYPES = {INT64_KEY: BIG_INT_KEY, BYTE_ARRAY_KEY: VARCHAR_KEY, INT32_KEY: INTEGER_KEY,
                                  INT96_KEY: TIME_STAMP_KEY, DOUBLE_KEY: DOUBLE_PRECISION_KEY, STRING_KEY: VARCHAR_KEY,
                                  LONG_KEY: BIG_INT_KEY}
REDSHIFT_DATA_TYPE_ALIAS_MAPPING = {
    INTEGER_KEY: [INT2_KEY, INT4_KEY, INT_KEY, SMALL_INT_KEY, INTEGER_KEY],
    BIG_INT_KEY: [INT8_KEY],
    FLOAT_KEY: [FLOAT4_KEY, REAL_KEY],
    DOUBLE_PRECISION_KEY: [FLOAT_KEY, FLOAT8_KEY, DOUBLE_KEY, DOUBLE_PRECISION_KEY],
    BOOLEAN_KEY: [BOOL_KEY, BOOLEAN_KEY],
    VARCHAR_KEY: [CHAR_KEY, CHARACTER_KEY, NCHAR_KEY, BPCHAR_KEY, NVARCHAR_KEY, TEXT_KEY, CHARACTER_VARYING_KEY]
}
END_TIME_STAMP_KEY = "end_time_stamp"
TEMP_TABLE_PREFIX = "temp"
TIME_OUT_ERROR = "cancelled on user's request"
MISSING_PARQUET_ERROR = "No parquet file found in the location - "
REDSHIFT_PARAMS_KEY = 'RedshiftParams'
LOGGING_CONNECTION_DETAILS_KEY = 'logging_connection_details'
DATABASE_CONNECTION_DETAILS_KEY = 'database_connection_details'
REDSHIFT_LOG_TABLE = 'redshift_log_table'
REDSHIFT_RELEASE_TABLE = 'redshift_release_table'
REDSHIFT_LOG_TABLE_SCHEMA = 'redshift_log_table_schema'
REDSHIFT_RELEASE_TABLE_SCHEMA = 'redshift_release_table_schema'
BATCH_ID_KEY = "batch_id"
FILE_ID_KEY = "file_id"
FILE_NAME_KEY = "file_name"
SCHEMA_PARAMETER = "$$schema$$"
VIEW_NAME_PARAMETER = "$$view$$"
COLUMNS_PARAMETER = "$$columns$$"
COMPRESSION_PARAMETER = "$$compression$$"
S3_LOCATION_PARAMETER = "$$s3_location$$"
BYTE_ARRAY_KEY = "BYTE_ARRAY"
VARCHAR_KEY = "VARCHAR"
TIMEOUT_PARAMETER = "$$timeout$$"
VACUUM_THRESHOLD_PARAMETER = "$$vacuum_threshold$$"
TEMP_TABLE_NAME_PARAMETER = "$$temp_table_name"

# New parameters added for pattern loads
PATTERN_NAME_KEY = "PATTERN_NAME"
PATTERN_NAME_COLUMN = "pt_pattern_name"

# Following constants will be used to map the datatypes across multiple formats
INT_KEY = "INT"
INT2_KEY = "INT2"
INT4_KEY = "INT4"
INT8_KEY = "INT8"
INT32_KEY = "INT32"
INT64_KEY = "INT64"
INT96_KEY = "INT96"
SMALL_INT_KEY = "SMALLINT"
BIG_INT_KEY = "BIGINT"
INTEGER_KEY = "INTEGER"
BOOL_KEY = "BOOL"
BOOLEAN_KEY = "BOOLEAN"
FLOAT_KEY = "FLOAT"
FLOAT4_KEY = "FLOAT4"
FLOAT8_KEY = "FLOAT8"
REAL_KEY = "REAL"
DOUBLE_PRECISION_KEY = "DOUBLE PRECISION"
DOUBLE_KEY = "DOUBLE"
CHAR_KEY = "CHAR"
NCHAR_KEY = "NCHAR"
BPCHAR_KEY = "BPCHAR"
CHARACTER_KEY = "CHARACTER"
NVARCHAR_KEY = "NVARCHAR"
CHARACTER_VARYING_KEY = "CHARACTER VARYING"
TEXT_KEY = "TEXT"
CDL_FILE_NAME_KEY = "cdl_file_name"
CDL_UUID_KEY = 'cdl_uuid'
CDL_INGESTION_LOG_KEY = 'CDL_INGESTION_LOG'
CDL_FILE_NAME_PARAMETER = "$$cdl_file_name_parameter$$"
CDL_PREFIX = "cdl_"
CDL_EFFECTIVE_DATE_KEY = "cdl_effective_date"
OVERWRITE_DEFAULT_FLAG_KEY = "N"
DEEP_COPY_DEFAULT_FLAG_KEY = "N"
TRUNCATE_DEFAULT_FLAG_KEY = "N"
STATUS_COLUMN = "status_column"
START_TIMESTAMP_COLUMN = "start_timestamp_column"
END_TIMESTAMP_COLUMN = "end_timestamp_column"
REDSHIFT_KEY = "redshift"
REDSHIFT_CONNECTION_NAME_KEY = "redshift_connection_name"
BUSINESS_UNIT_ALL = "ALL"
BUSINESS_UNIT_LIST = ["BCBU", "INBU", "OBU", "CA", "NUBU"]
SERVICE_NAME_LIST = ["bai_sales_data_service", "bai_activity_data_service",
                     "bai_claims_data_service", "bai_others_data_service",
                     "activity_data_service"]
REDSHIFT_WITHDRAWN = "WITHDRAWN"
REDSHIFT_TO_BE_WITHDRAWN = "TO_BE_WITHDRAWN"
REDSHIFT_WITHDRAW_FAILED = "WITHDRAW_FAILED"
RELEASE_FLAG_KEY = 'release_flag'
TABLE_FORMAT_DEFAULT_KEY = "parquet"
TABLE_COMPRESSION_KEY = "table_compression"
IAM_ROLE_PARAMETER = "$$iam_role$$"
SOURCE_COLUMNS_PARAMETER = "$$source_columns$$"
TARGET_COLUMNS_PARAMETER = "$$target_columns$$"
REDSHIFT_SCHEMA_PARAMETER = "$$redshift_schema$$"
# Following constants will be used to map the datatypes across multiple formats
TIME_STAMP_KEY = "TIMESTAMP"
REDSHIFT_INGESTION_SUMMARY_LOG = 'redshift_ingestion_summary_log'
REDSHIFT_RELEASE_PARAM_KEY = "RedshiftReleaseParams"
REDSHIFT_RELEASE_VALUES_LIST = ["TO_BE_WITHDRAWN", "WITHDRAWN", "RELEASE_CURRENT",
                                "RELEASE_PREVIOUS", "RELEASE_PRODUCTION", "RELEASE_CURRENT_PRODUCTION",
                                "RELEASED_TO_FIELD", "RELEASED_FOR_QA", "RELEASE_GOALS", '']
DEFAULT_VACUUM_THRESHOLD_KEY = 90
RETRY_INTERVAL = 10
CHARACTER_MAXIMUM_SIZE = "character_maximum_length"
NUMERIC_PRECISION_KEY = "numeric_precision"
NUMERIC_SCALE_KEY = "numeric_scale"
REDSHIFT_USER_EXCEPTION_LIST = []
REDSHIFT_GROUP_EXCEPTION_LIST = []
S3_BUCKET_KEY = "s3_bucket_name"
ENV_CRETENTIALS = "EnvCredentials.json"
SES_AWS_KEY = "PlatformEnvCredentials.ses_aws_access_key"
SES_SECRET_KEY = "PlatformEnvCredentials.ses_aws_secret_key"
SES_REGION = "PlatformEnvCredentials.ses_aws_region"
GET_GROUP_PERMISSION_QUERY = """
SELECT distinct select_flag, update_flag, insert_flag FROM 
(
 SELECT derived_table1.schemaname schema_name, 
        derived_table1.objectname table_name, 
        derived_table1.usename user_name,  
        'USER' user_type,        
        derived_table1.select_flag, 
        derived_table1.insert_flag, 
        derived_table1.update_flag, 
        derived_table1.delete_flag, 
        derived_table1.reference_flag
   FROM ( SELECT objs.schemaname, objs.objectname, usrs.usename, 
                CASE
                    WHEN has_table_privilege(usrs.usename, objs.fullobj::text, 'select'::text) THEN 1
                    ELSE 0
                END AS select_flag, 
                CASE
                    WHEN has_table_privilege(usrs.usename, objs.fullobj::text, 'insert'::text) THEN 1
                    ELSE 0
                END AS insert_flag, 
                CASE
                    WHEN has_table_privilege(usrs.usename, objs.fullobj::text, 'update'::text) THEN 1
                    ELSE 0
                END AS update_flag, 
                CASE
                    WHEN has_table_privilege(usrs.usename, objs.fullobj::text, 'delete'::text) THEN 1
                    ELSE 0
                END AS delete_flag, 
                CASE
                    WHEN has_table_privilege(usrs.usename, objs.fullobj::text, 'references'::text) THEN 1
                    ELSE 0
                END AS reference_flag
           FROM ( SELECT pg_tables.schemaname, 't'::character varying AS obj_type, pg_tables.tablename AS objectname, (pg_tables.schemaname::text + '.'::text + pg_tables.tablename::text)::character varying AS fullobj
                   FROM pg_tables
                  UNION 
                 SELECT pg_views.schemaname, 'v'::character varying AS obj_type, pg_views.viewname AS objectname, (pg_views.schemaname::text + '.'::text + pg_views.viewname::text)::character varying AS fullobj
                   FROM pg_views) objs, 
                   ( 
                      SELECT pg_user.usename 
                      FROM pg_user
                      ) usrs
          ORDER BY objs.fullobj) derived_table1
  WHERE (derived_table1.select_flag + derived_table1.insert_flag + derived_table1.update_flag + derived_table1.delete_flag + derived_table1.reference_flag) > 0
  and schemaname not in ('information_schema','pg_catalog')
union all
select schemname ,
       objectname ,
       username ,
       usertype ,
       CASE WHEN CHARINDEX('r', char_perms ) > 0 THEN 1 else 0 end select_flag,
       CASE WHEN CHARINDEX('a', char_perms ) > 0 THEN 1 else 0 end insert_flag,
       CASE WHEN CHARINDEX('w', char_perms ) > 0 THEN 1 else 0 end update_flag,
       CASE WHEN CHARINDEX('d', char_perms ) > 0 THEN 1 else 0 end delete_flag,
       CASE WHEN CHARINDEX('x', char_perms ) > 0 THEN 1 else 0 end references_flag/*,
       CASE WHEN CHARINDEX('R', char_perms ) > 0 THEN 1 else 0 end rule_flag,
       CASE WHEN CHARINDEX('t', char_perms ) > 0 THEN 1 else 0 end trigger_flag,
       CASE WHEN CHARINDEX('X', char_perms ) > 0 THEN 1 else 0 end execute_flag,
       CASE WHEN CHARINDEX('U', char_perms ) > 0 THEN 1 else 0 end usage_flag,
       CASE WHEN CHARINDEX('C', char_perms ) > 0 THEN 1 else 0 end create_flag,
       CASE WHEN CHARINDEX('T', char_perms ) > 0 THEN 1 else 0 end temporary_flag*/
from
(
    select namespace schemname,
           item objectname,
           groname username,
           'GROUP' usertype,
           SPLIT_PART( SPLIT_PART( ARRAY_TO_STRING( RELACL, '|' ), pu.groname, 2 ) , '/', 1 ) char_perms
    from
    (
    SELECT      use.usename AS subject
                        ,nsp.nspname AS namespace
                        ,cls.relname AS item
                        ,cls.relkind AS type
                        ,use2.usename AS owner
                        ,cls.relacl
            FROM        pg_user     use 
            CROSS JOIN  pg_class    cls
            LEFT JOIN   pg_namespace nsp 
            ON          cls.relnamespace = nsp.oid 
            LEFT JOIN   pg_user      use2 
            ON          cls.relowner = use2.usesysid
            WHERE       cls.relowner = use.usesysid
            AND         nsp.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
            ORDER BY     subject
                        ,namespace
                        ,item ) 
    JOIN    pg_group pu ON array_to_string(relacl, '|') LIKE '%'|| pu.groname ||'%' 
)   
) 
WHERE 
user_type = '{user_type}' 
and user_name = '{group_name}'
"""
DDL_VIEW_NAME = "admin.v_generate_tbl_ddl"
DDL_KEY = "ddl"
TABLE_DDL_VIEW_DEF = """
CREATE OR REPLACE VIEW admin.v_generate_tbl_ddl
AS
SELECT
 REGEXP_REPLACE (schemaname, '^zzzzzzzz', '') AS schemaname
 ,REGEXP_REPLACE (tablename, '^zzzzzzzz', '') AS tablename
 ,seq
 ,ddl
FROM
 (
 SELECT
  schemaname
  ,tablename
  ,seq
  ,ddl
 FROM
  (
  SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,2 AS seq
   ,'CREATE TABLE IF NOT EXISTS ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + '' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --OPEN PAREN COLUMN LIST
  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 5 AS seq, '(' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --COLUMN LIST
  UNION SELECT
   schemaname
   ,tablename
   ,seq
   ,'\t' + col_delim + col_name + ' ' + col_datatype + ' ' + col_nullable + ' ' + col_default + ' ' + col_encoding AS ddl
  FROM
   (
   SELECT
    n.nspname AS schemaname
    ,c.relname AS tablename
    ,100000000 + a.attnum AS seq
    ,CASE WHEN a.attnum > 1 THEN ',' ELSE '' END AS col_delim
    ,QUOTE_IDENT(a.attname) AS col_name
    ,CASE WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER VARYING', 'VARCHAR')
     WHEN STRPOS(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER') > 0
      THEN REPLACE(UPPER(format_type(a.atttypid, a.atttypmod)), 'CHARACTER', 'CHAR')
     ELSE UPPER(format_type(a.atttypid, a.atttypmod))
     END AS col_datatype
    ,CASE WHEN format_encoding((a.attencodingtype)::integer) = 'none'
     THEN ''
     ELSE 'ENCODE ' + format_encoding((a.attencodingtype)::integer)
     END AS col_encoding
    ,CASE WHEN a.atthasdef IS TRUE THEN 'DEFAULT ' + adef.adsrc ELSE '' END AS col_default
    ,CASE WHEN a.attnotnull IS TRUE THEN 'NOT NULL' ELSE '' END AS col_nullable
   FROM pg_namespace AS n
   INNER JOIN pg_class AS c ON n.oid = c.relnamespace
   INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
   LEFT OUTER JOIN pg_attrdef AS adef ON a.attrelid = adef.adrelid AND a.attnum = adef.adnum
   WHERE c.relkind = 'r'
     AND a.attnum > 0
   ORDER BY a.attnum
   )
  --CONSTRAINT LIST
  UNION (SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,200000000 + CAST(con.oid AS INT) AS seq
   ,'\t,' + pg_get_constraintdef(con.oid) AS ddl
  FROM pg_constraint AS con
  INNER JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid
  INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' AND pg_get_constraintdef(con.oid) NOT LIKE 'FOREIGN KEY%'
  ORDER BY seq)
  --CLOSE PAREN COLUMN LIST
  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 299999999 AS seq, ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --BACKUP
  UNION SELECT
  n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000000 AS seq
   ,'BACKUP NO' as ddl
FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN (SELECT
    SPLIT_PART(key,'_',5) id
    FROM pg_conf
    WHERE key LIKE 'pg_class_backup_%'
    AND SPLIT_PART(key,'_',4) = (SELECT
      oid
      FROM pg_database
      WHERE datname = current_database())) t ON t.id=c.oid
  WHERE c.relkind = 'r'
  --BACKUP WARNING
  UNION SELECT
  n.nspname AS schemaname
   ,c.relname AS tablename
   ,1 AS seq
   ,'--WARNING: This DDL inherited the BACKUP NO property from the source table' as ddl
FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN (SELECT
    SPLIT_PART(key,'_',5) id
    FROM pg_conf
    WHERE key LIKE 'pg_class_backup_%'
    AND SPLIT_PART(key,'_',4) = (SELECT
      oid
      FROM pg_database
      WHERE datname = current_database())) t ON t.id=c.oid
  WHERE c.relkind = 'r'
  --DISTSTYLE
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,300000001 AS seq
   ,CASE WHEN c.reldiststyle = 0 THEN 'DISTSTYLE EVEN'
    WHEN c.reldiststyle = 1 THEN 'DISTSTYLE KEY'
    WHEN c.reldiststyle = 8 THEN 'DISTSTYLE ALL'
    ELSE '<<Error - UNKNOWN DISTSTYLE>>'
    END AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
  --DISTKEY COLUMNS
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,400000000 + a.attnum AS seq
   ,'DISTKEY (' + QUOTE_IDENT(a.attname) + ')' AS ddl
  FROM pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND a.attisdistkey IS TRUE
    AND a.attnum > 0
  --SORTKEY COLUMNS
  UNION select schemaname, tablename, seq,
       case when min_sort <0 then 'INTERLEAVED SORTKEY (' else 'SORTKEY (' end as ddl
from (SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,499999999 AS seq
   ,min(attsortkeyord) min_sort FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
  AND abs(a.attsortkeyord) > 0
  AND a.attnum > 0
  group by 1,2,3 )
  UNION (SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,500000000 + abs(a.attsortkeyord) AS seq
   ,CASE WHEN abs(a.attsortkeyord) = 1
    THEN '\t' + QUOTE_IDENT(a.attname)
    ELSE '\t, ' + QUOTE_IDENT(a.attname)
    END AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  ORDER BY abs(a.attsortkeyord))
  UNION SELECT
   n.nspname AS schemaname
   ,c.relname AS tablename
   ,599999999 AS seq
   ,'\t)' AS ddl
  FROM pg_namespace AS n
  INNER JOIN  pg_class AS c ON n.oid = c.relnamespace
  INNER JOIN  pg_attribute AS a ON c.oid = a.attrelid
  WHERE c.relkind = 'r'
    AND abs(a.attsortkeyord) > 0
    AND a.attnum > 0
  --END SEMICOLON
  UNION SELECT n.nspname AS schemaname, c.relname AS tablename, 600000000 AS seq, ';' AS ddl
  FROM  pg_namespace AS n
  INNER JOIN pg_class AS c ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' )
  UNION (
    SELECT 'zzzzzzzz' || n.nspname AS schemaname,
       'zzzzzzzz' || c.relname AS tablename,
       700000000 + CAST(con.oid AS INT) AS seq,
       'ALTER TABLE ' + QUOTE_IDENT(n.nspname) + '.' + QUOTE_IDENT(c.relname) + ' ADD ' + pg_get_constraintdef(con.oid)::VARCHAR(1024) + ';' AS ddl
    FROM pg_constraint AS con
      INNER JOIN pg_class AS c
             ON c.relnamespace = con.connamespace
             AND c.oid = con.conrelid
      INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
    AND con.contype = 'f'
    ORDER BY seq
  )
 ORDER BY schemaname, tablename, seq
 )
;
"""
NOTIFICATION_TEMPLATE = """
Hi Team,<br><br>		
The redshift load was skipped since no files were available at s3 location. Please find details below.<br><br>		
Table: {table}<br>		
s3 location: {location}<br><br>		
Regards,<br>		
CT Team		
"""

REDSHIFT_DETAILS_KEY = "redshift_details"
REDSHIFT_HOST_KEY = "host"
REDSHIFT_PORT_KEY = "port"
REDSHIFT_USER_KEY = "user"
REDSHIFT_PASSWORD_KEY = "password"
REDSHIFT_DB_KEY = "db"
REDSHIFT_TIMEOUT_KEY = "timeout"
REDSHIFT_IAM_KEY = "iam_role"
REDSHIFT_RETRY_LIMIT_KEY = "retry_limit"
REDSHIFT_TARGET_SCHEMA_KEY = "schema"
REDSHIFT_VACCUUM_THRESHOLD_KEY = "vaccuum_threshold"

REDSHIFT_OPERATION_KEY = "operation"
REDSHIFT_SOURCE_S3_KEY = "source_s3_location"
REDSHIFT_TARGET_TABLE_KEY = "table"
REDSHIFT_TABLE_FORMAT_KEY = "table_format"
REDSHIFT_TABLE_DELIMITER_KEY = "delimiter"
REDSHIFT_WITHDRAW_FLAG_KEY = "withdraw_flag"
REDSHIFT_UNLOAD_S3_KEY = "target_s3_location"
PRIMARY_KEY = "primary_key"
PRIMARY_KEY_VALUE = "primary_key_value"

REDSHIFT_WITHDRAWN_STATUS = "WITHDRAWN"
REDSHIFT_LOAD_STATUS = ''


RS_PRIMARY_KEY_NOT_PROVIDED = "Primary key is not provided"
RS_PRIMARY_KEY_VALUE_NOT_PROVIDED = "Primary key value is not provided"


RS_SOURCE_S3_NOT_CONFIGURED = "Source s3 location is not configured for redshift operation"
RS_TBL_FORMAT_NOT_CONFIGURED = "Table format is not configured for redshift operation"
RS_HOST_KEY_NOT_IN_DICT = "Redshift host key is not present in input json"
RS_HOST_NOT_PROVIDED = "Redshift host is not provided"
RS_PORT_KEY_NOT_IN_DICT = "Redshift port key is not present in input json"
RS_PORT_NOT_PROVIDED = "Redshift port is not provided"

IAM_ROLE_KEY = "iam_role"
DELIMITER_KEY = "delimiter"
IGNORE_BLANK_LINES_PARAMETER = "ignoreblanklines"
IGNORE_HEADER_PARAMETER = "ignoreheader"
FORMAT_KEY_CMD = ' format as '

TABLE_NAME_PARAMETER = "$$table$$"
PRIMARY_KEY_PARAMETER = "$$primary_key$$"
PRIMARY_KEY_VALUE_PARAMETER = "$$primary_key_value_parameter"


RS_USER_NOT_PROVIDED = "User value is not provided"
RS_USER_KEY_NOT_IN_DICT = "User key is missing in input dictionary"
RS_PASSWORD_NOT_PROVIDED = "Password value is not provided"
RS_PASSWORD_KEY_NOT_IN_DICT = "Password key is missing in input dictionary"
RS_DB_NOT_PROVIDED = "DB value is not provided"
RS_DB_KEY_NOT_IN_DICT = "DB key is missing in input dictionary"
RS_IAM_NOT_PROVIDED = "IAM role value is not provided"
RS_IAM_KEY_NOT_IN_DICT = "IAM role key is missing in input dictionary"
RS_OPERATION_NOT_PROVIDED = "Redshift Operation is not provided"
RS_INVALID_OPERATION = "Redshift operation provided is invalid or is not supported"
RS_SCHEMA_NOT_PROVIDED = "Redshift schema is not provided"
RS_SCHEMA_KEY_NOT_IN_DICT = "Redshift schema key is missing in input dictionary"
RS_TARGET_TBL_NOT_PROVIDED = "Redshift Table name is not provided"
RS_TARGET_TBL_KEY_NOT_IN_DICT = "Redshift Table key is missing in input dictionary"
RS_TBL_FORMAT_NOT_PROVIDED = "Redshift target table format is not provided"
RS_TBL_FORMAT_KEY_NOT_IN_DICT = "Redshift target table format key is missing in input dictionary"
RS_TBL_DELIMITER_NOT_PROVIDED = "File delimiter is not provided for redshift operation"
RS_TBL_DELIMITER_KEY_NOT_IN_DICT = "Table delimiter key is missing in input dictionary"
RS_SOURCE_S3_NOT_PROVIDED = "S3 location is not provided for redshift operation"
RS_SOURCE_S3_KEY_NOT_IN_DICT = "S3 location key is missing for redshift operation"
RS_TIMEOUT_NOT_PROVIDED = "Timeout value is not provided for redshift operation"
RS_TIMEOUT_KEY_NOT_IN_DICT = "Timeout key is missing in input dictionary"
RS_RETRY_LIMIT_NOT_PROVIDED = "Retry limit value is not provided for Redshift operation"
RS_RETRY_LIMIT_KEY_NOT_IN_DICT = "Retry Limit key is missing in input dictionary"
RS_VACCUUM_THRESHOLD_NOT_PROVIDED = "Vaccuum threshold value is not provided"
RS_VACCUUM_THRESHOLD_KEY_NOT_IN_DICT = "Vaccuum threshold key is missing in input dictionary"

PARAMS_DICT = {SCHEMA_PARAMETER: "", TABLE_NAME_PARAMETER: "", COLUMNS_PARAMETER: "", COMPRESSION_PARAMETER: "",
               S3_LOCATION_PARAMETER: "", TIMEOUT_PARAMETER: "", TEMP_TABLE_NAME_PARAMETER: "",
               VACUUM_THRESHOLD_PARAMETER: "", PRIMARY_KEY: "", PRIMARY_KEY_VALUE: "", IAM_ROLE_PARAMETER: ""}
RS_LOG_TABLE = "redshift_log"
RS_RELEASE_TABLE = "redshift_release"
REDSHIFT_LOG_SCHEMA_KEY = "log_schema"
REDSHIFT_PARALLEL_THREAD_LIMIT = "thread_limit"
REDSHIFT_DEFAULT_PARALLEL_THREAD_LIMIT = 2
OPERATION_LOAD = "LOAD"
OPERATION_WITHDRAW = "WITHDRAW"
OPERATION_VACCUUM = "VACCUUM"
