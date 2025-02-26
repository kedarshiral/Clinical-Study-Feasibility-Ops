################################# Module Information #####################################################
#  Module Name         : USL for subject
#  Purpose             : To create the source data for the Trial Universe
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : ctms_refactoring_subject_final_df
#  Last changed on     : 23-05-2024
#  Last changed by     : Kashish
#  Reason for change   : Initial Code $db_envelopment
##########################################################################################################


# Importing python modules
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from CommonUtils import *
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
import os
from datetime import datetime
from pyspark.sql.functions import to_date, date_format, col
from pyspark.sql.functions import col, date_format, to_date, when


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
tdec_bucket = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "tdec_bucket"])


path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

#file_copy_command = "aws s3 cp {tdec_bucket}/SITFiles_2024Aug14/subject.csv {bucket_path}/USL/client_prod_data/".format(tdec_bucket=tdec_bucket, bucket_path=bucket_path)
#
# file_copy_status = os.system(file_copy_command)
# if file_copy_status != 0:
#     raise Exception("Failed to create backup of city mapping file on s3")

client_usl_subject = spark.read.format('csv') \
    .option('header', 'true') \
    .option('delimiter', ',') \
    .option('multiline', 'true') \
    .load('s3://aws-a0220-use1-00-d-s3b-shrd-cus-cdl01/clinical-data-lake/temp/220_usl_subject_data.csv')
client_usl_subject.write.mode('overwrite').saveAsTable('client_usl_subject')
# client_usl_subject = spark.read.format('csv') \
#     .option('header', 'true') \
#     .option('delimiter', ',') \
#     .option('multiline', 'true') \
#     .load('{bucket_path}/USL/client_prod_data/subject.csv'.format(bucket_path=bucket_path))
# client_usl_subject.write.mode('overwrite').saveAsTable('client_usl_subject')

date_columns = ['SCREEN_DATE', 'SCREEN_FAILURE_DATE', 'ENROLLMENT_DATE', 'DISCONTINUED_DATE', 'COMPLETED_DATE',
                'EARLY_TERMINATION_DATE', 'WITHDRAWN_DATE', 'ENROLLMENT_2_DATE', 'DISCONTINUED_TX_2_DATE',	'ENROLLMENT_3_DATE',
                'DISCONTINUED_TX_3_DATE']

#Convert the date values to the desired format
for col_name in date_columns:
    client_usl_subject = client_usl_subject.withColumn(
        col_name,
        date_format(
            when(
                to_date(col(col_name), 'yyyy-MM-dd').isNotNull(),
                to_date(col(col_name), 'yyyy-MM-dd')
            ).when(
                to_date(col(col_name), 'dd-MMM-yyyy').isNotNull(),
                to_date(col(col_name), 'dd-MMM-yyyy')
            ).when(
                to_date(col(col_name), 'MM/dd/yyyy').isNotNull(),
                to_date(col(col_name), 'MM/dd/yyyy')
            ),
            'yyyy-MM-dd'
        )
    )

client_usl_subject.write.mode('overwrite').saveAsTable('new_usl_subject')


violating_records_subject = spark.sql("""
SELECT src_trial_id, src_site_id, src_subject_id 
FROM new_usl_subject 
GROUP BY src_trial_id, src_site_id, src_subject_id 
HAVING COUNT(*) > 1
""")
violating_records_subject.registerTempTable('violating_records_subject')

######################################################  Final DF   #########################################


ctms_refactoring_subject_final_df = spark.sql("""SELECT
src_trial_id,
src_site_id,
src_subject_id,
subject_status,
SCREEN_DATE,
SCREEN_FAILURE_DATE,
ENROLLMENT_DATE,
DISCONTINUED_DATE,
COMPLETED_DATE,
EARLY_TERMINATION_DATE,
WITHDRAWN_DATE,
ENROLLMENT_2_DATE,
DISCONTINUED_TX_2_DATE,
ENROLLMENT_3_DATE,
DISCONTINUED_TX_3_DATE,
SUBJECT_NUMBER,
COHORT_ID,
COHORT_FULL_NAME
from new_usl_subject """)

#################
ctms_refactoring_subject_final_df = ctms_refactoring_subject_final_df.dropDuplicates()
ctms_refactoring_subject_final_df.write.mode('overwrite').saveAsTable('ctms_refactoring_subject_final_df')


usl_subject_filter = ctms_refactoring_subject_final_df.join(
    violating_records_subject,
    on=['src_trial_id', 'src_site_id', 'src_subject_id'],
    how='left_anti'
)
usl_subject_filter.write.mode('overwrite').saveAsTable('usl_subject_filter')



usl_subject = spark.sql("""SELECT
TRIM(src_trial_id) AS src_trial_id,
TRIM(src_site_id) AS src_site_id,
TRIM(src_subject_id) AS src_subject_id,
subject_status,
SCREEN_DATE,
SCREEN_FAILURE_DATE,
ENROLLMENT_DATE,
DISCONTINUED_DATE,
COMPLETED_DATE,
EARLY_TERMINATION_DATE,
WITHDRAWN_DATE,
ENROLLMENT_2_DATE,
DISCONTINUED_TX_2_DATE,
ENROLLMENT_3_DATE,
DISCONTINUED_TX_3_DATE,
SUBJECT_NUMBER,
COHORT_ID,
COHORT_FULL_NAME
from usl_subject_filter """)

#################
usl_subject = usl_subject.dropDuplicates()
usl_subject.write.mode('overwrite').saveAsTable('usl_subject')


#
# timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
# file_move_command = "aws s3 mv {bucket_path}/USL/client_prod_data/Incyte_to_Zaidyn_Study.csv {bucket_path}/USL/archive/study/Incyte_to_Zaidyn_Study_{timestamp}.csv".format(bucket_path=bucket_path, timestamp=timestamp)
# file_move_status = os.system(file_move_command)
# if file_move_status != 0:
#     raise Exception("Failed to archive usl site")


spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$env.usl_subject partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   usl_subject
""")

if ctms_refactoring_subject_final_df.count() == 0:
    print("Skipping copy_hdfs_to_s3 for usl_subject as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$env.usl_subject")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
