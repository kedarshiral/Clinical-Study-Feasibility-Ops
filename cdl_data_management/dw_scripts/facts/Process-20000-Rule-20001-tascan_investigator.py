################################# Module Information #####################################################
#  Module Name         : Trial Universe Metric Calculation Data
#  Purpose             : To create the source data for the CTMS Refactored Site
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : CTMS Refactored Site
#  Last changed on     : 10-12-2024
#  Last changed by     : Kedar
#  Reason for change   : Initial Code $db_envelopment
##########################################################################################################


# Importing python modules
import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col,concat
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType,StringType
from pyspark.sql.functions import col, first as first_
from pyspark.sql import functions as F
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

# person = spark.sql("""
# select *
# from $$client_name_ctfo_datastore_staging_$$db_env.tascan_persons p
# join $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials_investigators ti on p.id = ti.to_id
# join $$client_name_ctfo_datastore_staging_$$db_env.tascan_persons_organizations po on po.from_id = p.id
# """)
# person_0 = person.dropDuplicates()
# person_0.write.mode('overwrite').saveAsTable('person')

investigator_final_0 = spark.sql("""
SELECT
    person.id AS investigator_id,
    person.person_first_name AS investigator_first_name,
    '' AS investigator_middle_initial,
    person.person_name AS investigator_last_name,
    person.telephone AS investigator_phone_numbers,
    person.email AS investigator_emails,
    person.person_npi AS investigator_npi,
    '' AS past_trial_ids,
    trials.id as ongoing_trial_ids,
    person.person_speciality AS investigator_speciality,
    person.fee_disclosure_investigator_type,
    person.person_medical_credentials AS investigator_degree,
    person.person_tag,
    person.person_tags,
    person.person_last_activity,
    person.author_properties,
    organization.organization_name AS investigator_p_organization_name,
    organization.id AS investigator_site_id,
    locations.location_name AS investigator_location_city,
    locations.province_name AS investigator_location_state,
    locations.postcode AS investigator_location_post_code,
    locations.private_address_country AS investigator_location_country,
    locations.address AS investigator_location_street_address
FROM $$client_name_ctfo_datastore_staging_$$db_env.tascan_persons person
LEFT JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_persons_private_address tppa ON person.id = tppa.from_id
LEFT JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_locations locations ON tppa.to_id = locations.id
INNER JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials_investigators ti ON person.id = ti.to_id
LEFT JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials trials ON ti.from_id = trials.id
INNER JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_persons_organizations po ON person.id = po.from_id
LEFT JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_organizations organization ON po.to_id = organization.id

""")

investigator_final_1 = investigator_final_0.dropDuplicates()

write_path = path.replace("table_name", "tascan_investigator")
investigator_final_1.write.mode("overwrite").parquet(write_path)

investigator_final_1.write.mode('overwrite').saveAsTable('tascan_investigator')


spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.tascan_investigator partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   tascan_investigator where investigator_id is not null
""")


if investigator_final_1.count() == 0:
    print("Skipping copy_hdfs_to_s3 for tascan_investigator as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.tascan_investigator")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")

