################################# Module Information #####################################################
#  Module Name         : Trial Universe Metric Calculation Data
#  Purpose             : To create the source data for the CTMS Refactored COUNTRY
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : CTMS Refactored COUNTRY
#  Last changed on     : 10-12-2024
#  Last changed by     : Kedar
#  Reason for change   : Status Update
##########################################################################################################


# Importing python modules

from CommonUtils import *
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(
    CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

organization_location_df = spark.sql(
    """SELECT o.id AS site_id, o.organization_name AS site_name, ol.to_id as location_id 
    FROM $$client_name_ctfo_datastore_staging_$$db_env.tascan_organizations o 
    JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_organizations_locations ol ON o.id = ol.from_id""")
organization_location_df.createOrReplaceTempView("organization_location_df")
organization_location_df.write.mode('overwrite').saveAsTable('organization_location')

organization_city_state_id_df = spark.sql("""
select ol.site_id,l.province_name as state_name,l.private_address_country,l.location_name as city_name
from organization_location_df ol
left join $$client_name_ctfo_datastore_staging_$$db_env.tascan_locations l on ol.location_id = l.id
""")
organization_city_state_id_df.registerTempTable('organization_city_state_id')
organization_city_state_id_df.write.mode('overwrite').saveAsTable('organization_city_state_id')

# Deduplication query
site_final_0 = spark.sql("""
    SELECT 
        CAST(olj.site_id AS INT) AS site_id,
        olj.site_name AS site_name,
        ocs.city_name AS site_location_city,
        loc.private_address_country AS site_location_country,
        CAST(ts.from_id AS INT) AS site_trial_id,
        ocs.state_name AS site_location_state,
        loc.postcode AS site_location_postcode,
        loc.address AS site_location_address,
        loc.latitude AS site_geo_latitude,
        loc.longitude AS site_geo_longitude
    FROM organization_location olj
    INNER JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_locations loc ON olj.location_id = loc.id
    INNER JOIN $$client_name_ctfo_datastore_staging_$$db_env.tascan_trials_sites ts ON olj.site_id = ts.to_id
    LEFT JOIN organization_city_state_id ocs on ocs.site_id = olj.site_id
""")
site_final_0.registerTempTable("tascan_site")

write_path = path.replace("table_name", "tascan_site")
site_final_0.write.mode("overwrite").parquet(write_path)

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.tascan_site partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   tascan_site where site_id is not null
""")

if site_final_0.count() == 0:
    print("Skipping copy_hdfs_to_s3 for tascan_site as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3(
        "$$client_name_ctfo_datastore_app_commons_$$db_env.tascan_site")
try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
