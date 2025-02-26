import datetime
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from CommonUtils import *
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql import functions as F




configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

data_dt = "$$data_dt"
cycle_id = "$$cycle_id"


path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/commons/temp/" \
           "kpi_output_dimension/table_name"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

ihme_county_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/zip_county_mapping.csv".format(bucket_path=bucket_path))

ihme_county_mapping.createOrReplaceTempView("ihme_county_mapping")

disease_mapping_temp= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping""")
disease_mapping_temp.createOrReplaceTempView('disease_mapping_temp')

disease_mapping = spark.sql("""select distinct * from (select mesh, standard_mesh from disease_mapping_temp
) """)
disease_mapping.write.mode('overwrite').saveAsTable('disease_mapping')

ihme_0 = spark.sql("""
SELECT a.*, 
       regexp_replace(location, ' County AL$', '') AS county_name_raw
FROM $$client_name_ctfo_datastore_staging_$$db_env.ihme a
""")

# ihme_0.createOrReplaceTempView("ihme_0")
ihme_0 = ihme_0.filter(ihme_0.unit == 'Number')
ihme_0.write.mode("overwrite").saveAsTable("ihme_0")


# ------------------------------------------------------------

incidence_df = ihme_0.filter(col("Measure") == "Incidence")
prevalence_df = ihme_0.filter(col("Measure") == "Prevalence")

# Step 7: Fetch all distinct race values
distinct_races = ihme_0.select("Race").distinct().rdd.flatMap(lambda x: x)\
    .filter(lambda x: x is not None)\
    .collect()

# Step 8: Create expressions dynamically for each race for incidence calculation
incidence_exprs = []
prevalence_exprs = []

# Iterate over each race to create the necessary expressions
for race in distinct_races:
    # Clean race string for alias
    race_clean = (race or "unknown").replace(",", "").replace(" ", "_").replace("-", "_").lower()

    # Calculate the incidence and prevalence for the current race
    incidence_expr = (
            F.sum(F.when((F.col("Race") == race) & (F.col("Unit") == "Number"), F.col("value")).otherwise(0)) /
            F.sum(F.when(F.col("Race").isNotNull() & (F.col("Unit") == "Number"), F.col("value")).otherwise(0)) * 100
    ).alias(f"percent_{race_clean}_incidence")

    prevalence_expr = (
            F.sum(F.when((F.col("Race") == race) & (F.col("Unit") == "Number"), F.col("value")).otherwise(0)) /
            F.sum(F.when(F.col("Race").isNotNull() & (F.col("Unit") == "Number"), F.col("value")).otherwise(0)) * 100
    ).alias(f"percent_{race_clean}_prevalence")

    # Append these expressions to their respective lists
    incidence_exprs.append(incidence_expr)
    prevalence_exprs.append(prevalence_expr)

# Aggregate the data for incidence and prevalence based on the race
incidence_agg_df = incidence_df.groupBy("Location", "Condition", "Sex", "Age", "Year", "county_name_raw") \
    .agg(*incidence_exprs)

prevalence_agg_df = prevalence_df.groupBy("Location", "Condition", "Sex", "Age", "Year", "county_name_raw") \
    .agg(*prevalence_exprs)

# Join the two aggregated DataFrames (incidence and prevalence)
final_df = incidence_agg_df.join(prevalence_agg_df,
                                 on=["Location", "Condition", "Sex", "Age", "Year", "county_name_raw"],
                                 how="full_outer")

# Add 'zip' column and clean column names
final_df = final_df.withColumn("zip", F.lit(None).cast(StringType()))
final_df = final_df.toDF(*[col.lower() for col in final_df.columns])  # Convert all column names to lowercase
final_df = final_df.toDF(*[col.replace("-", "_") for col in final_df.columns])  # Replace hyphens with underscores

# Drop any NaN columns
final_df = final_df.drop("percent_nan_prevalence")
final_df = final_df.drop("percent_nan_incidence")
final_df.createOrReplaceTempView("ihme")
final_df.write.mode("overwrite").saveAsTable("ihme")


# -------------------------------------------------------------

ihme_county_zip_mapping = spark.sql("""
    SELECT a.location AS location
    ,TRIM(CAST(a.condition AS STRING)) AS condition
    ,TRIM(CAST(a.sex AS STRING)) AS sex
    ,TRIM(CAST(a.age AS STRING)) AS age
    ,a.year
    ,TRIM(CAST(a.percent_non_hispanic_black_incidence AS DOUBLE)) AS percent_non_hispanic_black_incidence
    ,TRIM(CAST(a.percent_non_hispanic_white_incidence AS DOUBLE)) AS percent_non_hispanic_white_incidence
    ,TRIM(CAST(a.percent_non_hispanic_american_indian_or_alaskan_native_incidence AS DOUBLE)) AS percent_non_hispanic_american_indian_or_alaskan_native_incidence
    ,TRIM(CAST(a.percent_hispanic_any_race_incidence AS DOUBLE))  AS percent_hispanic_any_race_incidence
    ,TRIM(CAST(a.percent_non_hispanic_asian_or_pacific_islander_incidence as DOUBLE))  AS percent_non_hispanic_asian_or_pacific_islander_incidence
    ,TRIM(CAST(a.percent_non_hispanic_black_prevalence AS DOUBLE)) AS  percent_non_hispanic_black_prevalence
    ,TRIM(CAST(a.percent_non_hispanic_white_prevalence AS DOUBLE)) AS  percent_non_hispanic_white_prevalence
    ,TRIM(CAST(a.percent_non_hispanic_american_indian_or_alaskan_native_prevalence AS DOUBLE)) AS percent_non_hispanic_american_indian_or_alaskan_native_prevalence
    ,TRIM(CAST(a.percent_hispanic_any_race_prevalence AS DOUBLE)) AS  percent_hispanic_any_race_prevalence
    ,trim(CAST(a.percent_non_hispanic_asian_or_pacific_islander_prevalence AS DOUBLE)) AS  percent_non_hispanic_asian_or_pacific_islander_prevalence
    ,TRIM(COALESCE(b.zip, null)) AS zip
    ,TRIM(CAST(a.county_name_raw AS STRING)) AS  county_name_raw
    FROM ihme AS a
    LEFT JOIN ihme_county_mapping AS b 
    ON LOWER(TRIM(a.county_name_raw)) = LOWER(TRIM(b.county_name))
""")

# ihme_county_zip_mapping.createOrReplaceTempView("ihme_county_zip_mapping")
ihme_county_zip_mapping.write.mode("overwrite").saveAsTable("ihme_county_zip_mapping")


ihme_county_site_mapping = spark.sql("""
    SELECT 
        a.*,
        TRIM(CAST(b.site_name AS STRING)) AS site_name,
        TRIM(CAST(b.ctfo_site_id as STRING)) AS ctfo_site_id
    FROM ihme_county_zip_mapping a
    LEFT JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.d_site b
    on LOWER(TRIM(a.zip)) = LOWER(TRIM(b.site_zip))
    """)

# ihme_county_site_mapping.createOrReplaceTempView("ihme_county_site_mapping")
ihme_county_site_mapping.write.mode("overwrite").saveAsTable("ihme_county_site_mapping")

ihme_county_disease_mapping = spark.sql("""
    SELECT 
        a.*,
        coalesce(b.standard_mesh, a.condition) as indication
    FROM ihme_county_site_mapping a
    LEFT JOIN disease_mapping b
    on LOWER(TRIM(a.condition)) = LOWER(TRIM(b.mesh))
    """)

ihme_county_disease_mapping.write.mode("overwrite").saveAsTable("ihme_county_disease_mapping")


ihme_county_diversity_final= spark.sql("""
        SELECT *,
    (
        ROUND(COALESCE(percent_non_hispanic_asian_or_pacific_islander_prevalence, 0), 2) +
        ROUND(COALESCE(percent_hispanic_any_race_prevalence, 0), 2) +
        ROUND(COALESCE(percent_non_hispanic_american_indian_or_alaskan_native_prevalence, 0), 2) +
        ROUND(COALESCE(percent_non_hispanic_white_prevalence, 0), 2) +
        ROUND(COALESCE(percent_non_hispanic_black_prevalence, 0), 2)
    ) AS total_prevalence,
    (
        ROUND(COALESCE(percent_non_hispanic_asian_or_pacific_islander_incidence, 0), 2) +
        ROUND(COALESCE(percent_hispanic_any_race_incidence, 0), 2) +
        ROUND(COALESCE(percent_non_hispanic_american_indian_or_alaskan_native_incidence, 0), 2) +
        ROUND(COALESCE(percent_non_hispanic_white_incidence, 0), 2) +
        ROUND(COALESCE(percent_non_hispanic_black_incidence, 0), 2)
    ) AS total_incidence
        from ihme_county_disease_mapping
""")

ihme_county_diversity_final.createOrReplaceTempView("ihme_county_diversity_final")


ihme_county_diversity_final1 = spark.sql("""

    SELECT *,
    (
        GREATEST(
            CASE
                WHEN COALESCE(percent_non_hispanic_asian_or_pacific_islander_prevalence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_non_hispanic_asian_or_pacific_islander_prevalence, 0), 2) / total_prevalence) *
                    (log(1.0 / (ROUND(COALESCE(percent_non_hispanic_asian_or_pacific_islander_prevalence, 0), 2) / total_prevalence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        ) +
        GREATEST(
            CASE
                WHEN COALESCE(percent_hispanic_any_race_prevalence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_hispanic_any_race_prevalence, 0), 2) / total_prevalence) *
                    (log(1.0 / (ROUND(COALESCE(percent_hispanic_any_race_prevalence, 0), 2) / total_prevalence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        ) +
        GREATEST(
            CASE
                WHEN COALESCE(percent_non_hispanic_american_indian_or_alaskan_native_prevalence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_non_hispanic_american_indian_or_alaskan_native_prevalence, 0), 2) / total_prevalence) *
                    (log(1.0 / (ROUND(COALESCE(percent_non_hispanic_american_indian_or_alaskan_native_prevalence, 0), 2) / total_prevalence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        ) +
        GREATEST(
            CASE
                WHEN COALESCE(percent_non_hispanic_white_prevalence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_non_hispanic_white_prevalence, 0), 2) / total_prevalence) *
                    (log(1.0 / (ROUND(COALESCE(percent_non_hispanic_white_prevalence, 0), 2) / total_prevalence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        ) +
        GREATEST(
            CASE
                WHEN COALESCE(percent_non_hispanic_black_prevalence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_non_hispanic_black_prevalence, 0), 2) / total_prevalence) *
                    (log(1.0 / (ROUND(COALESCE(percent_non_hispanic_black_prevalence, 0), 2) / total_prevalence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        )
    ) AS entropy_prevalence_score,

     (
        GREATEST(
            CASE
                WHEN COALESCE(percent_non_hispanic_asian_or_pacific_islander_incidence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_non_hispanic_asian_or_pacific_islander_incidence, 0), 2) / total_incidence) *
                    (log(1.0 / (ROUND(COALESCE(percent_non_hispanic_asian_or_pacific_islander_incidence, 0), 2) / total_incidence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        ) +
        GREATEST(
            CASE
                WHEN COALESCE(percent_hispanic_any_race_incidence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_hispanic_any_race_incidence, 0), 2) / total_incidence) *
                    (log(1.0 / (ROUND(COALESCE(percent_hispanic_any_race_incidence, 0), 2) / total_incidence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        ) +
        GREATEST(
            CASE
                WHEN COALESCE(percent_non_hispanic_american_indian_or_alaskan_native_incidence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_non_hispanic_american_indian_or_alaskan_native_incidence, 0), 2) / total_incidence) *
                    (log(1.0 / (ROUND(COALESCE(percent_non_hispanic_american_indian_or_alaskan_native_incidence, 0), 2) / total_incidence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        ) +
        GREATEST(
            CASE
                WHEN COALESCE(percent_non_hispanic_white_incidence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_non_hispanic_white_incidence, 0), 2) / total_incidence) *
                    (log(1.0 / (ROUND(COALESCE(percent_non_hispanic_white_incidence, 0), 2) / total_incidence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        ) +
        GREATEST(
            CASE
                WHEN COALESCE(percent_non_hispanic_black_incidence, 0) > 0 THEN
                    (ROUND(COALESCE(percent_non_hispanic_black_incidence, 0), 2) / total_incidence) *
                    (log(1.0 / (ROUND(COALESCE(percent_non_hispanic_black_incidence, 0), 2) / total_incidence + 1e-10)) / log(5.0))
                ELSE 0
            END,
            0
        )
    ) AS entropy_incidence_score
FROM ihme_county_diversity_final        
""")

# ihme_county_diversity_final.createOrReplaceTempView("ihme_county_diversity_final")
ihme_county_diversity_final1.write.mode("overwrite").saveAsTable("ihme_county_diversity_final1")


# Initialize rounded_df
rounded_df = ihme_county_diversity_final1

# List of columns to round
columns_to_round = [
    "percent_non_hispanic_black_incidence",
    "percent_non_hispanic_white_incidence",
    "percent_non_hispanic_american_indian_or_alaskan_native_incidence",
    "percent_hispanic_any_race_incidence",
    "percent_non_hispanic_asian_or_pacific_islander_incidence",
    "percent_non_hispanic_black_prevalence",
    "percent_non_hispanic_white_prevalence",
    "percent_non_hispanic_american_indian_or_alaskan_native_prevalence",
    "percent_hispanic_any_race_prevalence",
    "percent_non_hispanic_asian_or_pacific_islander_prevalence",
    "entropy_incidence_score",
    "entropy_prevalence_score"
]

# Round each column in the list
for col_name in columns_to_round:
    rounded_df = rounded_df.withColumn(col_name, F.round(F.col(col_name), 2))

# Create or replace temp view
rounded_df.createOrReplaceTempView("ihme_county_diversity_final1")


f_rpt_ihme_county_diversity = spark.sql(""" select 
  TRIM(CAST(indication AS STRING)) AS indication,
  TRIM(CAST(location AS STRING)) AS county_name,
  TRIM(CAST(site_name AS STRING)) AS site_name,
  TRIM(CAST(ctfo_site_id AS STRING)) AS ctfo_site_id,
  TRIM(CAST(zip AS STRING)) AS zip,
  TRIM(CAST( percent_non_hispanic_black_incidence AS DOUBLE)) AS percent_non_hispanic_black_incidence,
  TRIM(CAST( percent_non_hispanic_white_incidence AS DOUBLE)) AS percent_non_hispanic_white_incidence,
  TRIM(CAST( percent_non_hispanic_american_indian_or_alaskan_native_incidence AS DOUBLE)) AS percent_non_hispanic_american_indian_or_alaskan_native_incidence,
  TRIM(CAST( percent_hispanic_any_race_incidence AS DOUBLE))  AS percent_hispanic_any_race_incidence,
  TRIM(CAST( percent_non_hispanic_asian_or_pacific_islander_incidence as DOUBLE))  AS percent_non_hispanic_asian_or_pacific_islander_incidence,
  TRIM(CAST( percent_non_hispanic_black_prevalence AS DOUBLE)) AS  percent_non_hispanic_black_prevalence,
  TRIM(CAST( percent_non_hispanic_white_prevalence AS DOUBLE)) AS  percent_non_hispanic_white_prevalence,
  TRIM(CAST( percent_non_hispanic_american_indian_or_alaskan_native_prevalence AS DOUBLE)) AS percent_non_hispanic_american_indian_or_alaskan_native_prevalence,
  TRIM(CAST( percent_hispanic_any_race_prevalence AS DOUBLE)) AS  percent_hispanic_any_race_prevalence,
  TRIM(CAST( percent_non_hispanic_asian_or_pacific_islander_prevalence AS DOUBLE)) AS  percent_non_hispanic_asian_or_pacific_islander_prevalence,
  TRIM(CAST(entropy_prevalence_score AS STRING)) AS prevalence_diversity_score,
  TRIM(CAST(entropy_incidence_score AS STRING)) AS incidence_diversity_score,
  TRIM(CAST(year AS STRING)) AS year
from 
  ihme_county_diversity_final1

""")

f_rpt_ihme_county_diversity = f_rpt_ihme_county_diversity.dropDuplicates()
# f_rpt_ihme_county_diversity.createOrReplaceTempView("f_rpt_ihme_county_diversity")

# save to table
f_rpt_ihme_county_diversity.write.mode("overwrite").saveAsTable("f_rpt_ihme_county_diversity")
write_path = path.replace("table_name", "f_rpt_ihme_county_diversity")
write_path_csv = path_csv.replace("table_name", "f_rpt_ihme_county_diversity")
write_path_csv = write_path_csv + "_csv/"
f_rpt_ihme_county_diversity.write.format("parquet").mode("overwrite").save(path=write_path)

f_rpt_ihme_county_diversity.coalesce(1).write \
    .option("header", "true") \
    .option("sep", ",") \
    .option('quote', '"') \
    .option('escape', '"') \
    .option("multiLine", "true") \
    .mode('overwrite') \
    .csv(write_path_csv)

f_rpt_ihme_county_diversity.coalesce(1).write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$env.f_rpt_ihme_county_diversity partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   f_rpt_ihme_county_diversity
""")

if f_rpt_ihme_county_diversity.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_ihme_county_diversity as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$env.f_rpt_ihme_county_diversity")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")




