################################# Module Information #####################################################
#  Module Name         : Trial Universe Metric Calculation Data
#  Purpose             : To create the source data for the CTMS Refactored COUNTRY
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : CTMS Refactored COUNTRY
#  Last changed on     : 03-12-2021
#  Last changed by     : Vicky
#  Reason for change   : Status Update 
##########################################################################################################


# Importing python modules
import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, concat
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, first as first_
from pyspark.sql import functions as F
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')

Ongoing = trial_status_mapping.filter(trial_status_mapping.br_status == "Ongoing").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Completed = trial_status_mapping.filter(trial_status_mapping.br_status == "Completed").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Planned = trial_status_mapping.filter(trial_status_mapping.br_status == "Planned").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Others = trial_status_mapping.filter(trial_status_mapping.br_status == "Others").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()

# Open JSOn
import json

with open("Status_variablization.json", "r") as jsonFile:
    data = json.load(jsonFile)

# Save Values in Json
data["Ongoing"] = Ongoing
data["Completed"] = Completed
data["Planned"] = Planned
data["Others"] = Others

# JSON WRITE
with open("Status_variablization.json", "w") as jsonFile:
    json.dump(data, jsonFile)

Ongoing_variable = tuple(Ongoing)
Completed_variable = tuple(Completed)
Planned_variable = tuple(Planned)
Others_variable = tuple(Others)

Ongoing_Completed = tuple(Ongoing) + tuple(Completed)
Ongoing_Planned = tuple(Ongoing) + tuple(Planned)
Ongoing_Completed_Planned = tuple(Ongoing) + tuple(Completed) + tuple(Planned)

ctms_study_site = spark.sql("""SELECT aa.*
FROM   (select study_code from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study where lower(trim(meta_is_current)) = 'y'
                 and origin <> 'FPOS') a
inner join (SELECT a.*
        FROM   $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site a where Lower(Trim(cancelled)) = 'no'
       AND Lower(Trim(confirmed)) = 'yes'
       AND Lower(Trim(meta_is_current)) = 'y' and lower(trim(study_code)) not like '%iit%' and (center_id is not null or trim(center_id)<>'') ) aa on lower(trim(a.study_code))=lower(trim(aa.study_code))  """)
# ctms_study_site.registerTempTable('ctms_study_site')
ctms_study_site.write.mode('overwrite').saveAsTable('ctms_study_site')

df_country_details = spark.sql("""SELECT "ctms"               AS source,
       ""                   AS nct_id,
       a.study_code         AS src_trial_id,
       a.study_country_id   AS src_country_id,
       b.country        AS country,
       ""                   AS core_non_core_flag,
       x.no_of_sites_actual AS no_of_sites_actual,
       d.count_investigators,
       c.sites_planned      AS no_of_sites_planned,
       c.status             AS country_trial_status
FROM   (SELECT study_code,
               study.country AS src_country,
               study_country_id,
               center_id
        FROM   ctms_study_site study
        WHERE  Lower(Trim(confirmed)) = "yes" AND Lower(Trim(cancelled)) = "no" AND lower(Trim(meta_is_current)) = "y"
        and lower(trim(study_code)) not like '%iit%'

        GROUP  BY 1,2,3,4)a
        left join (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address 
              where lower(trim(primary_address))='yes') b
        on lower(trim(a.center_id))=lower(trim(b.center_id))
       LEFT JOIN (SELECT study_code,
                         study_country_id,
                         Count(distinct center_id) AS no_of_sites_actual
                  FROM   ctms_study_site
                  WHERE  Lower(Trim(confirmed)) = "yes"
                         AND Lower(Trim(cancelled)) = "no"
                         AND lower(Trim(meta_is_current)) = "y"
                         AND lower(trim(status)) in {on_com}
                  GROUP  BY 1,2)x
              ON lower(trim(a.study_code)) = lower(trim(x.study_code))
                 AND Lower(Trim(a.study_country_id)) = Lower(Trim(x.study_country_id))
       LEFT JOIN (SELECT study_code,
                         study_country_id,
                         sites_planned,
                         status
                  FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_country
                  GROUP  BY 1,2,3,4) c
              ON lower(trim(a.study_code)) = lower(trim(c.study_code))
                 AND Lower(Trim(a.study_country_id)) =Lower(Trim(c.study_country_id))
       LEFT JOIN (SELECT study_code,
                         study_country_id,
                         Count(distinct PRIMARY_INVESTIGATOR_ID) AS count_investigators
                  FROM ctms_study_site where Lower(Trim(confirmed)) = "yes" AND Lower(Trim(cancelled)) = "no" AND lower(Trim(meta_is_current)) = "y" GROUP  BY 1,2) d
              ON lower(trim(a.study_code))=lower(trim(d.study_code)) and Lower(Trim(a.study_country_id)) = Lower(Trim(d.study_country_id))
GROUP  BY 1,2,3,4,5,6,7,8,9,10 """.format(on_com=Ongoing_Completed))
df_country_details.createOrReplaceTempView("country_details")
df_country_details.write.mode('overwrite').saveAsTable('country_details')

random_src_country_id =spark.sql("""
select src_trial_id,country,src_country_id,country_trial_status from 
(select src_trial_id,country,src_country_id,country_trial_status,row_number() over (partition by src_trial_id,country order by prec) as rnk
from (select src_trial_id,country,country_trial_status,src_country_id, 
case when lower(trim(country_trial_status)) in {on} then 1 
when lower(trim(country_trial_status)) in {com} then 2 
when lower(trim(country_trial_status)) in {plan} then 3
when lower(trim(country_trial_status)) in {other} then 4 else 5 
end as prec
from country_details) b) a
where rnk=1
""".format(on=Ongoing_variable,com=Completed_variable,plan=Planned_variable,other=Others_variable))
random_src_country_id.write.mode('overwrite').saveAsTable('random_src_country_id')

#########################################################

df_patient_details_planned = spark.sql(""" SELECT a.study_code, e.country,a.study_country_id,
CAST(b.sum_entered_study AS int) AS patients_consented_planned, 
CAST(b.sum_entered_study AS int) AS patients_screened_planned, 
CAST(b.sum_entered_treatment AS int) AS patients_randomized_planned
FROM (SELECT  center_id,study_code, study_country_id, country FROM ctms_study_site WHERE LOWER(TRIM(confirmed)) = "yes" AND LOWER(TRIM(cancelled)) = "no" AND lower(Trim(meta_is_current)) = "y" group by 1,2,3,4) a
LEFT JOIN  (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address where lower(trim(primary_address))='yes') e
on lower(trim(a.center_id))=lower(trim(e.center_id))
left join 
(SELECT x.study_code, x.study_country_id, SUM(entered_study) AS sum_entered_study, SUM(entered_treatment) AS sum_entered_treatment FROM 
(SELECT study_code, study_country_id, entered_study, entered_treatment, picture_number, study_period_number FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_cntry_sbject_figures 
WHERE LOWER(TRIM(figure_type)) = 'planned'  GROUP BY 1, 2, 3, 4, 5, 6) x
INNER JOIN
(SELECT study_code, study_country_id, picture_number, study_period_number FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY study_code, study_country_id 
ORDER BY picture_number DESC, study_period_number DESC) AS row_num FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_cntry_sbject_figures 
WHERE LOWER(TRIM(figure_type)) = 'planned')where row_num=1) y
ON lower(trim(x.study_code)) = lower(trim(y.study_code)) AND lower(trim(x.study_country_id)) = lower(trim(y.study_country_id)) AND lower(trim(x.picture_number))=lower(trim(y.picture_number)) AND lower(trim(x.study_period_number))=lower(trim(y.study_period_number))
GROUP BY 1,2
) b ON lower(trim(a.study_code)) = lower(trim(b.study_code)) AND lower(trim(a.study_country_id)) = lower(trim(b.study_country_id)) GROUP BY 1,2,3,4,5,6 """)
# df_patient_details_planned.createOrReplaceTempView("patient_details_planned")
df_patient_details_planned.write.mode('overwrite').saveAsTable('patient_details_planned')

df_patient_details_actual = spark.sql('''SELECT a.study_code, e.country,a.study_country_id,
CAST(sum(b.sum_entered_study) AS int) AS patients_consented_actual, 
CAST(sum(b.sum_entered_study) AS int) AS patients_screened_actual,
CAST(sum(b.sum_dropped_at_screening) AS int) AS dropped_at_screening_actual, 
CAST(sum(b.sum_entered_treatment) AS int) AS patients_randomized_actual,
CAST(sum(b.sum_completed_treatment) AS int) AS patients_completed_actual,
CAST(sum(b.sum_dropped_treatment) AS int) AS patients_dropped_actual
FROM (select center_id,study_code, study_country_id,country,study_site_id  from ctms_study_site
where Lower(Trim(confirmed)) = 'yes' AND Lower(Trim(cancelled)) = 'no' and Lower(Trim(meta_is_current)) = 'y' AND lower(trim(status)) in {on_com} group by 1,2,3,4,5) a
LEFT JOIN (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address where lower(trim(primary_address))='yes') e
on lower(trim(a.center_id))=lower(trim(e.center_id))
left join 
(SELECT x.study_site_id,x.study_code, SUM(entered_study) AS sum_entered_study, SUM(dropped_at_screening) AS sum_dropped_at_screening, SUM(entered_treatment) AS sum_entered_treatment,
 SUM(completed_treatment) AS sum_completed_treatment, SUM(dropped_treatment) AS sum_dropped_treatment FROM 
(SELECT study_code,study_site_id, entered_study, entered_treatment,dropped_at_screening,dropped_treatment, completed_treatment,picture_number,picture_as_of, study_period_number 
FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures 
WHERE LOWER(TRIM(figure_type)) = 'confirmed'  GROUP BY 1, 2, 3, 4, 5, 6,7,8,9,10) x
INNER JOIN
(SELECT study_code,study_site_id, picture_number,picture_as_of, study_period_number FROM (SELECT *, ROW_NUMBER() OVER (partition by study_code,study_site_id order by picture_as_of desc ,picture_number desc,study_period_number desc ) AS row_num 
FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures
WHERE LOWER(TRIM(figure_type)) = 'confirmed'  )where row_num=1) y
ON lower(trim(x.study_code)) = lower(trim(y.study_code)) AND lower(trim(x.study_site_id)) = lower(trim(y.study_site_id)) AND lower(trim(x.picture_number))=lower(trim(y.picture_number)) and (lower(trim(x.picture_as_of)) = lower(trim(y.picture_as_of)) or x.picture_as_of is null and y.picture_as_of is null ) AND lower(trim(x.study_period_number))=lower(trim(y.study_period_number))
GROUP BY 1, 2
) b ON lower(trim(a.study_code)) = lower(trim(b.study_code)) AND lower(trim(a.study_site_id)) = lower(trim(b.study_site_id)) GROUP BY 1, 2, 3'''.format(
    on_com=Ongoing_Completed))
# df_patient_details_actual.createOrReplaceTempView("patient_details_actual")
df_patient_details_actual.write.mode('overwrite').saveAsTable('patient_details_actual')

df_patient_details_actual_completed = spark.sql('''SELECT a.study_code, e.country,a.study_country_id,
CAST(sum(b.sum_entered_treatment) AS int) AS patients_randomized_actual_completed,
CAST(sum(b.sum_dropped_treatment) AS int) AS patients_dropped_actual_completed
from
(select center_id,study_code, study_country_id,country,study_site_id  from ctms_study_site
where Lower(Trim(confirmed)) = 'yes' AND Lower(Trim(cancelled)) = 'no' and Lower(Trim(meta_is_current)) = 'y' AND lower(trim(status)) in {com} group by 1,2,3,4,5) a
LEFT JOIN (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address where lower(trim(primary_address))='yes') e
on lower(trim(a.center_id))=lower(trim(e.center_id))
left join 
(SELECT x.study_site_id,x.study_code, SUM(entered_study) AS sum_entered_study, SUM(dropped_at_screening) AS sum_dropped_at_screening, SUM(entered_treatment) AS sum_entered_treatment,
 SUM(completed_treatment) AS sum_completed_treatment, SUM(dropped_treatment) AS sum_dropped_treatment FROM 
(SELECT study_code,study_site_id, entered_study, entered_treatment,dropped_at_screening,dropped_treatment, completed_treatment,picture_number,picture_as_of, study_period_number 
FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures 
WHERE LOWER(TRIM(figure_type)) = 'confirmed'  GROUP BY 1, 2, 3, 4, 5, 6,7,8,9,10) x
INNER JOIN
(SELECT study_code,study_site_id, picture_number,picture_as_of, study_period_number FROM (SELECT *, ROW_NUMBER() OVER (partition by study_code,study_site_id order by picture_as_of desc ,picture_number desc,study_period_number desc ) AS row_num 
FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures
WHERE LOWER(TRIM(figure_type)) = 'confirmed'  )where row_num=1) y
ON lower(trim(x.study_code)) = lower(trim(y.study_code)) AND lower(trim(x.study_site_id)) = lower(trim(y.study_site_id)) AND lower(trim(x.picture_number))=lower(trim(y.picture_number)) and (lower(trim(x.picture_as_of)) = lower(trim(y.picture_as_of)) or x.picture_as_of is null and y.picture_as_of is null ) AND lower(trim(x.study_period_number))=lower(trim(y.study_period_number))
GROUP BY 1, 2
) b ON lower(trim(a.study_code)) = lower(trim(b.study_code)) AND lower(trim(a.study_site_id)) = lower(trim(b.study_site_id)) GROUP BY 1, 2, 3'''.format(
    com=Completed_variable))
# df_patient_details_actual_completed.createOrReplaceTempView("df_patient_details_actual_completed")
df_patient_details_actual_completed.write.mode('overwrite').saveAsTable('df_patient_details_actual_completed')

df_milestone_actual = spark.sql('''SELECT a.study_code, a.study_country_id, b.milestone, b.actual_date,e.country
FROM (SELECT  center_id,study_code, study_country_id, country FROM ctms_study_site WHERE LOWER(TRIM(confirmed)) = "yes" AND LOWER(TRIM(cancelled)) = "no" AND lower(Trim(meta_is_current)) = "y" ) a
LEFT JOIN (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address where lower(trim(primary_address))='yes') e
on lower(trim(a.center_id))=lower(trim(e.center_id))
left join 
(SELECT study_code, study_country_id, milestone, actual_date FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones 
WHERE lower(Trim(meta_is_current)) = "y"  )b
ON lower(trim(a.study_code)) = lower(trim(b.study_code)) AND lower(trim(a.study_country_id)) = lower(trim(b.study_country_id))
''')
df_milestone_actual.registerTempTable('df_milestone_actual')
df_milestone_actual = df_milestone_actual.withColumn("milestone_s", F.concat(
    F.regexp_replace(F.regexp_replace(F.lower(F.trim(F.col("milestone"))), ' ', '_'), '/', '_'), F.lit("_actual_dt")))
df_milestone_actual.registerTempTable('df_milestone_actual')
df_milestone_actual = df_milestone_actual.drop(F.col("milestone"))
df_milestone_actual.registerTempTable('df_milestone_actual')
df_milestone_actual_1 = df_milestone_actual.groupBy("study_code", "study_country_id","country").pivot("milestone_s").agg(
    first_("actual_date"))
df_milestone_actual_1.createOrReplaceTempView("milestone_actual_1")
df_milestone_actual_final = spark.sql("""SELECT country,study_code, study_country_id, local_cta_package_ready_actual_dt AS local_cta_pkcg_ready_actual_dt, ha_ca_submission_actual_dt, ha_ca_approval_actual_dt,
national_central_irb_iec_submission_actual_dt AS national_irb_iec_submission_actual_dt, first_local_irb_iec_submission_actual_dt,
national_central_irb_iec_approval_actual_dt AS national_irb_iec_approval_actual_dt, first_local_irb_iec_approval_actual_dt, first_site_initiation_visit_actual_dt,
first_subject_first_visit_actual_dt, first_subject_first_rando_treat_actual_dt AS first_subject_first_treatment_actual_dt, last_subject_first_visit_actual_dt,
last_subject_first_rando_treat_actual_dt AS last_subject_first_treatment_actual_dt, last_subject_last_treatment_actual_dt, last_subject_last_visit_actual_dt, all_sites_closed_actual_dt,
'' AS last_site_closed_actual_dt, '' AS last_site_initiation_visit_actual_dt FROM milestone_actual_1""")

df_milestone_actual_final.registerTempTable('df_milestone_actual_final')

df_milestone_planned = spark.sql('''SELECT a.study_code, a.study_country_id, b.milestone, b.current_plan_date,e.country
FROM (SELECT center_id, study_code, study_country_id, country FROM ctms_study_site WHERE LOWER(TRIM(confirmed)) = "yes" AND LOWER(TRIM(cancelled)) = "no" AND lower(Trim(meta_is_current)) = "y" ) a
LEFT JOIN (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address where lower(trim(primary_address))='yes') e
on lower(trim(a.center_id))=lower(trim(e.center_id))
left join 
(SELECT study_code, study_country_id, milestone, current_plan_date FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones WHERE lower(Trim(meta_is_current)) = "y")b
ON lower(trim(a.study_code)) = lower(trim(b.study_code)) AND lower(trim(a.study_country_id)) = lower(trim(b.study_country_id))
''')
df_milestone_planned.registerTempTable('df_milestone_planned')
df_milestone_planned = df_milestone_planned.withColumn("milestone_s", F.concat(
    F.regexp_replace(F.regexp_replace(F.lower(F.trim(F.col("milestone"))), ' ', '_'), '/', '_'), F.lit("_planned_dt")))
df_milestone_planned.registerTempTable('df_milestone_planned')
df_milestone_planned = df_milestone_planned.drop(F.col("milestone"))
df_milestone_planned.registerTempTable('df_milestone_planned')
df_milestone_planned_1 = df_milestone_planned.groupBy("study_code", "study_country_id","country").pivot("milestone_s").agg(
    first_("current_plan_date"))
df_milestone_planned_1.createOrReplaceTempView("milestone_planned_1")
df_milestone_planned_final = spark.sql(
    """SELECT country,study_code, study_country_id, local_cta_package_ready_planned_dt AS local_cta_pkcg_ready_planned_dt, ha_ca_submission_planned_dt, ha_ca_approval_planned_dt, national_central_irb_iec_submission_planned_dt AS national_irb_iec_submission_planned_dt, first_local_irb_iec_submission_planned_dt, national_central_irb_iec_approval_planned_dt AS national_irb_iec_approval_planned_dt, first_local_irb_iec_approval_planned_dt, first_site_initiation_visit_planned_dt, first_subject_first_visit_planned_dt, first_subject_first_rando_treat_planned_dt AS first_subject_first_treatment_planned_dt, last_subject_first_visit_planned_dt, last_subject_first_rando_treat_planned_dt AS last_subject_first_treatment_planned_dt, last_subject_last_treatment_planned_dt, last_subject_last_visit_planned_dt, all_sites_closed_planned_dt, '' AS last_site_closed_planned_dt, '' AS last_site_initiation_visit_planned_dt FROM milestone_planned_1""")
df_milestone_planned_final.registerTempTable('df_milestone_planned_final')

df_milestone_bsln_dt = spark.sql('''SELECT a.study_code, a.study_country_id, b.milestone, b.baseline_plan_date,e.country
FROM (SELECT  center_id,study_code, study_country_id, country FROM ctms_study_site WHERE LOWER(TRIM(confirmed)) = "yes" AND LOWER(TRIM(cancelled)) = "no" AND lower(Trim(meta_is_current)) = "y" ) a
LEFT JOIN (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address where lower(trim(primary_address))='yes') e
on lower(trim(a.center_id))=lower(trim(e.center_id))
left join 
(SELECT study_code, study_country_id, milestone, baseline_plan_date FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones WHERE lower(Trim(meta_is_current)) = "y")b
ON lower(trim(a.study_code)) = lower(trim(b.study_code)) AND lower(trim(a.study_country_id)) = lower(trim(b.study_country_id))
''')
df_milestone_bsln_dt.registerTempTable('df_milestone_bsln_dt')
df_milestone_bsln_dt = df_milestone_bsln_dt.withColumn("milestone_s", F.concat(
    F.regexp_replace(F.regexp_replace(F.lower(F.trim(F.col("milestone"))), ' ', '_'), '/', '_'), F.lit("_bsln_dt_dt")))
df_milestone_bsln_dt.registerTempTable('df_milestone_bsln_dt')
df_milestone_bsln_dt = df_milestone_bsln_dt.drop(F.col("milestone"))
df_milestone_bsln_dt.registerTempTable('df_milestone_bsln_dt')
df_milestone_bsln_dt_1 = df_milestone_bsln_dt.groupBy("study_code", "study_country_id","country").pivot("milestone_s").agg(
    first_("baseline_plan_date"))
df_milestone_bsln_dt_1.createOrReplaceTempView("milestone_bsln_dt_1")
df_milestone_bsln_dt_final = spark.sql("""SELECT country,study_code, study_country_id, local_cta_package_ready_bsln_dt_dt AS local_cta_pkcg_ready_bsln_dt, ha_ca_submission_bsln_dt_dt as ha_ca_submission_bsln_dt,
ha_ca_approval_bsln_dt_dt as ha_ca_approval_bsln_dt, 
national_central_irb_iec_submission_bsln_dt_dt AS national_irb_iec_submission_bsln_dt, first_local_irb_iec_submission_bsln_dt_dt as first_local_irb_iec_submission_bsln_dt, 
national_central_irb_iec_approval_bsln_dt_dt AS national_irb_iec_approval_bsln_dt, first_local_irb_iec_approval_bsln_dt_dt as first_local_irb_iec_approval_bsln_dt, 
first_site_initiation_visit_bsln_dt_dt as first_site_initiation_visit_bsln_dt,
first_subject_first_visit_bsln_dt_dt as first_subject_first_visit_bsln_dt, first_subject_first_rando_treat_bsln_dt_dt AS first_subject_first_treatment_bsln_dt,
last_subject_first_visit_bsln_dt_dt as last_subject_first_visit_bsln_dt, 
last_subject_first_rando_treat_bsln_dt_dt AS last_subject_first_treatment_bsln_dt, last_subject_last_treatment_bsln_dt_dt as last_subject_last_treatment_bsln_dt,
last_subject_last_visit_bsln_dt_dt as last_subject_last_visit_bsln_dt,
all_sites_closed_bsln_dt_dt as all_sites_closed_bsln_dt, '' AS last_site_closed_bsln_dt, '' AS last_site_initiation_visit_bsln_dt FROM milestone_bsln_dt_1""")
df_milestone_bsln_dt_final.registerTempTable('df_milestone_bsln_dt_final')

ctms_refactoring_country_final_df_temp = spark.sql("""SELECT                 
a.country  ,
a.src_trial_id  ,
a.nct_id                           ,
sum(b.patients_consented_planned) as   patients_consented_planned     ,
sum(c.patients_consented_actual) as patients_consented_actual        ,
sum(b.patients_screened_planned) as patients_screened_planned        ,
sum(c.patients_screened_actual) as patients_screened_actual         ,
sum(c.dropped_at_screening_actual) as dropped_at_screening_actual       ,
sum(b.patients_randomized_planned) as patients_randomized_planned      ,
sum(c.patients_randomized_actual) as  patients_randomized_actual     ,
sum(c.patients_completed_actual) as patients_completed_actual        ,
sum(c.patients_dropped_actual) as   patients_dropped_actual        ,
sum(a.count_investigators) as   count_investigators            ,
First(a.core_non_core_flag) as  core_non_core_flag              ,
sum(a.no_of_sites_planned) as  no_of_sites_planned            ,
sum(a.no_of_sites_actual) as no_of_sites_actual               ,
min(f.local_cta_pkcg_ready_bsln_dt) as  local_cta_pkcg_ready_bsln_dt    ,
min(g.local_cta_pkcg_ready_planned_dt) as   local_cta_pkcg_ready_planned_dt,
min(e.local_cta_pkcg_ready_actual_dt) as local_cta_pkcg_ready_actual_dt   ,
min(f.ha_ca_submission_bsln_dt) as ha_ca_submission_bsln_dt         ,
min(g.ha_ca_submission_planned_dt) as ha_ca_submission_planned_dt      ,
min(e.ha_ca_submission_actual_dt) as  ha_ca_submission_actual_dt      ,
max(f.ha_ca_approval_bsln_dt) as ha_ca_approval_bsln_dt           ,
max(g.ha_ca_approval_planned_dt) as  ha_ca_approval_planned_dt       ,
max(e.ha_ca_approval_actual_dt) as  ha_ca_approval_actual_dt        ,
min(f.national_irb_iec_submission_bsln_dt) as national_irb_iec_submission_bsln_dt        ,
min(g.national_irb_iec_submission_planned_dt) as national_irb_iec_submission_planned_dt     ,
min(e.national_irb_iec_submission_actual_dt) as national_irb_iec_submission_actual_dt      ,
min(f.first_local_irb_iec_submission_bsln_dt) as first_local_irb_iec_submission_bsln_dt     ,
min(g.first_local_irb_iec_submission_planned_dt) as first_local_irb_iec_submission_planned_dt  ,    
min(e.first_local_irb_iec_submission_actual_dt) as  first_local_irb_iec_submission_actual_dt  ,    
max(f.national_irb_iec_approval_bsln_dt) as national_irb_iec_approval_bsln_dt          ,
max(g.national_irb_iec_approval_planned_dt) as national_irb_iec_approval_planned_dt        ,
max(e.national_irb_iec_approval_actual_dt) as  national_irb_iec_approval_actual_dt       ,
max(f.first_local_irb_iec_approval_bsln_dt) as first_local_irb_iec_approval_bsln_dt       ,
max(g.first_local_irb_iec_approval_planned_dt) as first_local_irb_iec_approval_planned_dt    ,
max(e.first_local_irb_iec_approval_actual_dt) as first_local_irb_iec_approval_actual_dt      ,
min(f.first_site_initiation_visit_bsln_dt) as  first_site_initiation_visit_bsln_dt       ,
min(g.first_site_initiation_visit_planned_dt) as  first_site_initiation_visit_planned_dt    ,
min(e.first_site_initiation_visit_actual_dt) as  first_site_initiation_visit_actual_dt      ,
min(f.first_subject_first_visit_bsln_dt) as  first_subject_first_visit_bsln_dt         ,
min(g.first_subject_first_visit_planned_dt) as  first_subject_first_visit_planned_dt      ,
min(e.first_subject_first_visit_actual_dt) as  first_subject_first_visit_actual_dt       ,
min(f.first_subject_first_treatment_bsln_dt) as first_subject_first_treatment_bsln_dt      ,
min(g.first_subject_first_treatment_planned_dt) as  first_subject_first_treatment_planned_dt  ,    
min(e.first_subject_first_treatment_actual_dt) as first_subject_first_treatment_actual_dt    ,
max(f.last_subject_first_visit_bsln_dt) as last_subject_first_visit_bsln_dt           ,
max(g.last_subject_first_visit_planned_dt) as last_subject_first_visit_planned_dt         ,
max(e.last_subject_first_visit_actual_dt) as last_subject_first_visit_actual_dt         ,
max(f.last_subject_first_treatment_bsln_dt) as  last_subject_first_treatment_bsln_dt      ,
max(g.last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt     ,
max(e.last_subject_first_treatment_actual_dt) as last_subject_first_treatment_actual_dt     ,
max(f.last_subject_last_treatment_bsln_dt) as last_subject_last_treatment_bsln_dt         ,
max(g.last_subject_last_treatment_planned_dt) as last_subject_last_treatment_planned_dt     ,
max(e.last_subject_last_treatment_actual_dt) as last_subject_last_treatment_actual_dt      ,
max(f.last_subject_last_visit_bsln_dt) as  last_subject_last_visit_bsln_dt           ,
max(g.last_subject_last_visit_planned_dt) as last_subject_last_visit_planned_dt          ,
max(e.last_subject_last_visit_actual_dt) as  last_subject_last_visit_actual_dt        ,
max(f.all_sites_closed_bsln_dt) as  all_sites_closed_bsln_dt                  ,
max(g.all_sites_closed_planned_dt) as  all_sites_closed_planned_dt               ,
max(e.all_sites_closed_actual_dt) as all_sites_closed_actual_dt                 ,
max(f.last_site_closed_bsln_dt) as last_site_closed_bsln_dt                  ,
max(g.last_site_closed_planned_dt) as  last_site_closed_planned_dt               ,
max(e.last_site_closed_actual_dt) as  last_site_closed_actual_dt                 ,
max(f.last_site_initiation_visit_bsln_dt) as last_site_initiation_visit_bsln_dt         ,
max(g.last_site_initiation_visit_planned_dt) as last_site_initiation_visit_planned_dt       ,
max(e.last_site_initiation_visit_actual_dt) as last_site_initiation_visit_actual_dt       ,
First(a.source) as source,
sum(pdac.patients_randomized_actual_completed) as patients_randomized_actual_completed ,
sum(pdac.patients_dropped_actual_completed) as  patients_dropped_actual_completed
from country_details a
left join patient_details_planned  b on lower(trim(a.src_trial_id))=lower(trim(b.study_code)) and lower(trim(a.src_country_id))=lower(trim(b.study_country_id)) and lower(trim(a.country)) = lower(trim(b.country))
left join patient_details_actual c on lower(trim(b.study_code))=lower(trim(c.study_code)) and lower(trim(b.study_country_id))=lower(trim(c.study_country_id)) and lower(trim(a.country)) = lower(trim(c.country))
left join df_milestone_actual_final e on lower(trim(c.study_code))=lower(trim(e.study_code))and lower(trim(c.study_country_id))=lower(trim(e.study_country_id)) and lower(trim(a.country)) = lower(trim(e.country))
left join df_milestone_bsln_dt_final f on lower(trim(e.study_code))=lower(trim(f.study_code)) and lower(trim(e.study_country_id))=lower(trim(f.study_country_id)) and lower(trim(a.country)) = lower(trim(f.country))
left join df_milestone_planned_final g on lower(trim(f.study_code))=lower(trim(g.study_code)) and lower(trim(f.study_country_id))=lower(trim(g.study_country_id)) and lower(trim(a.country)) = lower(trim(g.country))
left join df_patient_details_actual_completed pdac on lower(trim(pdac.study_code))=lower(trim(a.src_trial_id)) and lower(trim(pdac.study_country_id))=lower(trim(a.src_country_id)) and lower(trim(a.country)) = lower(trim(pdac.country))
group by 1,2,3
""")


ctms_refactoring_country_final_df_temp = ctms_refactoring_country_final_df_temp.dropDuplicates()
ctms_refactoring_country_final_df_temp.registerTempTable('ctms_refactoring_country_final_df_temp')

######################################################  Final DF   #########################################

ctms_refactoring_country_final_df = spark.sql("""SELECT                
a.country  ,
b.src_country_id                 ,
a.src_trial_id as src_trial_id   ,
nct_id                           ,
patients_consented_planned       ,
patients_consented_actual        ,
patients_screened_planned        ,
patients_screened_actual         ,
dropped_at_screening_actual      ,
patients_randomized_planned      ,
patients_randomized_actual       ,
patients_completed_actual        ,
patients_dropped_actual          ,
count_investigators              ,
core_non_core_flag               ,
no_of_sites_planned              ,
no_of_sites_actual               ,
b.country_trial_status           ,
local_cta_pkcg_ready_bsln_dt     ,
local_cta_pkcg_ready_planned_dt  ,
local_cta_pkcg_ready_actual_dt   ,
ha_ca_submission_bsln_dt         ,
ha_ca_submission_planned_dt      ,
ha_ca_submission_actual_dt       ,
ha_ca_approval_bsln_dt           ,
ha_ca_approval_planned_dt        ,
ha_ca_approval_actual_dt         ,
national_irb_iec_submission_bsln_dt        ,
national_irb_iec_submission_planned_dt     ,
national_irb_iec_submission_actual_dt      ,
first_local_irb_iec_submission_bsln_dt     ,
first_local_irb_iec_submission_planned_dt  ,    
first_local_irb_iec_submission_actual_dt   ,    
national_irb_iec_approval_bsln_dt          ,
national_irb_iec_approval_planned_dt       ,
national_irb_iec_approval_actual_dt        ,
first_local_irb_iec_approval_bsln_dt       ,
first_local_irb_iec_approval_planned_dt    ,
first_local_irb_iec_approval_actual_dt     ,
first_site_initiation_visit_bsln_dt        ,
first_site_initiation_visit_planned_dt     ,
first_site_initiation_visit_actual_dt      ,
first_subject_first_visit_bsln_dt          ,
first_subject_first_visit_planned_dt       ,
first_subject_first_visit_actual_dt        ,
first_subject_first_treatment_bsln_dt      ,
first_subject_first_treatment_planned_dt   ,    
first_subject_first_treatment_actual_dt    ,
last_subject_first_visit_bsln_dt           ,
last_subject_first_visit_planned_dt        ,
last_subject_first_visit_actual_dt         ,
last_subject_first_treatment_bsln_dt       ,
last_subject_first_treatment_planned_dt    ,
last_subject_first_treatment_actual_dt     ,
last_subject_last_treatment_bsln_dt        ,
last_subject_last_treatment_planned_dt     ,
last_subject_last_treatment_actual_dt      ,
last_subject_last_visit_bsln_dt            ,
last_subject_last_visit_planned_dt         ,
last_subject_last_visit_actual_dt          ,
all_sites_closed_bsln_dt                   ,
all_sites_closed_planned_dt                ,
all_sites_closed_actual_dt                 ,
last_site_closed_bsln_dt                   ,
last_site_closed_planned_dt                ,
last_site_closed_actual_dt                 ,
last_site_initiation_visit_bsln_dt         ,
last_site_initiation_visit_planned_dt      ,
last_site_initiation_visit_actual_dt       ,
source                                     ,     
14 as fsiv_fsfr_delta,
'' as enrollment_rate,
'' as screen_failure_rate,
'' as lost_opp_time,
'' as enrollment_duration,
patients_randomized_actual_completed,
patients_dropped_actual_completed
from ctms_refactoring_country_final_df_temp a
left join
random_src_country_id b
on lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id)) and lower(trim(a.country))=lower(trim(b.country))
""")
ctms_refactoring_country_final_df = ctms_refactoring_country_final_df.dropDuplicates()
# ctms_refactoring_country_final_df.registerTempTable('ctms_refactoring_country_final_df')
ctms_refactoring_country_final_df.write.mode('overwrite').saveAsTable('usl_study_country')

# write_path = path.replace("table_name", "ctms_refactored_country")
# ctms_refactoring_country_final_df.write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   usl_study_country
""")

if ctms_refactoring_country_final_df.count() == 0:
    print("Skipping copy_hdfs_to_s3 for usl_study_country as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")




