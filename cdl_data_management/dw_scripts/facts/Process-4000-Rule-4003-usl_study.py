################################# Module Information #####################################################
#  Module Name         : USL for study
#  Purpose             : To create the source data for the Trial Universe
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : ctms_refactoring_study_final_df
#  Last changed on     : 12-02-2021
#  Last changed by     : Kuldeep/Sankarsh
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


Ongoing = trial_status_mapping.filter(trial_status_mapping.br_status == "Ongoing").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Completed = trial_status_mapping.filter(trial_status_mapping.br_status == "Completed").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Planned = trial_status_mapping.filter(trial_status_mapping.br_status == "Planned").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Others = trial_status_mapping.filter(trial_status_mapping.br_status == "Others").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()


# Open JSOn
import json
with open("Status_variablization.json", "r") as jsonFile:
    data = json.load(jsonFile)

#Save Values in Json
data["Ongoing"] = Ongoing
data["Completed"] = Completed
data["Planned"] = Planned
data["Others"] = Others

#JSON WRITE
with open("Status_variablization.json", "w") as jsonFile:
    json.dump(data, jsonFile)


Ongoing_variable = tuple(Ongoing)
Completed_variable = tuple(Completed)
Planned_variable = tuple(Planned)
Others_variable = tuple(Others)

Ongoing_Completed = tuple(Ongoing)+tuple(Completed)
Ongoing_Planned = tuple(Ongoing)+tuple(Planned)
Ongoing_Completed_Planned = tuple(Ongoing)+tuple(Completed)+tuple(Planned)


#########Site module queries#########

##########################study details##############################################



ctms_study_site = spark.sql("""SELECT aa.*
FROM   (select study_code from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study where lower(trim(meta_is_current)) = 'y'
                 and origin <> 'FPOS') a
inner join (SELECT a.*
        FROM   $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site a where Lower(Trim(cancelled)) = 'no'
       AND Lower(Trim(confirmed)) = 'yes'
       AND Lower(Trim(meta_is_current)) = 'y' and lower(trim(study_code)) not like '%iit%' and (center_id is not null or trim(center_id)<>'') ) aa on lower(trim(a.study_code))=lower(trim(aa.study_code))  """)
#ctms_study_site.registerTempTable('ctms_study_site')
ctms_study_site.write.mode('overwrite').saveAsTable('ctms_study_site')

number_of_sites_activated = spark.sql(""" SELECT a.study_code,
       count(distinct(a.center_id)) AS number_of_sites_activated 
FROM
  (SELECT study_site_id,
          study_code,
          center_id
   FROM ctms_study_site 
   WHERE lower(trim(confirmed)) = 'yes'
     AND lower(trim(cancelled))='no'
     AND lower(trim(meta_is_current)) = 'y' and lower(trim(study_code)) not like '%iit%'
     AND lower(trim(status)) in {on_com}
   GROUP BY 1,
            2,
            3) a
inner JOIN
  (SELECT actual_date,
          study_code,
          study_site_id
   FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones cssm
   WHERE lower(trim(milestone)) = 'site initiation visit'
     AND actual_date IS NOT NULL
   GROUP BY 1,
            2,
            3) b ON lower(trim(a.study_code))=lower(trim(b.study_code))
AND lower(trim(a.study_site_id))=lower(trim(b.study_site_id))
GROUP BY 1 """.format(on_com=Ongoing_Completed))
number_of_sites_activated.registerTempTable('number_of_sites_activated')

number_of_countries_activated = spark.sql(""" SELECT a.study_code,
       count(distinct(a.country)) AS number_of_countries_activated
FROM
  (SELECT study_country_id,
          study_code,
          country
   FROM ctms_study_site
   WHERE lower(trim(confirmed)) = 'yes'
     AND lower(trim(cancelled))='no'
     AND lower(trim(meta_is_current)) = 'y' and lower(trim(study_code)) not like '%iit%'
     AND lower(trim(status)) in {on_com}
   GROUP BY 1,
            2,
            3)a
inner JOIN
  (SELECT actual_date,
          study_code,
          study_country_id
   FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones 
  WHERE lower(trim(milestone)) = 'first site initiation visit'
     AND actual_date IS NOT NULL
   GROUP BY 1,
            2,
            3) b ON lower(trim(a.study_code))=lower(trim(b.study_code))
AND lower(trim(a.study_country_id))=lower(trim(b.study_country_id))
GROUP BY 1""".format(on_com=Ongoing_Completed))
number_of_countries_activated.registerTempTable('number_of_countries_activated')

number_of_sites_actual = spark.sql(""" SELECT study_code,
       count(distinct(center_id)) AS number_of_sites_actual
FROM ctms_study_site
   WHERE lower(trim(confirmed)) = 'yes'
     AND lower(trim(cancelled))='no'
     AND lower(trim(meta_is_current)) = 'y'
     AND lower(trim(status)) in {on_com}
GROUP BY 1""".format(on_com=Ongoing_Completed))
number_of_sites_actual.registerTempTable('number_of_sites_actual')

number_of_countries_actual = spark.sql(""" SELECT study_code,
       count(distinct(country)) AS number_of_countries_actual
FROM ctms_study_site
   WHERE lower(trim(confirmed)) = 'yes'
     AND lower(trim(cancelled))='no'
     AND lower(trim(meta_is_current)) = 'y'
     AND lower(trim(status)) in {on_com} 
GROUP BY 1""".format(on_com=Ongoing_Completed))
number_of_countries_actual.registerTempTable('number_of_countries_actual')

study_details01 = spark.sql(""" SELECT b.study_code AS src_trial_id,
       '' AS nct_id,
       '' AS protocol_type,
       coalesce(b.protocol_title, b.short_description) AS trial_title,
       b.phase_code AS trial_phase,
       b.status AS trial_status,
       '' AS trial_type,
       '' AS trial_design,
       '' AS patient_segment,
       '' AS mesh_terms,
       '' AS results_date,
       '' AS primary_endpoints_details,
       '' AS primary_endpoints_reported_date,
       '' AS secondary_endpoints_details,
       '' AS secondary_endpoints_reported_date,
       b.sites_planned AS number_of_sites_planned,
       b.countries_planned AS number_of_countries_planned,
       b.primary_investigator_id AS number_of_investigators,
       b.therapeutic_area AS therapeutic_area,
       '' AS inclusion_criteria,
       '' AS exclusion_criteria,
       '' AS study_objective,
       '' AS drug_synonyms,
       '' AS prior_concurrent_therapy,
       trial_end_dt,
       tsd.trial_start_dt,
       nsa.number_of_sites_activated,
       nca.number_of_countries_activated,
       nosa.number_of_sites_actual,
       noca.number_of_countries_actual
  FROM (select inner_study_codes.* from  ( select a.*,bb.country,bb.primary_investigator_id from  (   SELECT study_code,
                  count(DISTINCT country) AS country,
                  count(distinct primary_investigator_id) AS primary_investigator_id
             FROM ctms_study_site
            WHERE lower(trim(confirmed))       = 'yes'
              AND lower(trim(cancelled))       = 'no'
              AND lower(trim(meta_is_current)) = 'y'
              and lower(trim(study_code)) not like '%iit%'
            GROUP BY 1) bb
  INNER JOIN (   SELECT short_description,
                       protocol_title,
                       study_code,
                       phase_code,
                       status,
                       sites_planned,
                       countries_planned,
                       therapeutic_area,
                       investigational_product
                  FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study
                 WHERE lower(trim(meta_is_current)) = 'y'
                 and origin <> 'FPOS'
                 GROUP BY 1,
                          2,
                          3,
                          4,
                          5,
                          6,
                          7,
                          8,
                          9) a
    ON lower(trim(bb.study_code))   = lower(trim(a.study_code))) inner_study_codes )b
  LEFT JOIN (   SELECT max(actual_date) AS trial_end_dt,
                       study_code
                  FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_milestones
                 WHERE lower(trim(milestone)) = 'study completion'
                 GROUP BY 2) csm
    ON lower(trim(csm.study_code)) = lower(trim(b.study_code))
  LEFT JOIN (   SELECT aa.study_code,
                       min(bb.actual_date) AS trial_start_dt
                  FROM (   SELECT study_site_id,
                                  study_code
                             FROM ctms_study_site
                            WHERE lower(trim(confirmed))       = 'yes'
                              AND lower(trim(cancelled))       = 'no'
                              AND lower(trim(meta_is_current)) = 'y'
                            GROUP BY 1,
                                     2) aa
                  LEFT JOIN (   SELECT actual_date,
                                       study_code,
                                       study_site_id
                                  FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones
                                 WHERE lower(trim(milestone))       = 'site initiation visit'
                                   AND lower(trim(meta_is_current)) = 'y'
                                 GROUP BY 1,
                                          2,
                                          3) bb
                    ON lower(trim(aa.study_code))    = lower(trim(bb.study_code))
                   AND lower(trim(aa.study_site_id)) = lower(trim(bb.study_site_id))
                 GROUP BY 1) tsd
    ON lower(trim(tsd.study_code)) = lower(trim(b.study_code))
  LEFT JOIN number_of_sites_activated nsa
    ON lower(trim(b.study_code))   = lower(trim(nsa.study_code))
  LEFT JOIN number_of_countries_activated nca
    ON lower(trim(b.study_code))   = lower(trim(nca.study_code))
  LEFT JOIN number_of_sites_actual nosa
    ON lower(trim(b.study_code))= lower(trim(nosa.study_code))
  LEFT JOIN number_of_countries_actual noca
    ON lower(trim(b.study_code)) =lower(trim(noca.study_code))
GROUP BY 1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9,
          10,
          11,
          12,
          13,
          14,
          15,
          16,
          17,
          18,
          19,
          20,
          21,
          22,
          23,
          24,
          25,
          26,
          27,
          28,29,30
          """)
study_details01 = study_details01.dropDuplicates()
study_details01.registerTempTable('study_details01')
# study_details01.write.mode("overwrite").saveAsTable("study_details01")


study_details02 = spark.sql(""" SELECT b.study_code AS src_trial_id,
       '' AS nct_id,
       '' AS protocol_type,
       '' AS trial_type,
       '' AS trial_design,
       cast(concat(g.age_minimum, " ", g.age_minimum_unit)AS string) AS trial_age_min,
       cast(concat(g.age_maximum, " ", g.age_maximum_unit)AS string) AS trial_age_max,
       CASE
           WHEN g.FEMALE_PARTICIPATION = 'Yes'
                AND g.MALE_PARTICIPATION = 'Yes' THEN 'Both'
           WHEN g.FEMALE_PARTICIPATION = 'Yes'
                AND g.MALE_PARTICIPATION = 'No' THEN 'Female'
           WHEN g.FEMALE_PARTICIPATION = 'No'
                AND g.MALE_PARTICIPATION = 'Yes' THEN 'Male'
           WHEN g.FEMALE_PARTICIPATION = 'No'
                AND g.MALE_PARTICIPATION = 'No' THEN 'N/A'
           ELSE 'N/A'
       END AS patient_gender,
       '' AS patient_segment,
       '' AS mesh_terms,
       '' AS results_date,
       '' AS primary_endpoints_details,
       '' AS primary_endpoints_reported_date,
       '' AS secondary_endpoints_details,
       '' AS secondary_endpoints_reported_date,
       b.primary_investigator_id AS number_of_investigators,
       trim(concat_ws('\|', collect_set(e.INDICATION))) AS disease,
       '' AS inclusion_criteria,
       '' AS exclusion_criteria,
       '' AS study_objective,
       trim(concat_ws('\|', collect_set(coalesce(d.trade_name,d.PRODUCT_NAME)))) AS drug,
       '' AS drug_synonyms,
       '' AS prior_concurrent_therapy
FROM
  (SELECT study_code,
          count(DISTINCT country) AS country,
          count(primary_investigator_id) AS primary_investigator_id
   FROM ctms_study_site
   WHERE lower(trim(confirmed)) = 'yes'
     AND lower(trim(cancelled)) = 'no'
     AND lower(trim(meta_is_current)) = 'y'
   GROUP BY 1) b
INNER JOIN
  (SELECT short_description,
          protocol_title,
          study_code,
          phase_code,
          status,
          sites_planned,
          countries_planned,
          therapeutic_area,
          investigational_product
   FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study
   WHERE lower(trim(meta_is_current)) = 'y'
   and origin <> 'FPOS'
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
           8,
            9) a ON lower(trim(b.study_code)) = lower(trim(a.study_code))
LEFT JOIN
  (SELECT product_code,
          product_name,
          trade_name
   FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_product
   WHERE lower(trim(meta_is_current)) = 'y' ) d ON lower(trim(a.investigational_product)) = lower(trim(d.product_code))
LEFT JOIN
  (SELECT study_code,
          indication
   FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_indication
   WHERE lower(trim(meta_is_current)) = 'y' ) e ON lower(trim(b.study_code)) = lower(trim(e.study_code))
LEFT JOIN
  (SELECT study_code,
          max(actual_date) AS actual_dt
   FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_milestones
   WHERE lower(trim(meta_is_current)) = 'y'
   GROUP BY 1) f ON lower(trim(b.study_code)) = lower(trim(f.study_code))
LEFT JOIN
  (SELECT *
   FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_design
   WHERE lower(trim(meta_is_current)) = 'y' ) g ON lower(trim(g.study_code)) = lower(trim(b.study_code))        
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,22,23
""")

study_details02 = study_details02.dropDuplicates()
study_details02.registerTempTable('study_details02')
# study_details02.write.mode("overwrite").saveAsTable("study_details02")


study_details03 = spark.sql(""" select 
SD1.src_trial_id,
'' as nct_id,
'' as protocol_type,
SD1.trial_title,
SD1.trial_phase,
SD1.trial_status,
'' as trial_type,
'' as trial_design,
SD1.trial_start_dt,
SD1.trial_end_dt,
SD2.trial_age_min,
SD2.trial_age_max,
SD2.patient_gender,
'' as patient_segment,
'' as mesh_terms,
'' as results_date,
'' as primary_endpoints_details,
'' as primary_endpoints_reported_date,
'' as secondary_endpoints_details,
'' as secondary_endpoints_reported_date,
SD1.number_of_sites_planned,
SD1.number_of_sites_activated,
SD1.number_of_sites_actual, 
SD1.number_of_countries_planned,
SD1.number_of_countries_activated,
SD1.number_of_countries_actual,
SD1.number_of_investigators,
SD2.disease,
SD1.therapeutic_area,
'' as inclusion_criteria,
'' as exclusion_criteria,
'' as study_objective,
drug,
'' as drug_synonyms,
'' as prior_concurrent_therapy
from study_details01 SD1 left join study_details02 SD2
on lower(trim(SD1.src_trial_id)) = lower(trim(SD2.src_trial_id))""")

study_details03 = study_details03.dropDuplicates()
study_details03.registerTempTable('study_details03')
# study_details03.write.mode("overwrite").saveAsTable("study_details03")


###############################################study_details_end#############################


###############################patient_details_begin###################################
patient_details_confirmed = spark.sql(""" 
select one.study_code,
cast (sum(c.entered_study) as int)        as     patients_consented_actual,
cast (sum(c.entered_study) as int)         as     patients_screened_actual,
cast (sum(c.dropped_at_screening) as int)   as     dropped_at_screening_actual,
cast (sum(c.entered_treatment ) as int)     as     patients_randomized_actual,
cast (sum(c.completed_treatment ) as int) as     patients_completed_actual,
cast (sum(c.dropped_treatment ) as int)   as     patients_dropped_actual
from 
(select  study_code ,study_site_id,status from ctms_study_site 
where Lower(Trim(confirmed)) = 'yes' AND Lower(Trim(cancelled)) = 'no' and Lower(Trim(meta_is_current)) = 'y' and lower(trim(status)) in {on_com} group by 1,2,3) one
left join
(select  a.study_code, a.study_site_id,sum(a.entered_study) as entered_study , sum(a.entered_treatment) as entered_treatment, sum(a.completed_treatment) as completed_treatment ,sum(a.dropped_at_screening) as dropped_at_screening,sum(a.dropped_treatment) as dropped_treatment from
(select  study_code,study_site_id,entered_treatment,entered_study,picture_as_of,figure_type,picture_number,study_period_number, completed_treatment,dropped_at_screening,dropped_treatment
from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures 
where lower(trim(figure_type)) = 'confirmed' and lower(trim(meta_is_current)) = 'y'  group by 1,2,3,4,5,6,7,8,9,10,11) a
inner join
(select study_code,study_site_id,picture_number,picture_as_of,study_period_number from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,picture_number desc,study_period_number desc ) as rnk
from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures 
where lower(trim(figure_type)) = 'confirmed' and lower(trim(meta_is_current)) = 'y'  )inq where inq.rnk=1
) b
on lower(trim(a.study_code)) = lower(trim(b.study_code)) and lower(trim(a.picture_number))=lower(trim(b.picture_number)) and (lower(trim(a.picture_as_of)) = lower(trim(b.picture_as_of)) or a.picture_as_of is null and b.picture_as_of is null ) and lower(trim(a.study_period_number))=lower(trim(b.study_period_number)) 
and lower(trim(a.study_site_id))=lower(trim(b.study_site_id))
group by 1,2)c
on lower(trim(one.study_code)) = lower(trim(c.study_code)) and lower(trim(one.study_site_id)) = lower(trim(c.study_site_id)) 
group by 1""".format(on_com=Ongoing_Completed))
patient_details_confirmed = patient_details_confirmed.dropDuplicates()
patient_details_confirmed.registerTempTable('patient_details_confirmed')
# patient_details_confirmed.write.mode("overwrite").saveAsTable("patient_details_confirmed")


patient_details_planned = spark.sql(""" 
select one.study_code,
c.entered_study        as     patients_consented_planned,
c.entered_study         as     patients_screened_planned,
c.entered_treatment    as     patients_randomized_planned
from 
(select distinct study_code from ctms_study_site
where Lower(Trim(confirmed)) = 'yes' AND Lower(Trim(cancelled)) = 'no' and Lower(Trim(meta_is_current)) = 'y' group by 1) one
left join
(select  a.study_code, sum(a.entered_study) as entered_study , sum(a.entered_treatment) as entered_treatment, sum(a.completed_treatment) as completed_treatment ,sum(a.dropped_at_screening) as dropped_at_screening,sum(a.dropped_treatment) as dropped_treatment from
(select  study_code,entered_treatment,entered_study,figure_type,picture_number,study_period_number, completed_treatment,dropped_at_screening,dropped_treatment
from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_subject_figures where lower(trim(figure_type)) = 'planned' and lower(trim(meta_is_current)) = 'y'  group by 1,2,3,4,5,6,7,8,9) a
inner join
(select study_code,picture_number,study_period_number from (select *,row_number() over (partition by study_code order by picture_number desc ,study_period_number desc ) as rnk
from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_subject_figures where lower(trim(figure_type)) = 'planned' and lower(trim(meta_is_current)) = 'y'  )inq where inq.rnk=1
) b
on lower(trim(a.study_code)) = lower(trim(b.study_code)) and lower(trim(a.picture_number))=lower(trim(b.picture_number)) and lower(trim(a.study_period_number))=lower(trim(b.study_period_number)) 
group by 1)c
on lower(trim(one.study_code)) = lower(trim(c.study_code)) group by 1,2,3,4
    """)
patient_details_planned = patient_details_planned.dropDuplicates()
patient_details_planned.registerTempTable('patient_details_planned')
# patient_details_planned.write.mode("overwrite").saveAsTable("patient_details_planned")


#################################################patient_details_end####################################


######################################3milestone_begin##############################################3

#######################################################################################################################################################################################################################################################################
df_milestone = spark.sql("""select a.study_code,b.milestone,b.actual_date,b.baseline_plan_date,b.current_plan_date from (select * from ctms_study_site where Lower(Trim(meta_is_current)) = 'y' and  Lower(Trim(confirmed)) = 'yes'
       AND Lower(Trim(cancelled)) = 'no' ) a left join (select study_code,milestone,actual_date,baseline_plan_date,current_plan_date from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_milestones where meta_is_current='Y') b
       on lower(trim(a.study_code))=lower(trim(b.study_code)) 
       group by 1,2,3,4,5""")
df_milestone.registerTempTable('df_milestone')

df_milestone_actual_1 = df_milestone.groupBy("study_code").pivot("milestone").agg(first_("actual_date"))
df_milestone_actual_1.registerTempTable('df_milestone_actual_1')

# Remove Space from ColumnName
df_milestone_actual_2 = df_milestone_actual_1.select(
    [F.col(col).alias(col.replace(' ', '_')) for col in df_milestone_actual_1.columns])
df_milestone_actual_2.registerTempTable('df_milestone_actual_2')

# Remove / from column Name
df_milestone_actual_3 = df_milestone_actual_2.select(
    [F.col(col).alias(col.replace('/', '_')) for col in df_milestone_actual_2.columns])
df_milestone_actual_3.registerTempTable('df_milestone_actual_3')

df_milestone_actual_final = spark.sql("""select 
study_code,
Protocol_Approved                            as protocol_approval_actual_dt,
Core_CTA_Package_Ready                       as core_ctp_pckg_dlvrd_actual_dt,
First_HA_CA_Submission                       as first_ha_ca_submission_actual_dt,
First_HA_CA_Approval                         as first_ha_ca_approval_actual_dt,
First_National_Central_IRB_IEC_Submission    as first_national_irb_iec_submission_actual_dt,
First_Local_IRB_IEC_Submission               as first_local_irb_iec_submission_actual_dt,
First_National_Central_IRB_IEC_Approval_      as first_national_irb_iec_approval_actual_dt,
First_Local_IRB_IEC_Approval                 as first_local_irb_iec_approval_actual_dt,
First_Site_Initiation_Visit                  as first_site_initiation_visit_actual_dt,
First_Subject_First_Visit                    as first_subject_first_visit_actual_dt,
First_Subject_First_Rando_Treat              as first_subject_first_treatment_actual_dt,
Last_Subject_First_Visit                      as last_subject_first_visit_actual_dt,
Last_Subject_First_Rando_Treat               as last_subject_first_treatment_actual_dt,
Last_Subject_Last_Treatment                  as last_subject_last_treatment_actual_dt,
Last_Subject_Last_Visit                      as last_subject_last_visit_actual_dt,
Database_Lock                                as study_database_lock_actual_dt,
Last_Data_Received                           as last_data_received_actual_dt,
Study_Completion                             as study_completion_actual_dt
from df_milestone_actual_3 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19""")

df_milestone_actual_final = df_milestone_actual_final.dropDuplicates()
df_milestone_actual_final.registerTempTable('df_milestone_actual_final')
# df_milestone_actual_final.write.mode('overwrite').saveAsTable('df_milestone_actual_final')

#######################################################################################################################################################################################################################################################################


df_milestone_baseline_1 = df_milestone.groupBy("study_code").pivot("milestone").agg(first_("baseline_plan_date"))
df_milestone_baseline_1.registerTempTable('df_milestone_baseline_1')

# Remove Space from ColumnName
df_milestone_baseline_2 = df_milestone_baseline_1.select(
    [F.col(col).alias(col.replace(' ', '_')) for col in df_milestone_baseline_1.columns])
df_milestone_baseline_2.registerTempTable('df_milestone_baseline_2')

# Remove / from column Name
df_milestone_baseline_3 = df_milestone_baseline_2.select(
    [F.col(col).alias(col.replace('/', '_')) for col in df_milestone_baseline_2.columns])
df_milestone_baseline_3.registerTempTable('df_milestone_baseline_3')

df_milestone_baseline_final = spark.sql("""select 
study_code,
Protocol_Approved                            as protocol_approval_bsln_dt,
Core_CTA_Package_Ready                       as core_ctp_pckg_dlvrd_bsln_dt,
First_HA_CA_Submission                       as first_ha_ca_submission_bsln_dt,
First_HA_CA_Approval                         as first_ha_ca_approval_bsln_dt,
First_National_Central_IRB_IEC_Submission    as first_national_irb_iec_submission_bsln_dt,
First_Local_IRB_IEC_Submission               as first_local_irb_iec_submission_bsln_dt,
First_National_Central_IRB_IEC_Approval_      as first_national_irb_iec_approval_bsln_dt,
First_Local_IRB_IEC_Approval                 as first_local_irb_iec_approval_bsln_dt,
First_Site_Initiation_Visit                  as first_site_initiation_visit_bsln_dt,
First_Subject_First_Visit                    as first_subject_first_visit_bsln_dt,
First_Subject_First_Rando_Treat              as first_subject_first_treatment_bsln_dt,
Last_Subject_First_Visit                      as last_subject_first_visit_bsln_dt,
Last_Subject_First_Rando_Treat               as last_subject_first_treatment_bsln_dt,
Last_Subject_Last_Treatment                  as last_subject_last_treatment_bsln_dt,
Last_Subject_Last_Visit                      as last_subject_last_visit_bsln_dt,
Database_Lock                                as study_database_lock_bsln_dt,
Last_Data_Received                           as last_data_received_bsln_dt,
Study_Completion                             as study_completion_bsln_dt
from df_milestone_baseline_3 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19""")

df_milestone_baseline_final = df_milestone_baseline_final.dropDuplicates()
df_milestone_baseline_final.registerTempTable('df_milestone_baseline_final')
# df_milestone_baseline_final.write.mode('overwrite').saveAsTable('df_milestone_baseline_final')

spark.sql(
    """select protocol_approval_bsln_dt from df_milestone_baseline_final where study_code in ('CPT.GMA.301')""").show()
#######################################################################################################################################################################################################################################################################

df_milestone_current_1 = df_milestone.groupBy("study_code").pivot("milestone").agg(first_("current_plan_date"))
df_milestone_current_1.registerTempTable('df_milestone_current_1')

# Remove Space from ColumnName
df_milestone_current_2 = df_milestone_current_1.select(
    [F.col(col).alias(col.replace(' ', '_')) for col in df_milestone_current_1.columns])
df_milestone_current_2.registerTempTable('df_milestone_current_2')

# Remove / from column Name
df_milestone_current_3 = df_milestone_current_2.select(
    [F.col(col).alias(col.replace('/', '_')) for col in df_milestone_current_2.columns])
df_milestone_current_3.registerTempTable('df_milestone_current_3')

df_milestone_current_final = spark.sql("""select 
study_code,
Protocol_Approved                             as protocol_approval_planned_dt,
Core_CTA_Package_Ready                        as core_ctp_pckg_dlvrd_planned_dt,
First_HA_CA_Submission                        as first_ha_ca_submission_planned_dt,
First_HA_CA_Approval                          as first_ha_ca_approval_planned_dt,
First_National_Central_IRB_IEC_Submission     as first_national_irb_iec_submission_planned_dt,
First_Local_IRB_IEC_Submission                as first_local_irb_iec_submission_planned_dt,
First_National_Central_IRB_IEC_Approval_      as first_national_irb_iec_approval_planned_dt,
First_Local_IRB_IEC_Approval                  as first_local_irb_iec_approval_planned_dt,
First_Site_Initiation_Visit                   as first_site_initiation_visit_planned_dt,
First_Subject_First_Visit                     as first_subject_first_visit_planned_dt,
First_Subject_First_Rando_Treat               as first_subject_first_treatment_planned_dt,
Last_Subject_First_Visit                       as last_subject_first_visit_planned_dt,
Last_Subject_First_Rando_Treat                as last_subject_first_treatment_planned_dt,
Last_Subject_Last_Treatment                   as last_subject_last_treatment_planned_dt,
Last_Subject_Last_Visit                       as last_subject_last_visit_planned_dt,
Database_Lock                                 as study_database_lock_planned_dt,
Last_Data_Received                            as last_data_received_planned_dt,
Study_Completion                              as study_completion_planned_dt
from df_milestone_current_3 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19""")

df_milestone_current_final = df_milestone_current_final.dropDuplicates()
df_milestone_current_final.registerTempTable('df_milestone_current_final')
# df_milestone_current_final.write.mode('overwrite').saveAsTable('df_milestone_current_final')

#######################################################################################################################################################################################################################################################################

df_milestone_ss_final = spark.sql("""select a.study_code,min(b.current_plan_date) as study_start_planned_dt, min(b.baseline_plan_date) as study_start_bsln_dt, min(b.actual_date) as study_start_actual_dt from (select * from ctms_study_site where Lower(Trim(meta_is_current)) = 'y' and  Lower(Trim(confirmed)) = 'yes'
       AND Lower(Trim(cancelled)) = 'no'  ) a left join (select study_code,milestone,current_plan_date,actual_date,baseline_plan_date from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones 
       where  lower(trim(meta_is_current)) = 'y'  and  lower(trim(milestone)) = 'site initiation visit' ) b
       on lower(trim(a.study_code))=lower(trim(b.study_code)) 
       group by 1""")

df_milestone_ss_final = df_milestone_ss_final.dropDuplicates()
#df_milestone_ss_final.registerTempTable('df_milestone_ss_final')
df_milestone_ss_final.write.mode('overwrite').saveAsTable('df_milestone_ss_final')

# endmilestone


##########################################milestone_end######################################################


######################################################  Final DF   #########################################


ctms_refactoring_study_final_df = spark.sql("""SELECT
sd.src_trial_id as src_trial_id,
'' as nct_id,
'' as protocol_type,
sd.trial_title,
sd.trial_phase,
sd.trial_status,
'' as trial_type,
'' as trial_design,
sd.trial_start_dt,
sd.trial_end_dt,
sd.trial_age_max,
sd.patient_gender,
'' as patient_segment,
'' as mesh_terms,
'' as results_date,
'' as primary_endpoints_details,
'' as primary_endpoints_reported_date,
'' as secondary_endpoints_details,
'' as secondary_endpoints_reported_date,
sd.number_of_sites_planned,
sd.number_of_sites_activated,
sd.number_of_sites_actual,
sd.number_of_countries_planned,
sd.number_of_countries_activated,
sd.number_of_countries_actual,
sd.number_of_investigators,
sd.disease,
sd.therapeutic_area,
'' as inclusion_criteria,
'' as exclusion_criteria,
'' as study_objective,
sd.drug,
'' as drug_synonyms,
'' as prior_concurrent_therapy,
pdp.patients_consented_planned,
pdc.patients_consented_actual,
pdp.patients_screened_planned,
pdc.patients_screened_actual,
pdc.dropped_at_screening_actual,
pdp.patients_randomized_planned,
pdc.patients_randomized_actual,
pdc.patients_completed_actual,
pdc.patients_dropped_actual,
'Sanofi' as sponsor,
'' as sponsor_type,
mss.study_start_bsln_dt,
mss.study_start_planned_dt,
mss.study_start_actual_dt,
mb.protocol_approval_bsln_dt,
mc.protocol_approval_planned_dt,
ma.protocol_approval_actual_dt,
mb.core_ctp_pckg_dlvrd_bsln_dt,
mc.core_ctp_pckg_dlvrd_planned_dt,
ma.core_ctp_pckg_dlvrd_actual_dt,
mb.first_ha_ca_submission_bsln_dt,
mc.first_ha_ca_submission_planned_dt,
ma.first_ha_ca_submission_actual_dt,
mb.first_ha_ca_approval_bsln_dt,
mc.first_ha_ca_approval_planned_dt,
ma.first_ha_ca_approval_actual_dt,
mb.first_national_irb_iec_submission_bsln_dt,
mc.first_national_irb_iec_submission_planned_dt,
ma.first_national_irb_iec_submission_actual_dt,
mb.first_local_irb_iec_submission_bsln_dt,
mc.first_local_irb_iec_submission_planned_dt,
ma.first_local_irb_iec_submission_actual_dt,
mb.first_national_irb_iec_approval_bsln_dt,
mc.first_national_irb_iec_approval_planned_dt,
ma.first_national_irb_iec_approval_actual_dt,
mb.first_local_irb_iec_approval_bsln_dt,
mc.first_local_irb_iec_approval_planned_dt,
ma.first_local_irb_iec_approval_actual_dt,
mb.first_site_initiation_visit_bsln_dt,
mc.first_site_initiation_visit_planned_dt,
ma.first_site_initiation_visit_actual_dt,
'' as last_site_initiation_visit_bsln_dt,
'' as last_site_initiation_visit_planned_dt,
'' as last_site_initiation_visit_actual_dt,
mb.first_subject_first_visit_bsln_dt,
mc.first_subject_first_visit_planned_dt,
ma.first_subject_first_visit_actual_dt,
mb.first_subject_first_treatment_bsln_dt,
mc.first_subject_first_treatment_planned_dt,
ma.first_subject_first_treatment_actual_dt,
mb.last_subject_first_visit_bsln_dt,
mc.last_subject_first_visit_planned_dt,
ma.last_subject_first_visit_actual_dt,
mb.last_subject_first_treatment_bsln_dt,
mc.last_subject_first_treatment_planned_dt,
ma.last_subject_first_treatment_actual_dt,
mb.last_subject_last_treatment_bsln_dt,
mc.last_subject_last_treatment_planned_dt,
ma.last_subject_last_treatment_actual_dt,
mb.last_subject_last_visit_bsln_dt,
mc.last_subject_last_visit_planned_dt,
ma.last_subject_last_visit_actual_dt,
mb.study_database_lock_bsln_dt,
mc.study_database_lock_planned_dt,
ma.study_database_lock_actual_dt,
mb.last_data_received_bsln_dt,
mc.last_data_received_planned_dt,
ma.last_data_received_actual_dt,
'' as last_site_closed_bsln_dt,
'' as last_site_closed_planned_dt,
'' as last_site_closed_actual_dt,
mb.study_completion_bsln_dt,
mc.study_completion_planned_dt,
ma.study_completion_actual_dt,
sd.trial_age_min,
'ctms' as source,
14 as fsiv_fsfr_delta,
'' as enrollment_rate,
'' as screen_failure_rate,
'' as lost_opp_time,
'' as enrollment_duration
from study_details03 sd  left join patient_details_confirmed  pdc on lower(trim(sd.src_trial_id))= lower(trim(pdc.study_code))
                         left join patient_details_planned    pdp on  lower(trim(pdc.study_code)) = lower(trim(pdp.study_code))
                         left join df_milestone_actual_final  ma  on  lower(trim(pdp.study_code)) = lower(trim(ma.study_code))
                         left join df_milestone_baseline_final mb  on lower(trim(ma.study_code))  = lower(trim(mb.study_code))
                         left join df_milestone_current_final  mc on  lower(trim(mb.study_code))=lower(trim(mc.study_code))
                         left join df_milestone_ss_final       mss on  lower(trim(mc.study_code))=lower(trim(mss.study_code))

                         """)

ctms_refactoring_study_final_df = ctms_refactoring_study_final_df.dropDuplicates()
ctms_refactoring_study_final_df.registerTempTable('ctms_refactoring_study_final_df')
# ctms_refactoring_study_final_df.write.mode('overwrite').saveAsTable('ctms_refactoring_study_final_df')

# spark.sql(""" select src_trial_id, count(*) from ctms_refactoring_study_final_df group by 1 having count(*) >1""").show()


#################
ctms_refactoring_study_final_df = ctms_refactoring_study_final_df.dropDuplicates()
# ctms_refactoring_study_final_df.registerTempTable('ctms_refactoring_study_final_df')
ctms_refactoring_study_final_df.write.mode('overwrite').saveAsTable('usl_study')

# write_path = path.replace("table_name", "usl_study")
# ctms_refactoring_study_final_df.write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$env.usl_study partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   usl_study
""")

if ctms_refactoring_study_final_df.count() == 0:
    print("Skipping copy_hdfs_to_s3 for usl_study as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$env.usl_study")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
