
# ################################ Module Information ######################################
#  Module Name         : Investigator Site Study Fact Table
#  Purpose             : This will create below fact table for all KPI calculation
#                           a. f_trial_investigator
#  Pre-requisites      : Source table required:
#                           a. sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site
#                           b. sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site
#                           c. sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial
#                           d. sanofi_ctfo_datastore_staging_$$db_env.dqs_study
#                           e. country mapping file
#                           f. sanofi_ctfo_datastore_app_commons_$$db_env.d_trial
#  Last changed on     : 01-07-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
# ###########################################################################################

# ################################## High level Process #####################################
# 1. Create required table and stores data on HDFS
# ###########################################################################################
# ###########################################################################################
import datetime

from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from CommonUtils import CommonUtils
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

spark.conf.set("spark.sql.crossJoin.enabled", "true")

# spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version","2")

path = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/" \
       "commons/temp/kpi_output_dimension/table_name/" \
       "pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

standard_country_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/standard_country_mapping.csv")
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

customSchema = StructType([
    StructField("standard_country", StringType(), True),
    StructField("iso2_code", StringType(), True),
    StructField("iso3_code", StringType(), True),
    StructField("region", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("sanofi_cluster", StringType(), True),
    StructField("sanofi_csu", StringType(), True),
    StructField("post_trial_flag", StringType(), True),
    StructField("post_trial_details", StringType(), True)])

sanofi_cluster_pta_mapping = spark.read.format("csv").option("header", "false").option('multiline', 'True').schema(
    customSchema).load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
                       "uploads/sanofi_cluster_pta_mapping.csv")
sanofi_cluster_pta_mapping.createOrReplaceTempView("sanofi_cluster_pta_mapping")

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Others') as standard_country,scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.region_code,scpm.sanofi_cluster,scpm.sanofi_csu,scpm.post_trial_flag,
scpm.post_trial_details,country
from (select distinct(standard_country) as standard_country,country
    from standard_country_mapping )  cs
left join
        sanofi_cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

r_trial_inv_roles = spark.sql("""
select
    d.ctfo_trial_id, e.ctfo_site_id, e.ctfo_investigator_id
from
    sanofi_ctfo_datastore_app_commons_$$db_env.r_site_investigator e
inner join
    sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_investigator d
on e.ctfo_investigator_id = d.ctfo_investigator_id
inner join
    sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site f
on e.ctfo_site_id = f.ctfo_site_id and d.ctfo_trial_id = f.ctfo_trial_id
group by 1,2,3
""")
r_trial_inv_roles.createOrReplaceTempView("r_trial_inv_roles")
#r_trial_inv_roles.write.mode("overwrite").saveAsTable("r_trial_inv_roles")
r_trial_inv_roles.createOrReplaceTempView("r_trial_inv_roles")


golden_id_det_inv_2 = spark.sql("""select B.ctfo_trial_id, B.ctfo_investigator_id, C.data_src_nm, (src_investigator_id) as src_investigator_id
from
    r_trial_inv_roles B
inner join
    (select
        data_src_nm, ctfo_investigator_id,src_investigator_id
    from
        (select
            data_src_nm, ctfo_investigator_id, explode(split(src_investigator_id, ';')) as src_investigator_id
        from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_inv
        where data_src_nm in ('ir','ctms')
    ) group by 1,2,3) C
on B.ctfo_investigator_id = C.ctfo_investigator_id
group by 1,2,3,4
""")
golden_id_det_inv_2.createOrReplaceTempView("golden_id_det_inv_2")

golden_id_det_inv = spark.sql("""
select
    B.ctfo_trial_id,B.ctfo_investigator_id, B.data_src_nm,
    B.src_investigator_id,max(E.src_trial_id) as src_trial_id
from
    golden_id_det_inv_2 B
inner join
    (select data_src_nm, ctfo_trial_id, src_trial_id
    from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial where data_src_nm in ('ir','ctms')
    group by 1,2,3) E
on trim(B.ctfo_trial_id) = trim(E.ctfo_trial_id) and
lower(trim(B.data_src_nm)) = lower(trim(E.data_src_nm))
group by 1,2,3,4
""")
golden_id_det_inv = golden_id_det_inv.dropDuplicates()
golden_id_det_inv.createOrReplaceTempView("golden_id_det_inv")
#golden_id_det_inv.write.mode("overwrite").saveAsTable("golden_id_det_inv")

# Code for DQS Metrics Calculation
dqs_golden_source_map_inv = spark.sql("""
select
    ctfo_trial_id,  ctfo_investigator_id, data_src_nm, nct_number, member_study_id,
     max(B.person_golden_id) as person_golden_id,B.roles
from
    (select ctfo_trial_id, ctfo_investigator_id, data_src_nm,
    src_trial_id, src_investigator_id
    from golden_id_det_inv
    where lower(trim(data_src_nm)) = 'ir' group by 1,2,3,4,5) A
inner join
    (select nct_number, member_study_id,   person_golden_id,role as roles
    from sanofi_ctfo_datastore_staging_$$db_env.dqs_study
    group by 1,2,3,4) B
on trim(A.src_trial_id) = trim(B.member_study_id) and trim(A.src_investigator_id) = trim(B.person_golden_id)


group by 1,2,3,4,5,7
""")
dqs_golden_source_map_inv = dqs_golden_source_map_inv.dropDuplicates()
#dqs_golden_source_map_inv.createOrReplaceTempView("dqs_golden_source_map_inv")
dqs_golden_source_map_inv.write.mode("overwrite").saveAsTable("dqs_golden_source_map_inv")

centre_loc_map = spark.sql("""
select study_code as trial_no, (country_code) as country_code, study_country_id,
       center_id as centre_loc_id,primary_investigator_id
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
group by 1,2,3,4,5
""")
centre_loc_map = centre_loc_map.dropDuplicates()
centre_loc_map.createOrReplaceTempView("centre_loc_map")


ctms_golden_source_map_inv = spark.sql("""select cgsm_inv.* from (select A.ctfo_trial_id,
       A.ctfo_investigator_id,
       B.trial_no as study_code,
       A.src_investigator_id as src_investigator_id,
       B.primary_investigator_id as primary_investigator_id,
           B.study_country_id as study_country_id,
       row_number() over (partition by A.ctfo_trial_id,A.ctfo_investigator_id order by B.trial_no) as rnk
from (select ctfo_trial_id, ctfo_investigator_id, data_src_nm,
    src_trial_id, CAST(src_investigator_id as BIGINT) as src_investigator_id
    from golden_id_det_inv
    where lower(trim(data_src_nm)) = 'ctms' group by 1,2,3,4,5) A
inner join
    (select trial_no,
            centre_loc_id,CAST(primary_investigator_id as BIGINT) as primary_investigator_id,study_country_id from centre_loc_map group by 1,2,3,4) B
on lower(trim(A.src_trial_id)) = lower(trim(B.trial_no)) and lower(trim(A.src_investigator_id)) = lower(trim(B.primary_investigator_id))
group by 1,2,3,4,5,6) cgsm_inv where rnk=1
""")
ctms_golden_source_map_inv = ctms_golden_source_map_inv.dropDuplicates()
#ctms_golden_source_map_inv.createOrReplaceTempView("ctms_golden_source_map_inv")
ctms_golden_source_map_inv.write.mode("overwrite").saveAsTable("ctms_golden_source_map_inv")

ctms_entered_study_inv = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id,B.entered_treatment as entered_treatment
from
(select ctfo_trial_id, study_code, ctfo_investigator_id,src_investigator_id from ctms_golden_source_map_inv group by 1,2,3,4) cgsm
left join
(select sum(cpr.entered_treatment) as entered_treatment ,cpr.study_code,site_staff.person_id from
(select CAST(person_id as BIGINT) as person_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff where lower(trim(investigator)) = 'yes' group by 1) site_staff
left join
(select CAST(primary_investigator_id as BIGINT), study_code,study_site_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
group by 1,2,3) study_site
on site_staff.person_id = study_site.primary_investigator_id
left join
(select  a.study_code,a.entered_treatment as entered_treatment,a.study_site_id from
(select  study_code,study_site_id,entered_treatment,picture_as_of,study_period_number
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' group by 1,2,3,4,5) a
inner join
( select inq.study_site_id,study_code,study_period_number,picture_as_of  from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq  ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and (a.picture_as_of = d.picture_as_of OR (a.picture_as_of IS NULL AND d.picture_as_of IS NULL))
group by 1,2,3) cpr
on study_site.study_code=cpr.study_code and study_site.study_site_id=cpr.study_site_id group by 2,3 ) B
on cgsm.study_code=B.study_code and cgsm.src_investigator_id=B.person_id
group by 1,2,3""")
ctms_entered_study_inv = ctms_entered_study_inv.dropDuplicates()
#ctms_entered_study_inv.registerTempTable('ctms_entered_study_inv')
ctms_entered_study_inv.write.mode("overwrite").saveAsTable("ctms_entered_study_inv")

ctms_dates_inv = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id,
min(case when lower(a.milestone) = 'site initiation visit' then a.actual_date end) as siv_dt,
min(case when lower(a.milestone) = 'first subject first visit' then a.actual_date end) as fsfv_dt,
min(case when lower(a.milestone) = 'last subject first visit' then a.actual_date END) as lsfv_dt,
min(case when lower(b.milestone) = 'last subject first visit' then b.actual_date end) as cntry_lsfv_dt,
min(case when lower(a.milestone) = 'last subject first rando/treat' then a.actual_date end) as lsfr_dt,
min(case when lower(a.milestone) = 'first subject first rando/treat' then a.actual_date END) as fsfr_dt,
min(case when lower(a.milestone) = 'last subject last visit' then a.actual_date END) as lslv_dt,
min(case when lower(a.milestone) = 'last subject first rando/treat' then b.actual_date END) as cntry_lsfr_dt
from
(select ctfo_trial_id, study_code, ctfo_investigator_id,src_investigator_id,study_country_id from ctms_golden_source_map_inv group by 1,2,3,4,5) cgsm
left join
(select milestone,actual_date,study_code,study_country_id from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones group by 1,2,3,4) a
on cgsm.study_code = a.study_code and a.study_country_id = cgsm.study_country_id
left join
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones b on a.study_code = b.study_code and cgsm.study_country_id = b.study_country_id
group by 1,2""")
ctms_dates_inv = ctms_dates_inv.dropDuplicates()
#ctms_dates_inv.registerTempTable('ctms_dates_inv')
ctms_dates_inv.write.mode("overwrite").saveAsTable("ctms_dates_inv")

ctms_ppm_inv = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id,B.entered_treatment as ppm
from
(select ctfo_trial_id, study_code, ctfo_investigator_id,src_investigator_id from ctms_golden_source_map_inv group by 1,2,3,4) cgsm
left join
(select sum(cpr.entered_treatment) as entered_treatment ,cpr.study_code,site_staff.person_id from
(select CAST(person_id as BIGINT) as person_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff where lower(trim(investigator)) = 'yes' group by 1) site_staff
left join
(select CAST(primary_investigator_id as BIGINT), study_code,study_site_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
group by 1,2,3) study_site
on site_staff.person_id = study_site.primary_investigator_id
left join
(select  a.study_code,a.entered_treatment as entered_treatment,a.study_site_id from
(select  study_code,study_site_id,entered_treatment,picture_as_of,study_period_number
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'planned' group by 1,2,3,4,5) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of  from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'planned' having rnk=1)inq  ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and (a.picture_as_of = d.picture_as_of OR (a.picture_as_of IS NULL AND d.picture_as_of IS NULL))
group by 1,2,3) cpr
on study_site.study_code=cpr.study_code and study_site.study_site_id=cpr.study_site_id group by 2,3 ) B
on cgsm.study_code=B.study_code and cgsm.src_investigator_id=B.person_id
group by 1,2,3""")
ctms_ppm_inv = ctms_ppm_inv.dropDuplicates()
ctms_ppm_inv.registerTempTable('ctms_ppm_inv')
#ctms_ppm_inv.write.mode("overwrite").saveAsTable("ctms_ppm_inv")


ctms_percentage_randomized_inv = spark.sql(""" select a.ctfo_trial_id,a.ctfo_investigator_id ,a.entered_treatment/b.ppm as percentage_randomized
from
ctms_entered_study_inv a
left join
ctms_ppm_inv b on a.ctfo_trial_id = b.ctfo_trial_id and a.ctfo_investigator_id = b.ctfo_investigator_id
group by 1,2,3
""")
ctms_percentage_randomized_inv = ctms_percentage_randomized_inv.dropDuplicates()
#ctms_percentage_randomized_inv.registerTempTable('ctms_percentage_randomized_inv')
ctms_percentage_randomized_inv.write.mode("overwrite").saveAsTable("ctms_percentage_randomized_inv")

ctms_trial_status_inv = spark.sql("""select a.ctfo_trial_id,a.ctfo_investigator_id,a.trial_status from (select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id, c.trial_status,
row_number() over (partition by cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id order by c.trial_status asc ) as rnk from
(select ctfo_trial_id, study_code, ctfo_investigator_id,primary_investigator_id from ctms_golden_source_map_inv group by 1,2,3,4) cgsm
left join
(select study_site.study_code,study_site.trial_status,study_site.primary_investigator_id from
(select CAST(person_id as BIGINT) as person_id  from sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff where lower(trim(investigator)) = 'yes' group by 1) site_staff
left join (select CAST(primary_investigator_id as BIGINT) as primary_investigator_id , study_code,status as trial_status from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
where lower(trim(status)) in ( 'closed', 'last subject completed', 'discontinued', 'all sites closed','active', 'initiated', 'last subject enrolled', 'site ready to enrol','completed','active(recruiting)','active(non recruiting)','ongoing','recruiting')
  group by 1,2,3) study_site
on site_staff.person_id = study_site.primary_investigator_id
group by 1,2,3) c
on cgsm.study_code = c.study_code and cgsm.primary_investigator_id=c.primary_investigator_id
group by 1,2,3) as a where a.rnk=1""")
ctms_trial_status_inv = ctms_trial_status_inv.dropDuplicates()
ctms_trial_status_inv.registerTempTable('ctms_trial_status_inv')




ctms_elapsed_duration_inv = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id ,
min(case when lower(a.milestone) = 'last subject first rando/treat' then a.current_plan_date end) as lsfr_curr_dt,
min(case when lower(a.milestone) = 'first subject first visit' then a.current_plan_date end) as fsfv_curr_dt
from
(select ctfo_trial_id, study_code, ctfo_investigator_id,src_investigator_id from ctms_golden_source_map_inv group by 1,2,3,4) cgsm
left join
(select milestone,actual_date,study_code,study_country_id,study_id,current_plan_date from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones group by 1,2,3,4,5,6) a
on cgsm.study_code = a.study_code
group by 1,2""")
ctms_elapsed_duration_inv = ctms_elapsed_duration_inv.dropDuplicates()
ctms_elapsed_duration_inv.registerTempTable('ctms_elapsed_duration_inv')
#ctms_elapsed_duration_inv.write.mode("overwrite").saveAsTable("ctms_elapsed_duration_inv")




ctms_randomiztion_duration_inv = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id ,
case
when lower(trim(b.trial_status)) in ('active', 'initiated', 'last subject enrolled', 'site ready to enrol','active(recruiting)','active(non recruiting)','ongoing','recruiting')
then
case when d.siv_dt is not null and d.lsfr_dt is not null then datediff(d.lsfr_dt,d.siv_dt)/30.4
when d.siv_dt is null and d.fsfr_dt is not null and d.lsfr_dt is not null then datediff(d.lsfr_dt,date_sub(d.fsfr_dt,14))/30.4
when d.lsfr_dt is null and e.percentage_randomized>0.5 and (d.siv_dt is not null or d.fsfr_dt is not null) then datediff(current_date(),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14)))/30.4
when d.lsfr_dt is null and e.percentage_randomized<0.5 and (d.siv_dt is not null or d.fsfr_dt is not null) and (datediff(ced.lsfr_curr_dt,ced.fsfv_curr_dt))/30.4 > 0.5 then (datediff(current_date(),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14))))/30.4
else null
end
when lower(trim(b.trial_status)) in ( 'closed', 'last subject completed', 'discontinued', 'all sites closed','completed')
then  case when d.siv_dt is null then datediff(d.lsfr_dt,COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14)))/30.4
when d.siv_dt is not null and d.lsfr_dt is not null then datediff(d.lsfr_dt,d.siv_dt)/30.4
when d.siv_dt is null and d.fsfr_dt is not null and d.lsfr_dt is not null then datediff(d.lsfr_dt,date_sub(d.fsfr_dt,14))/30.4
when d.lsfr_dt is null and (d.siv_dt is not null or d.fsfr_dt is not null) and d.cntry_lsfr_dt is not null then datediff(d.cntry_lsfr_dt,COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14)))/30.4
end
else null
end as duration
from
(select ctfo_trial_id, study_code, ctfo_investigator_id,src_investigator_id from ctms_golden_source_map_inv group by 1,2,3,4) cgsm
left join ctms_dates_inv d on  cgsm.ctfo_trial_id = d.ctfo_trial_id and cgsm.ctfo_investigator_id = d.ctfo_investigator_id
left join ctms_elapsed_duration_inv ced on cgsm.ctfo_trial_id = ced.ctfo_trial_id and cgsm.ctfo_investigator_id = ced.ctfo_investigator_id
left join ctms_percentage_randomized_inv e on cgsm.ctfo_trial_id = e.ctfo_trial_id and cgsm.ctfo_investigator_id = e.ctfo_investigator_id
left join ctms_trial_status_inv b on cgsm.ctfo_trial_id = b.ctfo_trial_id and cgsm.ctfo_investigator_id = b.ctfo_investigator_id
group by 1,2,3""")
ctms_randomiztion_duration_inv = ctms_randomiztion_duration_inv.dropDuplicates()
#ctms_randomiztion_duration_inv.registerTempTable('ctms_randomiztion_duration_inv')
ctms_randomiztion_duration_inv.write.mode("overwrite").saveAsTable("ctms_randomiztion_duration_inv")


ctms_randomization_rate_inv = spark.sql("""
select a.ctfo_trial_id,a.ctfo_investigator_id, b.entered_treatment/a.duration as randomization_rate
from
ctms_randomiztion_duration_inv a
left join ctms_entered_study_inv b on a.ctfo_trial_id = b.ctfo_trial_id and a.ctfo_investigator_id = b.ctfo_investigator_id
group by 1,2,3
""")

ctms_randomization_rate_inv = ctms_randomization_rate_inv.dropDuplicates()
#ctms_randomization_rate_inv.registerTempTable('ctms_randomization_rate_inv')
ctms_randomization_rate_inv.write.mode("overwrite").saveAsTable("ctms_randomization_rate_inv")




#### Screen Failure rate
ctms_sfr_inv = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id,
case when lower(trim(B.status)) in ('active','initiated','last subject enrolled','site ready to enrol','selected','ongoing','recruiting')
             and lower(trim(B.milestone)) in  ('last subject first visit')
        then (case when  ((B.DROPPED_AT_SCREENING is not null) and (B.entered_study is not null)) then (B.DROPPED_AT_SCREENING)/(B.entered_study)
                  when  (B.DROPPED_AT_SCREENING == 0.0) and (B.entered_study>0.0) then null
                  when  (B.DROPPED_AT_SCREENING > 0.0) and (B.entered_study ==0.0) then 100.00
                  when  (B.DROPPED_AT_SCREENING == 0.0) and (B.entered_study ==0.0) then null
             end)
     when lower(trim(B.status)) in ('closed', 'last subject completed', 'discontinued')
        then (case when  B.DROPPED_AT_SCREENING is not null and B.entered_study is not null then (B.DROPPED_AT_SCREENING)/(B.entered_study)
                  when  B.DROPPED_AT_SCREENING == 0.0 and B.entered_study>0.0 then null
                  when  B.DROPPED_AT_SCREENING > 0.0 and B.entered_study==0.0 then 100.00
                  when  B.DROPPED_AT_SCREENING == 0.0 and B.entered_study==0.0 then null
             end)
end as ctms_screen_fail_rate from
(select ctfo_trial_id, study_code, ctfo_investigator_id,src_investigator_id from ctms_golden_source_map_inv group by 1,2,3,4) cgsm
left join
(select final.* from (select inner_block.*,row_number() over (partition by person_id,study_code order by entered_study desc) as rnk from (select study_site.status,cpr.study_code,mile.milestone,site_staff.person_id,sum(cpr.DROPPED_AT_SCREENING) as DROPPED_AT_SCREENING ,sum(cpr.entered_study) as entered_study  from
(select CAST(person_id as BIGINT) from sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff where lower(trim(investigator)) = 'yes' group by 1) site_staff
left join
(select CAST(primary_investigator_id as BIGINT) as primary_investigator_id,study_code,status,study_site_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
where lower(trim(status)) in ('active','initiated','last subject enrolled','site ready to enrol','selected','closed', 'last subject completed','discontinued','ongoing','recruiting')
group by 1,2,3,4) study_site
on site_staff.person_id = study_site.primary_investigator_id
left join
(select study_code,milestone from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones
  where lower(trim(milestone)) in ('last subject first visit') group by 1,2 ) mile
on study_site.study_code=mile.study_code
left join
(select  a.study_code,a.entered_study as entered_study,a.study_site_id,((a.entered_study)-(a.entered_treatment)) as DROPPED_AT_SCREENING from
(select  study_code,study_site_id,entered_treatment,entered_study,picture_as_of,study_period_number
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'planned' group by 1,2,3,4,5,6) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of  from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'planned' having rnk=1)inq  ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and (a.picture_as_of = d.picture_as_of OR (a.picture_as_of IS NULL AND d.picture_as_of IS NULL))
group by 1,2,3,4) cpr
on study_site.study_code=cpr.study_code and study_site.study_site_id=cpr.study_site_id group by 1,2,3,4) as inner_block) as final where rnk=1) B
on cgsm.study_code=B.study_code and cgsm.src_investigator_id=B.person_id
group by 1,2,3""")
ctms_sfr_inv = ctms_sfr_inv.dropDuplicates()
#ctms_sfr_inv.registerTempTable('ctms_sfr_inv')
ctms_sfr_inv.write.mode("overwrite").saveAsTable("ctms_sfr_inv")





dqs_status_value = spark.sql("""select nct_number,person_golden_id,country,site_status,
        case when lower(trim(site_status)) in ('active(not recruiting)','active (recruiting)','ongoing','recruiting') then 'ongoing'
             when lower(trim(site_status)) in ('completed','closed') then 'completed'
        end as status_value from  sanofi_ctfo_datastore_staging_$$db_env.dqs_study where lower(trim(site_status)) in ('completed','closed','ongoing', 'recruiting', 'active(not recruiting)','active (recruiting)') group by 1,2,3,4""")
dqs_status_value.registerTempTable("dqs_status_value")

trial_inv_status = spark.sql("""select nct_number,person_golden_id,country,site_status,status_value,
        case when lower(trim(status_value))='ongoing' then 1
        when lower(trim(status_value))='completed' then 2 end as final_rnk from  dqs_status_value""")
trial_inv_status.registerTempTable("trial_inv_status")

dqs_site_inv_details = spark.sql("""select inner_q.* from  (SELECT ctfo_trial_id,
ctfo_investigator_id,tis.site_status,tis.final_rnk,ROW_NUMBER() OVER (PARTITION BY ctfo_trial_id,ctfo_investigator_id ORDER BY tis.final_rnk ) pred_stat,
First(case when trim(lower(pct_screen_fail)) like '%\%%' then regexp_replace(pct_screen_fail,"\%","") else pct_screen_fail end) as pct_screen_fail,
sum(enrolled)                AS patients_randomized,
Min(first_subject_consented_dt)           AS first_subject_enrolled_dt,
Min(first_subject_enrolled_dt)            AS first_subject_randomized_dt,
Max(last_subject_consented_dt)            AS last_subject_enrolled_dt,
Max(last_subject_enrolled_dt)             AS last_subject_randomized_dt,
Max(last_subject_last_visit_dt)           AS last_subject_last_visit_dt,
Min(site_open_dt)                         AS site_ready_to_enroll_dt,
first(coalesce(E.standard_country,B.country))          AS xref_country
FROM   dqs_golden_source_map_inv A
LEFT JOIN sanofi_ctfo_datastore_staging_$$db_env.dqs_study B
       ON Lower(Trim(A.nct_number)) = Lower(Trim(B.nct_number))
        AND lower(trim(A.person_golden_id)) = lower(trim(B.person_golden_id))
LEFT JOIN trial_inv_status tis
       ON Lower(Trim(B.nct_number)) = Lower(Trim(tis.nct_number))
        AND lower(trim(B.person_golden_id)) = lower(trim(tis.person_golden_id))
        AND Lower(Trim(B.country)) =Lower(Trim(tis.country))
left join
    country_mapping E on lower(trim(B.country)) = lower(trim(E.country))
where lower(trim(B.site_status)) in ('completed','closed','ongoing', 'recruiting', 'active(not recruiting)','active (recruiting)') GROUP  BY 1,2,3,4) as inner_q where pred_stat=1  """)
dqs_site_inv_details = dqs_site_inv_details.dropDuplicates()
dqs_site_inv_details.createOrReplaceTempView("dqs_site_inv_details")




dqs_site_inv_dates = spark.sql("""
select
    ctfo_trial_id, ctfo_investigator_id,
    min(first_subject_consented_dt) as first_subject_enrolled_dt,
    min(first_subject_enrolled_dt) as first_subject_randomized_dt,
    max(last_subject_consented_dt) as last_subject_enrolled_dt,
    max(last_subject_enrolled_dt) as last_subject_randomized_dt,
    max(last_subject_last_visit_dt) as last_subject_last_visit_dt ,
    min(site_open_dt) as site_ready_to_enroll_dt,
    first(B.country) as country
from
    dqs_golden_source_map_inv A
left join
    sanofi_ctfo_datastore_staging_$$db_env.dqs_study B
on lower(trim(A.nct_number)) = lower(trim(B.nct_number))
and lower(trim(A.person_golden_id)) = lower(trim(B.person_golden_id))
group by 1,2""")
dqs_site_inv_dates = dqs_site_inv_dates.dropDuplicates()
dqs_site_inv_dates.createOrReplaceTempView("dqs_site_inv_dates")




dqs_study_fsi = spark.sql("""
select
    ctfo_trial_id,
    min(first_subject_consented_dt) as dqs_study_fsi_dt
from
    (select ctfo_trial_id, nct_number,person_golden_id from dqs_golden_source_map_inv group by 1,2,3) A
left join
    (select nct_number, first_subject_consented_dt,person_golden_id
    from sanofi_ctfo_datastore_staging_$$db_env.dqs_study group by 1,2,3) B
on lower(trim(A.nct_number)) = lower(trim(B.nct_number)) and lower(trim(A.person_golden_id)) = lower(trim(B.person_golden_id))
group by 1""")
dqs_study_fsi = dqs_study_fsi.dropDuplicates()
dqs_study_fsi.createOrReplaceTempView("dqs_study_fsi")

dqs_country_lsi = spark.sql("""
select
    ctfo_trial_id, B.country,
    max(last_subject_enrolled_dt) as dqs_country_lsi_dt
from
    (select ctfo_trial_id, nct_number, person_golden_id
    from dqs_golden_source_map_inv group by 1,2,3) A
left join
    (select nct_number, country, last_subject_enrolled_dt,person_golden_id
    from sanofi_ctfo_datastore_staging_$$db_env.dqs_study group by 1,2,3,4) B
on lower(trim(A.nct_number)) = lower(trim(B.nct_number)) and lower(trim(A.person_golden_id)) = lower(trim(B.person_golden_id))
group by 1,2""")
dqs_country_lsi = dqs_country_lsi.dropDuplicates()
dqs_country_lsi.createOrReplaceTempView("dqs_country_lsi")

dqs_randomization_rate_temp = spark.sql("""select A.ctfo_trial_id,A.ctfo_investigator_id ,b.nct_number ,Max(b.last_subject_enrolled_dt) as cntry_last_subject_randomized_dt
from dqs_golden_source_map_inv A
left join sanofi_ctfo_datastore_staging_$$db_env.dqs_study B
ON Lower(Trim(A.nct_number)) = Lower(Trim(B.nct_number))  and lower(trim(A.person_golden_id)) = lower(trim(B.person_golden_id)) group by 1,2,3""")
dqs_randomization_rate_temp.createOrReplaceTempView("dqs_randomization_rate_temp")

dqs_randomization_rate_inv = spark.sql("""select C.ctfo_trial_id,C.ctfo_investigator_id,C.recruitment_duration,(C.patients_randomized/C.recruitment_duration) as dqs_randomization_rate from (select A.ctfo_trial_id,A.ctfo_investigator_id, A.patients_randomized,
case when A.site_ready_to_enroll_dt is not null and A.first_subject_randomized_dt is not null then
case when lower(trim(A.trial_status)) in ('completed','closed')
then
case when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt), A.site_ready_to_enroll_dt)/30.4
when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt), A.first_subject_randomized_dt)/30.4
when A.first_subject_randomized_dt is null
then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt), A.site_ready_to_enroll_dt)/30.4
when A.site_ready_to_enroll_dt is null
then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt), A.first_subject_randomized_dt)/30.4
end
when lower(trim(A.trial_status)) in ('ongoing', 'recruiting', 'active(not recruiting)','active (recruiting)')
then
case
when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
then datediff(coalesce(A.last_subject_randomized_dt,current_date()), A.site_ready_to_enroll_dt)/30.4
when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt then datediff(A.first_subject_randomized_dt,coalesce(A.last_subject_randomized_dt,current_date()))

when A.first_subject_randomized_dt is null then datediff(coalesce(A.last_subject_randomized_dt,current_date()), A.site_ready_to_enroll_dt)/30.4
when A.site_ready_to_enroll_dt is null then datediff(coalesce(A.last_subject_randomized_dt,current_date()), A.first_subject_randomized_dt)/30.4
end
end
else null end as recruitment_duration
from
(select ctfo_trial_id,ctfo_investigator_id, patients_randomized, xref_country,site_ready_to_enroll_dt,first_subject_randomized_dt,last_subject_randomized_dt,site_status as trial_status  from dqs_site_inv_details group by 1,2,3,4,5,6,7,8) A
left join dqs_randomization_rate_temp B ON lower(Trim(A.ctfo_trial_id)) = lower(Trim(B.ctfo_trial_id)) and lower(Trim(A.ctfo_investigator_id)) = lower(Trim(B.ctfo_investigator_id))
 group by 1,2,3,4) C
group by 1,2,3,4""")
dqs_randomization_rate_inv = dqs_randomization_rate_inv.dropDuplicates()
#dqs_randomization_rate_inv.createOrReplaceTempView("dqs_randomization_rate_inv")
dqs_randomization_rate_inv.write.mode("overwrite").saveAsTable("dqs_randomization_rate_inv")

'''
dqs_randomization_rate_inv = spark.sql("""select C.ctfo_trial_id,C.ctfo_investigator_id,C.recruitment_duration,(C.patients_randomized/C.recruitment_duration) as dqs_randomization_rate from (select A.ctfo_trial_id,A.ctfo_investigator_id, A.patients_randomized,
case when A.site_ready_to_enroll_dt is not null and A.first_subject_randomized_dt is not null then
case when lower(trim(A.trial_status)) in ('completed','closed')
then
case when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt), A.site_ready_to_enroll_dt)/30.4
when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt), A.first_subject_randomized_dt)/30.4
end
when A.first_subject_randomized_dt is null
then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt), A.site_ready_to_enroll_dt)/30.4
when A.site_ready_to_enroll_dt is null
then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt), A.first_subject_randomized_dt)/30.4
when lower(trim(A.trial_status)) in ('ongoing', 'recruiting', 'active(not recruiting)','active (recruiting)')
then
case
when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
then datediff(coalesce(A.last_subject_randomized_dt,current_date()), A.site_ready_to_enroll_dt)/30.4
when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt then datediff(A.first_subject_randomized_dt,coalesce(A.last_subject_randomized_dt,current_date()))
end
when A.first_subject_randomized_dt is null then datediff(coalesce(A.last_subject_randomized_dt,current_date()), A.site_ready_to_enroll_dt)/30.4
when A.site_ready_to_enroll_dt is null then datediff(coalesce(A.last_subject_randomized_dt,current_date()), A.first_subject_randomized_dt)/30.4
end
else null end as recruitment_duration
from
(select ctfo_trial_id,ctfo_investigator_id, patients_randomized, xref_country,site_ready_to_enroll_dt,first_subject_randomized_dt,last_subject_randomized_dt,site_status as trial_status  from dqs_site_inv_details group by 1,2,3,4,5,6,7,8) A
left join dqs_randomization_rate_temp B ON lower(Trim(A.ctfo_trial_id)) = lower(Trim(B.ctfo_trial_id)) and lower(Trim(A.ctfo_investigator_id)) = lower(Trim(B.ctfo_investigator_id))
 group by 1,2,3,4) C
group by 1,2,3,4""")
dqs_randomization_rate_inv = dqs_randomization_rate_inv.dropDuplicates()
#dqs_randomization_rate_inv.createOrReplaceTempView("dqs_randomization_rate_inv")
dqs_randomization_rate_inv.write.mode("overwrite").saveAsTable("dqs_randomization_rate_inv")
'''



dqs_metrics_inv = spark.sql("""
select
    A.ctfo_trial_id, A.ctfo_investigator_id,
    cm.iso2_code as country_code, F.site_status as study_status,
    B.dqs_study_fsi_dt, dqs_country_lsi_dt, E.first_subject_enrolled_dt,
    E.first_subject_randomized_dt, E.last_subject_enrolled_dt, E.last_subject_randomized_dt,
    E.site_ready_to_enroll_dt,
    F.pct_screen_fail as dqs_screen_fail_rate,
    drr.dqs_randomization_rate as dqs_randomization_rate,
    (datediff((case when E.last_subject_randomized_dt is not null then
     E.last_subject_randomized_dt
    when E.last_subject_randomized_dt is null and lower(trim(F.site_status)) in ("active (recruiting)",
        "active (not recruiting)") then current_date()
    else C.dqs_country_lsi_dt end), coalesce(E.site_ready_to_enroll_dt,
    date_sub(E.first_subject_enrolled_dt, 14)))/30.4) as enrollment_duration,
    row_number() over (partition by A.ctfo_trial_id, A.ctfo_investigator_id order by cm.iso2_code desc ) as cntry_dup_rnk
from
dqs_golden_source_map_inv A
left join dqs_site_inv_details F on A.ctfo_trial_id = F.ctfo_trial_id  and A.ctfo_investigator_id = F.ctfo_investigator_id
left join dqs_randomization_rate_inv drr  on A.ctfo_trial_id = drr.ctfo_trial_id  and A.ctfo_investigator_id = drr.ctfo_investigator_id
left join dqs_site_inv_dates E on A.ctfo_trial_id = E.ctfo_trial_id and A.ctfo_investigator_id = E.ctfo_investigator_id
left join dqs_study_fsi B on A.ctfo_trial_id = B.ctfo_trial_id
left join dqs_country_lsi C on A.ctfo_trial_id = C.ctfo_trial_id and lower(trim(F.xref_country)) = lower(trim(C.country))
left join (select scm.country,scpm.iso2_code from standard_country_mapping scm left join sanofi_cluster_pta_mapping scpm on scm.standard_country=scpm.standard_country ) cm
on lower(trim(F.xref_country)) = lower(trim(cm.country))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14""")
dqs_metrics_inv = dqs_metrics_inv.dropDuplicates()
#dqs_metrics_inv.createOrReplaceTempView("dqs_metrics_inv")
dqs_metrics_inv.write.mode("overwrite").saveAsTable("dqs_metrics_inv")

# Write DQS Metrics to S3
write_path = path.replace("table_name", "dqs_metrics_inv")
dqs_metrics_inv.write.format("parquet").mode("overwrite").save(path=write_path)

ctms_metric_inv = spark.sql("""
SELECT
    A.ctfo_trial_id,
    A.ctfo_investigator_id,
    C.fsfv_dt AS ctms_study_fsi_dt,
    C.lsfv_dt AS ctms_country_lsi_dt,
    C.fsfv_dt AS first_subject_enrolled_dt,
    C.fsfr_dt AS first_subject_randomized_dt,
    C.lsfv_dt AS last_subject_enrolled_dt,
    C.lsfr_dt AS last_subject_randomized_dt,
    C.siv_dt AS site_ready_to_enroll_dt,
    D.trial_status as trial_status,
    cs.ctms_screen_fail_rate as ctms_screen_fail_rate,
    crr.randomization_rate as ctms_randomization_rate,
    I.duration as ctms_enrollment_duration
from
(select ctfo_trial_id, study_code,ctfo_investigator_id from ctms_golden_source_map_inv group by 1,2,3) A
LEFT JOIN ctms_entered_study_inv B on A.ctfo_trial_id = B.ctfo_trial_id and A.ctfo_investigator_id = B.ctfo_investigator_id
LEFT JOIN ctms_dates_inv C on A.ctfo_trial_id = C.ctfo_trial_id and A.ctfo_investigator_id = C.ctfo_investigator_id
LEFT JOIN ctms_trial_status_inv D on A.ctfo_trial_id = D.ctfo_trial_id and A.ctfo_investigator_id = D.ctfo_investigator_id
left join ctms_randomiztion_duration_inv I on A.ctfo_trial_id = I.ctfo_trial_id and A.ctfo_investigator_id = I.ctfo_investigator_id
left join ctms_sfr_inv cs on A.ctfo_trial_id = cs.ctfo_trial_id and A.ctfo_investigator_id = cs.ctfo_investigator_id
left join ctms_randomization_rate_inv crr  on A.ctfo_trial_id = crr.ctfo_trial_id and A.ctfo_investigator_id = crr.ctfo_investigator_id
left join
    (select study_code, concat_ws('\;',collect_set(milestone)) as milestone from
    sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones group by 1) mile
on A.study_code = mile.study_code
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13""")
ctms_metric_inv = ctms_metric_inv.dropDuplicates()
#ctms_metric_inv.createOrReplaceTempView("ctms_metric_inv")
ctms_metric_inv.write.mode("overwrite").saveAsTable("ctms_metric_inv")

# Write CTMS Metrics to S3
write_path = path.replace("table_name", "ctms_metric_inv")
ctms_metric_inv.write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("select ctfo_trial_id,ctfo_investigator_id,count(*) from ctms_metric_inv group by 1,2 having count(*)>1").show()


#  Code for Union Table
# In BRD rule for DQS  enrollment_rate and enrollment_duration is not specified so therefore Null
dqs_union_inv = spark.sql("""
select
    'ir' as datasource, ctfo_trial_id,  ctfo_investigator_id,
    study_status,
    cast(regexp_replace(dqs_randomization_rate,'-0.0','0.0') as double) as randomization_rate,
    cast(enrollment_duration as double) as enrollment_duration,
    dqs_screen_fail_rate as screen_failure_rate, last_subject_randomized_dt as last_subject_in_dt,
    site_ready_to_enroll_dt as ready_to_enroll_dt
from dqs_metrics_inv where cntry_dup_rnk=1
""")
dqs_union_inv = dqs_union_inv.dropDuplicates()
#dqs_union_inv.createOrReplaceTempView("dqs_union_inv")
dqs_union_inv.write.mode("overwrite").saveAsTable("dqs_union_inv")

#  Code for Union Table
ctms_union_inv = spark.sql("""
select
    'ctms' as datasource, ctfo_trial_id,  ctfo_investigator_id,
    trial_status as study_status,
    cast(regexp_replace(ctms_randomization_rate,'-0.0','0.0') as double) as randomization_rate,
    cast(ctms_enrollment_duration as double) as enrollment_duration,
    ctms_screen_fail_rate as screen_failure_rate, last_subject_randomized_dt as last_subject_in_dt,
    site_ready_to_enroll_dt as ready_to_enroll_dt
from ctms_metric_inv where
""")
ctms_union_inv = ctms_union_inv.dropDuplicates()
#ctms_union_inv.createOrReplaceTempView("ctms_union_inv")
ctms_union_inv.write.mode("overwrite").saveAsTable("ctms_union_inv")

df_union_final = spark.sql("""
select * from dqs_union_inv
union
select * from ctms_union_inv""")
df_union_final = df_union_final.dropDuplicates()
df_union_final.write.mode("overwrite").saveAsTable("df_union_final_inv")
#df_union_final.createOrReplaceTempView("df_union_final")


# Write Union Table To S3
write_path = path.replace("table_name", "all_kpi_union_inv")
df_union_final.write.format("parquet").mode("overwrite").save(path=write_path)

# Fact Table Calculations
read_path = path.replace("table_name", "all_kpi_union_inv")
df_union_final = spark.read.parquet(read_path)
df_union_final.createOrReplaceTempView("df_union_final")

f_investigator_trial_site_temp = spark.sql("""
select
    ctfo_trial_id,
    ctfo_investigator_id,
    datasource as datasource_nm,
    last_subject_in_dt,
    ready_to_enroll_dt,
    CASE
         WHEN cast(screen_failure_rate as double) < 0 THEN null
         ELSE cast(screen_failure_rate as double)
       END        AS screen_failure_rate,
    CASE
         WHEN cast(randomization_rate as double) < 0 THEN null
         ELSE cast(randomization_rate as double)
       END        AS randomization_rate,
    CASE
         WHEN cast(enrollment_duration as double) < 0 THEN null
         ELSE cast(enrollment_duration as double)
       END        AS total_recruitment_months
from df_union_final
group by 1,2,3,4,5,6,7,8
""")
f_investigator_trial_site_temp = f_investigator_trial_site_temp.dropDuplicates()
#f_investigator_trial_site_temp.createOrReplaceTempView("f_investigator_trial_site_temp")
f_investigator_trial_site_temp.write.mode("overwrite").saveAsTable("f_investigator_trial_site_temp")

# Write Final Table f_investigator_trial_site to S3
write_path = path.replace("table_name", "f_investigator_trial_site_temp")
f_investigator_trial_site_temp.write.format("parquet").mode("overwrite").save(path=write_path)

f_investigator_trial_site_prec = spark.sql("""
select
    union_table_inv.cctfo_trial_id as cctfo_trial_id,
    union_table_inv.ctfo_investigator_id as ctfo_investigator_id_prec,
    union_table_inv.datasource_nm as datasource_nm_prec,
    COALESCE(impact.last_subject_in_dt,dqs.last_subject_in_dt) as last_subject_in_dt_prec,
    COALESCE(impact.ready_to_enroll_dt,dqs.ready_to_enroll_dt) as ready_to_enroll_dt_prec,
    COALESCE(impact.screen_failure_rate,dqs.screen_failure_rate) as screen_fail_rate_prec,
    COALESCE(impact.randomization_rate,dqs.randomization_rate) as randomization_rate_prec,
    COALESCE(impact.total_recruitment_months,dqs.total_recruitment_months) as total_recruitment_months_prec
from
(select ctfo_trial_id as cctfo_trial_id,ctfo_investigator_id,datasource_nm from
f_investigator_trial_site_temp group by 1,2,3) union_table_inv
left join
(select ctfo_trial_id, ctfo_investigator_id, datasource_nm,last_subject_in_dt,ready_to_enroll_dt,
screen_failure_rate,total_recruitment_months,randomization_rate
from f_investigator_trial_site_temp where lower(datasource_nm)='ctms' group by 1,2,3,4,5,6,7,8) impact
on union_table_inv.cctfo_trial_id=impact.ctfo_trial_id  and union_table_inv.ctfo_investigator_id=impact.ctfo_investigator_id
left join
(select ctfo_trial_id, ctfo_investigator_id, datasource_nm,last_subject_in_dt,ready_to_enroll_dt,
screen_failure_rate,total_recruitment_months,randomization_rate
from f_investigator_trial_site_temp where lower(datasource_nm)='ir' group by 1,2,3,4,5,6,7,8) dqs
on union_table_inv.cctfo_trial_id=dqs.ctfo_trial_id and union_table_inv.ctfo_investigator_id=dqs.ctfo_investigator_id
""")
f_investigator_trial_site_prec = f_investigator_trial_site_prec.dropDuplicates()
#f_investigator_trial_site_prec.createOrReplaceTempView('f_investigator_trial_site_prec')
f_investigator_trial_site_prec.write.mode("overwrite").saveAsTable("f_investigator_trial_site_prec")

read_path = path.replace("table_name", "f_investigator_trial_site_temp")
f_investigator_trial_site_temp = spark.read.parquet(read_path)
f_investigator_trial_site_temp.createOrReplaceTempView("f_investigator_trial_site")

f_investigator_trial_site_final = spark.sql("""select * from f_investigator_trial_site left join f_investigator_trial_site_prec
on f_investigator_trial_site.ctfo_trial_id=f_investigator_trial_site_prec.cctfo_trial_id  and f_investigator_trial_site.ctfo_investigator_id=f_investigator_trial_site_prec.ctfo_investigator_id_prec """).drop(
    "cctfo_trial_id").drop("ctfo_investigator_id_prec").drop("datasource_nm_prec")
#f_investigator_trial_site_final.createOrReplaceTempView("f_investigator_trial_site_final")
f_investigator_trial_site_final.write.mode("overwrite").saveAsTable("f_investigator_trial_site_final")

# Write Final Table f_investigator_trial_site to S3
write_path = path.replace("table_name", "f_investigator_trial_site_final")
f_investigator_trial_site_final.write.format("parquet").mode("overwrite").save(path=write_path)

# Write to HDFS
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_commons_$$db_env.f_investigator_site_study_details
partition(pt_data_dt,pt_cycle_id)
select
    ctfo_investigator_id,
    ctfo_trial_id,
    datasource_nm as data_src_nm,
    last_subject_in_dt,
    last_subject_in_dt_prec,
    ready_to_enroll_dt,
    ready_to_enroll_dt_prec,
    screen_failure_rate,
    screen_fail_rate_prec,
    randomization_rate,
    randomization_rate_prec,
    total_recruitment_months,
    total_recruitment_months_prec,
    "$$data_dt" as pt_data_dt,
    "$$cycle_id" as pt_cycle_id
from f_investigator_trial_site_final
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")


# Closing spark context
try:
    print("Closing spark context")
except:
    print("Error while closing spark context")

CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_commons_$$db_env."
                              "f_investigator_site_study_details")




