
# ################################ Module Information ######################################
#  Module Name         : Site Study Fact Table
#  Purpose             : This will create below fact table for all KPI calculation
#                           a. f_trial_site
#  Pre-requisites      : Source table required:
#                           a. sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site
#                           b. sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site
#                           c. sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial
#                           d. sanofi_ctfo_datastore_staging_$$db_env.irt_tu_location
#                           e. sanofi_ctfo_datastore_staging_$$db_env.irt_trial
#                           f. sanofi_ctfo_datastore_staging_$$db_env.irt_trial_unit
#                           g. sanofi_ctfo_datastore_staging_$$db_env.irt_tu_additional_info
#                           h. sanofi_ctfo_datastore_staging_$$db_env.irt_tu_issue
#                           i. sanofi_ctfo_datastore_staging_$$db_env.irt_tu_event
#                           j. sanofi_ctfo_datastore_staging_$$db_env.irt_tr_event
#                           k. sanofi_ctfo_datastore_staging_$$db_env.irt_trc_event
#                           l. sanofi_ctfo_datastore_staging_$$db_env.ixrs_centre
#                           m. sanofi_ctfo_datastore_staging_$$db_env.ixrs_subject
#                           n. sanofi_ctfo_datastore_staging_$$db_env.dqs_study
#                           o. country mapping file
#                           p. sanofi_ctfo_datastore_app_commons_$$db_env.d_trial
#                           q. country timelines file
#  Last changed on     : 01-07-2021
#  Last changed by     : Sankarshana Kadambari
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
from pyspark.sql.types import StructType, StructField, IntegerType,StringType

data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

spark.conf.set("spark.sql.crossJoin.enabled", "true")

# spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version","2")

path = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/" \
       "commons/temp/kpi_output_dimension/table_name/" \
       "pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

# import country_mapping & irt_tr_event
# geo_mapping = spark.read.format("csv").option("header", "true") \
#     .load("s3://aws-a0199-use1-00-d-s3b-snfi-ctf-data01/clinical-data-lake/uploads/"
#           "country_standardization.csv")
# geo_mapping.createOrReplaceTempView("country_mapping")

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
    StructField("post_trial_details", StringType(), True)    ])

sanofi_cluster_pta_mapping = spark.read.format("csv").option("header", "false").option('multiline','True').schema(customSchema).load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/sanofi_cluster_pta_mapping.csv")
sanofi_cluster_pta_mapping.createOrReplaceTempView("sanofi_cluster_pta_mapping")

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Others') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.region_code,scpm.sanofi_cluster,scpm.sanofi_csu,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country, country
    from standard_country_mapping )  cs
left join
        sanofi_cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/'
          'uploads/trial_status.csv')
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')

# Start Metrics Calculation

# Extract Golden ID and Source ID Details
golden_id_det = spark.sql("""
SELECT B.ctfo_trial_id,
       C.ctfo_site_id,
       B.data_src_nm,
       src_trial_id,
       src_site_country,
       src_site_id
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id
        FROM   sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site
        GROUP  BY 1,
                  2) A
       LEFT JOIN (SELECT data_src_nm,
                         ctfo_trial_id,
                         src_trial_id
                  FROM
sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial
                  GROUP  BY 1,
                            2,
                            3) B
              ON A.ctfo_trial_id = B.ctfo_trial_id
       LEFT JOIN (SELECT data_src_nm,
                         ctfo_site_id,
                         src_site_country,
                         src_site_id
                  FROM   (SELECT data_src_nm,
                                 ctfo_site_id,
                                 site_country as src_site_country,
                                 Explode(Split(src_site_id, ';')) AS src_site_id
                          FROM
       sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site)
                  GROUP  BY 1,
                            2,
                            3,
                            4) C
              ON A.ctfo_site_id = C.ctfo_site_id
                 AND Lower(Trim(B.data_src_nm)) = Lower(Trim(C.data_src_nm))
GROUP  BY 1,
          2,
          3,
          4,
          5,
          6
""")
golden_id_det = golden_id_det.dropDuplicates()
#golden_id_det.createOrReplaceTempView('golden_id_details_site')
golden_id_det.write.mode('overwrite').saveAsTable('golden_id_details_site')
# Code for DQS Metrics Calculation
dqs_golden_source_map_site = spark.sql("""
SELECT ctfo_trial_id,
       ctfo_site_id,
       data_src_nm,
       max(nct_number) as nct_number,
       src_site_country as xref_country,
       country as dqs_country,
       max(facility_golden_id) as facility_golden_id,
       max(member_study_id) as member_study_id
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               data_src_nm,
               src_trial_id,
               src_site_id,
               src_site_country
        FROM   golden_id_details_site
        WHERE  Lower(Trim(data_src_nm)) = 'ir'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5,
                  6) A
       INNER JOIN (SELECT nct_number,
                          member_study_id,
                          country,
                          facility_golden_id
                   FROM   sanofi_ctfo_datastore_staging_$$db_env.dqs_study
                   GROUP  BY 1,
                             2,
                             3,
                             4) B
               ON A.src_trial_id = B.member_study_id
                  AND A.src_site_id = B.facility_golden_id
GROUP  BY 1,
          2,
          3,

          5,
          6
""")
dqs_golden_source_map_site = dqs_golden_source_map_site.dropDuplicates()
#dqs_golden_source_map_site.createOrReplaceTempView("dqs_golden_source_map_site")
dqs_golden_source_map_site.write.mode('overwrite').saveAsTable('dqs_golden_source_map_site')

centre_loc_map = spark.sql("""
select study_id as trial_no, study_site_id,study_code, center_id as centre_loc_id
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site where lower(trim(confirmed)) = 'yes'  and  lower(trim(cancelled))='no'
group by 1,2,3,4
""")
centre_loc_map = centre_loc_map.dropDuplicates()
centre_loc_map.createOrReplaceTempView("centre_loc_map")


ctms_golden_source_map_site = spark.sql("""select cgsms.* from (select ctfo_trial_id, ctfo_site_id, data_src_nm,country_mapping.iso2_code as xref_country_code,
country_mapping.country,country_mapping.standard_country,
 (study_site_id) as study_site_id,(B.study_code) as study_code,(B.trial_no) as trial_no,A.src_site_id,B.centre_loc_id,
 row_number() over (partition by ctfo_trial_id, ctfo_site_id order by study_code) as rnk
from
    (select ctfo_trial_id, ctfo_site_id, data_src_nm, src_trial_id, src_site_id, src_site_country
    from golden_id_details_site
    where lower(trim(data_src_nm)) = 'ctms' group by 1,2,3,4,5,6) A
inner join
    (select trial_no, study_site_id, centre_loc_id,study_code from centre_loc_map group by 1,2,3,4) B
on A.src_trial_id = B.study_code and A.src_site_id = B.centre_loc_id
left join country_mapping on lower(trim(A.src_site_country)) = lower(trim(country_mapping.country))
group by 1,2,3,4,5,6,7,8,9,10,11) cgsms where rnk=1
""")
ctms_golden_source_map_site = ctms_golden_source_map_site.dropDuplicates()
#ctms_golden_source_map_site.createOrReplaceTempView("ctms_golden_source_map_site")
ctms_golden_source_map_site.write.mode('overwrite').saveAsTable('ctms_golden_source_map_site')

# Code for CTMS Metrics Calculation - site_startup_time,lost_opportunity_time


ctms_sst_lpt_temp = spark.sql("""
select cgsm.ctfo_site_id,cgsm.ctfo_trial_id,a.study_code,
min(case when lower(a.milestone) = 'site qualification visit' then actual_date END) as sqv_dt,
min(case when lower(a.milestone) = 'site initiation visit' then actual_date end) as siv_dt,
min(case when lower(a.milestone) = 'first subject first visit' then actual_date END) as fsfv_dt,
min(case when lower(a.milestone) in ('local irb/iec submission') then actual_date END) as irb_iec_date_site
from
(select ctfo_trial_id, ctfo_site_id, trial_no,study_site_id,study_code from ctms_golden_source_map_site group by 1,2,3,4,5) cgsm
left join
(select study_id,study_code,study_site_id,milestone,actual_date from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones group by 1,2,3,4,5) a
on a.study_code = cgsm.study_code and a.study_site_id=cgsm.study_site_id
group by 1,2,3""")
ctms_sst_lpt_temp = ctms_sst_lpt_temp.dropDuplicates()
ctms_sst_lpt_temp.registerTempTable('ctms_sst_lpt_temp')


#HHHH

ctms_sst_lpt_temp2 = spark.sql("""
select cgsm.ctfo_trial_id,b.country,
min(case when lower(milestone) = 'first subject first visit' then actual_date END) as cntry_fsfv_dt,
    min(case when lower(milestone) in ('ha/ca submission') then actual_date END) as cta_date,
    min(case when lower(milestone) in ('first local irb/iec submission') then actual_date END) as irb_iec_date
from
(select ctfo_trial_id, trial_no,study_code,country from ctms_golden_source_map_site group by 1,2,3,4) cgsm
left join
(select study_id,country,milestone, actual_date,study_code from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones group by 1,2,3,4,5) b
on cgsm.study_code = b.study_code and lower(trim(cgsm.country)) = lower(trim(b.country))
group by 1,2""")

ctms_sst_lpt_temp2 = ctms_sst_lpt_temp2.dropDuplicates()
ctms_sst_lpt_temp2.registerTempTable('ctms_sst_lpt_temp2')





# HHHH
ctms_sst_lpt_final = spark.sql("""
select cgsm.ctfo_site_id,cgsm.ctfo_trial_id,
case when a.siv_dt is not null and a.fsfv_dt is not null then datediff(a.fsfv_dt,a.siv_dt)/30.4 end as lost_opportunity_time,
case when a.sqv_dt is not null and a.siv_dt is not null then datediff(a.siv_dt,a.sqv_dt)/30.4
     when a.sqv_dt is not null and a.fsfv_dt is not null then datediff(date_sub(a.fsfv_dt,14),a.sqv_dt)/30.4
     when a.sqv_dt is not null and a.fsfv_dt is null then datediff(date_sub(b.cntry_fsfv_dt,14),a.sqv_dt)/30.4
     when a.sqv_dt is null  then datediff(coalesce(a.siv_dt,date_sub(fsfv_dt,14),date_sub(b.cntry_fsfv_dt,14)),a.irb_iec_date_site)/30.4
end as site_startup_time
from (select ctfo_trial_id,ctfo_site_id, country from ctms_golden_source_map_site group by 1,2,3) cgsm
left join ctms_sst_lpt_temp a on cgsm.ctfo_site_id = a.ctfo_site_id and cgsm.ctfo_trial_id = a.ctfo_trial_id
left join ctms_sst_lpt_temp2 b on cgsm.ctfo_trial_id = b.ctfo_trial_id and lower(trim(cgsm.country)) = lower(trim(b.country))
group by 1,2,3,4""")
ctms_sst_lpt_final = ctms_sst_lpt_final.dropDuplicates()
#ctms_sst_lpt_final.registerTempTable('ctms_sst_lpt_final')

ctms_sst_lpt_final.write.mode("overwrite").saveAsTable("ctms_sst_lpt_final")

ctms_entered_study = spark.sql("""
select cgsm.ctfo_site_id, cgsm.ctfo_trial_id ,entered_study, enrolled,  entered_treatment,
entered_screening, dropped_treatment
from
(select ctfo_trial_id, trial_no, ctfo_site_id, study_site_id, study_code from ctms_golden_source_map_site group by 1,2,3,4,5) cgsm
left join
(select  a.study_site_id,a.study_code,max(entered_study) as entered_study  ,max(enrolled) as enrolled  ,max(entered_treatment) as entered_treatment ,max(entered_screening) as entered_screening,max(dropped_treatment) as dropped_treatment  from
(select  entered_study,study_code,study_site_id,enrolled,entered_treatment,entered_screening,dropped_treatment,picture_as_of,figure_type,study_period_number
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' group by 1,2,3,4,5,6,7,8,9,10) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and a.picture_as_of=d.picture_as_of
group by 1,2)A
on cgsm.study_code = A.study_code and cgsm.study_site_id  = A.study_site_id
group by 1,2,3,4,5,6,7
""")
ctms_entered_study = ctms_entered_study.dropDuplicates()
#ctms_entered_study.registerTempTable('ctms_entered_study')
ctms_entered_study.write.mode("overwrite").saveAsTable("ctms_entered_study")


ctms_dates = spark.sql("""select cgsm.ctfo_site_id,cgsm.ctfo_trial_id,
min(case when lower(a.milestone) = 'site initiation visit' then a.actual_date end) as siv_dt,
min(case when lower(a.milestone) = 'first subject first visit' then a.actual_date end) as fsfv_dt,
min(case when lower(a.milestone) = 'last subject first visit' then a.actual_date END) as lsfv_dt,
min(case when lower(b.milestone) = 'last subject first visit' then b.actual_date end) as cntry_lsfv_dt,
min(case when lower(b.milestone) = 'last subject first rando/treat' then b.actual_date end) as cntry_lsfr_dt,
min(case when lower(a.milestone) = 'last subject first rando/treat' then a.actual_date end) as lsfr_dt,
min(case when lower(a.milestone) = 'first subject first rando/treat' then a.actual_date END) as fsfr_dt,
min(case when lower(a.milestone) = 'last subject last visit' then a.actual_date END) as lslv_dt
from
(select ctfo_trial_id, trial_no, ctfo_site_id, study_site_id, study_code from ctms_golden_source_map_site group by 1,2,3,4,5) cgsm
left join
(select milestone,actual_date,study_code,study_country_id,study_site_id,study_id from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones group by 1,2,3,4,5,6) a
on cgsm.study_code = a.study_code and cgsm.study_site_id  = a.study_site_id
left join
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones b on a.study_code = b.study_code and a.study_country_id = b.study_country_id
group by 1,2""")
ctms_dates = ctms_dates.dropDuplicates()
ctms_dates.registerTempTable('ctms_dates')


ctms_ppm = spark.sql("""
select cgsm.ctfo_site_id, cgsm.ctfo_trial_id ,A.entered_treatment as ppm
from
(select ctfo_trial_id, trial_no, ctfo_site_id, study_site_id, study_code from ctms_golden_source_map_site group by 1,2,3,4,5) cgsm
left join
(select  a.study_site_id,a.study_code,max(entered_treatment) as entered_treatment  from
(select  study_code,study_site_id,entered_treatment,picture_as_of,figure_type,study_period_number
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'planned' group by 1,2,3,4,5,6) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'planned' having rnk=1)inq ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and a.picture_as_of=d.picture_as_of
group by 1,2)A
on cgsm.study_code = A.study_code and cgsm.study_site_id  = A.study_site_id
group by 1,2,3
""")
ctms_ppm = ctms_ppm.dropDuplicates()
ctms_ppm.registerTempTable('ctms_ppm')

ctms_percentage_enrollment = spark.sql(""" select a.ctfo_site_id,a.ctfo_trial_id, a.enrolled/b.ppm as percentage_enrollment
from ctms_entered_study a
left join
ctms_ppm b on a.ctfo_site_id = b.ctfo_site_id and a.ctfo_trial_id=b.ctfo_trial_id
group by 1,2,3
""")
ctms_percentage_enrollment = ctms_percentage_enrollment.dropDuplicates()
ctms_percentage_enrollment.registerTempTable('ctms_percentage_enrollment')


ctms_trial_status = spark.sql("""
select ctfo_site_id, ctfo_trial_id, trial_status from
(select ctfo_trial_id, ctfo_site_id, trial_no,study_site_id,study_code from ctms_golden_source_map_site group by 1,2,3,4,5) cgsm
left join
(select status as trial_status,study_site_id,study_code,study_id
from
    sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site where lower(trim(Confirmed)) = 'yes' and  lower(trim(cancelled))='no'  group by 1,2,3,4) b
on b.study_code = cgsm.study_code and b.study_site_id=cgsm.study_site_id
where lower(trim(trial_status)) in ('active','initiated','last subject enrolled','site ready to enrol','closed', 'last subject completed', 'discontinued','all sites closed','completed','active(recruiting)','active(non recruiting)')
""")
ctms_trial_status = ctms_trial_status.dropDuplicates()
ctms_trial_status.registerTempTable('ctms_trial_status')

# ctms_duration and recuuitment rate are not getting used becuase resuritment rate is enrollment rate


ctms_duration = spark.sql("""
select cgsm.ctfo_site_id,cgsm.ctfo_trial_id,
case
    when lower(trim(b.trial_status)) in ('active', 'initiated', 'last subject enrolled', 'site ready to enrol','active(recruiting)','active(non recruiting)','recruiting','ongoing')
    then
        case when d.lsfv_dt is not null then (datediff(d.lsfv_dt,coalesce(d.siv_dt,date_sub(d.fsfv_dt,14))))/30.4
             when d.lsfv_dt is null and e.percentage_enrollment>0.5  then (datediff(current_date(),d.siv_dt))/30.4
        end
    when lower(trim(b.trial_status)) in ( 'closed', 'last subject completed', 'discontinued', 'all sites closed','completed')
        then (datediff(coalesce(d.lsfv_dt,d.cntry_lsfv_dt),coalesce(d.siv_dt,date_sub(d.fsfv_dt,14))))/30.4
    end as duration
from
(select ctfo_trial_id, ctfo_site_id, trial_no,study_site_id from ctms_golden_source_map_site group by 1,2,3,4) cgsm
left join ctms_trial_status b on b.ctfo_trial_id = cgsm.ctfo_trial_id and b.ctfo_site_id=cgsm.ctfo_site_id
left join ctms_dates d on cgsm.ctfo_site_id = d.ctfo_site_id and cgsm.ctfo_trial_id = d.ctfo_trial_id
left join ctms_percentage_enrollment e on cgsm.ctfo_site_id = e.ctfo_site_id and cgsm.ctfo_trial_id = e.ctfo_trial_id
group by 1,2,3""")

ctms_duration = ctms_duration.dropDuplicates()
ctms_duration.registerTempTable('ctms_duration')

ctms_recruitment_rate = spark.sql("""
select cgsm.ctfo_site_id,cgsm.ctfo_trial_id, b.entered_study/a.duration as recruitment_rate
from
(select ctfo_trial_id, ctfo_site_id, trial_no,study_site_id from ctms_golden_source_map_site group by 1,2,3,4) cgsm
left join ctms_duration a on cgsm.ctfo_site_id = a.ctfo_site_id and cgsm.ctfo_trial_id = a.ctfo_trial_id
left join ctms_entered_study b on cgsm.ctfo_site_id = b.ctfo_site_id and cgsm.ctfo_trial_id = b.ctfo_trial_id
group by 1,2,3
""")
ctms_recruitment_rate = ctms_recruitment_rate.dropDuplicates()
#ctms_recruitment_rate.registerTempTable('ctms_recruitment_rate')

ctms_recruitment_rate.write.mode("overwrite").saveAsTable("ctms_recruitment_rate_site")

#### randomization_rate

ctms_percentage_randomized = spark.sql(""" select a.ctfo_site_id,a.ctfo_trial_id ,case when (a.entered_treatment is null or b.ppm  is null or a.entered_treatment=0 or b.ppm =0) then null else (a.entered_treatment/b.ppm )  end as percentage_randomized
from
ctms_entered_study a
left join
ctms_ppm b on a.ctfo_site_id = b.ctfo_site_id and a.ctfo_trial_id = b.ctfo_trial_id
group by 1,2,3
""")
ctms_percentage_randomized = ctms_percentage_randomized.dropDuplicates()
ctms_percentage_randomized.registerTempTable('ctms_percentage_randomized')


ctms_elapsed_duration = spark.sql("""select B.ctfo_trial_id,B.ctfo_site_id,B.study_code,case when (B.num is null or B.den is null or B.num=0 or B.den=0) then null else (B.num/B.den)  end as elapsed_duration from
(select A.ctfo_trial_id,A.ctfo_site_id,A.study_code,(datediff(COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,14)),A.lsfr_curr_dt)) as den,(datediff(COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,14)),CURRENT_DATE())) as num
from
(select cgsm.ctfo_trial_id,cgsm.ctfo_site_id,cgsm.study_code,
min(case when lower(a.milestone) = 'last subject first rando/treat' then a.current_plan_date end) as lsfr_curr_dt,
min(case when lower(a.milestone) = 'first subject first rando/treat' then a.current_plan_date end) as fsfr_dt,
min(case when lower(a.milestone) = 'first site initiation visit' then a.current_plan_date end) as fsiv_dt
from
(select ctfo_trial_id, trial_no, ctfo_site_id, study_site_id, study_code from ctms_golden_source_map_site group by 1,2,3,4,5) cgsm
left join
(select milestone,actual_date,study_code,study_id,current_plan_date from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_milestones group by 1,2,3,4,5) a
on cgsm.study_code = a.study_code
group by 1,2,3) A group by 1,2,3,4,5) B group by 1,2,3,4""")
ctms_elapsed_duration = ctms_elapsed_duration.dropDuplicates()
ctms_elapsed_duration.registerTempTable('ctms_elapsed_duration')


ctms_randomiztion_duration = spark.sql("""select cgsm.ctfo_site_id,cgsm.ctfo_trial_id,
case
when lower(trim(b.trial_status)) in ('active', 'initiated', 'last subject enrolled', 'site ready to enrol','active(recruiting)','active(non recruiting)','recruiting','ongoing')
then
case when d.siv_dt is not null and d.lsfr_dt is not null then datediff(d.lsfr_dt,d.siv_dt)/30.4
when d.siv_dt is null and d.fsfr_dt is not null and d.lsfr_dt is not null then datediff(d.lsfr_dt,date_sub(d.fsfr_dt,14))/30.4
when d.lsfr_dt is null and e.percentage_randomized>0.5 and (d.siv_dt is not null or d.fsfr_dt is not null) then datediff(current_date(),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14)))/30.4
when d.lsfr_dt is null and e.percentage_randomized<0.5 and (d.siv_dt is not null or d.fsfr_dt is not null) and ced.elapsed_duration > 0.5 then (datediff(current_date(),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14))))/30.4
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
(select ctfo_trial_id, ctfo_site_id, trial_no,study_site_id from ctms_golden_source_map_site group by 1,2,3,4) cgsm
left join ctms_dates d on cgsm.ctfo_site_id = d.ctfo_site_id and cgsm.ctfo_trial_id = d.ctfo_trial_id
left join ctms_elapsed_duration ced on cgsm.ctfo_site_id = ced.ctfo_site_id and cgsm.ctfo_trial_id = ced.ctfo_trial_id
left join ctms_percentage_randomized e on cgsm.ctfo_site_id = e.ctfo_site_id and cgsm.ctfo_trial_id = e.ctfo_trial_id
left join ctms_trial_status b on cgsm.ctfo_trial_id = b.ctfo_trial_id and cgsm.ctfo_site_id = b.ctfo_site_id
group by 1,2,3""")
ctms_randomiztion_duration = ctms_randomiztion_duration.dropDuplicates()
ctms_randomiztion_duration.registerTempTable('ctms_randomiztion_duration')



ctms_randomization_rate = spark.sql("""
select a.ctfo_site_id,a.ctfo_trial_id, b.entered_treatment/a.duration as randomization_rate
from
ctms_randomiztion_duration a
left join ctms_entered_study b on a.ctfo_site_id = b.ctfo_site_id and a.ctfo_trial_id = b.ctfo_trial_id
group by 1,2,3
""")

ctms_randomization_rate = ctms_randomization_rate.dropDuplicates()
#ctms_randomization_rate.registerTempTable('ctms_randomization_rate')

ctms_randomization_rate.write.mode("overwrite").saveAsTable("ctms_randomization_rate_site")




ctms_screen_fail_rate = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_site_id,
case when lower(trim(cntry.trial_status)) in ( 'active','initiated','last subject enrolled','site ready to enrol','selected','active(recruiting)','active(non recruiting)','recruiting','ongoing')
                         and lower(trim(mile.milestone)) like '%last subject first visit%'
                then case when  cpr.DROPPED_AT_SCREENING is not null and cpr.entered_study is not null then ((cpr.DROPPED_AT_SCREENING)/(cpr.entered_study))*100
                                  when  cpr.DROPPED_AT_SCREENING == 0 and cpr.entered_study>0 then 0.00
                                  when  cpr.DROPPED_AT_SCREENING > 0 and cpr.entered_study ==0 then 100.00
                                  when  cpr.DROPPED_AT_SCREENING == 0 and cpr.entered_study ==0 then null
                         end
         when lower(trim(cntry.trial_status)) in ('closed', 'last subject completed', 'discontinued','completed')
                then case when  cpr.DROPPED_AT_SCREENING is not  null and cpr.entered_study is not null then ((cpr.DROPPED_AT_SCREENING)/(cpr.entered_study))*100
                                  when  cpr.DROPPED_AT_SCREENING == 0 and cpr.entered_study>0 then 0.00
                                  when  cpr.DROPPED_AT_SCREENING > 0 and cpr.entered_study==0 then 100.00
                                  when  cpr.DROPPED_AT_SCREENING == 0 and cpr.entered_study==0 then null
                         end
end as ctms_screen_fail_rate from
(select ctfo_trial_id, ctfo_site_id, trial_no,study_site_id,study_code from ctms_golden_source_map_site group by 1,2,3,4,5) cgsm
left join
(select status as trial_status,study_site_id,study_code
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site where lower(trim(status)) in ( 'active','initiated','last subject enrolled','site ready to enrol','selected','closed', 'last subject completed', 'discontinued','active(recruiting)','active(non recruiting)','completed','recruiting','ongoing') group by 1,2,3) cntry
on cntry.study_code = cgsm.study_code and cntry.study_site_id = cgsm.study_site_id
left join
(select study_code,study_site_id,milestone  from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones where lower(trim(milestone)) in ('last subject last visit') group by 1,2,3) mile
on mile.study_code = cntry.study_code and mile.study_site_id=cntry.study_site_id
left join
(select m.entered_study,((m.entered_study)-(m.entered_treatment)) as DROPPED_AT_SCREENING,entered_treatment,m.country,m.study_code,m.study_site_id from (select sum(a.entered_study) as entered_study,sum(a.entered_treatment) as entered_treatment,a.country,d.study_code,d.study_site_id from
(select (entered_study) as entered_study,(entered_treatment) as entered_treatment , picture_as_of,country_name as country,study_code,study_site_id,study_period_number from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed'  group by 1,2,3,4,5,6,7) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and a.picture_as_of=d.picture_as_of
group by 3,4,5)m group by 1,2,3,4,5,6) cpr
on mile.study_code=cpr.study_code and mile.study_site_id=cpr.study_site_id
group by 1,2,3""")
ctms_screen_fail_rate = ctms_screen_fail_rate.dropDuplicates()
ctms_screen_fail_rate.write.mode("overwrite").saveAsTable("ctms_screen_fail_rate_site")

ctms_trial_site_status=spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_site_id,tsm.status as trial_status from
(select ctfo_trial_id, ctfo_site_id,study_site_id,study_code from ctms_golden_source_map_site group by 1,2,3,4) cgsm
left join (select * from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site where lower(trim(confirmed)) = 'yes'  and  lower(trim(cancelled))='no' ) css
on cgsm.study_code=css.study_code and cgsm.study_site_id=css.study_site_id
left join (select distinct raw_status,status from trial_status_mapping) tsm  on trim(lower(css.status))=trim(lower(tsm.raw_status))
group by 1,2,3
""")
ctms_trial_site_status = ctms_trial_site_status.dropDuplicates()
ctms_trial_site_status.write.mode("overwrite").saveAsTable("ctms_trial_site_status")

ctms_site_metrics = spark.sql("""
SELECT
    A.ctfo_trial_id,
    A.ctfo_site_id,
    B.entered_treatment AS patients_randomized,
    case when lower(trim(D.trial_status)) in ( 'active','initiated','last subject enrolled','site ready to enrol','closed', 'last subject completed', 'discontinued','all sites closed','active(recruiting)','active(non recruiting)','completed','recruiting','ongoing')
    then B.entered_study end as patients_enrolled ,
    B.dropped_treatment AS patients_dropped,
    C.fsfv_dt AS first_subject_enrolled_dt,
    C.fsfr_dt AS first_subject_randomized_dt,
    C.lsfv_dt AS last_subject_enrolled_dt,
    C.lsfr_dt AS last_subject_randomized_dt,
    C.lslv_dt AS last_subject_last_visit_dt,
    C.siv_dt AS site_ready_to_enroll_dt,
    A.xref_country_code as country_code,
    ctss.trial_status as site_status,
    B.entered_screening,
    csfr.ctms_screen_fail_rate,
    'null' as ctms_enrollment_rate,
    G.lost_opportunity_time as ctms_lost_opportunity_time,
    I.duration as ctms_total_recruitment_months,
    F.randomization_rate as ctms_randomization_rate,
    G.site_startup_time as ctms_site_startup_time
from
(select ctfo_trial_id,ctfo_site_id, trial_no, study_site_id,xref_country_code,study_code from ctms_golden_source_map_site group by 1,2,3,4,5,6) A
LEFT JOIN ctms_entered_study B on A.ctfo_trial_id = B.ctfo_trial_id and A.ctfo_site_id = B.ctfo_site_id
LEFT JOIN ctms_dates C on A.ctfo_trial_id = C.ctfo_trial_id and A.ctfo_site_id = C.ctfo_site_id
LEFT JOIN ctms_trial_status D on A.ctfo_trial_id = D.ctfo_trial_id and D.ctfo_site_id = D.ctfo_site_id
LEFT JOIN default.ctms_randomization_rate_site F on A.ctfo_trial_id = F.ctfo_trial_id and A.ctfo_site_id = F.ctfo_site_id
left join default.ctms_screen_fail_rate_site csfr on A.ctfo_trial_id = csfr.ctfo_trial_id and A.ctfo_site_id = csfr.ctfo_site_id
left join ctms_randomiztion_duration I on A.ctfo_trial_id = I.ctfo_trial_id and A.ctfo_site_id = I.ctfo_site_id
left join default.ctms_sst_lpt_final G on A.ctfo_trial_id = G.ctfo_trial_id and A.ctfo_site_id = G.ctfo_site_id
left join ctms_trial_site_status ctss on A.ctfo_trial_id = ctss.ctfo_trial_id and A.ctfo_site_id = ctss.ctfo_site_id
left join
    (select study_id, study_code, study_site_id, concat_ws('\;',collect_set(milestone)) as milestone from
    sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones group by 1,2,3) mile
on A.study_code = mile.study_code and A.study_site_id = mile.study_site_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20""")
ctms_site_metrics = ctms_site_metrics.dropDuplicates()
#ctms_site_metrics.createOrReplaceTempView("ctms_site_metrics")
ctms_site_metrics.write.mode("overwrite").saveAsTable("ctms_site_metrics")

'''
df_trial_status = spark.sql("""
SELECT A.ctfo_trial_id,
       trial_status
FROM   dqs_golden_source_map_site A
       LEFT JOIN (SELECT ctfo_trial_id,
                         trial_status
                  FROM   sanofi_ctfo_datastore_app_commons_$$db_env.d_trial
                  GROUP  BY 1,
                            2) B
              ON A.ctfo_trial_id = B.ctfo_trial_id
GROUP  BY 1,
          2
""")
df_trial_status = df_trial_status.dropDuplicates()
df_trial_status.createOrReplaceTempView("dqs_status")
'''

# taking all proxy columns and medians
dqs_site_details_temp = spark.sql("""(SELECT ctfo_trial_id,
        ctfo_site_id,
        site_startup_time,
        case when trim(lower(pct_screen_fail)) like '%\%%' then regexp_replace(pct_screen_fail,"\%","") else pct_screen_fail end as pct_screen_fail,
        tsm.status as site_status,
        enrolled                AS patients_randomized,
        consented               AS patients_enrolled,
        (enrolled - completed) AS patients_dropped,
        Min(first_subject_consented_dt)           AS first_subject_enrolled_dt,
        Min(first_subject_enrolled_dt)            AS first_subject_randomized_dt,
        Max(last_subject_consented_dt)            AS last_subject_enrolled_dt,
        Max(last_subject_enrolled_dt)             AS last_subject_randomized_dt,
        Max(last_subject_last_visit_dt)           AS last_subject_last_visit_dt,
        Min(site_open_dt)                         AS site_ready_to_enroll_dt,
        First(A.xref_country)                     AS xref_country
FROM   dqs_golden_source_map_site A
        LEFT JOIN sanofi_ctfo_datastore_staging_$$db_env.dqs_study B
               ON Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id))
                  AND Lower(Trim(A.facility_golden_id)) =
                      Lower(Trim(B.facility_golden_id))
                  AND Lower(Trim(A.xref_country)) =
                      Lower(Trim(B.country))
                  left join (select distinct raw_status,status from trial_status_mapping) tsm  on B.site_status=tsm.raw_status

GROUP  BY 1,2,3,4,5,6,7,8) """)
dqs_site_details=dqs_site_details_temp.withColumn("pct_screen_fail",dqs_site_details_temp.pct_screen_fail.cast('double'))
dqs_site_details = dqs_site_details.dropDuplicates()
dqs_site_details.write.mode("overwrite").saveAsTable("dqs_site_details")


dqs_randomization_rate_temp = spark.sql("""select A.ctfo_trial_id, b.nct_number,b.country,Max(b.last_subject_enrolled_dt) as cntry_last_subject_randomized_dt from dqs_golden_source_map_site A
left join sanofi_ctfo_datastore_staging_$$db_env.dqs_study B ON Lower(Trim(A.nct_number)) = Lower(Trim(B.nct_number))
AND Lower(Trim(A.xref_country)) = Lower(Trim(B.country)) group by 1,2,3""")
dqs_randomization_rate_temp.createOrReplaceTempView("dqs_randomization_rate_temp")

dqs_randomization_rate = spark.sql("""
select C.ctfo_trial_id,C.ctfo_site_id,C.recruitment_duration, C.patients_randomized/C.recruitment_duration as dqs_randomization_rate from
(select A.ctfo_trial_id,A.ctfo_site_id, A.patients_randomized,
case when lower(trim(trial_status)) in ('completed','closed')
            then
                    case when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
                    then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt),A.site_ready_to_enroll_dt)/30.4
                    when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
                    then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt),A.first_subject_randomized_dt)/30.4
                    when A.first_subject_randomized_dt is null
                    then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt),A.site_ready_to_enroll_dt)/30.4
                    when A.site_ready_to_enroll_dt is null
                    then datediff(coalesce(A.last_subject_randomized_dt,B.cntry_last_subject_randomized_dt),A.first_subject_randomized_dt)/30.4
                    end
     when lower(trim(trial_status)) in ('ongoing', 'recruiting', 'active(not recruiting)','active (recruiting)')
            then
                    case when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
                    then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.site_ready_to_enroll_dt)/30.4
                    when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
                    then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.first_subject_randomized_dt)/30.4
                    when A.first_subject_randomized_dt is null
                    then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.site_ready_to_enroll_dt)/30.4
                    when A.site_ready_to_enroll_dt is null
                    then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.first_subject_randomized_dt)/30.4
                    end
else null end as recruitment_duration
from
(select ctfo_trial_id,ctfo_site_id, patients_randomized, xref_country,site_ready_to_enroll_dt,first_subject_randomized_dt,
last_subject_randomized_dt,site_status as trial_status from
dqs_site_details group by 1,2,3,4,5,6,7,8) A
left join dqs_randomization_rate_temp B ON Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(B.ctfo_trial_id))
AND Lower(Trim(A.xref_country)) = Lower(Trim(B.country)) group by 1,2,3,4) C
group by 1,2,3,4""")
dqs_randomization_rate = dqs_randomization_rate.dropDuplicates()
#dqs_randomization_rate.createOrReplaceTempView("dqs_randomization_rate")
dqs_randomization_rate.write.mode("overwrite").saveAsTable("dqs_randomization_rate_site")


# calculation first subject in date
dqs_study_fsi = spark.sql("""SELECT ctfo_trial_id,
       Min(first_subject_enrolled_dt) AS dqs_study_fsi_dt
FROM   dqs_site_details
GROUP  BY 1 """)
dqs_study_fsi = dqs_study_fsi.dropDuplicates()
dqs_study_fsi.createOrReplaceTempView("dqs_study_fsi")

# calculation of country last subject in date
dqs_country_lsi = spark.sql("""SELECT ctfo_trial_id,
       xref_country as country,
       Max(last_subject_randomized_dt) AS dqs_country_lsi_dt
FROM   dqs_site_details
GROUP  BY 1,
          2 """)
dqs_country_lsi = dqs_country_lsi.dropDuplicates()
dqs_country_lsi.createOrReplaceTempView("dqs_country_lsi")

# calcualtion of KPIs - Column : Site Open Date, First Subject Enrolled, Last Subject Enrolled, # Enrolled

trial_site_dqs_kpis = spark.sql("""
SELECT A.ctfo_trial_id, A.ctfo_site_id,A.patients_randomized,A.patients_enrolled,A.patients_dropped,
    A.first_subject_enrolled_dt,A.first_subject_randomized_dt,A.last_subject_enrolled_dt,A.last_subject_randomized_dt,
    A.last_subject_last_visit_dt,A.site_ready_to_enroll_dt,E.iso2_code as country_code,
    pct_screen_fail AS dqs_screen_fail_rate,
    randomization_rate.dqs_randomization_rate AS dqs_enrollment_rate,
    Datediff( first_subject_enrolled_dt, site_ready_to_enroll_dt)/30.4 AS dqs_lost_opportunity_time,
    randomization_rate.recruitment_duration AS dqs_total_recruitment_months,
    randomization_rate.dqs_randomization_rate AS dqs_randomization_rate,
    A.site_startup_time AS dqs_startup_time,A.site_status
FROM dqs_site_details A
LEFT JOIN default.dqs_randomization_rate_site randomization_rate
    ON A.ctfo_trial_id = randomization_rate.ctfo_trial_id and A.ctfo_site_id = randomization_rate.ctfo_site_id
LEFT JOIN dqs_study_fsi B
    ON A.ctfo_trial_id = B.ctfo_trial_id
LEFT JOIN dqs_country_lsi C
    ON A.ctfo_trial_id = C.ctfo_trial_id
        AND Lower(Trim(A.xref_country)) = Lower(Trim(C.country))
LEFT JOIN
    (SELECT iso2_code,
            country,
         iec_timeline,
         regulatory_timeline
    FROM country_mapping
    GROUP BY  1, 2, 3, 4) E
    ON Lower(Trim(A.xref_country)) = Lower(Trim(E.country)) """)
trial_site_dqs_kpis.createOrReplaceTempView("dqs_metrics")

# Write DQS Metrics to S3
write_path = path.replace("table_name", "dqs_metrics")
trial_site_dqs_kpis.write.format("parquet").mode("overwrite").save(path=write_path)

read_path = path.replace("table_name", "dqs_metrics")
dqs_metrics = spark.read.parquet(read_path)
dqs_metrics.createOrReplaceTempView("dqs_metrics")

# Write CTMS Metrics to S3
write_path = path.replace("table_name", "ctms_site_metrics")
ctms_site_metrics.write.format("parquet").mode("overwrite").save(path=write_path)

read_path = path.replace("table_name", "ctms_site_metrics")
ctms_site_metrics = spark.read.parquet(read_path)
ctms_site_metrics.createOrReplaceTempView("ctms_metrics")

ctms_metrics_final = spark.sql("""
SELECT 'ctms'                        AS datasource,
       'ctms'                        AS datasource_temp,
       ctfo_trial_id,
       country_code,
       ctfo_site_id,
       site_status                 AS study_status,
       ctms_enrollment_rate          AS enrollment_rate,
       patients_enrolled,
       ctms_lost_opportunity_time    AS lost_opportunity_time,
       ctms_randomization_rate       AS randomization_rate,
       patients_randomized,
       ctms_screen_fail_rate         AS screen_fail_rate,
       ctms_total_recruitment_months AS total_recruitment_months,
       ctms_site_startup_time             AS avg_startup_time,
       entered_screening            AS patients_screened,
       patients_dropped             AS patients_dropped,
       site_ready_to_enroll_dt      AS ready_to_enroll_dt,
       first_subject_randomized_dt    AS first_subject_in_dt,
       last_subject_randomized_dt   AS last_subject_in_dt
FROM   default.ctms_site_metrics
""")
ctms_metrics_final = ctms_metrics_final.dropDuplicates()
ctms_metrics_final.createOrReplaceTempView("ctms_metrics_final")

dqs_metrics_final = spark.sql("""
SELECT 'ir'                        AS datasource,
       'ir'                        AS datasource_temp,
       ctfo_trial_id,
       country_code,
       ctfo_site_id,
       site_status                 AS study_status,
       dqs_enrollment_rate          AS enrollment_rate,
       patients_enrolled,
       dqs_lost_opportunity_time    AS lost_opportunity_time,
       dqs_randomization_rate       AS randomization_rate,
       patients_randomized,
       dqs_screen_fail_rate         AS screen_fail_rate,
       dqs_total_recruitment_months AS total_recruitment_months,
       dqs_startup_time             AS avg_startup_time,
       patients_enrolled            AS patients_screened,
       patients_dropped             AS patients_dropped,
       site_ready_to_enroll_dt      AS ready_to_enroll_dt,
       first_subject_randomized_dt    AS first_subject_in_dt,
       last_subject_randomized_dt   AS last_subject_in_dt
FROM   dqs_metrics """)
dqs_metrics_final = dqs_metrics_final.dropDuplicates()
dqs_metrics_final.createOrReplaceTempView("dqs_metrics_final")

df_union = spark.sql("""
select * from dqs_metrics_final
union
select * from ctms_metrics_final""")
df_union = df_union.dropDuplicates()
df_union.createOrReplaceTempView("df_union")

# Write Union Table To S3
write_path = path.replace("table_name", "all_kpi_union")
df_union.write.format("parquet").mode("overwrite").save(path=write_path)

# Fact Table Calculations
read_path = path.replace("table_name", "all_kpi_union")
union_df = spark.read.parquet(read_path)
union_df.createOrReplaceTempView("union_table")

# calculating kPI's from already calculated KPI
f_trial_site_kpi1 = spark.sql("""
SELECT ctfo_trial_id            AS ctfo_trial_id_kpi,
       datasource               AS datasource_kpi,
       Min(avg_startup_time)    AS min_startup_time,
       Max(avg_startup_time)    AS max_startup_time,
       Min(patients_enrolled)   AS min_patients_enrolled,
       Max(patients_enrolled)   AS max_patients_enrolled,
       Min(patients_randomized) AS min_patients_randomized,
       Max(patients_randomized) AS max_patients_randomized,
       Min(randomization_rate)  AS min_randomization_rate,
       Max(randomization_rate)  AS max_randomization_rate
FROM   df_union
GROUP  BY 1,
          2 """)
f_trial_site_kpi1 = f_trial_site_kpi1.dropDuplicates()
f_trial_site_kpi1.createOrReplaceTempView("f_trial_site_kpi1")

# integrating calculated KPI with final_base_fact
df_temp_kpi1 = spark.sql("""
SELECT A.ctfo_trial_id,
       A.ctfo_site_id,
       A.datasource AS datasource_nm,
       A.datasource_temp,
       A.total_recruitment_months,
       *
FROM   df_union A
       LEFT JOIN f_trial_site_kpi1 B
              ON A.ctfo_trial_id = B.ctfo_trial_id_kpi
                 AND A.datasource = B.datasource_kpi
""").drop("ctfo_trial_id_kpi", "datasource_kpi")
df_temp_kpi1 = df_temp_kpi1.dropDuplicates()
df_temp_kpi1.createOrReplaceTempView("df_temp_kpi1")

# calculate count of sites per trials
site_count = spark.sql("""SELECT ctfo_trial_id  AS ctfo_trial_id_kpi,
       datasource_nm,
       Count(DISTINCT ctfo_site_id) AS ctfo_site_id_count
FROM   df_temp_kpi1
GROUP  BY 1,
          2 """)
site_count.createOrReplaceTempView("df_site_count")

# calculate total patient per site and randomized patient site ratio
df_temp_kpi2 = spark.sql("""
SELECT A.*,
       ( patients_randomized / ctfo_site_id_count ) AS
       randomized_patient_site_ratio,
       ( patients_enrolled / ctfo_site_id_count )   AS total_patient_per_site
FROM   df_temp_kpi1 A
       INNER JOIN df_site_count B
               ON A.ctfo_trial_id = B.ctfo_trial_id_kpi
                  AND A.datasource_nm = B.datasource_nm
-- group by 1,2,3,4,5
""")
df_temp_kpi2 = df_temp_kpi2.dropDuplicates()
df_temp_kpi2.createOrReplaceTempView("temp_kpi_2")

# integration precedence with df_union
final_df = spark.sql("""select * from temp_kpi_2 A """)
final_df = final_df.dropDuplicates().drop("ctfo_site_id_prec").drop("ctfo_trial_id_prec")
final_df.createOrReplaceTempView("final_base_fact")

df_temp_kpi2.createOrReplaceTempView("final_base_fact")

f_trial_site = spark.sql("""
SELECT ctfo_trial_id,
       ctfo_site_id,
       datasource AS datasource_nm,
       first_subject_in_dt,
       last_subject_in_dt,
       ready_to_enroll_dt,
       study_status,
       CASE
         WHEN cast(screen_fail_rate as double) < 0 THEN null
         ELSE cast(screen_fail_rate as double)
       END        AS screen_fail_rate,
       CASE
         WHEN cast(avg_startup_time as double) < 0 THEN null
         ELSE cast(avg_startup_time as double)
       END        AS average_startup_time,
       CASE
         WHEN cast(enrollment_rate as double) < 0 THEN null
         ELSE cast(enrollment_rate as double)
       END        AS enrollment_rate,
       CASE
         WHEN cast(randomization_rate as double) < 0 THEN null
         ELSE cast(randomization_rate as double)
       END        AS randomization_rate,
       CASE
         WHEN cast(lost_opportunity_time as double) < 0 THEN null
         ELSE cast(lost_opportunity_time as double)
       END        AS lost_opportunity_time,
              patients_enrolled,
        CASE
         WHEN cast(total_recruitment_months as double) < 0 THEN null
         ELSE cast(total_recruitment_months as double)
       END        AS total_recruitment_months,
       row_number() over (partition by ctfo_trial_id,ctfo_site_id order by datasource asc ) as rnk
FROM   final_base_fact
""")
f_trial_site = f_trial_site.dropDuplicates()
f_trial_site.createOrReplaceTempView("f_trial_site")
f_trial_site.write.mode("overwrite").saveAsTable("f_trial_site")

# Read Temp Table f_trial_site and apply remaining column Prec
f_trial_site_prec = spark.sql("""
select
    union_table.ctfo_site_id as cctfo_site_id,
    union_table.ctfo_trial_id as cctfo_trial_id,
    COALESCE(ctms.first_subject_in_dt,ir.first_subject_in_dt) as first_subject_in_dt_prec,
COALESCE(ctms.last_subject_in_dt,ir.last_subject_in_dt) as last_subject_in_dt_prec,
COALESCE(ctms.ready_to_enroll_dt,ir.ready_to_enroll_dt) as ready_to_enroll_dt_prec,
COALESCE(ctms.screen_fail_rate,ir.screen_fail_rate) as screen_fail_rate_prec,
COALESCE(ctms.average_startup_time,ir.average_startup_time) as average_startup_time_prec,
COALESCE(ctms.enrollment_rate,ir.enrollment_rate) as enrollment_rate_prec,
COALESCE(ctms.randomization_rate,ir.randomization_rate) as randomization_rate_prec,
COALESCE(ctms.lost_opportunity_time,ir.lost_opportunity_time) as lost_opportunity_time_prec,
COALESCE(ctms.patients_enrolled,ir.patients_enrolled) as patients_enrolled_prec,
COALESCE(ctms.total_recruitment_months,ir.total_recruitment_months) as total_recruitment_months_prec,
COALESCE(ctms.study_status,ir.study_status) as study_status_prec
from
(select ctfo_site_id,ctfo_trial_id from default.f_trial_site group by 1,2) union_table
left join
(select ctfo_site_id, ctfo_trial_id, datasource_nm, first_subject_in_dt, last_subject_in_dt, ready_to_enroll_dt,
    screen_fail_rate, average_startup_time, enrollment_rate, randomization_rate, lost_opportunity_time,
    patients_enrolled, total_recruitment_months,study_status
    from default.f_trial_site where lower(datasource_nm)='ctms' group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14) ctms
on union_table.ctfo_site_id=ctms.ctfo_site_id and union_table.ctfo_trial_id=ctms.ctfo_trial_id
left join
    (select ctfo_site_id, ctfo_trial_id, datasource_nm, first_subject_in_dt, last_subject_in_dt, ready_to_enroll_dt,
    screen_fail_rate, average_startup_time, enrollment_rate, randomization_rate, lost_opportunity_time,
    patients_enrolled, total_recruitment_months,study_status
    from default.f_trial_site where lower(datasource_nm)='ir' group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14) ir
on union_table.ctfo_trial_id=ir.ctfo_trial_id and union_table.ctfo_site_id=ir.ctfo_site_id
""")
f_trial_site_prec = f_trial_site_prec.dropDuplicates()
#f_trial_site_prec.createOrReplaceTempView('f_trial_site_prec')
f_trial_site_prec.write.mode("overwrite").saveAsTable("f_trial_site_prec")




f_trial_site_final = spark.sql("""select * from f_trial_site
left join f_trial_site_prec
on f_trial_site.ctfo_trial_id=f_trial_site_prec.cctfo_trial_id and f_trial_site.ctfo_site_id=f_trial_site_prec.cctfo_site_id where f_trial_site.rnk=1
""").drop("cctfo_trial_id").drop("cctfo_site_id")

f_trial_site_final.createOrReplaceTempView("f_trial_site_final")
f_trial_site_final.write.mode("overwrite").saveAsTable("f_trial_site_final")

# Write Final Table f_trial_site to S3
write_path = path.replace("table_name", "f_trial_site_final")
f_trial_site_final.write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.f_trial_site
partition(pt_data_dt,pt_cycle_id)
select
    ctfo_trial_id,
    ctfo_site_id,
    datasource_nm,
    first_subject_in_dt,
    first_subject_in_dt_prec,
    last_subject_in_dt,
    last_subject_in_dt_prec,
    ready_to_enroll_dt,
    ready_to_enroll_dt_prec,
    screen_fail_rate,
    screen_fail_rate_prec,
    average_startup_time,
    average_startup_time_prec,
    enrollment_rate,
    enrollment_rate_prec,
    randomization_rate,
    randomization_rate_prec,
    lost_opportunity_time,
    lost_opportunity_time_prec,
    patients_enrolled,
    patients_enrolled_prec,
    total_recruitment_months,
    total_recruitment_months_prec,
    study_status,
    study_status_prec,
    "$$data_dt" as pt_data_dt,
    "$$cycle_id" as pt_cycle_id
from
    f_trial_site_final
""")

# Closing spark context
try:
    print("Closing spark context")
except:
    print("Error while closing spark context")

f_trial_site.write.format("parquet").mode("overwrite").save(path=write_path)
CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_commons_$$db_env.f_trial_site")


