################################# Module Information ######################################
#  Module Name         : Site reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_site_study_details_filters
#                           b. f_rpt_site_study_details
#                           c. f_rpt_src_site_details
#                           d. f_rpt_investigator_details
#                           e. f_rpt_site_study_details_non_performance
#  Pre-requisites      : Source table required:
#  Last changed on     : 23-01-2025
#  Last changed by     : Kedar
#  Reason for change   : incorporated tascan
#  Return Values       : NA
############################################################################################
####
################################### High level Process #####################################
# 1. Create required table and stores data on HDFS
############################################################################################
import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import initcap

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + "/applications/commons/temp/kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/" \
                         "commons/temp/" \
                         "kpi_output_dimension/table_name"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")
########## about trial, d_trial.nct_id not present

trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

trial_status_mapping = spark.sql(""" select distinct * from (select raw_status, status from trial_status_mapping_temp 
union select status, status from trial_status_mapping_temp )  """)
trial_status_mapping.registerTempTable('trial_status_mapping')

Ongoing = trial_status_mapping.filter(trial_status_mapping.status == "Ongoing").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Completed = trial_status_mapping.filter(trial_status_mapping.status == "Completed").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Planned = trial_status_mapping.filter(trial_status_mapping.status == "Planned").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Others = trial_status_mapping.filter(trial_status_mapping.status == "Others").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()

import json

with open("Status_variablization.json", "r") as jsonFile:
    data = json.load(jsonFile)

# Save Values in Json
Ongoing = data["Ongoing"]
Completed = data["Completed"]
Planned = data["Planned"]
Others = data["Others"]

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

standard_country_mapping_temp = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/standard_country_mapping.csv".format(bucket_path=bucket_path))
standard_country_mapping_temp.createOrReplaceTempView("standard_country_mapping_temp")

standard_country_mapping = spark.sql("""
select distinct * from (select   country, standard_country from standard_country_mapping_temp 
union 
select standard_country, standard_country from standard_country_mapping_temp)
""")
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

customSchema = StructType([
    StructField("standard_country", StringType(), True),
    StructField("iso2_code", StringType(), True),
    StructField("iso3_code", StringType(), True),
    StructField("region", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("$$client_name_cluster", StringType(), True),
    StructField("$$client_name_cluster_code", StringType(), True),
    StructField("$$client_name_csu", StringType(), True),
    StructField("post_trial_flag", StringType(), True),
    StructField("post_trial_details", StringType(), True)])

cluster_pta_mapping = spark.read.format("csv").option("header", "false").option('multiline', 'True').schema(
    customSchema).load("{bucket_path}/"
                       "uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.$$client_name_cluster_code as region_code,scpm.$$client_name_cluster,scpm.$$client_name_csu,scpm.post_trial_flag,
scpm.post_trial_details
from (select distinct *
	from standard_country_mapping )  cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

####################################### Site Reporting Table #####################################
######### calculate per site investigator count
site_info_inv_count = spark.sql("""
select
    temp_site_info.*, inv_count
from temp_site_info
left join
    (select ctfo_site_id, count(distinct ctfo_investigator_id) as inv_count
    from f_rpt_filters_site_inv group by 1) ctfo_site_inv_count
on lower(trim(temp_site_info.s_ctfo_site_id))=lower(trim(ctfo_site_inv_count.ctfo_site_id))

""")
site_info_inv_count = site_info_inv_count.dropDuplicates()
# site_info_inv_count.createOrReplaceTempView("site_info_inv_count")
site_info_inv_count.write.mode("overwrite").saveAsTable("site_info_inv_count")

################## creating table with all dimensions information ################

site_type_tab = spark.sql("""
select ctfo_site_id,site_type from
(select ctfo_site_id,concat_ws(';', collect_set(site_type)) site_type from (select ctfo_site_id,
src_site_id from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site where
lower(trim(data_src_nm)) = 'citeline' group by 1,2) A
left join ( select site_id,site_type from $$client_name_ctfo_datastore_staging_$$db_env.citeline_organization)
B
on lower(trim(A.src_site_id)) = lower(trim(B.site_id))
group by ctfo_site_id
)a

""")
site_type_tab = site_type_tab.dropDuplicates()
# site_type_tab.createOrReplaceTempView("site_type_tab")
site_type_tab.write.mode("overwrite").saveAsTable("site_type_tab")

# Mock Data for Enrollment, Screen Failure Rate
# Range for Enrollment Rate -> 0-2.7
# Range for Screen Failure Rate -> 0.05-0.4
mock_psm_scrn_fail = spark.sql("""
select ctfo_trial_id, ctfo_site_id , null as mock_enrol_rate, null as mock_scrn_fail_rate,
null as mock_lost_opportunity_time
from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site
""")
mock_psm_scrn_fail.createOrReplaceTempView("mock_psm_scrn_fail")

##Picking status from 'Source' as site level status are available in aact
aact_golden_source_map_site_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.ctfo_site_id,iq.status,iq.status_rnk,ROW_NUMBER() OVER
(PARTITION BY iq.ctfo_trial_id,iq.ctfo_site_id ORDER BY iq.status_rnk ) pred_stat from (SELECT ctfo_trial_id,
       ctfo_site_id,
       data_src_nm,
       tsm.status,
       case when lower(trim(tsm.status))='ongoing' then 1
            when lower(trim(tsm.status))='completed' then 2
            when lower(trim(tsm.status))='planned' then 3
            when lower(trim(tsm.status))='others' then 4 else 5 end as status_rnk 
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               data_src_nm,
               src_trial_id,
               src_site_id
        FROM   golden_id_details_site
        WHERE  Lower(Trim(data_src_nm)) = 'aact'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5) A
       INNER JOIN (SELECT nct_id,
                          id,status
                   FROM   $$client_name_ctfo_datastore_staging_$$db_env.aact_facilities
                   GROUP  BY 1,
                             2,3
                             ) B
               ON lower(trim(A.src_trial_id)) = lower(trim(B.nct_id))
                  AND lower(trim(A.src_site_id)) = lower(trim(B.id))
       inner join (select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(B.status))
GROUP  BY 1,
          2,
          3,4,5) iq )ab where pred_stat=1
""")
aact_golden_source_map_site_status = aact_golden_source_map_site_status.dropDuplicates()
aact_golden_source_map_site_status.createOrReplaceTempView("aact_golden_source_map_site_status")

##Picking status from d_trial because site level status not available in 'Citeline'
citeline_golden_source_map_site_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.ctfo_site_id,iq.status,iq.status_rnk,ROW_NUMBER() OVER
(PARTITION BY iq.ctfo_trial_id,iq.ctfo_site_id ORDER BY iq.status_rnk ) pred_stat from (SELECT A.ctfo_trial_id,
       A.ctfo_site_id,
       data_src_nm,
       tsm.status,
       case when lower(trim(tsm.status))='ongoing' then 1
            when lower(trim(tsm.status))='completed' then 2
            when lower(trim(tsm.status))='planned' then 3
            when lower(trim(tsm.status))='others' then 4 else 5 end as status_rnk
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               data_src_nm,
               src_trial_id,
               src_site_id
        FROM   golden_id_details_site
        WHERE  Lower(Trim(data_src_nm)) = 'citeline'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5) A
       INNER JOIN $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove B
               ON lower(trim(A.src_trial_id)) = lower(trim(B.trial_id))
       inner join (select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(B.trial_status))
GROUP  BY 1,
          2,
          3,4) iq )ab where pred_stat=1
""")
citeline_golden_source_map_site_status = citeline_golden_source_map_site_status.dropDuplicates()
# citeline_golden_source_map_site_status.createOrReplaceTempView("citeline_golden_source_map_site_status")
citeline_golden_source_map_site_status.write.mode("overwrite").saveAsTable("citeline_golden_source_map_site_status")

##Picking status from d_trial because site level status not available in 'tascan'
tascan_golden_source_map_site_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.ctfo_site_id,iq.status,iq.status_rnk,ROW_NUMBER() OVER
(PARTITION BY iq.ctfo_trial_id,iq.ctfo_site_id ORDER BY iq.status_rnk ) pred_stat from (SELECT A.ctfo_trial_id,
       A.ctfo_site_id,
       data_src_nm,
       tsm.status,
       case when lower(trim(tsm.status))='ongoing' then 1
            when lower(trim(tsm.status))='completed' then 2
            when lower(trim(tsm.status))='planned' then 3
            when lower(trim(tsm.status))='others' then 4 else 5 end as status_rnk
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               data_src_nm,
               src_trial_id,
               src_site_id
        FROM   golden_id_details_site
        WHERE  Lower(Trim(data_src_nm)) = 'tascan'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5) A
       INNER JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial B
               ON lower(trim(A.src_trial_id)) = lower(trim(B.trial_id))
       inner join (select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(B.state))
GROUP  BY 1,
          2,
          3,4) iq )ab where pred_stat=1
""")
tascan_golden_source_map_site_status = tascan_golden_source_map_site_status.dropDuplicates()
tascan_golden_source_map_site_status.write.mode("overwrite").saveAsTable("tascan_golden_source_map_site_status")


##updated for TA addition
f_rpt_site_study_details_temp = spark.sql("""
select /*broadcast (site_info_inv_count, site_type_tab, f_trial_site)*/
    r_trial_site.ctfo_trial_id, 
    r_trial_site.ctfo_site_id, 
    trim(nct_id) as nct_id,
    temp_site_info.region,
    temp_site_info.region_code,
    temp_site_info.site_country,
    temp_site_info.country_code, 
    inv_count as investigators,
    COALESCE(f_trial_site.study_status, cgsmss.status, tgsmss.status, agsm.status, 'Others') as trial_status,
    trial_start_dt,
    trial_end_dt,
    concat_ws("\|",sort_array(collect_set(distinct temp_sponsor_info.sponsor_nm),true)) as sponsor, 
    concat_ws("\|",sort_array(collect_set(distinct temp_sponsor_info.subsidiary_sponsors),true)) as subsidiary_sponsors,
    concat_ws("\|",sort_array(collect_set(distinct temp_sponsor_info.sponsor_type),true)) as sponsor_type,
    site_type,
    site_nm, 
    total_site_count,
    coalesce(f_trial_site.enrollment_rate, mock_psm_scrn_fail.mock_enrol_rate) as enrollment_rate,
    first_subject_in_dt, 
    last_subject_in_dt,
    ready_to_enroll_dt,
    coalesce(f_trial_site.lost_opportunity_time, mock_psm_scrn_fail.mock_lost_opportunity_time) as lost_opportunity_time,
    f_trial_site.patients_enrolled as patients_enrolled,
    f_trial_site.patients_screened as patients_screened,
    f_trial_site.total_recruitment_months,
    '' as patients_randomized,
    randomization_rate,
    coalesce(f_trial_site.screen_fail_rate, mock_psm_scrn_fail.mock_scrn_fail_rate) as screen_fail_rate,
    f_trial_site.average_startup_time as average_startup_time,
    f_trial_site.last_subject_last_visit_dt as last_subject_last_visit_dt,
    temp_nct_id_ctfo_trial_id_map.protocol_ids, 
    trial_title as study_name,
    completion_dt,
    temp_site_info.$$client_name_cluster,
    temp_site_info.site_address, 
    temp_site_info.site_city, 
    temp_site_info.site_state,
    temp_site_info.site_supporting_urls,
    primary_endpoints_reported_date,
    temp_site_info.site_zip_code
from
    (select ctfo_trial_id, ctfo_site_id 
    from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site) r_trial_site
inner join
    (select ctfo_trial_id, 
            trial_end_dt, 
            trial_phase, 
            trial_start_dt, 
            trial_title,
            enrollment_close_date as completion_dt,
            primary_endpoints_reported_date
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial 
    group by 1,2,3,4,5,6,7) d_trial
    on lower(trim(r_trial_site.ctfo_trial_id))=lower(trim(d_trial.ctfo_trial_id))
left join 
    (select n_ctfo_trial_id, 
            concat_ws(',',collect_set(nct_id)) as nct_id,
            concat_ws(',',collect_set(protocol_ids)) as protocol_ids 
    from temp_nct_id_ctfo_trial_id_map
    group by 1) temp_nct_id_ctfo_trial_id_map
    on lower(trim(temp_nct_id_ctfo_trial_id_map.n_ctfo_trial_id))=lower(trim(r_trial_site.ctfo_trial_id))
left join
    (select ctfo_trial_id, 
            ctfo_site_id, 
            enrollment_rate,
            first_subject_in_dt,
            last_subject_in_dt,
            ready_to_enroll_dt,
            lost_opportunity_time,
            randomization_rate,
            screen_fail_rate,
            average_startup_time,
            patients_enrolled,
            patients_screened, 
            total_recruitment_months,
            study_status,
            last_subject_last_visit_dt
    from $$client_name_ctfo_datastore_app_commons_$$db_env.f_trial_site
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) f_trial_site
    on lower(trim(r_trial_site.ctfo_trial_id)) = lower(trim(f_trial_site.ctfo_trial_id))
    and lower(trim(r_trial_site.ctfo_site_id)) = lower(trim(f_trial_site.ctfo_site_id))
left join 
    (select s_ctfo_site_id, 
            region, 
            site_country, 
            country, 
            inv_count
    from site_info_inv_count 
    group bY 1,2,3,4,5) site_info_inv_count
    on lower(trim(r_trial_site.ctfo_site_id))=lower(trim(site_info_inv_count.s_ctfo_site_id))
left join temp_site_info 
    on lower(trim(r_trial_site.ctfo_site_id))=lower(trim(temp_site_info.s_ctfo_site_id))
left join temp_sponsor_info 
    on lower(trim(r_trial_site.ctfo_trial_id))=lower(trim(temp_sponsor_info.s_ctfo_trial_id))
left join site_type_tab 
    on lower(trim(r_trial_site.ctfo_site_id))=lower(trim(site_type_tab.ctfo_site_id))
left join
    (select ctfo_trial_id as c_ctfo_trial_id, 
            count(ctfo_site_id) as total_site_count
    from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site
    group by 1) site_count
    on lower(trim(r_trial_site.ctfo_trial_id))=lower(trim(site_count.c_ctfo_trial_id))
left join mock_psm_scrn_fail
    on lower(trim(mock_psm_scrn_fail.ctfo_trial_id)) = lower(trim(r_trial_site.ctfo_trial_id))
    and lower(trim(mock_psm_scrn_fail.ctfo_site_id)) = lower(trim(r_trial_site.ctfo_site_id))
left join aact_golden_source_map_site_status agsm 
    on lower(trim(r_trial_site.ctfo_trial_id)) = lower(trim(agsm.ctfo_trial_id))
    and lower(trim(r_trial_site.ctfo_site_id)) = lower(trim(agsm.ctfo_site_id))
left join citeline_golden_source_map_site_status cgsmss 
    on lower(trim(r_trial_site.ctfo_trial_id)) = lower(trim(cgsmss.ctfo_trial_id))
    and lower(trim(r_trial_site.ctfo_site_id)) = lower(trim(cgsmss.ctfo_site_id))
left join tascan_golden_source_map_site_status tgsmss 
    on lower(trim(r_trial_site.ctfo_trial_id)) = lower(trim(tgsmss.ctfo_trial_id))
    and lower(trim(r_trial_site.ctfo_site_id)) = lower(trim(tgsmss.ctfo_site_id))
group by 1,2,3,4,5,6,7,8,9,10,11,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40
""")

f_rpt_site_study_details_temp = f_rpt_site_study_details_temp.dropDuplicates()
# f_rpt_site_study_details_temp.createOrReplaceTempView("f_rpt_site_study_details_temp")
f_rpt_site_study_details_temp.write.mode("overwrite").saveAsTable("f_rpt_site_study_details_temp")

f_rpt_site_study_details_temp2 = spark.sql("""
select
    a.ctfo_trial_id,
    a.ctfo_site_id,
    case when a.nct_id like '%|' then substr(a.nct_id,0,length(a.nct_id)-1) else a.nct_id end as nct_id,
    a.region,
    a.region_code,
    INITCAP(a.site_country) as site_country,
    a.$$client_name_cluster,
    a.country_code,
    a.investigators,
    a.trial_status,
    cast(coalesce(a.trial_start_dt,'') as date) as trial_start_dt,
    cast(coalesce(a.trial_end_dt,'') as date) as trial_end_dt,
    coalesce(NULLIF(a.sponsor, ''), 'Other') as sponsor,
    coalesce(NULLIF(a.subsidiary_sponsors, ''), 'Other') as  subsidiary_sponsors,
    coalesce(NULLIF(a.sponsor_type, ''), 'Other') as  sponsor_type,
    a.site_type as  type,
    a.site_nm as site_nm,
    a.total_site_count as total_no_sites,
    case when lower(trim(a.trial_status)) in {on_com} then cast(coalesce(a.randomization_rate,'') as double) else null end as randomization_rate,
    case when lower(trim(a.trial_status)) in {on_com} then cast(coalesce(a.total_recruitment_months,'') as double) else null end as total_recruitment_months,
    a.first_subject_in_dt,
    a.last_subject_in_dt,
    a.ready_to_enroll_dt,
    case when lower(trim(a.trial_status)) in {on_com} then cast(lost_opportunity_time as double) else null end as lost_opportunity_time,
	case when lower(trim(a.trial_status)) in {on_com} then cast(patients_enrolled as bigint) else null end as patients_enrolled,
    case when lower(trim(a.trial_status)) in {on_com} then cast(screen_fail_rate as double) else null end as screen_fail_rate,
    case when lower(trim(a.trial_status)) in {on_com} then cast(average_startup_time as double) else null end as average_startup_time,
    a.protocol_ids,
    a.study_name,
    cast(coalesce(a.completion_dt,'') as date) as completion_date,
    a.site_address,
    a.site_city,
    a.site_state,
    concat(a.ctfo_trial_id, a.ctfo_site_id) trial_site_id,
    a.site_supporting_urls,
    cast(coalesce(a.primary_endpoints_reported_date,'') as date) as primary_endpoints_reported_date,
        a.site_zip_code,
        a.last_subject_last_visit_dt,
    case when lower(trim(a.trial_status)) in {on_com} then cast(patients_screened as bigint) else null end as patients_screened,
    a.ready_to_enroll_dt as site_activation_dt
from f_rpt_site_study_details_temp a
     group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
     31,32,33,34,35,36,37,38,39,40
     order by a.ctfo_site_id, a.ctfo_trial_id, trial_site_id
""".format(on_com=Ongoing_Completed))

f_rpt_site_study_details_temp2 = f_rpt_site_study_details_temp2.dropDuplicates()
f_rpt_site_study_details_temp2.registerTempTable('f_rpt_site_study_details_temp2')

randomization_rate_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1 and randomization_rate = 0) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as randomization_rate_norm 
    from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id ) as max_r   from (select ctfo_trial_id,ctfo_site_id,randomization_rate,rank(randomization_rate) over (partition By ctfo_trial_id order by randomization_rate) as R_Rank from f_rpt_site_study_details_temp2 where randomization_rate IS NOT NULL) A) B """)
randomization_rate_df = randomization_rate_df.dropDuplicates()
randomization_rate_df.registerTempTable('randomization_rate_df')

lost_opportunity_time_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as lost_opportunity_time_norm,
    case when (R_Rank = 1 and max_r = 1) then 1
    else (1 - COALESCE(((R_Rank-1 )/(max_r-1)),1)) end as  neg_lost_opportunity_time_norm 
    from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id ) as max_r   from (select ctfo_trial_id,ctfo_site_id,lost_opportunity_time,rank(lost_opportunity_time) over (partition By ctfo_trial_id order by lost_opportunity_time) as R_Rank from f_rpt_site_study_details_temp2 where lost_opportunity_time IS NOT NULL) A) B """)
lost_opportunity_time_df = lost_opportunity_time_df.dropDuplicates()
lost_opportunity_time_df.registerTempTable('lost_opportunity_time_df')

screen_fail_rate_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as screen_fail_rate_norm,
    case when (R_Rank = 1 and max_r = 1) then 1
    else (1 - COALESCE(((R_Rank-1 )/(max_r-1)),1)) end as neg_screen_fail_rate_norm from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id ) as max_r   from (select ctfo_trial_id,ctfo_site_id,screen_fail_rate,rank(screen_fail_rate) over (partition By ctfo_trial_id order by screen_fail_rate) as R_Rank from f_rpt_site_study_details_temp2 where screen_fail_rate IS NOT NULL) A) B """)
screen_fail_rate_df = screen_fail_rate_df.dropDuplicates()
screen_fail_rate_df.registerTempTable('screen_fail_rate_df')

average_startup_time_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as average_startup_time_norm,
    case when (R_Rank = 1 and max_r = 1) then 1
    else (1 - COALESCE(((R_Rank-1 )/(max_r-1)),1)) end as neg_average_startup_time_norm
    from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id ) as max_r   from (select ctfo_trial_id,ctfo_site_id,average_startup_time,rank(average_startup_time) over (partition By ctfo_trial_id order by average_startup_time) as R_Rank from f_rpt_site_study_details_temp2 where average_startup_time IS NOT NULL) A) B """)
average_startup_time_df = average_startup_time_df.dropDuplicates()
average_startup_time_df.registerTempTable('average_startup_time_df')

f_rpt_site_study_details = spark.sql("""select rpt.*,rnd.randomization_rate_norm,sfr.screen_fail_rate_norm,sfr.neg_screen_fail_rate_norm,lot.lost_opportunity_time_norm,lot.neg_lost_opportunity_time_norm,Ast.average_startup_time_norm,Ast.neg_average_startup_time_norm from f_rpt_site_study_details_temp2 as rpt 
 left join randomization_rate_df as rnd on lower(trim(rpt.ctfo_trial_id))=lower(trim(rnd.ctfo_trial_id)) and lower(trim(rpt.ctfo_site_id))=lower(trim(rnd.ctfo_site_id))
 left join screen_fail_rate_df sfr on lower(trim(rpt.ctfo_trial_id))=lower(trim(sfr.ctfo_trial_id)) and lower(trim(rpt.ctfo_site_id))=lower(trim(sfr.ctfo_site_id)) 
 left join lost_opportunity_time_df as lot on lower(trim(rpt.ctfo_trial_id))=lower(trim(lot.ctfo_trial_id)) and lower(trim(rpt.ctfo_site_id))=lower(trim(lot.ctfo_site_id))
 left join average_startup_time_df as Ast on lower(trim(rpt.ctfo_trial_id))=lower(trim(Ast.ctfo_trial_id)) and lower(trim(rpt.ctfo_site_id))=lower(trim(Ast.ctfo_site_id)) """)
f_rpt_site_study_details = f_rpt_site_study_details.dropDuplicates()
f_rpt_site_study_details.registerTempTable('f_rpt_site_study_details')

write_path = path.replace("table_name", "f_rpt_site_study_details")
write_path_csv = path_csv.replace("table_name", "f_rpt_site_study_details")
write_path_csv = write_path_csv + "_csv/"

f_rpt_site_study_details_final = spark.sql("""
select f_rpt_site_study_details.*, "$$cycle_id" as pt_cycle_id from f_rpt_site_study_details
""")
f_rpt_site_study_details_final = f_rpt_site_study_details_final.dropDuplicates()
f_rpt_site_study_details_final.createOrReplaceTempView("f_rpt_site_study_details_final")
f_rpt_site_study_details_final.write.format("parquet").mode("overwrite").save(path=write_path)

if "$$flag" == "Y":
    f_rpt_site_study_details_final.coalesce(1).write.option("emptyValue", None).option("nullValue", None).option(
        "header", "false").option("sep", "|") \
        .option('quote', '"').option(
        'escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_site_study_details partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_site_study_details
""")

if f_rpt_site_study_details.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_site_study_details as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_site_study_details")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")