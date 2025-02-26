################################# Module Information ######################################
#  Module Name         : Site reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_site_study_details_filters
#                           b. f_rpt_site_study_details
#                           c. f_rpt_src_site_details
#                           d. f_rpt_investigator_details
#                           e. f_rpt_site_study_details_non_performance
#  Pre-requisites      : Source table required:
#  Last changed on     : 01-09-2022
#  Last changed by     : Vicky
#  Reason for change   : Added therapeutic_area, indication, phase, trial_title ,trial_start_dt,enrollment_close_dt,  enrollment_duration,patient_dropout_rate, no_of_sites
#  Return Values       : NA
############################################################################################

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
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/commons/temp/" \
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

##############create country reporting table
temp_site_count = spark.sql("""
select d_site.site_country as country,r_trial_site.ctfo_trial_id
from
    (select ctfo_site_id, coalesce(site_country, 'Other') as site_country
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_site) d_site
        inner join (select ctfo_trial_id, ctfo_site_id from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site) r_trial_site
        on lower(trim(r_trial_site.ctfo_site_id))=lower(trim(d_site.ctfo_site_id))
group by 1,2""")
#temp_site_count.createOrReplaceTempView("temp_site_count")
temp_site_count.write.mode("overwrite").saveAsTable("temp_site_count")


aact_golden_source_map_country_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.xref_country,iq.status,iq.status_rnk,ROW_NUMBER() OVER (PARTITION BY iq.ctfo_trial_id,iq.xref_country ORDER BY iq.status_rnk ) pred_stat from (
SELECT ctfo_trial_id,
       data_src_nm,
       tsm.status,
           src_site_country as xref_country,
       case when lower(trim(tsm.status))='ongoing' then 1
            when lower(trim(tsm.status))='completed' then 2
            when lower(trim(tsm.status))='planned' then 3
            when lower(trim(tsm.status))='others' then 4 else 5 end as status_rnk,
       max(nct_id) as aact_nct_id
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               data_src_nm,
               src_trial_id,
               src_site_id,
                           src_site_country
        FROM   golden_id_details_country
        WHERE  Lower(Trim(data_src_nm)) = 'aact'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5,
                                  6) A
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
aact_golden_source_map_country_status = aact_golden_source_map_country_status.dropDuplicates()
#aact_golden_source_map_country_status.createOrReplaceTempView("aact_golden_source_map_country_status")
aact_golden_source_map_country_status.write.mode("overwrite").saveAsTable("aact_golden_source_map_country_status")

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
#citeline_golden_source_map_site_status.createOrReplaceTempView("citeline_golden_source_map_site_status")
citeline_golden_source_map_site_status.write.mode("overwrite").saveAsTable("citeline_golden_source_map_site_status")


# removed market_launch_flag from the query as it is coming from different source
country_trial_details = spark.sql("""
select  iso2_code,iso3_code,region,region_code,$$client_name_cluster,$$client_name_csu,post_trial_flag,post_trial_details,
standard_country as country from country_mapping group by 1,2,3,4,5,6,7,8,9""")
#country_trial_details.createOrReplaceTempView("country_trial_details")
country_trial_details = country_trial_details.dropDuplicates()
country_trial_details.write.mode("overwrite").saveAsTable("country_trial_details")


f_rpt_trial_new = spark.sql(""" 
select a.ctfo_trial_id, b.trial_status, 
concat_ws('||',collect_set(a.therapeutic_area)) as therapeutic_area, concat_ws('||',collect_set(a.disease)) as disease , a.trial_phase, a.study_name,
c.trial_start_dt , c.enrollment_close_date
from $$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_setup_all a
left join
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_trial b
on lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id))
left join 
$$client_name_ctfo_datastore_app_commons_$$db_env.d_trial c
on lower(trim(a.ctfo_trial_id)) = lower(trim(c.ctfo_trial_id))
group by 1,2,5,6,7,8
""")

f_rpt_trial_new = f_rpt_trial_new.dropDuplicates()
#f_rpt_trial_new.registerTempTable("f_rpt_trial_new")
f_rpt_trial_new.write.mode("overwrite").saveAsTable("f_rpt_trial_new")

aact_no_of_sites = spark.sql(""" 
SELECT A.ctfo_trial_id,
B.standard_country AS country,
SUM(B.no_of_sites) as no_of_sites
from
golden_id_details_country A 
inner join
(select a.nct_id , c.standard_country, count(b.id) as no_of_sites 
from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies a
inner join 
( select nct_id,id,status,country from $$client_name_ctfo_datastore_staging_$$db_env.aact_facilities where status in {on_com})b
on lower(trim(a.nct_id)) = lower(trim(b.nct_id))
left join country_mapping c on lower(trim(b.country)) = lower(trim(c.country))
group by 1,2) B
on lower(trim(A.src_trial_id)) = lower(trim(B.nct_id)) and lower(trim(A.src_site_country)) = lower(trim(B.standard_country)) 
where A.data_src_nm = 'aact'
group by 1,2
""".format(on_com=Ongoing_Completed))
aact_no_of_sites = aact_no_of_sites.dropDuplicates()
#aact_no_of_sites.registerTempTable('aact_no_of_sites')
aact_no_of_sites.write.mode("overwrite").saveAsTable("aact_no_of_sites")

temp_citeline_trial_data = spark.sql("""
select
trim(trial_id) as src_trial_id,
trim(src_trial_countries) as country
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove
lateral view outer explode(split(coalesce(trial_countries,'Others'),'\\\|'))one as src_trial_countries
group by 1,2""")
temp_citeline_trial_data = temp_citeline_trial_data.dropDuplicates()
temp_citeline_trial_data.registerTempTable('temp_citeline_trial_data')

temp_citeline_org_data = spark.sql("""select * from (select
    trim(site_trial_id_exp) as trial_id,
    trim(site_id) as site_id,
    trim(site_trial_status_exp) as trial_status,
    trim(site_location_country) as country
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_organization
lateral view posexplode(split(site_trial_id,'\;'))one as pos1,site_trial_id_exp
lateral view posexplode(split(site_trial_status,'\;'))two as pos2,site_trial_status_exp where pos1=pos2
group by 1,2,3,4)a where lower(trim(trial_status)) in {on_com}
""".format(on_com=Ongoing_Completed))
temp_citeline_org_data = temp_citeline_org_data.dropDuplicates()
temp_citeline_org_data.registerTempTable('temp_citeline_org_data')

citeline_no_of_sites = spark.sql("""
select aa.ctfo_trial_id,coalesce(c.standard_country,'Other') as country,aa.no_of_sites from 
( 
SELECT A.ctfo_trial_id,A.src_site_country,SUM(B.no_of_sites) as no_of_sites 
from 
(select distinct ctfo_trial_id, data_src_nm,src_trial_id,src_site_country from golden_id_details_country where lower(trim(data_src_nm)) = 'citeline') A 
left join
(select b.trial_id,b.country,count(distinct b.site_id) as no_of_sites from 
temp_citeline_trial_data a
inner join 
(select trial_id,site_id,country from temp_citeline_org_data group by 1,2,3) b  on lower(trim(a.src_trial_id)) = lower(trim(b.trial_id)) and lower(trim(a.country)) = lower(trim(b.country))
group by 1,2) B
on lower(trim(A.src_trial_id)) = lower(trim(B.trial_id)) and lower(trim(A.src_site_country)) = lower(trim(B.country)) 
group by 1,2
) aa
left join country_mapping c on lower(trim(aa.src_site_country)) = lower(trim(c.country))
""".format(on_com=Ongoing_Completed))
citeline_no_of_sites = citeline_no_of_sites.dropDuplicates()
#citeline_no_of_sites.registerTempTable('citeline_no_of_sites')
citeline_no_of_sites.write.mode("overwrite").saveAsTable("citeline_no_of_sites")

##Fetching all calculated metric by Metric Engine config sheet

f_rpt_country_temp = spark.sql("""
select 
tsc.country,
tsc.ctfo_trial_id,
cast(cmef.patients_enrolled as bigint) as patients_enrolled,
cast(cmef.patients_screened as bigint) as patients_screened ,
cast(cmef.screen_failure_rate as double) as screen_failure_rate,
cast(cmef.enrollment_rate as double) AS enrollment_rate,
cmef.patient_retention,
cmef.planned_patient_deviation,
cmef.number_patients_planned,
cmef.number_patients_screened_planned,
cast(cmef.planned_fsiv_deviation as double) as planned_fsiv_deviation,
cmef.planned_fpfv_deviation,
h.post_trial_flag,
h.post_trial_details,
h.iso2_code,
h.iso3_code,
h.region,
h.region_code,
h.$$client_name_cluster,
h.$$client_name_csu,
cast(cmef.country_startup_time as double) as country_startup_time,
cast(cmef.regulatory_iec_time as double) as regulatory_iec_time,
coalesce(cmef.trial_status,citeline_status.status,agsm.status,'Others') as trial_status,
ts.therapeutic_area as therapeutic_area, 
ts.disease as indication, 
ts.trial_phase as phase, 
ts.study_name as trial_title,
ts.trial_start_dt as trial_start_dt,
ts.enrollment_close_date as enrollment_close_dt,
cmef.enrollment_duration,
cmef.patient_dropout_rate,
coalesce(cmef.no_of_sites , citeline_ns.no_of_sites , aact_ns.no_of_sites ) as no_of_sites
from temp_site_count tsc	
left join country_metric_engine_KPI_final cmef 	
on lower(trim(tsc.ctfo_trial_id)) = lower(trim(cmef.ctfo_trial_id)) and trim(tsc.country) = trim(cmef.country)	
left join country_trial_details h on lower(trim(tsc.country)) = lower(trim(h.country))	
left join f_rpt_trial_new ts on lower(trim(tsc.ctfo_trial_id))=lower(trim(ts.ctfo_trial_id))	
left join citeline_golden_source_map_site_status citeline_status on lower(trim(tsc.ctfo_trial_id))=lower(trim(citeline_status.ctfo_trial_id))	
left join aact_golden_source_map_country_status agsm on lower(trim(tsc.country)) = lower(trim(agsm.xref_country)) and lower(trim(tsc.ctfo_trial_id))=lower(trim(agsm.ctfo_trial_id))	
left join aact_no_of_sites aact_ns on lower(trim(tsc.country)) = lower(trim(aact_ns.country)) and lower(trim(tsc.ctfo_trial_id))=lower(trim(aact_ns.ctfo_trial_id))	
left join citeline_no_of_sites citeline_ns on lower(trim(tsc.country)) = lower(trim(citeline_ns.country)) and  lower(trim(tsc.ctfo_trial_id))=lower(trim(citeline_ns.ctfo_trial_id))""")
f_rpt_country_temp = f_rpt_country_temp.dropDuplicates()
#f_rpt_country_temp.createOrReplaceTempView('f_rpt_country_temp')
f_rpt_country_temp.write.mode("overwrite").saveAsTable("f_rpt_country_temp")

f_rpt_country_temp2 = spark.sql("""Select INITCAP(country) as country,
ctfo_trial_id,
case when lower(trim(trial_status)) in {on_com} then cast(patients_enrolled as bigint) else null end as patients_enrolled,

CASE when lower(trim(trial_status)) in {on_com} then 
case
               WHEN cast(screen_failure_rate as double) < 0 THEN 0
			   WHEN cast(screen_failure_rate as double) > 100 THEN 100
               ELSE cast(screen_failure_rate as double)
END 
else null end as screen_failure_rate,
CASE when lower(trim(trial_status)) in {on_com} then
case
               WHEN cast(enrollment_rate as double) < 0 THEN 0
               ELSE cast(enrollment_rate as double)
END 
else null end as enrollment_rate,

CASE when lower(trim(trial_status)) in {com} then
case
               WHEN cast(patient_retention as double) >100  THEN 100
               WHEN cast(patient_retention as double) <0    THEN 0
               ELSE cast(patient_retention as double)
END 
else null end as patient_retention,
case when lower(trim(trial_status)) in {on_com} then cast(planned_patient_deviation as double) else null end as planned_patient_deviation,
case when lower(trim(trial_status)) in {on_com_plan} then cast(number_patients_planned as bigint) else null end as number_patients_planned,
case when lower(trim(trial_status)) in {on_com} then cast(planned_fsiv_deviation as double) else null end as planned_fsiv_deviation,
case when lower(trim(trial_status)) in {on_com} then cast(planned_fpfv_deviation as double) else null end as planned_fpfv_deviation,
post_trial_flag,
post_trial_details,
iso2_code,
iso3_code,
region,
region_code,
$$client_name_cluster,
$$client_name_csu,

CASE when lower(trim(trial_status)) in {on_com} then 
case
               WHEN cast(country_startup_time as double) < 0 THEN 0
               ELSE cast(country_startup_time as double)
END 
else null end as country_startup_time,

CASE when lower(trim(trial_status)) in {on_com} then 
case
               WHEN cast(regulatory_iec_time as double) < 0 THEN 0
               ELSE cast(regulatory_iec_time as double)
END 
else null end as regulatory_iec_time,
trial_status,
case when lower(trim(trial_status)) in {on_com} then cast(patients_screened as bigint) else null end as patients_screened,
case when lower(trim(trial_status)) in {on_com_plan} then cast(number_patients_screened_planned as bigint) else null end as number_patients_screened_planned,
therapeutic_area, indication, phase, trial_title ,cast(trial_start_dt as date) as  trial_start_dt,
cast(enrollment_close_dt as date) as  enrollment_close_dt ,
case when lower(trim(trial_status)) in {on_com} then 
CASE
  WHEN cast(enrollment_duration as double) < 0 THEN 0
  ELSE cast(enrollment_duration as double)
END
 else null end as enrollment_duration,
case when lower(trim(trial_status)) in {on_com} then 
CASE
  WHEN cast(patient_dropout_rate as double) < 0 THEN 0
  WHEN cast(patient_dropout_rate as double)>100 THEN 100
  ELSE cast(patient_dropout_rate as double)
END
 else null end as patient_dropout_rate ,
no_of_sites
from f_rpt_country_temp """.format(on_com=Ongoing_Completed, on_com_plan=Ongoing_Completed_Planned,
                                   com=Completed_variable))
f_rpt_country_temp2 = f_rpt_country_temp2.dropDuplicates()
f_rpt_country_temp2.createOrReplaceTempView('f_rpt_country_temp2')

enrollment_rate_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1 and enrollment_rate = 0) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as enrollment_rate_norm 
    from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id ) as max_r from (select ctfo_trial_id,country,enrollment_rate,rank(enrollment_rate) over (partition By ctfo_trial_id order by enrollment_rate) as R_Rank from f_rpt_country_temp2 where enrollment_rate IS NOT NULL) A) B """)
enrollment_rate_df = enrollment_rate_df.dropDuplicates()
enrollment_rate_df.registerTempTable('enrollment_rate_df')

screen_fail_rate_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as screen_fail_rank_norm,
    case when (R_Rank = 1 and max_r = 1) then 1
    else (1 - COALESCE(((R_Rank-1 )/(max_r-1)),1)) end as neg_screen_fail_rank_norm 
    from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id ) as max_r   from (select ctfo_trial_id,country,screen_failure_rate,rank(screen_failure_rate) over (partition By ctfo_trial_id order by screen_failure_rate) as R_Rank from f_rpt_country_temp2 where screen_failure_rate IS NOT NULL) A) B """)
screen_fail_rate_df = screen_fail_rate_df.dropDuplicates()
screen_fail_rate_df.registerTempTable('screen_fail_rate_df')

country_startup_time_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as country_startup_time_norm,
    case when (R_Rank = 1 and max_r = 1) then 1
    else (1 - COALESCE(((R_Rank-1 )/(max_r-1)),1)) end as neg_country_startup_time_norm 
    from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id ) as max_r   from (select ctfo_trial_id,country,country_startup_time,rank(country_startup_time) over (partition By ctfo_trial_id order by country_startup_time) as R_Rank from f_rpt_country_temp2 where country_startup_time IS NOT NULL) A) B """)
country_startup_time_df = country_startup_time_df.dropDuplicates()
country_startup_time_df.registerTempTable('country_startup_time_df')

f_rpt_country = spark.sql("""select rpt.*,rnd.enrollment_rate_norm,sfr.screen_fail_rank_norm,sfr.neg_screen_fail_rank_norm,cst.country_startup_time_norm,cst.neg_country_startup_time_norm from f_rpt_country_temp2 rpt
 left join enrollment_rate_df as rnd on lower(trim(rpt.ctfo_trial_id))=lower(trim(rnd.ctfo_trial_id)) and lower(trim(rpt.country))=lower(trim(rnd.country)) 
 left join screen_fail_rate_df sfr on lower(trim(rpt.ctfo_trial_id))=lower(trim(sfr.ctfo_trial_id)) and lower(trim(rpt.country))=lower(trim(sfr.country))
 left join country_startup_time_df cst on lower(trim(rpt.ctfo_trial_id))=lower(trim(cst.ctfo_trial_id)) and lower(trim(rpt.country))=lower(trim(cst.country))
 """)
f_rpt_country = f_rpt_country.dropDuplicates()
f_rpt_country.registerTempTable('f_rpt_country')

columns_for_replacement = ["patients_enrolled", "screen_failure_rate", "enrollment_rate", "patient_retention",
                           "planned_patient_deviation", "number_patients_planned", "planned_fsiv_deviation",
                           "planned_fpfv_deviation", "country_startup_time", "regulatory_iec_time"]
for i in columns_for_replacement:
    f_rpt_country = f_rpt_country.withColumn(i, when((col("trial_status") == 'Others'), lit(None)).otherwise(col(i)))
f_rpt_country.write.mode("overwrite").saveAsTable("f_rpt_country")

write_path = path.replace("table_name", "f_rpt_study_country_details")

f_rpt_study_country = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/$$client_name_cntryhead_ml_mapping_v0.2.csv".format(bucket_path=bucket_path))
f_rpt_study_country.registerTempTable("f_rpt_study_country")
f_rpt_study_country_final = spark.sql("""
select f_rpt_study_country.*, b.$$client_name_cluster from f_rpt_study_country
left join 
(select distinct standard_country,$$client_name_cluster from country_mapping) b on lower(trim(f_rpt_study_country.standard_country)) = lower(trim(b.standard_country))
""")
f_rpt_study_country_final = f_rpt_study_country_final.dropDuplicates()
f_rpt_study_country_final.registerTempTable("f_rpt_study_country_final")
f_rpt_study_country_final.write.format("parquet").mode("overwrite").save(path=write_path)

write_path = path.replace("table_name", "f_rpt_country")
write_path_csv = path_csv.replace("table_name", "f_rpt_country")
write_path_csv = write_path_csv + "_csv/"

f_rpt_country_final = spark.sql("""
select f_rpt_country.*, "$$cycle_id" as pt_cycle_id from f_rpt_country
""")
#f_rpt_country_final.registerTempTable("f_rpt_country_final")
f_rpt_country_final.write.mode("overwrite").saveAsTable("f_rpt_country_final")
f_rpt_country_final.write.format("parquet").mode("overwrite").save(path=write_path)

if "$$flag" == "Y":
    f_rpt_country_final.coalesce(1).write.option("emptyValue", None).option("nullValue", None).option("header",
                                                                                                      "false").option(
        "sep", "|").option('quote',
                           '"') \
        .option('escape', '"').option(
        "multiLine", "true").mode('overwrite').csv(write_path_csv)

    write_path_csv = path_csv.replace("table_name", "f_rpt_study_country_details")
    write_path_csv = write_path_csv + "_csv/"
    f_rpt_study_country_final.coalesce(1).write.option("emptyValue", None).option("nullValue", None).option("header",
                                                                                                            "false").option(
        "sep", "|").option('quote',
                           '"') \
        .option('escape', '"').option(
        "multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_country partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_country
""")

if f_rpt_country.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_country as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_country")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
