################################# Module Information #####################################################
#  Module Name         : Trial Country Metric Engine Calculation Data
#  Purpose             : To create the Trial Country KPI
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : Trial Site KPIs
#  Last changed on     : 31-01-2022
#  Last changed by     : Vicky Soni
#  Reason for change   : Initial Code $db_envelopment
##########################################################################################################


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

path = bucket_path + "/applications/commons/mce/mce_src/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")
########## about trial, d_trial.nct_id not present


trial_status_mapping_temp = spark.read.format('csv').option('header','true').option('delimiter', ',') \
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
from (select distinct(standard_country) as standard_country,country
    from standard_country_mapping )  cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

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
        FROM   $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site
        GROUP  BY 1,
                  2) A
       LEFT JOIN (SELECT data_src_nm,
                         ctfo_trial_id,
                         src_trial_id
                  FROM
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
                  GROUP  BY 1,
                            2,
                            3) B
              ON lower(trim(A.ctfo_trial_id)) = lower(trim(B.ctfo_trial_id))
       LEFT JOIN (SELECT data_src_nm,
                         ctfo_site_id,
                         src_site_country,
                         src_site_id
                  FROM   (SELECT data_src_nm,
                                 ctfo_site_id,
                                 site_country as src_site_country,
                                 Explode(Split(src_site_id, ';')) AS src_site_id
                          FROM
       $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site)
                  GROUP  BY 1,
                            2,
                            3,
                            4) C
              ON lower(trim(A.ctfo_site_id)) = lower(trim(C.ctfo_site_id))
                 AND Lower(Trim(B.data_src_nm)) = Lower(Trim(C.data_src_nm))
GROUP  BY 1,
          2,
          3,
          4,
          5,
          6
""")
golden_id_det = golden_id_det.dropDuplicates()
# golden_id_det.createOrReplaceTempView('golden_id_details_country')
golden_id_det.write.mode("overwrite").saveAsTable("golden_id_details_country")

# Code for DQS Metrics Calculation
dqs_golden_source_map_country = spark.sql("""
SELECT ctfo_trial_id,
       data_src_nm,
       nct_number as nct_number,
       member_study_id as member_study_id,
       src_site_country as xref_country,
       facility_country as dqs_country
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               data_src_nm,
               src_trial_id,
               src_site_id,
               src_site_country
        FROM   golden_id_details_country
        WHERE  Lower(Trim(data_src_nm)) = 'ir'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5,
                  6) A
       INNER JOIN (SELECT nct_number,
                          member_study_id,
                          facility_country,
                          facility_golden_id
                   FROM   $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
                   GROUP  BY 1,
                             2,
                             3,
                             4) B
               ON lower(trim(A.src_trial_id)) = lower(trim(B.member_study_id))
                  AND lower(trim(A.src_site_id)) = lower(trim(B.facility_golden_id))
GROUP  BY 1,
          2,
          3,
          4,
          5,
          6
""")
dqs_golden_source_map_country = dqs_golden_source_map_country.dropDuplicates()
# dqs_golden_source_map_country.createOrReplaceTempView("dqs_golden_source_map_country")
dqs_golden_source_map_country.write.mode("overwrite").saveAsTable("dqs_golden_source_map_country")

dqs_country_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.xref_country as country,iq.status,iq.status_rnk,
ROW_NUMBER() OVER (PARTITION BY iq.ctfo_trial_id,iq.xref_country ORDER BY iq.status_rnk ) pred_stat
from (select A.ctfo_trial_id,tsm.status,A.xref_country,
case when lower(trim(tsm.status))='ongoing' then 1
when lower(trim(tsm.status))='completed' then 2
when lower(trim(tsm.status))='planned' then 3
when lower(trim(tsm.status))='others' then 4 else 5 end as status_rnk
FROM
(select nct_number,ctfo_trial_id,dqs_country as country,xref_country ,member_study_id from dqs_golden_source_map_country group by 1,2,3,4,5) A
left join (
select member_study_id,facility_golden_id,facility_country,site_status from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir   group by 1,2,3,4) B
ON lower(trim(A.member_study_id)) = lower(trim(B.member_study_id)) and lower(trim(A.country)) = lower(trim(B.facility_country))
left join (select distinct raw_status,status from trial_status_mapping) tsm  on lower(trim(B.site_status))=lower(trim(tsm.raw_status)))iq
)ab where pred_stat=1
""")
dqs_country_status = dqs_country_status.dropDuplicates()
dqs_country_status.registerTempTable('dqs_country_status')

dqs_lsfr = spark.sql("""select ctfo_trial_id,  Max(last_subject_enrolled_dt) AS last_subject_randomized_dt from
dqs_golden_source_map_country A
left join (select member_study_id,last_subject_enrolled_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir where
lower(trim(site_status)) in {on_com}  ) B
               ON Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id))
group by 1
""".format(on_com=Ongoing_Completed))
dqs_lsfr.createOrReplaceTempView("dqs_lsfr")

dqs_dates = spark.sql("""
select A.ctfo_trial_id,
A.xref_country as country,
Min(first_subject_enrolled_dt)           AS first_subject_enrolled_dt,
Min(first_subject_randomized_dt)         AS first_subject_randomized_dt,
Max(last_subject_enrolled_dt)            AS last_subject_enrolled_dt,
Max(last_subject_last_visit_dt)          AS last_subject_last_visit_dt,
Min(site_ready_to_enroll_dt)             AS site_ready_to_enroll_dt,
Max(C.last_subject_randomized_dt)          AS last_subject_randomized_dt
from
(select member_study_id,ctfo_trial_id,dqs_country as country,xref_country  from dqs_golden_source_map_country) A
left join(
select member_study_id,facility_country,
Min(first_subject_consented_dt)           AS first_subject_enrolled_dt,
Min(first_subject_enrolled_dt)            AS first_subject_randomized_dt,
Max(last_subject_consented_dt)            AS last_subject_enrolled_dt,
Max(last_subject_last_visit_dt)           AS last_subject_last_visit_dt,
Min(site_open_dt)                         AS site_ready_to_enroll_dt

from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir where
lower(trim(site_status)) in {on_com}  group by 1,2)B
on Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id))
and Lower(Trim(A.country)) = Lower(Trim(B.facility_country))
left join dqs_lsfr C on lower(Trim(A.ctfo_trial_id)) = lower(Trim(C.ctfo_trial_id))
group by 1,2
""".format(on_com=Ongoing_Completed))
dqs_dates = dqs_dates.dropDuplicates()
dqs_dates.createOrReplaceTempView("dqs_dates")

dqs_on_com = spark.sql("""select A.ctfo_trial_id,
A.xref_country as country,
percentile(b.irb_cycle_time, 0.5, 10000) as regulatory_iec_time,
sum(b.enrolled) as enrolled,
sum(b.consented) as consented,
sum(b.completed) as completed
from
(select nct_number,ctfo_trial_id,dqs_country as country,xref_country ,member_study_id from dqs_golden_source_map_country group by 1,2,3,4,5) A
left join
(select member_study_id,facility_golden_id,facility_country,percentile(irb_cycle_time,0.5,10000) as irb_cycle_time,
max(enrolled) as enrolled,max(consented) as consented,max(completed) as completed from  $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
where lower(trim(site_status)) in {on_com} group by 1,2,3) b on lower(trim(A.member_study_id)) = lower(trim(b.member_study_id))
and lower(trim(A.country))=lower(trim(b.facility_country))
group by 1,2
""".format(on_com=Ongoing_Completed))
dqs_on_com = dqs_on_com.dropDuplicates()
# dqs_on_com.registerTempTable('dqs_on_com')
dqs_on_com.write.mode("overwrite").saveAsTable("dqs_on_com")

dqs_on_com_no_sites = spark.sql("""select A.ctfo_trial_id,
A.xref_country as country,
sum(sites.no_of_sites) as no_of_sites
from
(select nct_number,ctfo_trial_id,dqs_country as country,xref_country ,member_study_id from dqs_golden_source_map_country group by 1,2,3,4,5) A
left join
(select member_study_id,facility_country,count(distinct facility_golden_id) as no_of_sites from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
where lower(trim(site_status)) in {on_com} group by 1,2) sites
on lower(trim(A.member_study_id)) = lower(trim(sites.member_study_id))
and lower(trim(A.country))=lower(trim(sites.facility_country))
group by 1,2
""".format(on_com=Ongoing_Completed))
dqs_on_com_no_sites = dqs_on_com_no_sites.dropDuplicates()
dqs_on_com_no_sites.registerTempTable('dqs_on_com_no_sites')
# dqs_on_com_no_sites.write.mode("overwrite").saveAsTable("dqs_on_com_no_sites")

dqs_com = spark.sql("""select A.ctfo_trial_id,A.xref_country as country,sum(b.enrolled) as enrolled_completed,sum(b.consented) as consented_completed , sum(b.completed) as completed_completed  from
(select nct_number,ctfo_trial_id,dqs_country as country,xref_country ,member_study_id from dqs_golden_source_map_country group by 1,2,3,4,5) A
left join (select member_study_id,facility_golden_id,facility_country,max(enrolled) as enrolled,max(consented) as consented,max(completed) as completed from  $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
where lower(trim(site_status)) in {com} group by 1,2,3) b on lower(trim(A.member_study_id)) = lower(trim(b.member_study_id))
and lower(trim(A.country))=lower(trim(B.facility_country))
group by 1,2
""".format(com=Completed_variable))
dqs_com = dqs_com.dropDuplicates()
# dqs_com.registerTempTable('dqs_com')
dqs_com.write.mode("overwrite").saveAsTable("dqs_com")

dqs_temp_final = spark.sql("""select A.xref_country,A.ctfo_trial_id, B.enrolled,B.completed ,B.consented,
docnms.no_of_sites,D.enrolled_completed, D.completed_completed,s.status,regulatory_iec_time,
rr.last_subject_randomized_dt,rr.first_subject_enrolled_dt,rr.first_subject_randomized_dt,rr.last_subject_enrolled_dt,rr.last_subject_last_visit_dt,
rr.site_ready_to_enroll_dt
from
(select member_study_id,ctfo_trial_id,xref_country ,dqs_country as country from dqs_golden_source_map_country) A
left join
dqs_on_com B
on Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(B.ctfo_trial_id))
AND Lower(Trim(A.xref_country)) = Lower(Trim(B.country))
left join dqs_on_com_no_sites docnms
on Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(docnms.ctfo_trial_id))
AND Lower(Trim(A.xref_country)) = Lower(Trim(docnms.country))
left join
dqs_com D
on Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(D.ctfo_trial_id))
AND Lower(Trim(A.xref_country)) = Lower(Trim(D.country))
left join
dqs_country_status s on lower(trim(A.xref_country)) = lower(trim(s.country)) and trim(A.ctfo_trial_id)=lower(trim(s.ctfo_trial_id))
left join
dqs_dates rr on lower(trim(A.xref_country)) = lower(trim(rr.country)) and lower(trim(A.ctfo_trial_id))=lower(trim(rr.ctfo_trial_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16""")
dqs_temp_final = dqs_temp_final.dropDuplicates()
dqs_temp_final.createOrReplaceTempView("dqs_temp_final")

dqs_trial_country_mce = spark.sql("""SELECT
country_mapping.standard_country as country,
ctfo_trial_id,
'' as patients_consented_planned ,
consented as patients_consented_actual ,
'' as patients_screened_planned ,
consented as patients_screened_actual,
'' as dropped_at_screening_actual ,
'' as patients_randomized_planned,
enrolled  as patients_randomized_actual,
enrolled_completed as patients_randomized_actual_completed,
completed as patients_completed_actual ,
completed_completed as patients_completed_actual_completed,
'' as patients_dropped_actual ,
'' as count_investigators,
'' as core_non_core_flag ,
'' as no_of_sites_planned ,
no_of_sites as no_of_sites_actual ,
status  as country_trial_status ,
'' as ha_ca_submission_bsln_dt  ,
'' as ha_ca_submission_planned_dt  ,
'' as ha_ca_submission_actual_dt ,
'' as ha_ca_approval_bsln_dt ,
'' as ha_ca_approval_planned_dt ,
'' as ha_ca_approval_actual_dt ,
'' as first_local_irb_iec_submission_bsln_dt ,
'' as first_local_irb_iec_submission_planned_dt  ,
'' as first_local_irb_iec_submission_actual_dt   ,
'' as first_local_irb_iec_approval_bsln_dt       ,
'' as first_local_irb_iec_approval_planned_dt    ,
'' as first_local_irb_iec_approval_actual_dt     ,
'' as first_site_initiation_visit_bsln_dt        ,
'' as first_site_initiation_visit_planned_dt     ,
'' as first_site_initiation_visit_actual_dt      ,
'' as first_subject_first_visit_bsln_dt          ,
'' as first_subject_first_visit_planned_dt       ,
first_subject_enrolled_dt as first_subject_first_visit_actual_dt ,
'' as first_subject_first_treatment_bsln_dt      ,
'' as first_subject_first_treatment_planned_dt   ,
first_subject_randomized_dt as first_subject_first_treatment_actual_dt,
'' as country_lsfr,
'' as last_subject_first_visit_bsln_dt           ,
'' as last_subject_first_visit_planned_dt        ,
last_subject_enrolled_dt as last_subject_first_visit_actual_dt ,
'' as last_subject_first_treatment_bsln_dt       ,
'' as last_subject_first_treatment_planned_dt    ,
'' as  last_subject_first_treatment_actual_dt    ,
'' as last_subject_last_treatment_bsln_dt        ,
'' as last_subject_last_treatment_planned_dt     ,
'' as last_subject_last_treatment_actual_dt      ,
'' as last_subject_last_visit_bsln_dt            ,
'' as last_subject_last_visit_planned_dt         ,
last_subject_last_visit_dt as last_subject_last_visit_actual_dt ,
site_ready_to_enroll_dt as site_ready_to_enrol_actual_dt,
last_subject_randomized_dt as lsfr_dt,
'ir' as source ,
regulatory_iec_time,
'' as fsiv_fsfr_delta
from dqs_temp_final
left join
country_mapping on lower(trim(dqs_temp_final.xref_country)) = lower(trim(country_mapping.country))
""")
dqs_trial_country_mce = dqs_trial_country_mce.dropDuplicates()
# dqs_trial_country_mce.createOrReplaceTempView("dqs_trial_country_mce")

dqs_trial_country_mce.write.mode('overwrite').saveAsTable('dqs_trial_country_mce')

centre_loc_map = spark.sql("""
select src_trial_id,src_country_id,usl_study_country.country,country_mapping.standard_country as standard_country,tsm.status
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country  usl_study_country
left join country_mapping on lower(trim(usl_study_country.country)) = lower(trim(country_mapping.country))
left join
(select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(usl_study_country.country_trial_status))


group by 1,2,3,4,5
""")
centre_loc_map = centre_loc_map.dropDuplicates()
# centre_loc_map.createOrReplaceTempView("centre_loc_map_country")
centre_loc_map.write.mode("overwrite").saveAsTable("centre_loc_map_country")

ctms_golden_source_map_country_0 = spark.sql(""" select cgsm_cntry.* from (
select ctfo_trial_id,A.src_trial_id ,B.standard_country as standard_country,src_country_id,B.country,
  row_number() over (partition by ctfo_trial_id,standard_country order by A.src_trial_id) as rnk
  from
(select ctfo_trial_id,  data_src_nm, src_trial_id,  src_site_country
    from golden_id_details_country
    where lower(trim(data_src_nm)) = 'ctms' group by 1,2,3,4) A
inner join
    (select  src_trial_id,src_country_id,country,standard_country from centre_loc_map_country group by 1,2,3,4) B
on lower(trim(A.src_trial_id)) = lower(trim(B.src_trial_id)) and lower(trim(A.src_site_country))=lower(trim(B.standard_country))
group by 1,2,3,4,5) cgsm_cntry
""")
ctms_golden_source_map_country_0 = ctms_golden_source_map_country_0.dropDuplicates()
# ctms_golden_source_map_country_0.createOrReplaceTempView("ctms_golden_source_map_country_0")

ctms_golden_source_map_country_0.write.mode("overwrite").saveAsTable(
    "ctms_golden_source_map_country_0")

ctms_golden_source_map_country_1 = spark.sql("""select ctfo_trial_id,standard_country,status from ( select fin.*,    row_number() over (partition by ctfo_trial_id,standard_country order by fin.status_rnk) as rnk
from
(select ctfo_trial_id, standard_country,status,
 case when lower(trim(status))='ongoing' then 1
            when lower(trim(status))='completed' then 2
            when lower(trim(status))='planned' then 3
            when lower(trim(status))='others' then 4 else 5 end as status_rnk from
(select ctfo_trial_id,A.src_trial_id ,B.standard_country as standard_country,src_country_id ,status from
(select ctfo_trial_id,  data_src_nm, src_trial_id,  src_site_country
    from golden_id_details_country
    where lower(trim(data_src_nm)) = 'ctms' group by 1,2,3,4) A
inner join
    (select  src_trial_id,src_country_id,country,standard_country,status from centre_loc_map_country group by 1,2,3,4,5) B
on lower(trim(A.src_trial_id)) = lower(trim(B.src_trial_id)) and lower(trim(A.src_site_country))=lower(trim(B.standard_country))
group by 1,2,3,4 ,5))fin )fin1 where fin1.rnk = 1

""")
ctms_golden_source_map_country_1 = ctms_golden_source_map_country_1.dropDuplicates()
ctms_golden_source_map_country_1.createOrReplaceTempView(
    "ctms_golden_source_map_country_1")

ctms_golden_source_map_country = spark.sql(""" select a.ctfo_trial_id,a.src_trial_id ,a.standard_country ,a.src_country_id,a.country,b.status from
ctms_golden_source_map_country_0 a left join ctms_golden_source_map_country_1 b on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id)) and  lower(trim(a.standard_country))=lower(trim(b.standard_country))
""")
ctms_golden_source_map_country = ctms_golden_source_map_country.dropDuplicates()
# ctms_golden_source_map_country.createOrReplaceTempView("ctms_golden_source_map_country")

ctms_golden_source_map_country.write.mode("overwrite").saveAsTable(
    "ctms_golden_source_map_country")

ctms_dates = spark.sql("""select
cgsm.ctfo_trial_id,
cgsm.standard_country,
min(a.fsiv_dt) as fsiv_dt,
min(a.fsfr_dt) as fsfr_dt,
max(a.lsfv_dt) as lsfv_dt,
max(b.lsfr_dt) as lsfr_dt,
max(c.cntry_lsfr_dt) as cntry_lsfr_dt

from
(select ctfo_trial_id,src_trial_id,country as src_country,standard_country from ctms_golden_source_map_country group by 1,2,3,4) cgsm
left join
(select src_trial_id,src_country_id,first_site_initiation_visit_actual_dt as fsiv_dt,first_subject_first_treatment_actual_dt as fsfr_dt,last_subject_first_visit_actual_dt
as lsfv_dt,fsiv_fsfr_delta,country from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country where lower(trim(country_trial_status)) in {on_com}group by 1,2,3,4,5,6,7) a
on lower(trim(a.src_trial_id))=lower(trim(cgsm.src_trial_id)) and lower(trim(a.country))=lower(trim(cgsm.src_country))
left join
(select src_trial_id,last_subject_first_treatment_actual_dt as lsfr_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study where lower(trim(trial_status)) in {on_com}) b on lower(trim(b.src_trial_id))=lower(trim(cgsm.src_trial_id))
left join
(select src_trial_id,max(last_subject_first_treatment_actual_dt) as cntry_lsfr_dt, src_country_id,country from
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country where lower(trim(country_trial_status)) in {on_com} group by 1,3,4) c on lower(trim(c.src_trial_id))=lower(trim(cgsm.src_trial_id)) and lower(trim(c.country))=lower(trim(cgsm.src_country))
group by 1,2""".format(on_com=Ongoing_Completed))

ctms_dates = ctms_dates.dropDuplicates()
# ctms_dates.registerTempTable('ctms_dates')
ctms_dates.write.mode("overwrite").saveAsTable("ctms_dates")

####################################################################33


ctms_trial_country_mce = spark.sql("""SELECT
cgsm.standard_country as country,
cgsm.ctfo_trial_id,
sum(cntry_on_com_plan.patients_consented_planned) as  patients_consented_planned,
sum(cntry_on_com.patients_consented_actual) as  patients_consented_actual,
sum(cntry_on_com_plan.patients_screened_planned)  as patients_screened_planned,
sum(cntry_on_com.patients_screened_actual) as    patients_screened_actual,
sum(cntry_on_com.dropped_at_screening_actual) as dropped_at_screening_actual,
sum(cntry_on_com_plan.patients_randomized_planned) as   patients_randomized_planned,
sum(cntry_on_com.patients_randomized_actual ) as  patients_randomized_actual ,
sum(cntry_com.patients_randomized_actual) as patients_randomized_actual_completed,
sum(cntry_on_com.patients_completed_actual)  as    patients_completed_actual,
sum(cntry_com.patients_completed_actual) as  patients_completed_actual_completed,
sum(cntry_on_com.patients_dropped_actual) as    patients_dropped_actual,
sum(s.count_investigators) as   count_investigators,
first(s.core_non_core_flag) as core_non_core_flag,
sum(cntry_on_com_plan.no_of_sites_planned)   as  no_of_sites_planned,
sum(cntry_on_com.no_of_sites_actual)  as  no_of_sites_actual,
cgsm.status as country_trial_status,
min(s.ha_ca_submission_bsln_dt) as ha_ca_submission_bsln_dt ,
min(cntry_on_com_plan.ha_ca_submission_planned_dt)  as ha_ca_submission_planned_dt,
min(cntry_on_com.ha_ca_submission_actual_dt)  as  ha_ca_submission_actual_dt,
max(s.ha_ca_approval_bsln_dt) as ha_ca_approval_bsln_dt ,
max(cntry_on_com_plan.ha_ca_approval_planned_dt)    as  ha_ca_approval_planned_dt,
max(cntry_on_com.ha_ca_approval_actual_dt) as  ha_ca_approval_actual_dt,
min(s.first_local_irb_iec_submission_bsln_dt) as  first_local_irb_iec_submission_bsln_dt,
min(cntry_on_com_plan.first_local_irb_iec_submission_planned_dt) as first_local_irb_iec_submission_planned_dt ,
min(cntry_on_com.first_local_irb_iec_submission_actual_dt) as first_local_irb_iec_submission_actual_dt  ,
max(s.first_local_irb_iec_approval_bsln_dt) as first_local_irb_iec_approval_bsln_dt ,
max(cntry_on_com_plan.first_local_irb_iec_approval_planned_dt) as  first_local_irb_iec_approval_planned_dt  ,
max(cntry_on_com.first_local_irb_iec_approval_actual_dt) as first_local_irb_iec_approval_actual_dt ,
min(s.first_site_initiation_visit_bsln_dt) as first_site_initiation_visit_bsln_dt ,
min(cntry_on_com_plan.first_site_initiation_visit_planned_dt)  as first_site_initiation_visit_planned_dt  ,
min(cntry_on_com.first_site_initiation_visit_actual_dt) as  first_site_initiation_visit_actual_dt,
min(s.first_subject_first_visit_bsln_dt) as first_subject_first_visit_bsln_dt,
min(cntry_on_com_plan.first_subject_first_visit_planned_dt) as   first_subject_first_visit_planned_dt,
min(cntry_on_com.first_subject_first_visit_actual_dt)  as   first_subject_first_visit_actual_dt,
min(s.first_subject_first_treatment_bsln_dt) as first_subject_first_treatment_bsln_dt ,
min(cntry_on_com_plan.first_subject_first_treatment_planned_dt) as first_subject_first_treatment_planned_dt ,
min(cntry_on_com.first_subject_first_treatment_actual_dt)  as first_subject_first_treatment_actual_dt ,
max(cd.cntry_lsfr_dt) as country_lsfr,
max(s.last_subject_first_visit_bsln_dt) as last_subject_first_visit_bsln_dt ,
max(cntry_on_com_plan.last_subject_first_visit_planned_dt)   as   last_subject_first_visit_planned_dt  ,
max(cntry_on_com.last_subject_first_visit_actual_dt)  as   last_subject_first_visit_actual_dt    ,
max(s.last_subject_first_treatment_bsln_dt) as last_subject_first_treatment_bsln_dt,
max(cntry_on_com_plan.last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt  ,
max(cntry_on_com.last_subject_first_treatment_actual_dt)  as  last_subject_first_treatment_actual_dt ,
max(s.last_subject_last_treatment_bsln_dt) as last_subject_last_treatment_bsln_dt,
max(cntry_on_com_plan.last_subject_last_treatment_planned_dt) as  last_subject_last_treatment_planned_dt  ,
max(cntry_on_com.last_subject_last_treatment_actual_dt)  as  last_subject_last_treatment_actual_dt  ,
max(s.last_subject_last_visit_bsln_dt) as last_subject_last_visit_bsln_dt ,
max(cntry_on_com_plan.last_subject_last_visit_planned_dt)  as   last_subject_last_visit_planned_dt    ,
max(cntry_on_com.last_subject_last_visit_actual_dt)  as   last_subject_last_visit_actual_dt     ,
'' as site_ready_to_enrol_actual_dt,
max(cd.lsfr_dt) as lsfr_dt,
s.source,
'' as regulatory_iec_time,
14 as fsiv_fsfr_delta
from
(select ctfo_trial_id,src_trial_id,country,standard_country,status from ctms_golden_source_map_country group by 1,2,3,4,5) cgsm
left join $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country s on
lower(trim(s.src_trial_id)) = lower(trim(cgsm.src_trial_id)) and lower(trim(s.country))=lower(trim(cgsm.country))
left join
ctms_dates cd on lower(trim(cd.ctfo_trial_id))=lower(trim(cgsm.ctfo_trial_id)) and lower(trim(cd.standard_country))=lower(trim(cgsm.standard_country))
left join
(select distinct raw_status,status from trial_status_mapping) tsm  on lower(trim(s.country_trial_status))=lower(trim(tsm.raw_status))
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country where lower(trim(country_trial_status)) in {on_com}) cntry_on_com on
lower(trim(cntry_on_com.src_trial_id))=lower(trim(cgsm.src_trial_id)) and lower(trim(cntry_on_com.country))=lower(trim(cgsm.country))
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country where lower(trim(country_trial_status)) in {on_com_plan}) cntry_on_com_plan on
lower(trim(cntry_on_com_plan.src_trial_id))=lower(trim(cgsm.src_trial_id)) and lower(trim(cntry_on_com_plan.country))=lower(trim(cgsm.country))
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country where lower(trim(country_trial_status)) in {com}) cntry_com on
lower(trim(cntry_com.src_trial_id))=lower(trim(cgsm.src_trial_id)) and lower(trim(cntry_com.country))=lower(trim(cgsm.country))
group by 1,2,18,53,55,56,57
""".format(on_com=Ongoing_Completed, on_com_plan=Ongoing_Completed_Planned,
           com=Completed_variable))
ctms_trial_country_mce = ctms_trial_country_mce.dropDuplicates()
# ctms_trial_country_mce.createOrReplaceTempView("ctms_trial_country_mce")
ctms_trial_country_mce.write.mode('overwrite').saveAsTable(
    'ctms_trial_country_mce')

trial_country_mce = spark.sql("""select * from dqs_trial_country_mce
union
 select * from ctms_trial_country_mce""")
trial_country_mce = trial_country_mce.dropDuplicates()
# trial_country_mce.registerTempTable('trial_country_mce')
trial_country_mce.write.mode('overwrite').saveAsTable('trial_country_mce')

# write_path = path.replace("table_name", "country")
# trial_country_mce.coalesce(1).write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_country partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   trial_country_mce
""")

if trial_country_mce.count() == 0:
    print(
        "Skipping copy_hdfs_to_s3 for trial_country_mce as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3(
        "$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_country")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")