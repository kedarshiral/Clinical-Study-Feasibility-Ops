################################# Module Information #####################################################
#  Module Name         : Trial Site Metric Engine Calculation Data
#  Purpose             : To create the Trial Site KPI
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : Trial Site KPIs
#  Last changed on     : 06-01-2022
#  Last changed by     : Kashish
#  Reason for change   : Initial Code $db_envelopment
##########################################################################################################


import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from CommonUtils import CommonUtils
from pyspark.sql.functions import lower, col
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')


path = bucket_path + "/applications/commons/mce/mce_src/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")
########## Status Variablization ###################

trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

trial_status_mapping = spark.sql(""" select distinct * from (select raw_status, status from trial_status_mapping_temp 
union select status, status from trial_status_mapping_temp )  """)
trial_status_mapping.registerTempTable('trial_status_mapping')

# Values in Variables

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

status_mapping = spark.read.format("csv").option("header", "true").load(
    "{bucket_path}/uploads/trial_status.csv".format(bucket_path=bucket_path))
# status_mapping.createOrReplaceTempView("status_mapping")
status_mapping.write.mode('overwrite').saveAsTable('status_mapping')

ctms_status_mapping = spark.sql(
    """select raw_status,status from status_mapping  where lower(trim(data_source))='ctms'""")

ctms_status_mapping = ctms_status_mapping.dropDuplicates()
# ctms_status_mapping.registerTempTable('ctms_status_mapping')
ctms_status_mapping.write.mode('overwrite').saveAsTable('ctms_status_mapping')

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.$$client_name_cluster_code as region_code,scpm.$$client_name_cluster,scpm.$$client_name_csu,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country, country
    from standard_country_mapping )  cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')

golden_id_det = spark.sql("""
SELECT B.ctfo_trial_id,
       C.ctfo_site_id,
       B.data_src_nm,
       src_trial_id,
       src_site_country,
       src_site_id
FROM (SELECT ctfo_trial_id, ctfo_site_id
    FROM $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site
    GROUP  BY 1, 2) A
LEFT JOIN (SELECT data_src_nm, ctfo_trial_id, src_trial_id
        FROM $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial 
        GROUP  BY 1, 2, 3) B
ON lower(trim(A.ctfo_trial_id)) = lower(trim(B.ctfo_trial_id))
LEFT JOIN (SELECT data_src_nm, ctfo_site_id, src_site_country, src_site_id
        FROM (SELECT data_src_nm, ctfo_site_id, site_country as src_site_country, Explode(Split(src_site_id, ';')) AS src_site_id 
                                  FROM $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site)
        GROUP  BY 1, 2, 3, 4) C
ON lower(trim(A.ctfo_site_id)) = lower(trim(C.ctfo_site_id))
AND LOWER(TRIM(B.data_src_nm)) = LOWER(TRIM(C.data_src_nm))
GROUP  BY 1, 2, 3, 4, 5, 6
""")
golden_id_det = golden_id_det.dropDuplicates()
# golden_id_det.createOrReplaceTempView('golden_id_details_site')
golden_id_det.write.mode('overwrite').saveAsTable('golden_id_details_site')

# Code for DQS Metrics Calculation
dqs_golden_source_map_site = spark.sql("""
SELECT ctfo_trial_id,
       ctfo_site_id,
       data_src_nm,
       nct_number,
       src_site_country as xref_country,
       facility_country as dqs_country,
       facility_golden_id,
       member_study_id
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
          6,
          7,
          8
""")
dqs_golden_source_map_site = dqs_golden_source_map_site.dropDuplicates()
# dqs_golden_source_map_site.createOrReplaceTempView("dqs_golden_source_map_site")
dqs_golden_source_map_site.write.mode('overwrite').saveAsTable('dqs_golden_source_map_site')

dqs_country_standard = spark.sql(""" select
distinct A.facility_country,coalesce(B.standard_country,A.facility_country) as standard_country,C.iso2_code as xref_country_code
from 
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir A
left join
standard_country_mapping B
on lower(trim(A.facility_country))=lower(trim(B.country))
left join country_mapping C
on lower(trim(A.facility_country))=lower(trim(C.country))
""").registerTempTable('dqs_country_standard')

# dqs_randomization_rate_temp = spark.sql("""select A.ctfo_trial_id, b.nct_number,b.country,Max(b.last_subject_enrolled_dt) as cntry_last_subject_randomized_dt
# from dqs_golden_source_map_site A
# left join $$client_name_ctfo_datastore_staging_$$db_env.dqs_study B ON Lower(Trim(A.nct_number)) = Lower(Trim(B.nct_number))
# AND Lower(Trim(A.xref_country)) = Lower(Trim(B.country)) group by 1,2,3""")
# dqs_randomization_rate_temp.createOrReplaceTempView("dqs_randomization_rate_temp")

dqs_randomization_rate_temp = spark.sql("""select A.ctfo_trial_id,C.standard_country,Max(b.last_subject_enrolled_dt) as cntry_last_subject_randomized_dt from dqs_golden_source_map_site A
left join dqs_country_standard C on lower(trim(A.dqs_country))=lower(trim(C.facility_country))
left join (select member_study_id,last_subject_enrolled_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir where 
lower(trim(site_status)) in {on_com}) B on Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id))
AND Lower(Trim(A.xref_country)) = Lower(Trim(C.standard_country)) group by 1,2""".format(on_com=Ongoing_Completed))
dqs_randomization_rate_temp.createOrReplaceTempView("dqs_randomization_rate_temp")

dqs_site_details_temp_11 = spark.sql("""select ctfo_trial_id, ctfo_site_id ,Max(last_subject_enrolled_dt) AS last_subject_randomized_dt_final from 
dqs_golden_source_map_site A
left join (select member_study_id,facility_golden_id,last_subject_enrolled_dt from  $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir) B
               ON Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id)) and Lower(Trim(A.facility_golden_id)) = Lower(Trim(B.facility_golden_id))
group by 1,2""")
dqs_site_details_temp_11.createOrReplaceTempView("dqs_site_details_temp_11")

dqs_site_details_temp_1 = spark.sql("""select ctfo_trial_id,  Max(last_subject_enrolled_dt) AS last_subject_randomized_dt from 
dqs_golden_source_map_site A
left join (select member_study_id,last_subject_enrolled_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
where lower(trim(site_status)) in {on_com}) B
               ON Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id))
group by 1
""".format(on_com=Ongoing_Completed))
dqs_site_details_temp_1.createOrReplaceTempView("dqs_site_details_temp_1")

dqs_site_details_temp_2 = spark.sql("""SELECT A.ctfo_trial_id,
        A.ctfo_site_id,
      A.member_study_id,
        A.facility_golden_id,
        site_startup_time,
        case when trim(lower(pct_screen_fail)) like '%\%%' then regexp_replace(pct_screen_fail,"\%","") else pct_screen_fail end as pct_screen_fail,
        tsm.status as site_status,
        enrolled                AS patients_enrolled, 
        consented              AS patients_screened, 
        completed as patients_completed,
        first_subject_consented_dt           AS first_subject_enrolled_dt,
        first_subject_enrolled_dt            AS first_subject_randomized_dt,
        last_subject_consented_dt            AS last_subject_enrolled_dt,
        C.last_subject_randomized_dt,
        CC.last_subject_randomized_dt_final,
        last_subject_last_visit_dt,           
        site_open_dt                         AS site_ready_to_enroll_dt,
        A.xref_country                     AS xref_country,
        D.xref_country_code
FROM   dqs_golden_source_map_site A
       left join dqs_country_standard D on lower(trim(A.dqs_country))=lower(trim(D.facility_country))
        LEFT JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir B
               ON Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id))
                  AND Lower(Trim(A.facility_golden_id)) =
                      Lower(Trim(B.facility_golden_id))
                  AND Lower(Trim(A.xref_country)) =
                      Lower(Trim(D.standard_country))
                  left join (select distinct raw_status,status from trial_status_mapping) tsm  on lower(trim(B.site_status))=lower(trim(tsm.raw_status))
                  left join dqs_site_details_temp_1 C  on Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(C.ctfo_trial_id))
                  left join dqs_site_details_temp_11 CC  on Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(CC.ctfo_trial_id)) and 
                   Lower(Trim(A.ctfo_site_id)) = Lower(Trim(CC.ctfo_site_id))


GROUP  BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19""")
dqs_site_details_temp_2.createOrReplaceTempView("dqs_site_details_temp_2")
dqs_site_details_temp_2 = dqs_site_details_temp_2.withColumn("pct_screen_fail",
                                                             dqs_site_details_temp_2.pct_screen_fail.cast('double'))

####Df for ongong and completed filter
dqs_site_details_temp_2_on_com = spark.sql("""SELECT A.ctfo_trial_id,
        A.ctfo_site_id,
      A.member_study_id,
        A.facility_golden_id,
        BB.site_startup_time as site_startup_time,
        case when trim(lower(pct_screen_fail)) like '%\%%' then regexp_replace(pct_screen_fail,"\%","") else pct_screen_fail end as pct_screen_fail,
        tsm.status as site_status,
        enrolled                AS patients_enrolled, 
        consented              AS patients_screened, 
        completed as patients_completed,
        first_subject_consented_dt           AS first_subject_enrolled_dt,
        first_subject_enrolled_dt            AS first_subject_randomized_dt,
        last_subject_consented_dt            AS last_subject_enrolled_dt,
        C.last_subject_randomized_dt,
        CC.last_subject_randomized_dt_final,
        E.cntry_last_subject_randomized_dt,
        last_subject_last_visit_dt,           
        site_open_dt                         AS site_ready_to_enroll_dt,
        A.xref_country                     AS xref_country
FROM   dqs_golden_source_map_site A
       left join dqs_country_standard D on lower(trim(A.dqs_country))=lower(trim(D.facility_country))
        LEFT JOIN (select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir where lower(trim(site_status)) in {on_com}) B
               ON Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id))
                  AND Lower(Trim(A.facility_golden_id)) =
                      Lower(Trim(B.facility_golden_id))
                  AND Lower(Trim(A.xref_country)) =
                      Lower(Trim(D.standard_country))
        LEFT JOIN (select member_study_id, facility_golden_id, percentile(site_startup_time,0.5,10000) as site_startup_time
        from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir where lower(trim(site_status)) in {on_com} group by 1,2) BB
               ON Lower(Trim(A.member_study_id)) = Lower(Trim(BB.member_study_id))
                  AND Lower(Trim(A.facility_golden_id)) =
                      Lower(Trim(BB.facility_golden_id))
                  AND Lower(Trim(A.xref_country)) =
                      Lower(Trim(D.standard_country))            
                  left join (select distinct raw_status,status from trial_status_mapping) tsm  on lower(trim(B.site_status))=lower(trim(tsm.raw_status))
                  left join dqs_site_details_temp_1 C  on Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(C.ctfo_trial_id))
                  left join dqs_site_details_temp_11 CC  on Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(CC.ctfo_trial_id)) and 
                   Lower(Trim(A.ctfo_site_id)) = Lower(Trim(CC.ctfo_site_id))
                   left join dqs_randomization_rate_temp E on Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(E.ctfo_trial_id)) and 
                   Lower(Trim(A.xref_country)) = Lower(Trim(E.standard_country))


GROUP  BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19""".format(on_com=Ongoing_Completed))
dqs_site_details_temp_2_on_com.createOrReplaceTempView("dqs_site_details_temp_2_on_com")
dqs_site_details_temp_2_on_com = dqs_site_details_temp_2_on_com.withColumn("pct_screen_fail",
                                                                           dqs_site_details_temp_2_on_com.pct_screen_fail.cast(
                                                                               'double'))

dqs_trial_status = spark.sql("""
select distinct ctfo_trial_id, ctfo_site_id,
case when site_status is null then 'Others' else site_status end as site_status
from
(select ctfo_trial_id,ctfo_site_id,site_status,row_number() over(partition by ctfo_trial_id,ctfo_site_id order by prec) as rnk
from
(select ctfo_trial_id,ctfo_site_id,site_status, case when lower(trim(site_status))='ongoing' then 1 when lower(trim(site_status))='completed' then 2 when lower(trim(site_status))='planned' then 3
when lower(trim(site_status))='others' then 4 else 5 end as prec
from dqs_site_details_temp_2) a) b where b.rnk=1 """)

dqs_trial_status = dqs_trial_status.dropDuplicates()
# dqs_trial_status.registerTempTable('dqs_trial_status')
dqs_trial_status.write.mode("overwrite").saveAsTable("dqs_trial_status")

dqs_site_details_temp_enrolled_creened_completed = spark.sql("""
select 
a.ctfo_trial_id,
a.ctfo_site_id,
a.member_study_id,
a.facility_golden_id,
b.site_startup_time as site_startup_time,
c.pct_screen_fail as pct_screen_fail,
a.patients_enrolled as patients_enrolled, 
a.patients_screened as patients_screened, 
a.patients_completed as patients_completed
from (select ctfo_trial_id, ctfo_site_id, member_study_id, facility_golden_id,
max(patients_enrolled) as patients_enrolled, 
max(patients_screened) as patients_screened, 
max(patients_completed) as patients_completed  
from dqs_site_details_temp_2_on_com group by 1,2,3,4) a
LEFT JOIN (select ctfo_trial_id, ctfo_site_id, member_study_id, facility_golden_id,percentile(site_startup_time,0.5,10000) as site_startup_time  
from dqs_site_details_temp_2_on_com group by 1,2,3,4) b
on lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id)) and lower(trim(a.ctfo_site_id)) = lower(trim(b.ctfo_site_id))
and lower(trim(a.member_study_id)) = lower(trim(b.member_study_id)) and lower(trim(a.facility_golden_id)) = lower(trim(b.facility_golden_id))
LEFT JOIN (select ctfo_trial_id, ctfo_site_id, member_study_id, facility_golden_id,percentile(pct_screen_fail,0.5,10000) as pct_screen_fail  
from dqs_site_details_temp_2_on_com group by 1,2,3,4) c
on lower(trim(a.ctfo_trial_id)) = lower(trim(c.ctfo_trial_id)) and lower(trim(a.ctfo_site_id)) = lower(trim(c.ctfo_site_id))
and lower(trim(a.member_study_id)) = lower(trim(c.member_study_id)) and lower(trim(a.facility_golden_id)) = lower(trim(c.facility_golden_id))
""")
#dqs_site_details_temp_enrolled_creened_completed.registerTempTable('dqs_site_details_temp_enrolled_creened_completed')
dqs_site_details_temp_enrolled_creened_completed.write.mode("overwrite").saveAsTable(
    "dqs_site_details_temp_enrolled_creened_completed")

dates_kpi = spark.sql("""
select 
ctfo_trial_id,
ctfo_site_id,
min(first_subject_randomized_dt) as first_subject_randomized_dt_kpi,
max(last_subject_randomized_dt_final) as last_subject_randomized_dt_kpi,
max(last_subject_last_visit_dt) as last_subject_last_visit_dt_kpi,
min(site_ready_to_enroll_dt) as site_ready_to_enroll_dt_kpi
from dqs_site_details_temp_2
group by 1,2
""")
dates_kpi.dropDuplicates()
dates_kpi.registerTempTable('dates_kpi')

dqs_site_details_temp_2_on_com_dates = spark.sql("""select 
ctfo_trial_id,
ctfo_site_id,
min(first_subject_enrolled_dt) as first_subject_enrolled_dt ,
min(first_subject_randomized_dt) as first_subject_randomized_dt,
max(last_subject_enrolled_dt) as last_subject_enrolled_dt,
max(last_subject_randomized_dt) as last_subject_randomized_dt,
max(last_subject_last_visit_dt) as last_subject_last_visit_dt,
min(site_ready_to_enroll_dt) as site_ready_to_enroll_dt,
max(cntry_last_subject_randomized_dt) as cntry_last_subject_randomized_dt
from dqs_site_details_temp_2_on_com
group by 1,2
""")
dqs_site_details_temp_2_on_com_dates.dropDuplicates()
dqs_site_details_temp_2_on_com_dates.createOrReplaceTempView("dqs_site_details_temp_2_on_com_dates")

dqs_site_details_temp = spark.sql("""
select 
a.ctfo_trial_id,
a.ctfo_site_id,
b.site_status,
e.site_startup_time as site_startup_time,
f.pct_screen_fail as pct_screen_fail,
c.patients_enrolled as patients_enrolled,
c.patients_screened as patients_screened, 
c.patients_completed as patients_completed,
c.patients_dropped as patients_dropped,
d.first_subject_enrolled_dt as first_subject_enrolled_dt ,
d.first_subject_randomized_dt as first_subject_randomized_dt,
d.last_subject_enrolled_dt as last_subject_enrolled_dt,
d.last_subject_randomized_dt as last_subject_randomized_dt,
d.last_subject_last_visit_dt as last_subject_last_visit_dt,
d.site_ready_to_enroll_dt as site_ready_to_enroll_dt,
d.cntry_last_subject_randomized_dt as cntry_last_subject_randomized_dt,
xref_country,
xref_country_code
from 
(select distinct ctfo_trial_id, ctfo_site_id,First(xref_country) as xref_country,
First(xref_country_code) as xref_country_code from dqs_site_details_temp_2 group by 1,2) a
left join dqs_site_details_temp_2_on_com_dates d
on lower(trim(a.ctfo_trial_id))=lower(trim(d.ctfo_trial_id)) and lower(trim(a.ctfo_site_id))=lower(trim(d.ctfo_site_id))
left join (select ctfo_trial_id, ctfo_site_id , 
sum(patients_enrolled) as patients_enrolled,
sum(patients_screened) as patients_screened, 
sum(patients_completed) as patients_completed,
sum(patients_enrolled) - sum(patients_completed) as patients_dropped 
from dqs_site_details_temp_enrolled_creened_completed group by 1,2) c
on lower(trim(a.ctfo_trial_id))=lower(trim(c.ctfo_trial_id)) and lower(trim(a.ctfo_site_id))=lower(trim(c.ctfo_site_id))
left join (select ctfo_trial_id, ctfo_site_id , percentile(site_startup_time,0.5,10000) as site_startup_time 
from dqs_site_details_temp_enrolled_creened_completed group by 1,2) e
on lower(trim(a.ctfo_trial_id))=lower(trim(e.ctfo_trial_id)) and lower(trim(a.ctfo_site_id))=lower(trim(e.ctfo_site_id))
left join (select ctfo_trial_id, ctfo_site_id , percentile(pct_screen_fail,0.5,10000) as pct_screen_fail
from dqs_site_details_temp_enrolled_creened_completed group by 1,2) f
on lower(trim(a.ctfo_trial_id))=lower(trim(f.ctfo_trial_id)) and lower(trim(a.ctfo_site_id))=lower(trim(f.ctfo_site_id))
left join
dqs_trial_status b
on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id)) and lower(trim(a.ctfo_site_id))=lower(trim(b.ctfo_site_id))
""")
#dqs_site_details_temp.createOrReplaceTempView("dqs_site_details_temp")
dqs_site_details_temp.write.mode("overwrite").saveAsTable("dqs_site_details_temp")

dqs_site_details = spark.sql("""
select 
a.ctfo_trial_id,
a.ctfo_site_id,
site_status,
site_startup_time,
case when (lower(trim(site_status)) in {on_status} and last_subject_enrolled_dt is not null) then  pct_screen_fail
when  (lower(trim(site_status)) in {com_status} ) then pct_screen_fail else null  end as pct_screen_fail,
patients_enrolled, 
patients_screened,  
patients_dropped,
patients_completed,
first_subject_enrolled_dt ,
first_subject_randomized_dt,
b.first_subject_randomized_dt_kpi,
last_subject_enrolled_dt,
last_subject_randomized_dt,
b.last_subject_randomized_dt_kpi,
last_subject_last_visit_dt,
b.last_subject_last_visit_dt_kpi,
cntry_last_subject_randomized_dt,
site_ready_to_enroll_dt,
b.site_ready_to_enroll_dt_kpi,
xref_country,
xref_country_code
from 
dqs_site_details_temp a
left join 
dates_kpi b
on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id)) and lower(trim(a.ctfo_site_id))=lower(trim(b.ctfo_site_id))

""".format(on_status=Ongoing_variable, com_status=Completed_variable))

# dqs_site_details.createOrReplaceTempView("dqs_site_details")
dqs_site_details.write.mode("overwrite").saveAsTable("dqs_site_details")

dqs_site_final = spark.sql("""SELECT 
ctfo_site_id,
ctfo_trial_id,                                      
'' as patients_consented_planned,      
patients_screened as patients_consented_actual,       
'' as patients_screened_planned,       
patients_screened as patients_screened_actual,        
'' as dropped_at_screening_actual ,    
'' as patients_randomized_planned,     
patients_enrolled as patients_randomized_actual,      
patients_completed as patients_completed_actual,       
patients_dropped as patients_dropped_actual ,
'' as site_contract_signed_bsln_dt,  
'' as site_contract_signed_planned_dt ,
'' as site_contract_signed_actual_dt,
'' as site_qualification_visit_bsln_dt ,   
'' as site_qualification_visit_planned_dt ,
'' as site_qualification_visit_actual_dt  ,
'' as irb_iec_submission_bsln_dt,      
'' as irb_iec_submission_planned_dt,   
'' as irb_iec_submission_actual_dt ,   
'' as irb_iec_approval_bsln_dt  ,      
'' as irb_iec_approval_planned_dt,     
'' as irb_iec_approval_actual_dt ,     
'' as site_initiation_visit_bsln_dt   ,
'' as site_initiation_visit_planned_dt,
'' as site_initiation_visit_actual_dt, 
'' as site_ready_to_enrol_bsln_dt ,    
'' as site_ready_to_enrol_planned_dt,  
site_ready_to_enroll_dt as site_ready_to_enrol_actual_dt ,  
'' as first_subject_first_visit_bsln_dt ,  
'' as first_subject_first_visit_planned_dt,
first_subject_enrolled_dt as first_subject_first_visit_actual_dt ,
'' as first_subject_first_treatment_bsln_dt,
'' as first_subject_first_treatment_planned_dt,
first_subject_randomized_dt as first_subject_first_treatment_actual_dt,
'' as last_subject_first_visit_bsln_dt,    
'' as last_subject_first_visit_planned_dt, 
last_subject_enrolled_dt as last_subject_first_visit_actual_dt , 
'' as last_subject_first_treatment_bsln_dt,
'' as last_subject_first_treatment_planned_dt,
last_subject_randomized_dt as last_subject_first_treatment_actual_dt,
'' as last_subject_last_treatment_bsln_dt ,
'' as last_subject_last_treatment_planned_dt,
'' as last_subject_last_treatment_actual_dt,
'' as last_subject_last_visit_bsln_dt,
'' as last_subject_last_visit_planned_dt , 
last_subject_last_visit_dt as last_subject_last_visit_actual_dt ,  
'' as site_closure_visit_bsln_dt  ,    
'' as site_closure_visit_planned_dt ,  
'' as site_closure_visit_actual_dt ,   
'' as fsiv_fsfr_delta,
'ir' as source,
xref_country_code,
'' as cntry_first_subject_first_visit_actual_dt,
cntry_last_subject_randomized_dt as cntry_last_subject_first_treatment_actual_dt,
'' as study_last_subject_first_treatment_planned_dt,
'' as study_first_subject_first_treatment_planned_dt ,
'' as study_first_site_initiation_visit_planned_dt ,
'' as enrollment_rate,
pct_screen_fail as screen_failure_rate,
'' as lost_opp_time,
'' as enrollment_duration, 
site_startup_time,
site_status as site_trial_status,
first_subject_randomized_dt_kpi,
last_subject_randomized_dt_kpi,
last_subject_last_visit_dt_kpi,
site_ready_to_enroll_dt_kpi
from dqs_site_details""")
dqs_site_final.createOrReplaceTempView("dqs_site_final")

centre_loc_map = spark.sql("""
select src_trial_id, src_site_id,site_country
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator
group by 1,2,3
""")
centre_loc_map = centre_loc_map.dropDuplicates()
centre_loc_map.createOrReplaceTempView("centre_loc_map")

ctms_golden_source_map_site = spark.sql("""select cgsm.* from (select ctfo_trial_id, ctfo_site_id, data_src_nm, 
country_mapping.iso2_code as xref_country_code,
B.site_country as country, coalesce(country_mapping.standard_country,B.site_country) as standard_country, 
A.src_trial_id, A.src_site_id
from
    (select ctfo_trial_id, ctfo_site_id, data_src_nm, src_trial_id, src_site_id, src_site_country
    from golden_id_details_site
    where lower(trim(data_src_nm)) = 'ctms' group by 1,2,3,4,5,6) A
inner join
    (select src_site_id, src_trial_id,site_country from centre_loc_map group by 1,2,3) B
on lower(trim(A.src_trial_id)) = lower(trim(B.src_trial_id)) and lower(trim(A.src_site_id)) = lower(trim(B.src_site_id))
left join country_mapping on lower(trim(B.site_country)) = lower(trim(country_mapping.country))
group by 1,2,3,4,5,6,7,8) cgsm 
""")
ctms_golden_source_map_site = ctms_golden_source_map_site.dropDuplicates()
# ctms_golden_source_map_site.createOrReplaceTempView("ctms_golden_source_map_site")
ctms_golden_source_map_site.write.mode('overwrite').saveAsTable('ctms_golden_source_map_site')

ctms_trial_status_temp = spark.sql("""
select distinct ctfo_site_id, ctfo_trial_id, c.status as trial_status from
(select ctfo_trial_id,ctfo_site_id,src_site_id,src_trial_id from ctms_golden_source_map_site group by 1,2,3,4) cgsm
left join
(select site_trial_status, src_site_id, src_trial_id
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator  group by 1,2,3) b
on lower(trim(b.src_site_id))=lower(trim(cgsm.src_site_id)) and lower(trim(b.src_trial_id))=lower(trim(cgsm.src_trial_id))
left join ctms_status_mapping c on  lower(trim(b.site_trial_status))=lower(trim(c.raw_status)) 
where lower(trim(b.site_trial_status)) in {status}
""".format(status=Ongoing_Completed))
ctms_trial_status_temp = ctms_trial_status_temp.dropDuplicates()
# ctms_trial_status_temp.registerTempTable('ctms_trial_status_temp')
ctms_trial_status_temp.write.mode("overwrite").saveAsTable("ctms_trial_status_temp")

ctms_trial_site_status = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_site_id, tsm.status as trial_status from 
(select ctfo_trial_id, ctfo_site_id, src_trial_id, src_site_id from ctms_golden_source_map_site group by 1,2,3,4) cgsm
left join $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator css 
on lower(trim(cgsm.src_trial_id))=lower(trim(css.src_trial_id)) and lower(trim(cgsm.src_site_id))=lower(trim(css.src_site_id))
left join (select distinct raw_status, status from trial_status_mapping) tsm  on trim(lower(css.site_trial_status))=trim(lower(tsm.raw_status))
group by 1,2,3
""")
ctms_trial_site_status = ctms_trial_site_status.dropDuplicates()
# ctms_trial_site_status.createOrReplaceTempView("ctms_trial_site_status")
ctms_trial_site_status.write.mode("overwrite").saveAsTable("ctms_trial_site_status")

ctms_trial_status = spark.sql("""
select ctfo_trial_id, ctfo_site_id,
case when trial_status is null then 'Others' else trial_status end as trial_status
from
(select ctfo_trial_id,ctfo_site_id,trial_status,row_number() over(partition by ctfo_trial_id,ctfo_site_id order by prec) as rnk
from
(select ctfo_trial_id,ctfo_site_id,trial_status, case when lower(trim(trial_status))='ongoing' then 1 when lower(trim(trial_status))='completed' then 2 when lower(trim(trial_status))='planned' then 3
when lower(trim(trial_status))='others' then 4 else 5 end as prec
from ctms_trial_site_status) a) b where b.rnk=1 """)

ctms_trial_status = ctms_trial_status.dropDuplicates()
# ctms_trial_status.registerTempTable('ctms_trial_status')
ctms_trial_status.write.mode("overwrite").saveAsTable("ctms_trial_status")

metrics_actual = spark.sql(""" select 
a_sum_metrics_actual.ctfo_trial_id as ctfo_trial_id,
a_sum_metrics_actual.ctfo_site_id as ctfo_site_id,
sum(a_sum_metrics_actual.patients_consented_actual) as patients_consented_actual,       
sum(a_sum_metrics_actual.patients_screened_actual) as patients_screened_actual,        
sum(a_sum_metrics_actual.dropped_at_screening_actual) as dropped_at_screening_actual ,    
sum(a_sum_metrics_actual.patients_randomized_actual) as patients_randomized_actual,      
sum(a_sum_metrics_actual.patients_completed_actual) as patients_completed_actual,       
sum(a_sum_metrics_actual.patients_dropped_actual) as patients_dropped_actual 
from(
(select ctfo_trial_id,ctfo_site_id,src_site_id,src_trial_id,country,xref_country_code from ctms_golden_source_map_site) cgsm
left join 
(select src_trial_id,src_site_id,max(patients_consented_actual) as 
patients_consented_actual,max(patients_screened_actual) as patients_screened_actual,
max(dropped_at_screening_actual) as dropped_at_screening_actual,
max(patients_randomized_actual) as patients_randomized_actual,max(patients_completed_actual) as patients_completed_actual,
max(patients_dropped_actual) as patients_dropped_actual
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator
 where lower(trim(site_trial_status)) in {on_com} group by 1,2) as metrics on cgsm.src_trial_id=metrics.src_trial_id and cgsm.src_site_id = metrics.src_site_id
 ) as a_sum_metrics_actual group by 1,2
""".format(on_com=Ongoing_Completed))
metrics_actual = metrics_actual.dropDuplicates()
metrics_actual.registerTempTable("a_sum_metrics_actual")

metrics_planned = spark.sql(""" select 
a_sum_metrics_planned.ctfo_trial_id as ctfo_trial_id,
a_sum_metrics_planned.ctfo_site_id as ctfo_site_id,
sum(a_sum_metrics_planned.patients_consented_planned) as patients_consented_planned,       
sum(a_sum_metrics_planned.patients_screened_planned) as patients_screened_planned,            
sum(a_sum_metrics_planned.patients_randomized_planned) as patients_randomized_planned         
from(
(select ctfo_trial_id,ctfo_site_id,src_site_id,src_trial_id,country,xref_country_code from ctms_golden_source_map_site) cgsm
left join 
(select src_trial_id,src_site_id,max(patients_consented_planned) as patients_consented_planned,max(patients_screened_planned) as patients_screened_planned
,max(patients_randomized_planned) as patients_randomized_planned
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator where
  lower(trim(site_trial_status)) in {on_com_plan} group by 1,2) a_sum_metrics_planned1 on lower(trim(a_sum_metrics_planned1.src_trial_id)) = lower(trim(cgsm.src_trial_id)) and lower(trim(a_sum_metrics_planned1.src_site_id)) = lower(trim(cgsm.src_site_id))
 ) as a_sum_metrics_planned group by 1,2
""".format(on_com_plan=Ongoing_Completed_Planned))
metrics_planned = metrics_planned.dropDuplicates()
metrics_planned.registerTempTable("a_sum_metrics_planned")

ctms_site_final = spark.sql("""SELECT 
cgsm.ctfo_site_id,
cgsm.ctfo_trial_id,                                           
a_sum_metrics_planned.patients_consented_planned as patients_consented_planned,      
a_sum_metrics_actual.patients_consented_actual as patients_consented_actual,       
a_sum_metrics_planned.patients_screened_planned as patients_screened_planned,       
a_sum_metrics_actual.patients_screened_actual as patients_screened_actual,        
a_sum_metrics_actual.dropped_at_screening_actual as dropped_at_screening_actual ,    
a_sum_metrics_planned.patients_randomized_planned as patients_randomized_planned,     
a_sum_metrics_actual.patients_randomized_actual as patients_randomized_actual,      
a_sum_metrics_actual.patients_completed_actual as patients_completed_actual,       
a_sum_metrics_actual.patients_dropped_actual as patients_dropped_actual ,  
min(a_baseline.site_contract_signed_bsln_dt) as site_contract_signed_bsln_dt ,  
min(a_on_com_plan.site_contract_signed_planned_dt) as site_contract_signed_planned_dt  ,
min(a_on_com.site_contract_signed_actual_dt) as site_contract_signed_actual_dt,
min(a_baseline.site_qualification_visit_bsln_dt) as site_qualification_visit_bsln_dt ,   
min(a_on_com_plan.site_qualification_visit_planned_dt) as site_qualification_visit_planned_dt ,
min(a_on_com.site_qualification_visit_actual_dt)  as site_qualification_visit_actual_dt,    
min(a_baseline.irb_iec_submission_bsln_dt) as irb_iec_submission_bsln_dt,      
min(a_on_com_plan.irb_iec_submission_planned_dt) as  irb_iec_submission_planned_dt,   
min(a_on_com.irb_iec_submission_actual_dt) as irb_iec_submission_actual_dt  ,   
max(a_baseline.irb_iec_approval_bsln_dt) as irb_iec_approval_bsln_dt  ,      
max(a_on_com_plan.irb_iec_approval_planned_dt) as irb_iec_approval_planned_dt,     
max(a_on_com.irb_iec_approval_actual_dt) as irb_iec_approval_actual_dt ,     
min(a_baseline.site_initiation_visit_bsln_dt) as site_initiation_visit_bsln_dt   ,
min(a_on_com_plan.site_initiation_visit_planned_dt) as site_initiation_visit_planned_dt,
min(a_on_com.site_initiation_visit_actual_dt) as site_initiation_visit_actual_dt, 
min(a_baseline.site_ready_to_enrol_bsln_dt) as site_ready_to_enrol_bsln_dt ,    
min(a_on_com_plan.site_ready_to_enrol_planned_dt) as site_ready_to_enrol_planned_dt,
min(a_on_com.site_initiation_visit_actual_dt) as site_ready_to_enrol_actual_dt ,  
min(a_baseline.first_subject_first_visit_bsln_dt) as first_subject_first_visit_bsln_dt  ,  
min(a_on_com_plan.first_subject_first_visit_planned_dt) as first_subject_first_visit_planned_dt ,
min(a_on_com.first_subject_first_visit_actual_dt) as first_subject_first_visit_actual_dt,
min(a_baseline.first_subject_first_treatment_bsln_dt) as first_subject_first_treatment_bsln_dt,
min(a_on_com_plan.first_subject_first_treatment_planned_dt) as first_subject_first_treatment_planned_dt,
min(a_on_com.first_subject_first_treatment_actual_dt) as first_subject_first_treatment_actual_dt,
max(a_baseline.last_subject_first_visit_bsln_dt) as last_subject_first_visit_bsln_dt,    
max(a_on_com_plan.last_subject_first_visit_planned_dt) as  last_subject_first_visit_planned_dt, 
max(a_on_com.last_subject_first_visit_actual_dt) as  last_subject_first_visit_actual_dt , 
max(a_baseline.last_subject_first_treatment_bsln_dt) as last_subject_first_treatment_bsln_dt,
max(a_on_com_plan.last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt ,
max(study_actual.last_subject_first_treatment_actual_dt) as last_subject_first_treatment_actual_dt ,
max(a_baseline.last_subject_last_treatment_bsln_dt) as last_subject_last_treatment_bsln_dt,
max(a_on_com_plan.last_subject_last_treatment_planned_dt) as last_subject_last_treatment_planned_dt,
max(a_on_com.last_subject_last_treatment_actual_dt) as last_subject_last_treatment_actual_dt,
max(a_baseline.last_subject_last_visit_bsln_dt) as last_subject_last_visit_bsln_dt,
max(a_on_com_plan.last_subject_last_visit_planned_dt)  as last_subject_last_visit_planned_dt, 
max(a_on_com.last_subject_last_visit_actual_dt) as last_subject_last_visit_actual_dt ,  
max(a_baseline.site_closure_visit_bsln_dt) as  site_closure_visit_bsln_dt ,    
max(a_on_com_plan.site_closure_visit_planned_dt) as site_closure_visit_planned_dt  ,  
max(a_on_com.site_closure_visit_actual_dt) as site_closure_visit_actual_dt  ,    
a_baseline.fsiv_fsfr_delta,
a_baseline.source,
xref_country_code,
min(b.first_subject_first_visit_actual_dt) as cntry_first_subject_first_visit_actual_dt,
max(cntry_lsfr.last_subject_first_treatment_actual_dt) as cntry_last_subject_first_treatment_actual_dt,
max(study_planned.last_subject_first_treatment_planned_dt) as study_last_subject_first_treatment_planned_dt,
min(study_planned.first_subject_first_treatment_planned_dt) as study_first_subject_first_treatment_planned_dt ,
min(study_planned.first_site_initiation_visit_planned_dt) as study_first_site_initiation_visit_planned_dt ,
a_baseline.enrollment_rate,
a_baseline.screen_failure_rate,
a_baseline.lost_opp_time,
a_baseline.enrollment_duration,
'' as site_startup_time,
status.trial_status as site_trial_status,
min(a_baseline.first_subject_first_treatment_actual_dt) as first_subject_randomized_dt_kpi,
max(a_baseline.last_subject_first_treatment_actual_dt) as last_subject_randomized_dt_kpi,
max(a_baseline.last_subject_last_visit_actual_dt) as last_subject_last_visit_dt_kpi,
min(a_baseline.site_initiation_visit_actual_dt) as site_ready_to_enroll_dt_kpi
from ( select ctfo_trial_id,ctfo_site_id,src_site_id,src_trial_id,country,xref_country_code from ctms_golden_source_map_site ) cgsm
left join
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator a_baseline on lower(trim(a_baseline.src_trial_id)) = lower(trim(cgsm.src_trial_id)) and lower(trim(a_baseline.src_site_id))=lower(trim(cgsm.src_site_id))
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator where 
lower(trim(site_trial_status)) in {on_com}) a_on_com on lower(trim(a_on_com.src_trial_id)) = lower(trim(cgsm.src_trial_id)) and lower(trim(a_on_com.src_site_id))=lower(trim(cgsm.src_site_id))
left join
(select * from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator where
 lower(trim(site_trial_status)) in {on_com_plan}) a_on_com_plan on lower(trim(a_on_com_plan.src_trial_id)) = lower(trim(cgsm.src_trial_id)) and lower(trim(a_on_com_plan.src_site_id))=lower(trim(cgsm.src_site_id))
 left join 
 a_sum_metrics_actual on lower(trim(a_sum_metrics_actual.ctfo_trial_id)) = lower(trim(cgsm.ctfo_trial_id)) and lower(trim(a_sum_metrics_actual.ctfo_site_id))=lower(trim(cgsm.ctfo_site_id))
left join
 a_sum_metrics_planned on lower(trim(a_sum_metrics_planned.ctfo_trial_id)) = lower(trim(cgsm.ctfo_trial_id)) and lower(trim(a_sum_metrics_planned.ctfo_site_id))=lower(trim(cgsm.ctfo_site_id))
  left join 
(select src_trial_id,last_subject_first_treatment_actual_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study
where lower(trim(trial_status)) in {on_com}) study_actual
on lower(trim(cgsm.src_trial_id)) = lower(trim(study_actual.src_trial_id)) 
left join 
(select src_trial_id,last_subject_first_treatment_planned_dt, 
first_subject_first_treatment_planned_dt, first_site_initiation_visit_planned_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study
where lower(trim(trial_status)) in {on_com_plan}) study_planned
on lower(trim(cgsm.src_trial_id)) = lower(trim(study_planned.src_trial_id))
left join
(select src_trial_id, country, min(first_subject_first_visit_actual_dt) as first_subject_first_visit_actual_dt,
max(last_subject_first_treatment_actual_dt) as last_subject_first_treatment_actual_dt
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country
where lower(trim(country_trial_status)) in {on_com} group by 1,2) b
on lower(trim(cgsm.src_trial_id)) = lower(trim(b.src_trial_id)) and lower(trim(cgsm.country)) = lower(trim(b.country)) 
left join 
(select src_trial_id,max(last_subject_first_treatment_actual_dt) as last_subject_first_treatment_actual_dt
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country
where lower(trim(country_trial_status)) in {on_com} group by 1) cntry_lsfr
on lower(trim(cgsm.src_trial_id)) = lower(trim(cntry_lsfr.src_trial_id))
left join ctms_trial_status status on lower(trim(status.ctfo_trial_id))=lower(trim(cgsm.ctfo_trial_id)) and lower(trim(status.ctfo_site_id))=lower(trim(cgsm.ctfo_site_id))
group by 1,2,3,4,5,6,7,8,9,10,11,51,52,53,59,60,61,62,63,64""".format(on_com=Ongoing_Completed, on_com_plan=Ongoing_Completed_Planned))
ctms_site_final = ctms_site_final.dropDuplicates()
#ctms_site_final.registerTempTable('ctms_site_final')
ctms_site_final.write.mode('overwrite').saveAsTable('ctms_site_final')

trial_site_mce = spark.sql("""select * from ctms_site_final
union
 select * from dqs_site_final""")
trial_site_mce.registerTempTable('trial_site_mce')
trial_site_mce.write.mode('overwrite').saveAsTable('trial_site_mce')

#path = path.replace('table_name', 'site')
#trial_site_mce.repartition(10).write.mode('overwrite').parquet(path)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_site partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   trial_site_mce
""")

if trial_site_mce.count() == 0:
    print("Skipping copy_hdfs_to_s3 for mce_trial_site as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_site")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")