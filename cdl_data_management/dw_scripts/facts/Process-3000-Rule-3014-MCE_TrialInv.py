################################# Module Information #####################################################
#  Module Name         : Trial Investigator Metric Engine Calculation Data
#  Purpose             : To create the Trial Investigator KPI
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : Trial Site KPIs
#  Last changed on     : 06-01-2022
#  Last changed by     : Nirmal/Vicky
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
from CommonUtils import CommonUtils
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lower, col
from pyspark.sql.functions import initcap
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')


# spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version","2")

path = bucket_path + "/applications/commons/mce/mce_src/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")


trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

trial_status_mapping = spark.sql(""" select distinct * from (select raw_status, status from trial_status_mapping_temp 
union select status, status from trial_status_mapping_temp )  """)
trial_status_mapping.registerTempTable('trial_status_mapping')

#Values in Variables

Ongoing = trial_status_mapping.filter(trial_status_mapping.status == "Ongoing").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Completed = trial_status_mapping.filter(trial_status_mapping.status == "Completed").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Planned = trial_status_mapping.filter(trial_status_mapping.status == "Planned").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Others = trial_status_mapping.filter(trial_status_mapping.status == "Others").select(lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()


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

################################# For CTMS #############################

r_trial_inv_roles = spark.sql("""
select
    d.ctfo_trial_id, e.ctfo_site_id, e.ctfo_investigator_id
from
    $$client_name_ctfo_datastore_app_commons_$$db_env.r_site_investigator e
inner join
    $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_investigator d
on lower(trim(e.ctfo_investigator_id)) = lower(trim(d.ctfo_investigator_id))
inner join
    $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site f
on lower(trim(e.ctfo_site_id)) = lower(trim(f.ctfo_site_id)) and lower(trim(d.ctfo_trial_id)) = lower(trim(f.ctfo_trial_id))
group by 1,2,3
""")
r_trial_inv_roles.createOrReplaceTempView("r_trial_inv_roles")
r_trial_inv_roles.write.mode("overwrite").saveAsTable("r_trial_inv_roles")
#r_trial_inv_roles.createOrReplaceTempView("r_trial_inv_roles")


golden_id_det_inv_2 = spark.sql("""select B.ctfo_trial_id, B.ctfo_investigator_id,B.ctfo_site_id, C.data_src_nm, (src_investigator_id) as src_investigator_id
from
    r_trial_inv_roles B
inner join
    (select
        data_src_nm, ctfo_investigator_id,src_investigator_id
    from
        (select
            data_src_nm, ctfo_investigator_id, src_investigator_id
        from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_inv
        where lower(trim(data_src_nm)) in ('ctms','ir')
    ) group by 1,2,3) C
on lower(trim(B.ctfo_investigator_id)) = lower(trim(C.ctfo_investigator_id))
group by 1,2,3,4,5
""")
golden_id_det_inv_2.createOrReplaceTempView("golden_id_det_inv_2")

golden_id_det_inv_3 = spark.sql("""
select
    B.ctfo_trial_id,B.ctfo_investigator_id,B.ctfo_site_id, B.data_src_nm,
    B.src_investigator_id,E.src_trial_id
from
    golden_id_det_inv_2 B
inner join
    (select data_src_nm, ctfo_trial_id, src_trial_id
    from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial where lower(trim(data_src_nm)) in ('ctms','ir')
    group by 1,2,3) E
on lower(trim(B.ctfo_trial_id)) = lower(trim(E.ctfo_trial_id)) and
lower(trim(B.data_src_nm)) = lower(trim(E.data_src_nm))
group by 1,2,3,4,5,6
""")
golden_id_det_inv_3 = golden_id_det_inv_3.dropDuplicates()
#golden_id_det_inv_3.createOrReplaceTempView("golden_id_det_inv_3")
golden_id_det_inv_3.write.mode("overwrite").saveAsTable("golden_id_det_inv_3")

golden_id_det_inv= spark.sql("""
select
B.ctfo_trial_id,B.ctfo_investigator_id,B.ctfo_site_id,
B.data_src_nm,src_investigator_id ,B.src_trial_id,E.src_site_id
from golden_id_det_inv_3 B
inner join
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site E on lower(trim(E.ctfo_site_id))=lower(trim(B.ctfo_site_id))
and lower(trim(E.data_src_nm))=lower(trim(B.data_src_nm))
where lower(trim(E.data_src_nm)) in ('ctms','ir')
group by 1,2,3,4,5,6,7
""").registerTempTable('golden_id_det_inv')

centre_loc_map = spark.sql("""
select src_trial_id, 
src_investigator_id,
src_site_id
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator
where src_investigator_id is not null
group by 1,2,3
""")
centre_loc_map = centre_loc_map.dropDuplicates()
centre_loc_map.createOrReplaceTempView("centre_loc_map")

ctms_golden_source_map_inv = spark.sql("""
select aa.src_trial_id,aa.ctfo_trial_id,aa.src_investigator_id,aa.ctfo_investigator_id,aa.src_site_id,aa.ctfo_site_id
from (select b.src_trial_id,a.ctfo_trial_id,b.src_investigator_id,a.ctfo_investigator_id,b.src_site_id,a.ctfo_site_id,row_number() over(partition by b.src_trial_id, b.src_site_id,a.ctfo_investigator_id order by b.src_trial_id ) as rnk
from 
(select ctfo_trial_id,ctfo_investigator_id,ctfo_site_id,src_site_id,src_trial_id,src_investigator_id from golden_id_det_inv group by 1,2,3,4,5,6) a left join centre_loc_map b on 
lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id)) and lower(trim(a.src_investigator_id))=lower(trim(b.src_investigator_id))
and lower(trim(a.src_site_id))=lower(trim(b.src_site_id)) where b.src_investigator_id is not null) aa where aa.rnk=1
group by 1,2,3,4,5,6""")
ctms_golden_source_map_inv = ctms_golden_source_map_inv.dropDuplicates()
#ctms_golden_source_map_inv.createOrReplaceTempView("ctms_golden_source_map_inv")
ctms_golden_source_map_inv.write.mode("overwrite").saveAsTable("ctms_golden_source_map_inv")


ctms_trial_status_inv_temp = spark.sql(""" select  fin.ctfo_trial_id,fin.ctfo_investigator_id,fin.src_trial_id,fin.src_investigator_id,fin.ctfo_site_id,fin.src_site_id, fin.new_status,fin.site_trial_status,
case when lower(trim(fin.new_status))='ongoing' then 1
     when lower(trim(fin.new_status))='completed' then 2
	 when lower(trim(fin.new_status))='planned' then 3
	 when lower(trim(fin.new_status))='others' then 4 else 5 end as precedence from
(select a.ctfo_trial_id,a.ctfo_investigator_id,a.src_trial_id,a.src_investigator_id,a.src_site_id,a.ctfo_site_id, a.status as new_status,site_trial_status from (select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id, c.status,c.site_trial_status,cgsm.src_trial_id,cgsm.src_investigator_id,cgsm.ctfo_site_id,cgsm.src_site_id
from
(select ctfo_trial_id, src_trial_id, ctfo_investigator_id,src_investigator_id,ctfo_site_id,src_site_id from ctms_golden_source_map_inv group by 1,2,3,4,5,6) cgsm
left join
(select study_site.src_trial_id,t.status,site_trial_status,study_site.src_investigator_id,study_site.src_site_id from
(select src_investigator_id , src_trial_id ,src_site_id ,site_trial_status  from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator
where lower(trim(site_trial_status)) in {on_com_status}
  group by 1,2,3,4) study_site
  left join trial_status_mapping t on lower(trim(t.raw_status))=lower(trim(study_site.site_trial_status))
group by 1,2,3,4,5) c
on lower(trim(cgsm.src_trial_id)) = lower(trim(c.src_trial_id)) and lower(trim(cgsm.src_investigator_id))=lower(trim(c.src_investigator_id)) and lower(trim(cgsm.src_site_id))=lower(trim(c.src_site_id))
group by 1,2,3,4,5,6,7,8) as a group by 1,2,3,4,5,6,7,8)fin group by 1,2,3,4,5,6,7,8,9
 """.format(on_com_status=Ongoing_Completed))
ctms_trial_status_inv_temp = ctms_trial_status_inv_temp.dropDuplicates()
ctms_trial_status_inv_temp.registerTempTable('ctms_trial_status_inv_temp')

ctms_trial_status_inv = spark.sql(""" select ctfo_trial_id,ctfo_investigator_id,ctfo_site_id,new_status as trial_status from (
select  ctfo_trial_id,ctfo_investigator_id,src_trial_id,src_investigator_id,ctfo_site_id,src_site_id ,new_status,site_trial_status, precedence ,row_number() over(partition by ctfo_trial_id,ctfo_investigator_id,ctfo_site_id order by precedence asc) as rnk from ctms_trial_status_inv_temp) where rnk=1""".format(on_com_status=Ongoing_Completed))
ctms_trial_status_inv = ctms_trial_status_inv.dropDuplicates()
#ctms_trial_status_inv.registerTempTable('ctms_trial_status_inv')
ctms_trial_status_inv.write.mode("overwrite").saveAsTable("ctms_trial_status_inv")


################################# For DQS #############################
# r_trial_inv_roles = spark.sql("""
# select
#     d.ctfo_trial_id, e.ctfo_site_id, e.ctfo_investigator_id
# from
#     $$client_name_ctfo_datastore_app_commons_$$db_env.r_site_investigator e
# inner join
#     $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_investigator d
# on lower(trim(e.ctfo_investigator_id)) = lower(trim(d.ctfo_investigator_id))
# inner join
#     $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site f
# on lower(trim(e.ctfo_site_id)) = lower(trim(f.ctfo_site_id)) and lower(trim(d.ctfo_trial_id)) = lower(trim(f.ctfo_trial_id))
# group by 1,2,3
# """)
# r_trial_inv_roles.createOrReplaceTempView("r_trial_inv_roles")
# #r_trial_inv_roles.write.mode("overwrite").saveAsTable("r_trial_inv_roles")
# r_trial_inv_roles.createOrReplaceTempView("r_trial_inv_roles")


# golden_id_det_inv_2 = spark.sql("""select B.ctfo_trial_id, B.ctfo_investigator_id,B.ctfo_site_id, C.data_src_nm, (src_investigator_id) as src_investigator_id
# from
#     r_trial_inv_roles B
# inner join
#     (select
#         data_src_nm, ctfo_investigator_id,src_investigator_id
#     from
#         (select
#             data_src_nm, ctfo_investigator_id, src_investigator_id
#         from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_inv
#         where lower(trim(data_src_nm)) in ('ctms','ir')
#     ) group by 1,2,3) C
# on lower(trim(B.ctfo_investigator_id)) = lower(trim(C.ctfo_investigator_id))
# group by 1,2,3,4,5
# """)
# golden_id_det_inv_2.createOrReplaceTempView("golden_id_det_inv_2")
#
# golden_id_det_inv_3 = spark.sql("""
# select
#     B.ctfo_trial_id,B.ctfo_investigator_id,B.ctfo_site_id, B.data_src_nm,
#     B.src_investigator_id,E.src_trial_id
# from
#     golden_id_det_inv_2 B
# inner join
#     (select data_src_nm, ctfo_trial_id, src_trial_id
#     from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial where lower(trim(data_src_nm)) in ('ctms','ir')
#     group by 1,2,3) E
# on lower(trim(B.ctfo_trial_id)) = lower(trim(E.ctfo_trial_id)) and
# lower(trim(B.data_src_nm)) = lower(trim(E.data_src_nm))
# group by 1,2,3,4,5,6
# """)
# golden_id_det_inv_3 = golden_id_det_inv_3.dropDuplicates()
# golden_id_det_inv_3.createOrReplaceTempView("golden_id_det_inv_3")
# #golden_id_det_inv_3.write.mode("overwrite").saveAsTable("golden_id_det_inv_3")
#
# golden_id_det_inv= spark.sql("""
# select
# B.ctfo_trial_id,B.ctfo_investigator_id,B.ctfo_site_id,
# B.data_src_nm,src_investigator_id ,B.src_trial_id,E.src_site_id
# from golden_id_det_inv_3 B
# inner join
# $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site E on lower(trim(E.ctfo_site_id))=lower(trim(B.ctfo_site_id))
# and lower(trim(E.data_src_nm))=lower(trim(B.data_src_nm))
# where lower(trim(E.data_src_nm)) in ('ctms','ir')
# group by 1,2,3,4,5,6,7
# """).registerTempTable('golden_id_det_inv')
#


ctms_dates_kpi = spark.sql("""
select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id,cgsm.ctfo_site_id,
min(a.siv_dt) as site_initiation_visit_actual_dt,
max(a.lslv_dt) as last_subject_last_visit_actual_dt,
max(c.cntry_lsfr_dt) as lsfr_dt_usl_sc,
max(b.lsfr_dt) as lsfr_dt_usl_s
from 
(select ctfo_trial_id, src_trial_id, ctfo_investigator_id,src_investigator_id ,src_site_id,ctfo_site_id from ctms_golden_source_map_inv group by 1,2,3,4,5,6) cgsm
left join
(select src_trial_id,src_investigator_id,src_site_id,min(site_initiation_visit_actual_dt) as siv_dt,max(last_subject_last_visit_actual_dt) as lslv_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator group by 1,2,3) a
on lower(trim(cgsm.src_trial_id)) = lower(trim(a.src_trial_id)) and lower(trim(cgsm.src_investigator_id))=lower(trim(a.src_investigator_id)) and lower(trim(cgsm.src_site_id))=lower(trim(a.src_site_id)) 
left join
(select src_trial_id,cntry_lsfr_dt from
(select src_trial_id, max(last_subject_first_treatment_actual_dt) as cntry_lsfr_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country group by 1)) c 
on lower(trim(a.src_trial_id)) = lower(trim(c.src_trial_id))
left join 
(select src_trial_id,max(last_subject_first_treatment_actual_dt) as lsfr_dt  from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study group by 1) b 
on lower(trim(a.src_trial_id)) = lower(trim(b.src_trial_id)) 
group by 1,2,3""")

ctms_dates_kpi = ctms_dates_kpi.dropDuplicates()
#ctms_dates_kpi.registerTempTable('ctms_dates_kpi')
ctms_dates_kpi.write.mode("overwrite").saveAsTable("ctms_dates_kpi")



ctms_dates_filtered = spark.sql("""
select cgsm.ctfo_trial_id,
cgsm.ctfo_investigator_id,
cgsm.ctfo_site_id,
min(a.siv_dt) as siv_dt,
min(a.fsfr_dt) as first_subject_first_treatment_actual_dt,
max(c.cntry_lsfr_dt) as lsfr_dt_usl_sc,
max(b.lsfr_dt) as lsfr_dt_usl_s,
max(a.lsfv_dt) as lsfv_dt
from
(select ctfo_trial_id, src_trial_id, ctfo_investigator_id,src_investigator_id ,src_site_id,ctfo_site_id from ctms_golden_source_map_inv group by 1,2,3,4,5,6) cgsm
left join
(select src_trial_id,src_investigator_id,src_site_id,min(site_initiation_visit_actual_dt) as siv_dt,min(first_subject_first_treatment_actual_dt) as fsfr_dt, max(last_subject_first_visit_actual_dt) as lsfv_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator where lower(trim(site_trial_status)) in {on_com} group by 1,2,3) a
on lower(trim(cgsm.src_trial_id)) = lower(trim(a.src_trial_id)) and lower(trim(cgsm.src_investigator_id))=lower(trim(a.src_investigator_id)) and lower(trim(cgsm.src_site_id))=lower(trim(a.src_site_id))
left join
(select src_trial_id,cntry_lsfr_dt from
(select src_trial_id, max(last_subject_first_treatment_actual_dt) as cntry_lsfr_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country where lower(trim(country_trial_status)) in {on_com} group by 1)) c 
on lower(trim(a.src_trial_id)) = lower(trim(c.src_trial_id))
left join 
(select src_trial_id,max(last_subject_first_treatment_actual_dt) as lsfr_dt  from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study where lower(trim(trial_status)) in {on_com} group by 1) b 
on lower(trim(a.src_trial_id)) = lower(trim(b.src_trial_id)) group by 1,2,3
 """.format(on_com=Ongoing_Completed))

ctms_dates_filtered = ctms_dates_filtered.dropDuplicates()
ctms_dates_filtered.registerTempTable('ctms_dates_filtered')

 
 #patients_enrolled and patient screened,dropped_at_scrrening(for sfr)
ctms_PS_PE_PDS=spark.sql(""" select aa.ctfo_trial_id,aa.ctfo_investigator_id,aa.ctfo_site_id,aa.patients_screened_actual as patients_screened,aa.patients_randomized_actual as patients_enrolled,dropped_at_screening_actual from (
(select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id,cgsm.ctfo_site_id,sum(B.patients_screened_actual) as patients_screened_actual ,sum(B.patients_randomized_actual) as patients_randomized_actual,sum(B.dropped_at_screening_actual) as dropped_at_screening_actual  from 
(select ctfo_trial_id, src_trial_id, ctfo_investigator_id,src_investigator_id,src_site_id,ctfo_site_id from ctms_golden_source_map_inv group by 1,2,3,4,5,6) cgsm
left join
(select src_trial_id, src_investigator_id,src_site_id,sum(patients_screened_actual) as patients_screened_actual, sum(patients_randomized_actual) as patients_randomized_actual,sum(dropped_at_screening_actual) as dropped_at_screening_actual from 
(select src_trial_id, src_site_id,src_investigator_id,patients_screened_actual,patients_randomized_actual,dropped_at_screening_actual from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator where lower(trim(site_trial_status)) in  {on_com}
group by 1,2,3,4,5,6) a group by 1,2,3)B on lower(trim (cgsm.src_trial_id))=lower(trim(B.src_trial_id)) and lower(trim (cgsm.src_investigator_id))=lower(trim(B.src_investigator_id)) and lower(trim (cgsm.src_site_id))=lower(trim(B.src_site_id))
group by 1,2,3))aa
""".format(on_com=Ongoing_Completed))

ctms_PS_PE_PDS = ctms_PS_PE_PDS.dropDuplicates()
ctms_PS_PE_PDS.registerTempTable('ctms_PS_PE_PDS')

#ON_COM_PLANNED
ctms_planned_dt = spark.sql("""
select  
ctms_plan_dt.ctfo_trial_id,
ctms_plan_dt.ctfo_investigator_id,
ctms_plan_dt.ctfo_site_id,
max(ctms_plan_dt.lsfr_curr_dt) as last_subject_first_treatment_planned_dt,
min(ctms_plan_dt.fsiv_dt) as first_site_initiation_visit_planned_dt,
min(ctms_plan_dt.fsfr_dt) as first_subject_first_treatment_planned_dt
from 
(select cgsm.ctfo_trial_id,
cgsm.ctfo_investigator_id,
cgsm.ctfo_site_id,
max(a.lsfr_curr_dt) as lsfr_curr_dt,
min(a.fsiv_dt) as fsiv_dt,
min(a.fsfr_dt) as fsfr_dt
from
(select ctfo_trial_id,src_trial_id,ctfo_investigator_id,src_investigator_id,ctfo_site_id,src_site_id from ctms_golden_source_map_inv group by 1,2,3,4,5,6) cgsm
left join
(select src_trial_id,max(last_subject_first_treatment_planned_dt) as lsfr_curr_dt,min(first_site_initiation_visit_planned_dt) as fsiv_dt,min(first_subject_first_treatment_planned_dt) as fsfr_dt  from
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study where lower(trim(trial_status)) in {on_com_plan} group by 1) a
on lower(trim(cgsm.src_trial_id)) = lower(trim(a.src_trial_id))
group by 1,2,3) ctms_plan_dt group by 1,2,3
""".format(on_com_plan=Ongoing_Completed_Planned))

ctms_planned_dt = ctms_planned_dt.dropDuplicates()
ctms_planned_dt.registerTempTable('ctms_planned_dt')

#patients_randomized_planned
ctms_prp = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id,cgsm.ctfo_site_id,
sum(B.patients_randomized_planned) as patients_randomized_planned,
sum(patients_consented_planned) as patients_consented_planned,
sum(patients_screened_planned) as patients_screened_planned,
min(irb_iec_submission_planned_dt) as irb_iec_submission_planned_dt,
max(irb_iec_approval_planned_dt) as irb_iec_approval_planned_dt,
min(site_initiation_visit_planned_dt) as site_initiation_visit_planned_dt,
min(site_ready_to_enrol_planned_dt) as site_ready_to_enrol_planned_dt,
min(first_subject_first_visit_planned_dt) as first_subject_first_visit_planned_dt,
max(last_subject_first_visit_planned_dt) as last_subject_first_visit_planned_dt,
max(last_subject_last_treatment_planned_dt) as last_subject_last_treatment_planned_dt,
max(last_subject_last_visit_planned_dt)as last_subject_last_visit_planned_dt,
max(site_closure_visit_planned_dt) as site_closure_visit_planned_dt
from
(select ctfo_trial_id, src_trial_id, ctfo_investigator_id,src_investigator_id,ctfo_site_id,src_site_id from ctms_golden_source_map_inv group by 1,2,3,4,5,6) cgsm
left join
(select src_trial_id ,src_investigator_id,src_site_id,
patients_randomized_planned,
patients_consented_planned,
patients_screened_planned,
irb_iec_submission_planned_dt,
irb_iec_approval_planned_dt,
site_initiation_visit_planned_dt,
site_ready_to_enrol_planned_dt,
first_subject_first_visit_planned_dt,
last_subject_first_visit_planned_dt,
last_subject_last_treatment_planned_dt,
last_subject_last_visit_planned_dt,
site_closure_visit_planned_dt from
(select src_trial_id ,src_investigator_id,src_site_id,sum(patients_randomized_planned) as patients_randomized_planned,sum(patients_consented_planned) as patients_consented_planned,
sum(patients_screened_planned) as patients_screened_planned,
min(irb_iec_submission_planned_dt) as irb_iec_submission_planned_dt,
max(irb_iec_approval_planned_dt) as irb_iec_approval_planned_dt,
min(site_initiation_visit_planned_dt) as site_initiation_visit_planned_dt,
min(site_ready_to_enrol_planned_dt) as site_ready_to_enrol_planned_dt,
min(first_subject_first_visit_planned_dt) as first_subject_first_visit_planned_dt,
max(last_subject_first_visit_planned_dt) as last_subject_first_visit_planned_dt,
max(last_subject_last_treatment_planned_dt) as last_subject_last_treatment_planned_dt,
max(last_subject_last_visit_planned_dt)as last_subject_last_visit_planned_dt,
max(site_closure_visit_planned_dt) as site_closure_visit_planned_dt  from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator where lower(trim(site_trial_status)) in {on_com_plan} group by 1,2,3 )) B
on lower(trim(cgsm.src_trial_id))=lower(trim(B.src_trial_id)) and lower(trim(cgsm.src_investigator_id))=lower(trim(B.src_investigator_id)) and lower(trim(cgsm.src_site_id))=lower(trim(B.src_site_id))
group by 1,2,3""".format(on_com_plan=Ongoing_Completed_Planned))

ctms_prp = ctms_prp.dropDuplicates()
ctms_prp.registerTempTable('ctms_prp')

#patients_randomized_actual
ctms_pra = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id,cgsm.ctfo_site_id,sum(B.patients_randomized_actual) as patients_randomized_actual
from
(select ctfo_trial_id, src_trial_id, ctfo_investigator_id,src_investigator_id ,src_site_id,ctfo_site_id from ctms_golden_source_map_inv group by 1,2,3,4,5,6) cgsm
left join
(select src_trial_id ,src_investigator_id,src_site_id,patients_randomized_actual from
(select src_trial_id ,src_investigator_id,src_site_id, sum(patients_randomized_actual) as patients_randomized_actual from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator where lower(trim(site_trial_status)) in {on_com} group by 1,2,3 )) B
on lower(trim(cgsm.src_trial_id))=lower(trim(B.src_trial_id)) and lower(trim(cgsm.src_investigator_id))=lower(trim(B.src_investigator_id)) and lower(trim(cgsm.src_site_id))=lower(trim(B.src_site_id))
group by 1,2,3""".format(on_com=Ongoing_Completed))

ctms_pra = ctms_pra.dropDuplicates()
ctms_pra.registerTempTable('ctms_pra')

dqs_golden_source_map_inv = spark.sql(""" 
select aa.ctfo_trial_id,aa.ctfo_investigator_id,aa.ctfo_site_id,aa.data_src_nm,aa.member_study_id,aa.person_golden_id,aa.facility_golden_id from (select
    A.ctfo_trial_id,  A.ctfo_investigator_id, A.ctfo_site_id,A.data_src_nm, B.member_study_id,
     B.person_golden_id as person_golden_id,B.facility_golden_id,row_number() over(partition by A.src_trial_id,A.src_site_id,A.ctfo_investigator_id order by A.src_trial_id asc) as rnk 
     from
    (select ctfo_trial_id, ctfo_investigator_id, data_src_nm,
    src_trial_id, src_investigator_id,src_site_id,ctfo_site_id
    from golden_id_det_inv
    where lower(trim(data_src_nm)) = 'ir' group by 1,2,3,4,5,6,7) A
inner join
    (select  member_study_id,person_golden_id,facility_golden_id
    from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
    group by 1,2,3) B
on lower(trim(A.src_trial_id)) = lower(trim(B.member_study_id)) and lower(trim(A.src_investigator_id)) = lower(trim(B.person_golden_id)) and lower(trim(A.src_site_id))=lower(trim(B.facility_golden_id))
) aa 
group by 1,2,3,4,5,6,7
""")
dqs_golden_source_map_inv = dqs_golden_source_map_inv.dropDuplicates()
#dqs_golden_source_map_inv.createOrReplaceTempView("dqs_golden_source_map_inv")
dqs_golden_source_map_inv.write.mode("overwrite").saveAsTable("dqs_golden_source_map_inv")



dqs_site_inv_dates = spark.sql("""
select
    ctfo_trial_id, ctfo_investigator_id,ctfo_site_id,
    min(first_subject_consented_dt) as first_subject_enrolled_dt,
    min(first_subject_enrolled_dt) as first_subject_randomized_dt,
    max(last_subject_consented_dt) as last_subject_enrolled_dt,
    max(last_subject_enrolled_dt) as last_subject_randomized_dt,
    max(last_subject_last_visit_dt) as last_subject_last_visit_dt ,
    min(site_open_dt) as site_ready_to_enroll_dt
from
    dqs_golden_source_map_inv A
left join
    ( select member_study_id,person_golden_id,facility_golden_id,    min(first_subject_consented_dt) as first_subject_consented_dt ,
    min(first_subject_enrolled_dt) as first_subject_enrolled_dt,
    max(last_subject_consented_dt) as last_subject_consented_dt,
    max(last_subject_enrolled_dt) as last_subject_enrolled_dt,
    max(last_subject_last_visit_dt) as last_subject_last_visit_dt ,
    min(site_open_dt) as site_open_dt from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir group by 1,2,3) B
on lower(trim(A.member_study_id)) = lower(trim(B.member_study_id))
and lower(trim(A.person_golden_id)) = lower(trim(B.person_golden_id))
                             AND lower(trim(A.facility_golden_id)) = lower(trim(B.facility_golden_id))

group by 1,2,3""")
dqs_site_inv_dates = dqs_site_inv_dates.dropDuplicates()
dqs_site_inv_dates.createOrReplaceTempView("dqs_site_inv_dates")

dates_kpi = spark.sql("""
select 
ctfo_trial_id,
ctfo_site_id,
ctfo_investigator_id,
first_subject_enrolled_dt as first_subject_enrolled_kpi,
first_subject_randomized_dt as first_subject_randomized_dt_kpi,
last_subject_enrolled_dt as last_subject_enrolled_dt_kpi,
last_subject_randomized_dt as last_subject_randomized_dt_kpi,
last_subject_last_visit_dt as last_subject_last_visit_dt_kpi ,
site_ready_to_enroll_dt as site_ready_to_enroll_dt_kpi
from dqs_site_inv_dates
""")
dates_kpi.registerTempTable('dates_kpi')


dqs_study_fsi = spark.sql("""
select
    ctfo_trial_id,
    min(first_subject_consented_dt) as dqs_study_fsi_dt,
    max(last_subject_enrolled_dt) as study_last_subject_randomized_dt

from
    (select ctfo_trial_id, member_study_id from dqs_golden_source_map_inv group by 1,2) A
left join
    (select member_study_id, min(first_subject_consented_dt) as first_subject_consented_dt ,max(last_subject_enrolled_dt) as last_subject_enrolled_dt
    from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir where lower(trim(site_status)) in {on_com} group by 1) B
on lower(trim(A.member_study_id)) = lower(trim(B.member_study_id)) 
group by 1""".format(on_com=Ongoing_Completed))
dqs_study_fsi = dqs_study_fsi.dropDuplicates()
dqs_study_fsi.createOrReplaceTempView("dqs_study_fsi")

dqs_study_fsi_11 = spark.sql("""
select
    A.ctfo_trial_id,
    A.ctfo_site_id,
    A.ctfo_investigator_id,
    min(B.first_subject_consented_dt) as dqs_study_fsi_dt,
    max(B.last_subject_enrolled_dt_final) as study_last_subject_randomized_dt_final
from
    (select ctfo_trial_id,ctfo_site_id, ctfo_investigator_id,member_study_id, facility_golden_id, person_golden_id from dqs_golden_source_map_inv group by 1,2,3,4,5,6) A
left join
    (select member_study_id,person_golden_id,facility_golden_id, min(first_subject_consented_dt) as first_subject_consented_dt ,max(last_subject_enrolled_dt) as last_subject_enrolled_dt_final
    from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir group by 1, 2,3) B
on lower(trim(A.member_study_id)) = lower(trim(B.member_study_id)) and Lower(Trim(A.facility_golden_id)) = Lower(Trim(B.facility_golden_id))
and  Lower(Trim(A.person_golden_id)) = Lower(Trim(B.person_golden_id))
group by 1,2,3""")
dqs_study_fsi_11 = dqs_study_fsi_11.dropDuplicates()
dqs_study_fsi_11.createOrReplaceTempView("dqs_study_fsi_11")

dqs_status_value = spark.sql("""select member_study_id,person_golden_id,facility_golden_id,t.status as site_status,a.site_status as raw_status
         from  $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir a left join trial_status_mapping t on lower(trim(t.raw_status))=lower(trim(a.site_status)) where lower(trim(a.site_status)) in {on_com_status}
         """.format(on_status=Ongoing_variable, com_status=Completed_variable, on_com_status=Ongoing_Completed))
dqs_status_value.registerTempTable("dqs_status_value")

trial_inv_status_new = spark.sql("""select B.* from (select A.member_study_id,A.person_golden_id,A.facility_golden_id,A.site_status, row_number() over (partition by A.member_study_id,A.person_golden_id,A.facility_golden_id order by A.final_rnk asc) as status_rank from (select member_study_id,person_golden_id,facility_golden_id,site_status,
        case when lower(trim(site_status))='ongoing' then 1
        when lower(trim(site_status))='completed' then 2 else 3 end as final_rnk from  dqs_status_value) A)B where status_rank =1""")
trial_inv_status_new.registerTempTable("trial_inv_status_new")

trial_inv_status = spark.sql("""select B.* from (select A.ctfo_investigator_id, A.ctfo_site_id,A.ctfo_trial_id,A.site_status,
row_number() over (partition by A.ctfo_investigator_id, A.ctfo_site_id,A.ctfo_trial_id order by A.rank asc) as status_rank from (select  ctfo_investigator_id, ctfo_site_id,ctfo_trial_id,site_status,
        case when lower(trim(site_status))='ongoing' then 1
        when lower(trim(site_status))='completed' then 2 else 3 end as rank 
from dqs_golden_source_map_inv a left join trial_inv_status_new b
on lower(trim(a.member_study_id))=lower(trim(b.member_study_id)) and lower(trim(a.person_golden_id))=lower(trim(b.person_golden_id)) and lower(trim(a.facility_golden_id))=lower(trim(b.facility_golden_id))) A)  B where status_rank =1
""")
trial_inv_status.registerTempTable("trial_inv_status")

dqs_site_inv_details_temp = spark.sql("""select
A.ctfo_trial_id,
A.ctfo_investigator_id,
A.ctfo_site_id,
tis.site_status,
PERCENTILE(B.pct_screen_fail,0.5,10000) as pct_screen_fail,
sum(B.enrolled)                AS patients_randomized,
sum(B.consented)                AS patients_screened,
Min(B.first_subject_consented_dt)           AS first_subject_enrolled_dt,
Min(B.first_subject_enrolled_dt)            AS first_subject_randomized_dt,
Max(B.last_subject_consented_dt)            AS last_subject_enrolled_dt,
Max(B.last_subject_enrolled_dt)             AS last_subject_randomized_dt,
Max(B.last_subject_last_visit_dt)           AS last_subject_last_visit_dt,
Min(B.site_open_dt)                         AS site_ready_to_enroll_dt
FROM  

dqs_golden_source_map_inv A
LEFT JOIN (select member_study_id,person_golden_id,facility_golden_id,PERCENTILE(pct_screen_fail,0.5,10000) as pct_screen_fail,
max(enrolled)                AS enrolled,
max(consented)               AS consented,
Min(first_subject_consented_dt)           AS first_subject_consented_dt,
Min(first_subject_enrolled_dt)            AS first_subject_enrolled_dt,
Max(last_subject_consented_dt)            AS last_subject_consented_dt,
Max(last_subject_enrolled_dt)             AS last_subject_enrolled_dt,
Max(last_subject_last_visit_dt)           AS last_subject_last_visit_dt,
Min(site_open_dt)                         AS site_open_dt from 
(select member_study_id,person_golden_id,facility_golden_id,case when trim(lower(pct_screen_fail)) like '%\%%' then regexp_replace(pct_screen_fail,"\%","") else pct_screen_fail end as pct_screen_fail,

enrolled              AS enrolled,
first_subject_consented_dt           AS first_subject_consented_dt,
first_subject_enrolled_dt           AS first_subject_enrolled_dt,
last_subject_consented_dt           AS last_subject_consented_dt,
last_subject_enrolled_dt             AS last_subject_enrolled_dt,
last_subject_last_visit_dt          AS last_subject_last_visit_dt,
site_open_dt                       AS site_open_dt ,
consented AS consented
from 

$$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir where lower(trim(site_status)) in {on_com}) group by 1,2,3) B
       ON Lower(Trim(A.member_study_id)) = Lower(Trim(B.member_study_id))
        AND lower(trim(A.person_golden_id)) = lower(trim(B.person_golden_id))
                             AND lower(trim(A.facility_golden_id)) = lower(trim(B.facility_golden_id))
LEFT JOIN trial_inv_status tis
       ON Lower(Trim(A.ctfo_investigator_id)) = Lower(Trim(tis.ctfo_investigator_id))
        AND lower(trim(A.ctfo_site_id)) = lower(trim(tis.ctfo_site_id))
                             AND lower(trim(A.ctfo_trial_id)) = lower(trim(tis.ctfo_trial_id))
where lower(trim(tis.site_status)) in {on_com}  group by 1,2,3,4 """.format(on_com=Ongoing_Completed))
dqs_site_inv_details_temp = dqs_site_inv_details_temp.dropDuplicates()
#dqs_site_inv_details_temp.createOrReplaceTempView("dqs_site_inv_details_temp")
dqs_site_inv_details_temp.write.mode("overwrite").saveAsTable("dqs_site_inv_details_temp")



dqs_trial_inv_mce = spark.sql("""SELECT 
a.ctfo_trial_id,
a.ctfo_site_id,
a.ctfo_investigator_id,    
d.site_status as site_trial_status,  
''  as patients_consented_planned,      
'' as patients_consented_actual,       
'' as patients_screened_planned,       
d.patients_screened as patients_screened_actual,        
'' as dropped_at_screening_actual ,    
'' as patients_randomized_planned,     
d.patients_randomized as patients_randomized_actual,      
''  as patients_completed_actual ,       
'' as patients_dropped_actual  ,
'' as irb_iec_submission_bsln_dt ,      
'' as irb_iec_submission_planned_dt,   
'' as irb_iec_submission_actual_dt,   
'' as irb_iec_approval_bsln_dt,
'' as irb_iec_approval_planned_dt,  
'' as irb_iec_approval_actual_dt ,
'' as  site_initiation_visit_bsln_dt ,
'' as site_initiation_visit_planned_dt,
'' as site_initiation_visit_actual_dt, 
'' as site_ready_to_enrol_bsln_dt,    
'' as site_ready_to_enrol_planned_dt,  
d.site_ready_to_enroll_dt  as site_ready_to_enroll_actual_dt,
'' as first_subject_first_visit_bsln_dt,  
'' as first_subject_first_visit_planned_dt,
'' as first_subject_first_visit_actual_dt,
'' as  first_subject_first_treatment_bsln_dt,
'' as first_subject_first_treatment_planned_dt,
d.first_subject_randomized_dt as first_subject_first_treatment_actual_dt,
'' as last_subject_first_visit_bsln_dt,    
'' as last_subject_first_visit_planned_dt, 
'' as last_subject_first_visit_actual_dt, 
'' as  last_subject_first_treatment_bsln_dt,
'' as last_subject_first_treatment_planned_dt,
c.study_last_subject_randomized_dt as cntry_lsfr_dt,
'' as last_subject_last_treatment_bsln_dt,
'' as last_subject_last_treatment_planned_dt,
'' as last_subject_last_treatment_actual_dt,
'' as last_subject_last_visit_bsln_dt,
'' as last_subject_last_visit_planned_dt, 
b.last_subject_last_visit_dt as last_subject_last_visit_actual_dt,  
'' as site_closure_visit_bsln_dt,    
'' as site_closure_visit_planned_dt,  
'' as site_closure_visit_actual_dt,   
'' as  cntry_lsfv_dt,
'' as first_site_initiation_visit_planned_dt,
d.last_subject_enrolled_dt as study_last_subject_first_treatment_actual_dt,
'ir' as source ,                 
'' as fsiv_fsfr_delta,
'' as enrollment_rate,
case 
when lower(trim(d.site_status)) in  {on_status}
    and d.last_subject_enrolled_dt is not null then d.pct_screen_fail
when lower(trim(d.site_status)) in  {com_status} then d.pct_screen_fail
when lower(trim(d.site_status)) in  {on_status}
    and d.last_subject_enrolled_dt is  null then null
        end as screen_failure_rate,
'' as lost_opp_time,
'' as enrollment_duration,
bb.study_last_subject_randomized_dt_final as lsfr_dt_final,
'' as siv_dt_f
from dqs_golden_source_map_inv a
left join dqs_site_inv_dates b on lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id)) and lower(trim(a.ctfo_investigator_id)) = lower(trim(b.ctfo_investigator_id)) 
and lower(trim(a.ctfo_site_id)) = lower(trim(b.ctfo_site_id))
left join dqs_study_fsi c on lower(trim(a.ctfo_trial_id)) = lower(trim(c.ctfo_trial_id))
left join dqs_study_fsi_11 BB on lower(trim(a.ctfo_trial_id)) = lower(trim(BB.ctfo_trial_id)) and  Lower(Trim(a.ctfo_site_id)) = Lower(Trim(BB.ctfo_site_id)) and lower(trim(a.ctfo_investigator_id)) = lower(trim(BB.ctfo_investigator_id))
left join dqs_site_inv_details_temp d on lower(trim(a.ctfo_trial_id)) = lower(trim(d.ctfo_trial_id))  and lower(trim(a.ctfo_investigator_id)) = lower(trim(d.ctfo_investigator_id)) 
and lower(trim(a.ctfo_site_id)) = lower(trim(d.ctfo_site_id))
""".format(on_status=Ongoing_variable,com_status=Completed_variable))
dqs_trial_inv_mce = dqs_trial_inv_mce.dropDuplicates()
#dqs_trial_inv_mce.createOrReplaceTempView("dqs_trial_inv_mce")
dqs_trial_inv_mce.write.mode('overwrite').saveAsTable('dqs_trial_inv_mce')


ctms_trial_inv_mce = spark.sql("""SELECT 
cgsm.ctfo_trial_id,
cgsm.ctfo_site_id,
cgsm.ctfo_investigator_id,    
D.trial_status as site_trial_status,  
ctms_prp.patients_consented_planned as patients_consented_planned,      
sum(patients_consented_actual) as patients_consented_actual,       
ctms_prp.patients_screened_planned as patients_screened_planned,       
ctms_PS_PE_PDS.patients_screened as patients_screened_actual,        
ctms_PS_PE_PDS.dropped_at_screening_actual as dropped_at_screening_actual ,    
ctms_prp.patients_randomized_planned as patients_randomized_planned,     
ctms_PS_PE_PDS.patients_enrolled as patients_randomized_actual,      
sum(patients_completed_actual) as patients_completed_actual ,       
sum(patients_dropped_actual) as patients_dropped_actual  ,
min(irb_iec_submission_bsln_dt) as irb_iec_submission_bsln_dt ,      
min(ctms_prp.irb_iec_submission_planned_dt) as irb_iec_submission_planned_dt,   
min(irb_iec_submission_actual_dt)  as irb_iec_submission_actual_dt,   
max(irb_iec_approval_bsln_dt)  as    irb_iec_approval_bsln_dt,
max(ctms_prp.irb_iec_approval_planned_dt)  as   irb_iec_approval_planned_dt,  
max(irb_iec_approval_actual_dt)   as   irb_iec_approval_actual_dt ,
min(site_initiation_visit_bsln_dt) as   site_initiation_visit_bsln_dt ,
min(ctms_prp.site_initiation_visit_planned_dt) as site_initiation_visit_planned_dt,
min(ctms_dates_kpi.site_initiation_visit_actual_dt) as site_initiation_visit_actual_dt, 
min(site_ready_to_enrol_bsln_dt) as site_ready_to_enrol_bsln_dt,    
min(ctms_prp.site_ready_to_enrol_planned_dt) as site_ready_to_enrol_planned_dt,  
min(site_ready_to_enrol_actual_dt)  as site_ready_to_enrol_actual_dt,  
min(first_subject_first_visit_bsln_dt) as first_subject_first_visit_bsln_dt,  
min(ctms_prp.first_subject_first_visit_planned_dt) as first_subject_first_visit_planned_dt,
min(first_subject_first_visit_actual_dt)  as first_subject_first_visit_actual_dt,
min(first_subject_first_treatment_bsln_dt) as first_subject_first_treatment_bsln_dt,
min(ctms_planned_dt.first_subject_first_treatment_planned_dt) as first_subject_first_treatment_planned_dt,
min(ctms_dates_filtered.first_subject_first_treatment_actual_dt) as first_subject_first_treatment_actual_dt,
max(last_subject_first_visit_bsln_dt) as last_subject_first_visit_bsln_dt,    
max(ctms_prp.last_subject_first_visit_planned_dt) as last_subject_first_visit_planned_dt, 
max(ctms_dates_filtered.lsfv_dt)  as last_subject_first_visit_actual_dt, 
max(last_subject_first_treatment_bsln_dt) as last_subject_first_treatment_bsln_dt,
max(ctms_planned_dt.last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt,
max(ctms_dates_filtered.lsfr_dt_usl_sc) as cntry_lsfr_dt_f,
max(last_subject_last_treatment_bsln_dt) as last_subject_last_treatment_bsln_dt,
max(ctms_prp.last_subject_last_treatment_planned_dt) as last_subject_last_treatment_planned_dt,
max(last_subject_last_treatment_actual_dt) as last_subject_last_treatment_actual_dt,
max(last_subject_last_visit_bsln_dt) as last_subject_last_visit_bsln_dt,
max(ctms_prp.last_subject_last_visit_planned_dt) as last_subject_last_visit_planned_dt, 
max(ctms_dates_kpi.last_subject_last_visit_actual_dt) as last_subject_last_visit_actual_dt_kpi,  
max(site_closure_visit_bsln_dt) as site_closure_visit_bsln_dt,    
max(ctms_prp.site_closure_visit_planned_dt) as site_closure_visit_planned_dt,  
max(site_closure_visit_actual_dt) as site_closure_visit_actual_dt,   
max(c.cntry_lsfv_dt) as cntry_lsfv_dt,
min(ctms_planned_dt.first_site_initiation_visit_planned_dt) as first_site_initiation_visit_planned_dt,
max(ctms_dates_filtered.lsfr_dt_usl_s) as lsfr_dt_f,
source ,                 
fsiv_fsfr_delta,
enrollment_rate,
screen_failure_rate,
lost_opp_time,
enrollment_duration,
max(last_subject_first_treatment_actual_dt) as lsfr_dt_final,
min(ctms_dates_filtered.siv_dt) as siv_dt_f
from
(select ctfo_trial_id, src_trial_id, ctfo_investigator_id,src_investigator_id,ctfo_site_id,src_site_id 
from ctms_golden_source_map_inv group by 1,2,3,4,5,6) cgsm

left join
ctms_dates_kpi
on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(ctms_dates_kpi.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(ctms_dates_kpi.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(ctms_dates_kpi.ctfo_site_id))

left join
ctms_dates_filtered
on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(ctms_dates_filtered.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(ctms_dates_filtered.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(ctms_dates_filtered.ctfo_site_id))

left join
ctms_PS_PE_PDS
on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(ctms_PS_PE_PDS.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(ctms_PS_PE_PDS.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(ctms_PS_PE_PDS.ctfo_site_id))

left join
ctms_planned_dt
on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(ctms_planned_dt.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(ctms_planned_dt.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(ctms_planned_dt.ctfo_site_id))

left join
ctms_prp
on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(ctms_prp.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(ctms_prp.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(ctms_prp.ctfo_site_id))

left join
ctms_pra
on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(ctms_pra.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(ctms_pra.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(ctms_pra.ctfo_site_id))

left join 
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator  a on lower(trim(cgsm.src_trial_id)) = lower(trim(a.src_trial_id)) 
and lower(trim(cgsm.src_investigator_id))=lower(trim(a.src_investigator_id)) and lower(trim(cgsm.src_site_id))=lower(trim(a.src_site_id))
left join 
(select src_trial_id,max(last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt,
        min(first_site_initiation_visit_planned_dt) as first_site_initiation_visit_planned_dt,
        min(first_subject_first_treatment_planned_dt) as first_subject_first_treatment_planned_dt,
        max(last_subject_first_treatment_actual_dt) as study_last_subject_first_treatment_actual_dt  
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study group by 1) b 
on lower(trim(cgsm.src_trial_id)) = lower(trim(b.src_trial_id)) 
left join
(select src_trial_id,max(last_subject_first_visit_actual_dt) as cntry_lsfv_dt,max(last_subject_first_treatment_actual_dt) as cntry_lsfr_dt from 
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_country group by 1) c 
on lower(trim(cgsm.src_trial_id)) = lower(trim(c.src_trial_id))
LEFT JOIN ctms_trial_status_inv D on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(D.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(D.ctfo_investigator_id)) 
and lower(trim(cgsm.ctfo_site_id))=lower(trim(D.ctfo_site_id))
group by 1,2,3,4,5,7,8,9,10,11,50,51,52,53,54,55
""".format(on_com=Ongoing_Completed))


ctms_trial_inv_mce = ctms_trial_inv_mce.dropDuplicates()
#ctms_trial_inv_mce.createOrReplaceTempView("ctms_trial_inv_mce")
ctms_trial_inv_mce.write.mode('overwrite').saveAsTable('ctms_trial_inv_mce')



trial_inv_mce=spark.sql("""select * from dqs_trial_inv_mce
union
 select * from ctms_trial_inv_mce""")
trial_inv_mce = trial_inv_mce.dropDuplicates()
#trial_inv_mce.registerTempTable('trial_inv_mce')
trial_inv_mce.write.mode('overwrite').saveAsTable('trial_inv_mce')




#write_path = path.replace("table_name", "Investigator")
#trial_inv_mce.coalesce(1).write.format("parquet").mode("overwrite").save(path=write_path)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_inv partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   trial_inv_mce
""")

if trial_inv_mce.count() == 0:
    print("Skipping copy_hdfs_to_s3 for mce_trial_inv as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.mce_trial_inv")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")


