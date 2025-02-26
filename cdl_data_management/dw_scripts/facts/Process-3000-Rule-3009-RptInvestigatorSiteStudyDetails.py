################################# Module Information ######################################
#  Module Name         : Site Investigator reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_site_study_details_filters
#                           b. f_rpt_site_study_details
#                           c. f_rpt_src_site_details
#                           d. f_rpt_investigator_details
#                           e. f_rpt_site_study_details_non_performance
#  Pre-requisites      : Source table required:
#  Last changed on     : 03-02-2021
#  Last changed by     : Kuldeep
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Create required table and stores data on HDFS
############################################################################################
import datetime
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql.types import *
from CommonUtils import CommonUtils
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = bucket_path + "/applications/commons/temp/" \
                         "kpi_output_dimension/table_name"

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

################## creating table with all dimensions information ################

cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/"
    "uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

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

citeline_exp = spark.sql("""SELECT   trial_id,
         Trim(ta)    AS therapeutic_area,
         Trim(ps)   AS patient_segment,
         case when mesh is null or trim(mesh) = '' then disease
else mesh end as trial_mesh_term_name 

FROM     $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove 
        lateral view outer posexplode(split(trial_therapeutic_areas_name,'\\\|'))one AS pos1,ta
        lateral view outer posexplode(split(trial_mesh_term_name,'\\\|'))two AS pos2,mesh
        lateral view outer posexplode(split(disease_name,'\\\|'))three AS pos3,disease
        lateral VIEW outer posexplode(split(patient_segment,'\\\|'))four AS pos4,ps
WHERE     pos1=pos2 and pos2=pos3 and pos3=pos4 and pos4=pos1
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
citeline_exp.registerTempTable('citeline_exp')

citeline_exp_1 = spark.sql("""SELECT   trial_id,
         therapeutic_area,
         Trim(ps)   AS patient_segment,
         Trim(mesh) as trial_mesh_term_name

FROM     citeline_exp 
        lateral view outer posexplode(split(trial_mesh_term_name,'\\\^'))two AS pos1,mesh
        lateral VIEW outer posexplode(split(patient_segment,'\\\^'))three AS pos2,ps
WHERE     pos1=pos2
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
citeline_exp_1.registerTempTable('citeline_exp_1')

citeline_exp_2 = spark.sql("""SELECT   'citeline' as datasource, trial_id, therapeutic_area,
         Trim(ps)   AS patient_segment,
         Trim(mesh) as trial_mesh_term_name 

FROM     citeline_exp_1
        lateral view outer explode(split(trial_mesh_term_name,'\\\#'))one AS mesh
        lateral VIEW outer explode(split(patient_segment,'\\\#'))two AS ps
GROUP BY 1,2,3,4,5 ORDER BY therapeutic_area """)

citeline_exp_2.registerTempTable('citeline_exp_2')

temp_disease_info = spark.sql("""
select d_ctfo_trial_id, therapeutic_area, INITCAP(disease_nm) as disease_nm, patient_segment from
(select
    r_trial_disease.ctfo_trial_id as d_ctfo_trial_id,
    (case when trim(d_disease.therapeutic_area) = 'Other' then 'Other' else
    d_disease.therapeutic_area end) as therapeutic_area ,
    (case when trim(disease_nm) in ('Other','(n/a)') then 'Other' else disease_nm end) as disease_nm,
        (case when trim(citeline.patient_segment) = 'Other' then 'Other' else citeline.patient_segment end)
        as patient_segment
from
    (select distinct ctfo_trial_id, disease_id from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_disease
    group by 1,2) r_trial_disease
left join
    (select distinct disease_id, therapeutic_area, disease_nm
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_disease) d_disease
on lower(trim(r_trial_disease.disease_id))=lower(trim(d_disease.disease_id))
left join
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial xref_trial
on lower(trim(r_trial_disease.ctfo_trial_id))=lower(trim(xref_trial.ctfo_trial_id))
and lower(trim(xref_trial.data_src_nm)) = 'citeline'
left join
citeline_exp_2 citeline
on lower(trim(xref_trial.src_trial_id))=lower(trim(citeline.trial_id))
group by 1,2,3,4)
""")
temp_disease_info = temp_disease_info.dropDuplicates()
temp_disease_info.registerTempTable("temp_disease_info")

tascan_exp = spark.sql("""SELECT   trial_id,
         Trim(ta)    AS therapeutic_area,
         Trim(ps)   AS patient_segment,
         case when mesh is null or trim(mesh) = '' then disease
else mesh end as trial_mesh_term_name

FROM     $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial 
        lateral view outer posexplode(split(trial_therapeutic_area_name,'\\\|'))one AS pos1,ta
        lateral view outer posexplode(split(mesh_term_names,'\\\|'))two AS pos2,mesh
        lateral view outer posexplode(split(disease_name,'\\\|'))three AS pos3,disease
        lateral VIEW outer posexplode(split(patient_segment,'\\\|'))four AS pos4,ps
WHERE     pos1=pos2 and pos2=pos3 and pos3=pos4 and pos4=pos1
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
tascan_exp.registerTempTable('tascan_exp')

tascan_exp_1 = spark.sql("""SELECT   trial_id,
         therapeutic_area,
         Trim(ps)   AS patient_segment,
         Trim(mesh) as trial_mesh_term_name

FROM     tascan_exp 
        lateral view outer posexplode(split(trial_mesh_term_name,'\\\^'))two AS pos1,mesh
        lateral VIEW outer posexplode(split(patient_segment,'\\\^'))three AS pos2,ps
WHERE     pos1=pos2
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
tascan_exp_1.registerTempTable('tascan_exp_1')

tascan_exp_2 = spark.sql("""SELECT   'tascan' as datasource, trial_id, therapeutic_area,
         Trim(ps)   AS patient_segment,
         Trim(mesh) as trial_mesh_term_name 

FROM     tascan_exp_1
        lateral view outer explode(split(trial_mesh_term_name,'\\\#'))one AS mesh
        lateral VIEW outer explode(split(patient_segment,'\\\#'))two AS ps
GROUP BY 1,2,3,4,5 ORDER BY therapeutic_area """)

tascan_exp_2.registerTempTable('tascan_exp_2')

temp_disease_info_tascan = spark.sql("""
select d_ctfo_trial_id, therapeutic_area, INITCAP(disease_nm) as disease_nm, patient_segment from
(select
    r_trial_disease.ctfo_trial_id as d_ctfo_trial_id,
    (case when trim(d_disease.therapeutic_area) = 'Other' then 'Other' else
    d_disease.therapeutic_area end) as therapeutic_area ,
    (case when trim(disease_nm) in ('Other','(n/a)') then 'Other' else disease_nm end) as disease_nm,
        (case when trim(tascan.patient_segment) = 'Other' then 'Other' else tascan.patient_segment end)
        as patient_segment
from
    (select distinct ctfo_trial_id, disease_id from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_disease
    group by 1,2) r_trial_disease
left join
    (select distinct disease_id, therapeutic_area, disease_nm
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_disease) d_disease
on lower(trim(r_trial_disease.disease_id))=lower(trim(d_disease.disease_id))
left join
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial xref_trial
on lower(trim(r_trial_disease.ctfo_trial_id))=lower(trim(xref_trial.ctfo_trial_id))
and lower(trim(xref_trial.data_src_nm)) = 'tascan'
left join
tascan_exp_2 tascan
on lower(trim(xref_trial.src_trial_id))=lower(trim(tascan.trial_id))
group by 1,2,3,4)
""")
temp_disease_info_tascan = temp_disease_info.dropDuplicates()
temp_disease_info_tascan.registerTempTable("temp_disease_info_tascan")
temp_disease_info_combined = spark.sql("""SELECT * from temp_disease_info UNION SELECT * FROM temp_disease_info_tascan""")
temp_disease_info_combined.registerTempTable('temp_disease_info_combined')

r_trial_inv_site_roles = spark.sql("""
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
r_trial_inv_site_roles.createOrReplaceTempView("r_trial_inv_site_roles")
# r_trial_inv_site_roles.write.mode("overwrite").saveAsTable("r_trial_inv_site_roles")

golden_id_det_inv_1_rpt = spark.sql("""select B.ctfo_trial_id, B.ctfo_site_id,B.ctfo_investigator_id, C.data_src_nm, src_site_id
from
    r_trial_inv_site_roles B
inner join
    (select
        data_src_nm, ctfo_site_id,src_site_id
    from
        (select
            data_src_nm, ctfo_site_id, trim(src_site_id_exp) as src_site_id
        from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site
        lateral view outer explode(split(src_site_id, '\\\;')) as src_site_id_exp
    ) group by 1,2,3) C
on lower(trim(B.ctfo_site_id)) = lower(trim(C.ctfo_site_id))
group by 1,2,3,4,5
""")
golden_id_det_inv_1_rpt.createOrReplaceTempView("golden_id_det_inv_1_rpt")

golden_id_det_inv_2_rpt = spark.sql("""select B.ctfo_trial_id, B.ctfo_site_id,B.ctfo_investigator_id,B.src_site_id,C.data_src_nm, src_investigator_id
from
    golden_id_det_inv_1_rpt B
inner join
    (select
        data_src_nm, ctfo_investigator_id,src_investigator_id
    from
        (select
            data_src_nm, ctfo_investigator_id, explode(split(src_investigator_id, ';')) as src_investigator_id
        from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_inv
    ) group by 1,2,3) C
on lower(trim(B.ctfo_investigator_id)) = lower(trim(C.ctfo_investigator_id)) AND Lower(Trim(C.data_src_nm)) = Lower(Trim(B.data_src_nm))
group by 1,2,3,4,5,6
""")
golden_id_det_inv_2_rpt.createOrReplaceTempView("golden_id_det_inv_2_rpt")

goldeniddet_its_rpt = spark.sql("""
select
    B.ctfo_trial_id, B.ctfo_site_id,B.ctfo_investigator_id, B.data_src_nm,B.src_site_id,
    B.src_investigator_id,E.src_trial_id
from
    golden_id_det_inv_2_rpt B
inner join
    (select data_src_nm, ctfo_trial_id, src_trial_id
    from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
    group by 1,2,3) E
on lower(trim(B.ctfo_trial_id)) = lower(trim(E.ctfo_trial_id)) and
lower(trim(B.data_src_nm)) = lower(trim(E.data_src_nm))
group by 1,2,3,4,5,6,7
""")
goldeniddet_its_rpt = goldeniddet_its_rpt.dropDuplicates()
goldeniddet_its_rpt.createOrReplaceTempView("goldeniddet_its_rpt")
# goldeniddet_its_rpt.write.mode("overwrite").saveAsTable("goldeniddet_its_rpt")


usl_study_site_investigator = spark.sql("""
select
src_trial_id ,
src_country_id,
src_site_id ,
site_trial_status,src_investigator_id
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator
group by 1,2,3,4,5
""")
usl_study_site_investigator.createOrReplaceTempView("usl_study_site_investigator")

centre_loc_map = spark.sql("""
select
src_trial_id as trial_no,
src_country_id as study_country_id,
src_site_id as centre_loc_id,
src_investigator_id as primary_investigator_id
from usl_study_site_investigator
group by 1,2,3,4
""")
centre_loc_map = centre_loc_map.dropDuplicates()
centre_loc_map.createOrReplaceTempView("centre_loc_map")

ctms_gsm_its_rpt = spark.sql(""" select  fin.ctfo_trial_id,fin.ctfo_investigator_id,fin.src_trial_id,fin.src_investigator_id,fin.ctfo_site_id,fin.src_site_id, fin.new_status,fin.site_trial_status,
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
  group by 1,2,3,4) study_site
  left join trial_status_mapping t on lower(trim(t.raw_status))=lower(trim(study_site.site_trial_status))
group by 1,2,3,4,5) c
on lower(trim(cgsm.src_trial_id)) = lower(trim(c.src_trial_id)) and lower(trim(cgsm.src_investigator_id))=lower(trim(c.src_investigator_id)) and lower(trim(cgsm.src_site_id))=lower(trim(c.src_site_id))
group by 1,2,3,4,5,6,7,8) as a group by 1,2,3,4,5,6,7,8)fin group by 1,2,3,4,5,6,7,8,9
""")
ctms_gsm_its_rpt = ctms_gsm_its_rpt.dropDuplicates()
# ctms_gsm_its_rpt.registerTempTable('ctms_gsm_its_rpt')
ctms_gsm_its_rpt.write.mode("overwrite").saveAsTable("ctms_gsm_its_rpt")

ctms_its_status_rpt = spark.sql(""" select ctfo_trial_id,ctfo_investigator_id,ctfo_site_id,new_status as status from (
select  ctfo_trial_id,ctfo_investigator_id,src_trial_id,src_investigator_id,ctfo_site_id,src_site_id ,new_status,site_trial_status, precedence ,row_number() over(partition by ctfo_trial_id,ctfo_investigator_id,ctfo_site_id order by precedence asc) as rnk from ctms_gsm_its_rpt) where rnk=1""")
ctms_its_status_rpt = ctms_its_status_rpt.dropDuplicates()
# ctms_its_status_rpt.registerTempTable('ctms_its_status_rpt')
ctms_its_status_rpt.write.mode("overwrite").saveAsTable("ctms_its_status_rpt")

dqs_gsm_its_rpt = spark.sql("""
select * from(select
    A.ctfo_trial_id,A.ctfo_site_id,A.ctfo_investigator_id, A.data_src_nm, B.nct_number, B.member_study_id,
  B.facility_golden_id,B.person_golden_id
from
    (select ctfo_trial_id,ctfo_site_id, ctfo_investigator_id, data_src_nm,
    src_trial_id, src_investigator_id,src_site_id
    from goldeniddet_its_rpt
    where lower(trim(data_src_nm)) = 'ir' group by 1,2,3,4,5,6,7) A
inner join
    (select nct_number, member_study_id,person_golden_id,facility_golden_id
    from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
    group by 1,2,3,4) B
on lower(trim(A.src_trial_id)) = lower(trim(B.member_study_id)) and lower(trim(A.src_investigator_id)) = lower(trim(B.person_golden_id)) and lower(trim(A.src_site_id)) = lower(trim(B.facility_golden_id))
group by 1,2,3,4,5,6,7,8)a
""")
dqs_gsm_its_rpt = dqs_gsm_its_rpt.dropDuplicates()
dqs_gsm_its_rpt.createOrReplaceTempView("dqs_gsm_its_rpt")
dqs_gsm_its_rpt.write.mode("overwrite").saveAsTable("dqs_gsm_its_rpt")

ir_status_its_rpt_1 = spark.sql("""select A.ctfo_trial_id,A.ctfo_site_id,A.ctfo_investigator_id,tsm.status,case when lower(trim(tsm.status))='ongoing' then 1 when lower(trim(tsm.status))='completed' then 2 when lower(trim(tsm.status))='planned' then 3
 when lower(trim(tsm.status))='others' then 4 else 5 end as prec from (select ctfo_trial_id,ctfo_site_id, ctfo_investigator_id,member_study_id,person_golden_id,facility_golden_id from dqs_gsm_its_rpt) A left join  (select member_study_id,facility_golden_id,person_golden_id,site_status from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir   group by 1,2,3,4) B on lower(trim(A.member_study_id)) = lower(trim(B.member_study_id)) and lower(trim(A.person_golden_id)) = lower(trim(B.person_golden_id)) and lower(trim(A.facility_golden_id)) = lower(trim(B.facility_golden_id))
left join (select distinct raw_status,status from trial_status_mapping) tsm on lower(trim(B.site_status))=lower(trim(tsm.raw_status)) group by 1,2,3,4""")
ir_status_its_rpt_1.registerTempTable("ir_status_its_rpt_1")

ir_status_its_rpt_2 = spark.sql(
    """select * from (select ctfo_trial_id,ctfo_site_id,ctfo_investigator_id,status,row_number() over(partition by ctfo_trial_id,ctfo_site_id,ctfo_investigator_id order by prec) as rnk from ir_status_its_rpt_1) """)
ir_status_its_rpt_2.registerTempTable("ir_status_its_rpt_2")

ir_status_its_rpt = spark.sql(
    """select ctfo_trial_id,ctfo_site_id,ctfo_investigator_id,status from ir_status_its_rpt_2 where rnk=1 group by 1,2,3,4""")
ir_status_its_rpt.registerTempTable("ir_status_its_rpt")
ir_status_its_rpt.write.mode("overwrite").saveAsTable("ir_status_its_rpt")

aact_raw_data = spark.sql(
    """SELECT a.nct_id as src_trial_id,b.id as src_investigator_id,a.status,a.id as src_site_id       FROM   $$client_name_ctfo_datastore_staging_$$db_env.aact_facilities a inner join  $$client_name_ctfo_datastore_staging_$$db_env.aact_facility_investigators b on lower(trim(a.nct_id))=lower(trim(b.nct_id))  and lower(trim(a.id))=lower(trim(b.facility_id)) where lower(trim(b.role))='principal investigator' """)
aact_raw_data = aact_raw_data.dropDuplicates()
aact_raw_data.registerTempTable('aact_raw_data')

aact_gsm_its_status = spark.sql("""select iq.* from (SELECT ctfo_trial_id,
       ctfo_site_id,
       ctfo_investigator_id,
       data_src_nm,
       src_trial_id,
       src_site_id,
       src_investigator_id,
       status,
    ROW_NUMBER() OVER (PARTITION BY ab.ctfo_trial_id,ab.ctfo_site_id ,ab.ctfo_investigator_id ORDER BY ab.status_rnk ) pred_stat
    FROM
(SELECT B.ctfo_trial_id,
       C.ctfo_site_id,
       D.ctfo_investigator_id,
       B.data_src_nm,
       B.src_trial_id,
       C.src_site_id,
       D.src_investigator_id,
       tsm.status,
       case when lower(trim(tsm.status))='ongoing' then 1
            when lower(trim(tsm.status))='completed' then 2
            when lower(trim(tsm.status))='planned' then 3
            when lower(trim(tsm.status))='others' then 4 else 5 end as status_rnk
FROM  aact_raw_data aa inner join (SELECT data_src_nm,
                         ctfo_trial_id,
                         src_trial_id
                  FROM
$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial where  Lower(Trim(data_src_nm)) = 'aact'
                  GROUP  BY 1,
                            2,
                            3) B
              ON lower(trim(aa.src_trial_id)) = lower(trim(B.src_trial_id))
       INNER JOIN (SELECT data_src_nm,
                         ctfo_site_id,
                         src_site_id
                  FROM   (SELECT data_src_nm,
                                 ctfo_site_id,
                                src_site_id
                          FROM
       $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_site where  Lower(Trim(data_src_nm)) = 'aact' )
                  GROUP  BY 1,
                            2,
                            3 ) C
              ON lower(trim(aa.src_site_id)) = lower(trim(C.src_site_id))
                 INNER JOIN (SELECT data_src_nm, ctfo_investigator_id,src_investigator_id
                  FROM   (SELECT data_src_nm, ctfo_investigator_id,
                                 src_investigator_id
                          FROM
       $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_inv where  Lower(Trim(data_src_nm)) = 'aact')
                  GROUP  BY 1,
                            2,
                            3
                            ) D
              ON lower(trim(aa.src_investigator_id)) = lower(trim(D.src_investigator_id))
              inner join (select distinct raw_status,status from trial_status_mapping) tsm
              on lower(trim(aa.status))=lower(trim(tsm.raw_status))
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5,
                  6,7,8,9)ab ) iq where iq.pred_stat=1
""")
aact_gsm_its_status = aact_gsm_its_status.dropDuplicates()
# aact_gsm_its_status.createOrReplaceTempView('aact_gsm_its_status')
aact_gsm_its_status.write.mode("overwrite").saveAsTable("aact_gsm_its_status")

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country,scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.$$client_name_cluster_code as region_code,scpm.$$client_name_cluster,scpm.$$client_name_csu,scpm.post_trial_flag,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country
    from standard_country_mapping )  cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

f_rpt_investigator_details_temp_1 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,
     investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address
from
    (select
        i_ctfo_investigator_id as ctfo_investigator_id, i_ctfo_site_id as ctfo_site_id,
        i_ctfo_trial_id as ctfo_trial_id
    from temp_inv_info) A
inner join
    (select ctfo_investigator_id, investigator_name as investigator_full_nm, investigator_specialty,
    investigator_email, investigator_phone, investigator_address, investigator_city,
    investigator_state,
    investigator_country,
    geo_cd
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_investigator) d_investigator
on lower(trim(A.ctfo_investigator_id)) = lower(trim(d_investigator.ctfo_investigator_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
f_rpt_investigator_details_temp_1 = f_rpt_investigator_details_temp_1.dropDuplicates()

f_rpt_investigator_details_temp_1.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_1")

f_rpt_investigator_details_temp_2 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,
     investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address,
        temp_site_info.site_nm, temp_site_info.site_address,
    temp_site_info.site_city, temp_site_info.site_state, temp_site_info.site_country
from
    f_rpt_investigator_details_temp_1 A
        inner join
    temp_site_info
on lower(trim(A.ctfo_site_id))=lower(trim(temp_site_info.s_ctfo_site_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
""")
f_rpt_investigator_details_temp_2 = f_rpt_investigator_details_temp_2.dropDuplicates()

f_rpt_investigator_details_temp_2.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_2")

f_rpt_investigator_details_temp_3 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,
     investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address,
        site_nm,site_address,
    site_city, site_state, site_country,
        trial_status, trial_start_dt, trial_phase,trial_end_dt, trial_title as
    study_name,temp_nct_id_ctfo_trial_id_map.protocol_ids as protocol_ids,
    case when (trim(temp_nct_id_ctfo_trial_id_map.nct_id) is null or trim(temp_nct_id_ctfo_trial_id_map.nct_id)='') then null else temp_nct_id_ctfo_trial_id_map.nct_id end as nct_id
from
f_rpt_investigator_details_temp_2 A
inner join
    (select
        ctfo_trial_id, trial_phase, trial_start_dt,trial_status,trial_title,
        trial_end_dt
        from $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial
        group by 1,2,3,4,5,6) d_trial
on lower(trim(A.ctfo_trial_id))=lower(trim(d_trial.ctfo_trial_id))
left join
    temp_nct_id_ctfo_trial_id_map
on lower(trim(A.ctfo_trial_id))=lower(trim(temp_nct_id_ctfo_trial_id_map.n_ctfo_trial_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
""")
f_rpt_investigator_details_temp_3 = f_rpt_investigator_details_temp_3.dropDuplicates()
f_rpt_investigator_details_temp_3.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_3")

f_rpt_investigator_details_temp_4 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,
     investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address,
        site_nm,site_address,
    site_city, site_state, site_country,
        trial_status, trial_start_dt, trial_phase, trial_end_dt,
    study_name,protocol_ids,nct_id,
        concat_ws("\|",sort_array(collect_set(distinct NULLIF(trim(temp_sponsor_info.sponsor_nm),'')),true)) as sponsor_nm, 
        concat_ws("\|",sort_array(collect_set(distinct NULLIF(trim(temp_sponsor_info.subsidiary_sponsors),'')),true)) as subsidiary_sponsors,
        concat_ws("\|",sort_array(collect_set(distinct NULLIF(trim(temp_sponsor_info.sponsor_type),'')),true)) as sponsor_type
from
f_rpt_investigator_details_temp_3 A
left join
    temp_sponsor_info
on lower(trim(A.ctfo_trial_id))=lower(trim(temp_sponsor_info.s_ctfo_trial_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24

""")
f_rpt_investigator_details_temp_4 = f_rpt_investigator_details_temp_4.dropDuplicates()

f_rpt_investigator_details_temp_4.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_4")

f_rpt_investigator_details_temp_5 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address,
    site_nm,site_address,site_city, site_state, site_country,
    trial_status, trial_start_dt,trial_end_dt, trial_phase,
    study_name,protocol_ids,nct_id,sponsor_nm,subsidiary_sponsors,sponsor_type,
    total_recruitment_months,last_subject_in_dt,ready_to_enroll_dt,
    last_subject_last_visit_dt,
    randomization_rate,screen_fail_rate,
    patients_enrolled,
    patients_screened
from
f_rpt_investigator_details_temp_4 A
left join f_investigator_trial_site_final
on lower(trim(A.ctfo_investigator_id)) = lower(trim(f_investigator_trial_site_final.ctfo_investigator_id))
and lower(trim(A.ctfo_trial_id)) = lower(trim(f_investigator_trial_site_final.ctfo_trial_id))
and lower(trim(A.ctfo_site_id)) = lower(trim(f_investigator_trial_site_final.ctfo_site_id))
""")
f_rpt_investigator_details_temp_5 = f_rpt_investigator_details_temp_5.dropDuplicates()
f_rpt_investigator_details_temp_5.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_5")

temp_disease_info_final = spark.sql("""select d_ctfo_trial_id as ctfo_trial_id,therapeutic_area,
concat_ws('|',sort_array(collect_set(NULLIF(trim(disease_nm),'')),true)) as disease_nm
    from temp_disease_info_combined group by 1,2""")
temp_disease_info_final = temp_disease_info_final.dropDuplicates()
temp_disease_info_final.createOrReplaceTempView("temp_disease_info_final")

f_inv_disease_info = spark.sql(""" select A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id from 
f_rpt_investigator_details_temp_5 A
""")
f_inv_disease_info = f_inv_disease_info.dropDuplicates()
f_inv_disease_info.write.mode("overwrite").saveAsTable(
    "f_inv_disease_info")

f_inv_disease_info_final = spark.sql("""
select distinct
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,concat_ws(';',collect_set(B.disease_nm)) as disease_nm
from
f_inv_disease_info A
left join temp_disease_info_final B
on lower(trim(A.ctfo_trial_id)) = lower(trim(B.ctfo_trial_id))
group by 1,2,3
""")
f_inv_disease_info_final = f_inv_disease_info_final.dropDuplicates()
f_inv_disease_info_final.write.mode("overwrite").saveAsTable(
    "f_inv_disease_info_final")

f_rpt_investigator_details_temp_6 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id,
        A.ctfo_trial_id,
        A.ctfo_site_id,
    investigator_full_nm,
        investigator_specialty,
        investigator_email,
        investigator_phone,
     investigator_city,
    investigator_state,
        investigator_country,
        investigator_address,
        site_nm,
        site_address,
    site_city,
        site_state,
        site_country,
        trial_status,
        trial_start_dt,
        trial_end_dt,
        trial_phase,
    study_name,
        protocol_ids,
        trim(nct_id) as nct_id,
         sponsor_nm,
         subsidiary_sponsors,
     sponsor_type,
     total_recruitment_months,
    last_subject_in_dt,
        ready_to_enroll_dt,
        screen_fail_rate,
        B.disease_nm,
        d_geo.country,
     d_geo.country_code,
    d_geo.region,
    d_geo.region_code,
    randomization_rate,
    last_subject_last_visit_dt,
    A.patients_enrolled,
    A.patients_screened

from
f_rpt_investigator_details_temp_5 A
left join
    f_inv_disease_info_final B
on lower(trim(A.ctfo_trial_id)) = lower(trim(B.ctfo_trial_id)) and lower(trim(A.ctfo_site_id)) = lower(trim(B.ctfo_site_id)) and lower(trim(A.ctfo_investigator_id)) = lower(trim(B.ctfo_investigator_id))
left join
    (select geo_cd, standard_country as country, iso2_code as country_code, region, region_code
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_geo group by 1,2,3,4,5) d_geo
on lower(trim(A.geo_cd)) = lower(trim(d_geo.geo_cd))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39
""")
f_rpt_investigator_details_temp_6 = f_rpt_investigator_details_temp_6.dropDuplicates()
f_rpt_investigator_details_temp_6.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_6")

'''
inv_workload = spark.sql("""
select
    ctfo_investigator_id,
    count(A.ctfo_trial_id) as total_trials_all_ta,
    count(case when trial_status in ('Active (Not Recruiting)', 'Active (Recruiting)',
     'Enrolling by Invitation',
    'Initiating', 'Open for Recruitment')
        then A.ctfo_trial_id
    end) as current_workload,
    trial_status
from
    (select ctfo_investigator_id, ctfo_trial_id
    from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_investigator group by 1,2) A
left join
    (select ctfo_trial_id, trial_status from $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial
     group by 1,2) B
on lower(trim(A.ctfo_trial_id)) = lower(trim(B.ctfo_trial_id))
group by ctfo_investigator_id,trial_status
""")
inv_workload = inv_workload.dropDuplicates()
inv_workload.write.mode("overwrite").saveAsTable("inv_workload")
'''

d_trial_citeline_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.ctfo_site_id,iq.ctfo_investigator_id,iq.status,iq.status_rnk,ROW_NUMBER() OVER
(PARTITION BY iq.ctfo_trial_id,iq.ctfo_site_id,iq.ctfo_investigator_id ORDER BY iq.status_rnk ) pred_stat from (SELECT A.ctfo_trial_id,
       A.ctfo_site_id,
       A.ctfo_investigator_id,
       data_src_nm,
       tsm.status,
       case when lower(trim(tsm.status))='ongoing' then 1
            when lower(trim(tsm.status))='completed' then 2
            when lower(trim(tsm.status))='planned' then 3
            when lower(trim(tsm.status))='others' then 4 else 5 end as status_rnk
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               ctfo_investigator_id,
               data_src_nm,
               src_trial_id,
               src_site_id,
               src_investigator_id
        FROM   goldeniddet_its_rpt
        WHERE  Lower(Trim(data_src_nm)) = 'citeline'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7) A
       INNER JOIN $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove B
               ON lower(trim(A.src_trial_id)) = lower(trim(B.trial_id))
       inner join (select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(B.trial_status))
GROUP  BY 1,
          2,
          3,4,5) iq )ab where pred_stat=1
""")
d_trial_citeline_status = d_trial_citeline_status.dropDuplicates()
d_trial_citeline_status.write.mode("overwrite").saveAsTable("d_trial_citeline_status")

d_trial_tascan_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.ctfo_site_id,iq.ctfo_investigator_id,iq.status,iq.status_rnk,ROW_NUMBER() OVER
(PARTITION BY iq.ctfo_trial_id,iq.ctfo_site_id,iq.ctfo_investigator_id ORDER BY iq.status_rnk ) pred_stat from (SELECT A.ctfo_trial_id,
       A.ctfo_site_id,
       A.ctfo_investigator_id,
       data_src_nm,
       tsm.status,
       case when lower(trim(tsm.status))='ongoing' then 1
            when lower(trim(tsm.status))='completed' then 2
            when lower(trim(tsm.status))='planned' then 3
            when lower(trim(tsm.status))='others' then 4 else 5 end as status_rnk
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               ctfo_investigator_id,
               data_src_nm,
               src_trial_id,
               src_site_id,
               src_investigator_id
        FROM   goldeniddet_its_rpt
        WHERE  Lower(Trim(data_src_nm)) = 'tascan'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5,
                  6,
                  7) A
       INNER JOIN $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial B
               ON lower(trim(A.src_trial_id)) = lower(trim(B.trial_id))
       inner join (select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(B.trial_status))
GROUP  BY 1,
          2,
          3,4,5) iq )ab where pred_stat=1
""")
d_trial_tascan_status = d_trial_tascan_status.dropDuplicates()
d_trial_tascan_status.write.mode("overwrite").saveAsTable("d_trial_tascan_status")
d_trial_tascan_citeline_status = spark.sql("""SELECT * FROM d_trial_tascan_status union SELECT * FROM d_trial_citeline_status""")

final_status = spark.sql(""" select  A.ctfo_investigator_id,
    A.ctfo_site_id,
    A.ctfo_trial_id, 
    coalesce(ctms_gsm.status,dqs_gsm.status,B.status,aact_gsm.status,'Others') as trial_status 
    from f_rpt_investigator_details_temp_6 A
 left join d_trial_tascan_citeline_status B on lower(trim(A.ctfo_investigator_id)) = lower(trim(B.ctfo_investigator_id)) and lower(trim(A.ctfo_trial_id)) = lower(trim(B.ctfo_trial_id)) and lower(trim(A.ctfo_site_id)) = lower(trim(B.ctfo_site_id))
 left join aact_gsm_its_status aact_gsm on lower(trim(A.ctfo_investigator_id)) = lower(trim(aact_gsm.ctfo_investigator_id)) and lower(trim(A.ctfo_trial_id)) = lower(trim(aact_gsm.ctfo_trial_id)) and lower(trim(A.ctfo_site_id)) = lower(trim(aact_gsm.ctfo_site_id))
 left join ir_status_its_rpt dqs_gsm on lower(trim(A.ctfo_investigator_id)) = lower(trim(dqs_gsm.ctfo_investigator_id)) and lower(trim(A.ctfo_trial_id)) = lower(trim(dqs_gsm.ctfo_trial_id)) and lower(trim(A.ctfo_site_id)) = lower(trim(dqs_gsm.ctfo_site_id))
 left join ctms_its_status_rpt ctms_gsm on lower(trim(A.ctfo_investigator_id)) = lower(trim(ctms_gsm.ctfo_investigator_id)) and lower(trim(A.ctfo_trial_id)) = lower(trim(ctms_gsm.ctfo_trial_id)) and lower(trim(A.ctfo_site_id)) = lower(trim(ctms_gsm.ctfo_site_id))
 """)
final_status = final_status.dropDuplicates()
final_status.registerTempTable('final_status')

f_rpt_investigator_site_study_details_temp = spark.sql("""
select /* broadcast(inv_workload) */
    A.ctfo_investigator_id,
    A.ctfo_site_id,
    A.ctfo_trial_id,
    case when A.nct_id like '%|' then substr(A.nct_id,0,length(A.nct_id)-1) else A.nct_id end as nct_id,
    trial_phase,
    fs.trial_status as trial_status,
    disease_nm as disease,
    coalesce(NULLIF(sponsor_nm, ''), 'Other') as sponsor,
    coalesce(NULLIF(subsidiary_sponsors, ''), 'Other') as  subsidiary_sponsors,
    coalesce(NULLIF(sponsor_type, ''), 'Other') as  sponsor_type,
    A.region,
    A.region_code,
    A.country,
    A.country_code,
    site_nm,
    site_address,
    site_city,
    site_state,
    site_country,
    investigator_full_nm,
    investigator_specialty,
    investigator_email,
    investigator_phone,
    investigator_city,
    investigator_state,
    investigator_country,
    COALESCE(country_mapping.$$client_name_cluster , 'Other') as $$client_name_cluster,
    cast(trial_start_dt as date),
    cast(last_subject_in_dt as date),
    cast(ready_to_enroll_dt as date),
    cast(trial_end_dt as date),
    case when lower(trim(fs.trial_status)) in {on_com} then cast(screen_fail_rate as double) else null end as screen_fail_rate,
    case when lower(trim(fs.trial_status)) in {on_com} then cast(total_recruitment_months as double) else null end as total_recruitment_months,
    case when lower(trim(fs.trial_status)) in {on_com} then cast(A.randomization_rate as double) else null end as randomization_rate,
    CAST(null as string) as total_trials_all_ta,
    CAST(null as string) as current_workload,
    investigator_address,
    concat_ws('',NULLIF(trim(A.ctfo_trial_id),''), NULLIF(trim(A.ctfo_site_id),'')) as trial_site_id,
    concat_ws('',NULLIF(trim(A.ctfo_trial_id),''), NULLIF(trim(A.ctfo_site_id),''), 
    NULLIF(trim(A.ctfo_investigator_id),'')) as trial_site_inv_id,
    cast(last_subject_last_visit_dt as date),
    case when lower(trim(fs.trial_status)) in {on_com} then cast(A.patients_enrolled as bigint) else null end as patients_enrolled,
    case when lower(trim(fs.trial_status)) in {on_com} then cast(A.patients_screened as bigint) else null end as patients_screened
from f_rpt_investigator_details_temp_6 A
left join country_mapping on lower(trim(A.investigator_country)) = lower(trim(country_mapping.standard_country))
left join final_status fs on lower(trim(A.ctfo_investigator_id)) = lower(trim(fs.ctfo_investigator_id)) and lower(trim(A.ctfo_trial_id)) = lower(trim(fs.ctfo_trial_id)) and lower(trim(A.ctfo_site_id)) = lower(trim(fs.ctfo_site_id))
 """.format(on_com=Ongoing_Completed))
f_rpt_investigator_site_study_details_temp = f_rpt_investigator_site_study_details_temp.dropDuplicates()
f_rpt_investigator_site_study_details_temp.registerTempTable('f_rpt_investigator_site_study_details_temp')

randomization_rate_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1 and randomization_rate = 0) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as randomization_rate_norm 
    from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id,ctfo_site_id ) as max_r   from (select ctfo_trial_id,ctfo_investigator_id,ctfo_site_id,randomization_rate,rank(randomization_rate) over (partition By ctfo_trial_id,ctfo_site_id order by randomization_rate) as R_Rank from f_rpt_investigator_site_study_details_temp where randomization_rate IS NOT NULL) A) B """)

randomization_rate_df = randomization_rate_df.dropDuplicates()
randomization_rate_df.registerTempTable('randomization_rate_df')

screen_fail_rate_df = spark.sql(
    """ select B.* ,
    case when (R_Rank = 1 and max_r = 1) then 0
    else COALESCE(((R_Rank-1 )/(max_r-1)),1) end as screen_fail_rate_norm,
    case when (R_Rank = 1 and max_r = 1) then 1
    else (1 - COALESCE(((R_Rank-1 )/(max_r-1)),1)) end as neg_screen_fail_rate_norm
    from  (select A.*,max(R_Rank) over (partition By ctfo_trial_id,ctfo_site_id ) as max_r   from (select ctfo_trial_id,ctfo_investigator_id,ctfo_site_id,screen_fail_rate,rank(screen_fail_rate) over (partition By ctfo_trial_id,ctfo_site_id order by screen_fail_rate) as R_Rank from f_rpt_investigator_site_study_details_temp where screen_fail_rate IS NOT NULL) A) B """)

screen_fail_rate_df = screen_fail_rate_df.dropDuplicates()
screen_fail_rate_df.registerTempTable('screen_fail_rate_df')

f_rpt_investigator_site_study_details = spark.sql("""select distinct rpt.*,rnd.randomization_rate_norm,sfr.screen_fail_rate_norm,sfr.neg_screen_fail_rate_norm from f_rpt_investigator_site_study_details_temp rpt
 left join randomization_rate_df as rnd on lower(trim(rpt.ctfo_trial_id)) = lower(trim(rnd.ctfo_trial_id)) and lower(trim(rpt.ctfo_site_id))=lower(trim(rnd.ctfo_site_id)) and lower(trim(rpt.ctfo_investigator_id))=lower(trim(rnd.ctfo_investigator_id))
 left join screen_fail_rate_df sfr on lower(trim(rpt.ctfo_trial_id))=lower(trim(sfr.ctfo_trial_id)) and lower(trim(rpt.ctfo_site_id))=lower(trim(sfr.ctfo_site_id)) and lower(trim(rpt.ctfo_investigator_id))=lower(trim(sfr.ctfo_investigator_id))
 """)
f_rpt_investigator_site_study_details = f_rpt_investigator_site_study_details.dropDuplicates()
f_rpt_investigator_site_study_details.registerTempTable('f_rpt_investigator_site_study_details')

columns_for_replacement = ["screen_fail_rate", "randomization_rate", "total_recruitment_months"]
for i in columns_for_replacement:
    f_rpt_investigator_site_study_details = f_rpt_investigator_site_study_details.withColumn(i, when(
        (col("trial_status") == 'Others'), lit(None)).otherwise(col(i)))
f_rpt_investigator_site_study_details.write.mode("overwrite").saveAsTable("f_rpt_investigator_site_study_details")
# f_rpt_investigator_site_study_details.registerTempTable('f_rpt_investigator_site_study_details')
write_path = path.replace("table_name", "f_rpt_investigator_details")
write_path_csv = path_csv.replace("table_name", "f_rpt_investigator_site_study_details")
write_path_csv = write_path_csv + "_csv/"

f_rpt_investigator_site_study_details_final = spark.sql(
    """select distinct f_rpt_investigator_site_study_details.*, "$$cycle_id" as pt_cycle_id from f_rpt_investigator_site_study_details """)
f_rpt_investigator_site_study_details_final = f_rpt_investigator_site_study_details_final.dropDuplicates()
f_rpt_investigator_site_study_details_final.createOrReplaceTempView("f_rpt_investigator_site_study_details_final")
f_rpt_investigator_site_study_details_final.write.format("parquet").mode("overwrite") \
    .save(path=write_path)

if "$$flag" == "Y":
    f_rpt_investigator_site_study_details_final.coalesce(1).write.option("emptyValue", None).option("nullValue",
                                                                                                    None).option(
        "header", "false") \
        .option("sep", "|").option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$env.f_rpt_investigator_site_study_details
partition(pt_data_dt, pt_cycle_id)
	select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_investigator_site_study_details
""")

if f_rpt_investigator_site_study_details.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_investigator_site_study_details as "
          "zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$env"
                                  ".f_rpt_investigator_site_study_details")

try:
    print("Closing spark context")
except:
    print("Error while closing spark context")

