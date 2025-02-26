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
#  Last changed by     : ANIRUDH
#  Reason for change   : incorporated TASCAN
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
from pyspark.sql.functions import initcap
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

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

#updated custom schema for region code error

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

geo_mapping= spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.$$client_name_cluster_code as region_code,scpm.$$client_name_cluster,scpm.$$client_name_csu,scpm.post_trial_flag,
scpm.post_trial_details
from (select distinct *
	from standard_country_mapping )   cs
left join
        cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")

#disease_mapping = spark.read.format('csv').option("header", "true"). \
#    option("delimiter", ","). \
#    load("{bucket_path}/uploads/FINAL_DISEASE_MAPPING/"
#         "disease_mapping.csv".format(bucket_path=bucket_path))
disease_mapping_temp= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping""")
disease_mapping_temp.createOrReplaceTempView('disease_mapping_temp')

disease_mapping = spark.sql("""select distinct * from (select mesh, standard_mesh from disease_mapping_temp
union select standard_mesh, standard_mesh from disease_mapping_temp) """)
# disease_mapping_1.registerTempTable('disease_mapping_1')
disease_mapping.write.mode('overwrite').saveAsTable('disease_mapping')

#ta_mapping = spark.read.format('csv').option("header", "true"). \
#    option("delimiter", ","). \
#    load("{bucket_path}/uploads/TA_MAPPING/"
#         "ta_mapping.csv".format(bucket_path=bucket_path))
ta_mapping= spark.sql("""select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_to_ta_mapping""")
ta_mapping.createOrReplaceTempView('ta_mapping')

trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

trial_status_mapping = spark.sql(""" select distinct * from (select raw_status, status from trial_status_mapping_temp 
union select status, status from trial_status_mapping_temp )  """)
trial_status_mapping.registerTempTable('trial_status_mapping')

########## about trial, d_trial.nct_id not present

temp_inv_info = spark.sql("""
select
        r_trial_site.ctfo_trial_id as i_ctfo_trial_id, r_trial_site.ctfo_site_id as i_ctfo_site_id,
        ctfo_investigator_id as i_ctfo_investigator_id
from (select ctfo_trial_id, ctfo_site_id from $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site
group by 1,2) r_trial_site
left join
        (select r_site_investigator.ctfo_site_id,r_site_investigator.ctfo_investigator_id,
        r_trial_site.ctfo_trial_id from
        (select ctfo_site_id, ctfo_investigator_id  from
        $$client_name_ctfo_datastore_app_commons_$$db_env.r_site_investigator group by 1, 2)
         r_site_investigator
       inner join
        (select ctfo_trial_id, ctfo_investigator_id from
        $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_investigator group by 1,2)
        r_trial_investigator
        on lower(trim(r_site_investigator.ctfo_investigator_id)) =lower(trim(r_trial_investigator.ctfo_investigator_id))
        inner join
        (select ctfo_trial_id, ctfo_site_id from
        $$client_name_ctfo_datastore_app_commons_$$db_env.r_trial_site group by 1,2) r_trial_site
        on lower(trim(r_trial_site.ctfo_site_id)) =lower(trim(r_site_investigator.ctfo_site_id))   and
        lower(trim(r_trial_site.ctfo_trial_id)) =lower(trim(r_trial_investigator.ctfo_trial_id))
        group by 1,2,3
        ) site_inv_trial
        on
        lower(trim(r_trial_site.ctfo_site_id))=lower(trim(site_inv_trial.ctfo_site_id)) and
        lower(trim(r_trial_site.ctfo_trial_id)) = lower(trim(site_inv_trial.ctfo_trial_id))
        group by 1,2,3
""")
temp_inv_info.registerTempTable("temp_inv_info")
temp_inv_info = temp_inv_info.dropDuplicates()
temp_inv_info.write.mode("overwrite").saveAsTable("temp_inv_info")
write_path = path.replace("table_name", "temp_inv_info")

# Adding citeline for patient segement

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
citeline_exp_2.write.mode("overwrite").saveAsTable("citeline_exp_2")

#adding tascan for patient segment
tascan_trial_exp = spark.sql("""SELECT   trial_id,
         Trim(ta)    AS therapeutic_area,
         Trim(ps)   AS patient_segment,
         case when mesh is null or trim(mesh) = '' then disease
else mesh end as trial_mesh_term_name 

FROM     $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial 
        lateral view outer posexplode(split(trial_therapeutic_area_name,'\\\|'))one AS pos1,ta
        lateral view outer posexplode(split(mesh_term_names,'\\\;'))two AS pos2,mesh
        lateral view outer posexplode(split(disease_name,'\\\|'))three AS pos3,disease
        lateral VIEW outer posexplode(split(patient_segment,'\\\|'))four AS pos4,ps
WHERE     pos1=pos2 and pos2=pos3 and pos3=pos4 and pos4=pos1
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
tascan_trial_exp.registerTempTable('tascan_trial_exp')

tascan_trial_exp_1 = spark.sql("""SELECT   trial_id,
         therapeutic_area,
         Trim(ps)   AS patient_segment,
         Trim(mesh) as trial_mesh_term_name

FROM     tascan_trial_exp 
        lateral view outer posexplode(split(trial_mesh_term_name,'\\\^'))two AS pos1,mesh
        lateral VIEW outer posexplode(split(patient_segment,'\\\^'))three AS pos2,ps
WHERE     pos1=pos2
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
tascan_trial_exp_1.registerTempTable('tascan_trial_exp_1')

tascan_trial_exp_2 = spark.sql("""SELECT   'tascan' as datasource, trial_id, therapeutic_area,
         Trim(ps)   AS patient_segment,
         Trim(mesh) as trial_mesh_term_name 

FROM     tascan_trial_exp_1
        lateral view outer explode(split(trial_mesh_term_name,'\\\#'))one AS mesh
        lateral VIEW outer explode(split(patient_segment,'\\\#'))two AS ps
GROUP BY 1,2,3,4,5 ORDER BY therapeutic_area """)
tascan_trial_exp_2.write.mode("overwrite").saveAsTable("tascan_trial_exp_2")

#enhancement made during ctfoh1 integration
temp_disease_info = spark.sql("""
select
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
group by 1,2,3,4
""")



temp_disease_info = temp_disease_info.dropDuplicates()
#temp_disease_info.registerTempTable("temp_disease_info")
temp_disease_info.write.mode("overwrite").saveAsTable("temp_disease_info")
# write_path = path.replace("table_name", "temp_disease_info")

#enhancement tascan
temp_disease_info_tascan = spark.sql("""
select
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
tascan_trial_exp_2 tascan
on lower(trim(xref_trial.src_trial_id))=lower(trim(tascan.trial_id))
group by 1,2,3,4
""")

temp_disease_info_tascan = temp_disease_info_tascan.dropDuplicates()
#temp_disease_info_tascan.registerTempTable("temp_disease_info_tascan")
temp_disease_info_tascan.write.mode("overwrite").saveAsTable("temp_disease_info_tascan")
# write_path = path.replace("table_name", "temp_disease_info_tascan")


#used in rptsitestudydetails
temp_site_info = spark.sql("""
select  /*+ broadcast(d_site) */
    ctfo_site_id as s_ctfo_site_id, site_nm, site_address, site_city, site_state, site_country,
    coalesce(d_geo.country,'Other') as country, coalesce(cm.$$client_name_cluster,'Other') as $$client_name_cluster,coalesce(country_code, 'Other') as country_code,
    case when d_geo.region is null then 'Other' else d_geo.region end as region,
    coalesce(d_geo.region_code,'Other') as region_code,site_supporting_urls,site_zip_code
from
    (select ctfo_site_id, site_name as site_nm, site_address, site_city, site_state, site_country,
     geo_cd,site_supporting_urls,site_zip as site_zip_code
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_site
    group by 1,2,3,4,5,6,7,8,9) d_site
left join
    (select geo_cd, standard_country as country, iso2_code as country_code, region, region_code
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_geo group by 1,2,3,4,5) d_geo
on lower(trim(d_site.geo_cd)) = lower(trim(d_geo.geo_cd))
left join country_mapping cm on lower(trim(d_geo.country))=lower(trim(cm.standard_country))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
temp_site_info = temp_site_info.dropDuplicates()
#temp_site_info.createOrReplaceTempView("temp_site_info")
temp_site_info.write.mode("overwrite").saveAsTable("temp_site_info")
# write_path = path.replace("table_name", "temp_site_info")

union_nct_id = spark.sql("""
select
    'ir' as datasource, member_study_id as trial_no,
case when (nct_number is null or trim(nct_number)='') then null else nct_number end as nct_id ,member_study_id as
    protocol_ids
    from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
union
select
    'citeline' as datasource, trial_id as trial_no,
    case when (nctid is null or trim(nctid)='') then null else nctid end as nct_id,
    protocol_ids
    from $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove LATERAL VIEW outer
    explode(split(nct_id, '\\\|'))nct as nctid
union
    select 'aact' as datasource, nct_id as trial_no, 
    case when (nct_id is null or trim(nct_id)='') then null else nct_id end as nct_id,nct_id as protocol_ids
    from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies
union
    select 'ctms' as datasource, src_trial_id as trial_no,null as nct_id,src_trial_id as protocol_ids
    from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study
union
select
    'tascan' as datasource, trial_id as trial_no,
    case when (nctid is null or trim(nctid)='') then null else nctid end as nct_id,
    '' AS protocol_ids
    from $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial LATERAL VIEW outer
    explode(split(nct_id, '\\\|'))nct as nctid
""")
union_nct_id = union_nct_id.dropDuplicates()
#union_nct_id.registerTempTable("union_nct_id")
union_nct_id.write.mode("overwrite").saveAsTable("union_nct_id")

temp_nct_id_ctfo_trial_id_map_1 = spark.sql("""
select
    ctfo_trial_id as n_ctfo_trial_id,union_nct_id.datasource, union_nct_id.trial_no,nct_id,
     protocol_ids
from (select datasource, trial_no,nct_id,protocol_id as protocol_ids from union_nct_id lateral view
     outer explode  (split(protocol_ids, "\\\|"))p  as protocol_id) union_nct_id
left join
    (select ctfo_trial_id, src_trial_id, data_src_nm
       from $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial
    group by 1, 2, 3) xref_src_trial
on lower(trim(union_nct_id.trial_no)) = lower(trim(xref_src_trial.src_trial_id)) and
lower(trim(union_nct_id.datasource)) = lower(trim(xref_src_trial.data_src_nm))
where ctfo_trial_id is not null
""")
temp_nct_id_ctfo_trial_id_map_1 = temp_nct_id_ctfo_trial_id_map_1.dropDuplicates()
temp_nct_id_ctfo_trial_id_map_1.createOrReplaceTempView("temp_nct_id_ctfo_trial_id_map_1")

temp_nct_id_ctfo_trial_id_map_final = spark.sql("""
select
    A.n_ctfo_trial_id,
    case when rlike(nct_id_combine,'NCT|') then replace(nct_id_combine,'NCT|','')
    else nct_id_combine end as nct_id,
    protocol_ids_combine as protocol_ids
    from
    (select
    n_ctfo_trial_id, concat_ws(';', collect_set(datasource)) data_source_combine,
    concat_ws(';', collect_set(trial_no)) trial_no_combine, 
    concat_ws('|', sort_array(collect_set(CASE WHEN nct_id<>'' THEN nct_id end),true)) as nct_id_combine,
    concat_ws('|', collect_set(protocol_ids)) protocol_ids_combine
    from temp_nct_id_ctfo_trial_id_map_1 where protocol_ids is not null and trim(protocol_ids) <> '' group by 1)
    A
group by 1,2,3
""")
temp_nct_id_ctfo_trial_id_map_final = temp_nct_id_ctfo_trial_id_map_final.dropDuplicates()
#temp_nct_id_ctfo_trial_id_map_final.createOrReplaceTempView("temp_nct_id_ctfo_trial_id_map")

# having nct_id != 'null' -- excluded from above query, part to get only dqs id also excluded
temp_nct_id_ctfo_trial_id_map_final.write.mode("overwrite"). \
    saveAsTable("temp_nct_id_ctfo_trial_id_map")

# temp_nct_id_ctfo_trial_id_map_final.registerTempTable("temp_nct_id_ctfo_trial_id_map")
write_path = path.replace("table_name", "temp_nct_id_ctfo_trial_id_map")
write_path_csv = path_csv.replace("table_name", "temp_nct_id_ctfo_trial_id_map")
write_path_csv = write_path_csv + "_csv/"
# temp_nct_id_ctfo_trial_id_map_final.write.format("parquet").mode("overwrite")
# .save(path=write_path)


exp_nct = spark.sql("""
select n_ctfo_trial_id,nct_id
    from temp_nct_id_ctfo_trial_id_map
    where nct_id is not null and trim(nct_id) <>''
    group by 1,2
""")
exp_nct = exp_nct.dropDuplicates()
#exp_nct.registerTempTable('exp_nct')
exp_nct.write.mode("overwrite").saveAsTable("exp_nct")

######################## creating filter tables #############################

################### Set Up Filters
temp_disease_info_combined = spark.sql("""SELECT * from temp_disease_info UNION SELECT * FROM temp_disease_info_tascan""")
temp_disease_info_combined.registerTempTable('temp_disease_info_combined')
f_rpt_filters_setup_temp = spark.sql("""
select
    temp_inv_info.ctfo_trial_id, therapeutic_area, disease_nm as disease, patient_segment,
     trial_phase, exp_nct.nct_id,
    d_trial.protocol_id as protocol_ids, study_name
from
    (select i_ctfo_trial_id as ctfo_trial_id from temp_inv_info group by 1) temp_inv_info
left join temp_disease_info_combined on lower(trim(temp_inv_info.ctfo_trial_id))=lower(trim(temp_disease_info_combined.d_ctfo_trial_id))
left join
    (select ctfo_trial_id, trial_phase, trial_title as study_name,protocol_id
    from $$client_name_ctfo_datastore_app_commons_$$db_env.d_trial group by 1,2,3,4) d_trial
on lower(trim(d_trial.ctfo_trial_id)) = lower(trim(temp_inv_info.ctfo_trial_id))
left join exp_nct on lower(trim(exp_nct.n_ctfo_trial_id)) = lower(trim(temp_inv_info.ctfo_trial_id))
    group by 1,2,3,4,5,6,7,8
""")
f_rpt_filters_setup_temp = f_rpt_filters_setup_temp.dropDuplicates()
f_rpt_filters_setup_temp.registerTempTable("f_rpt_filters_setup_temp")

##enhancement made during ctfoh1 integration
f_rpt_filters_setup_patient_segment = spark.sql("""
select ctfo_trial_id, therapeutic_area, disease,
(case when patient_segment_exploded = 'Other' then 'Other'
when lower(trim(patient_segment_exploded)) in ('','(n/a)','line of therapy n/a','n/a','(n/a)') then 'Other' 
when lower(trim(patient_segment_exploded)) is null then 'Other' else patient_segment_exploded end) as patient_segment,
trial_phase, nct_id,
protocol_ids, study_name
from f_rpt_filters_setup_temp
lateral view outer explode(split(patient_segment,"\#")) one as patient_segment_exploded
""")
f_rpt_filters_setup_patient_segment = f_rpt_filters_setup_patient_segment.dropDuplicates()
#f_rpt_filters_setup_patient_segment.registerTempTable("f_rpt_filters_setup_patient_segment")
f_rpt_filters_setup_patient_segment.write.mode('overwrite').saveAsTable('f_rpt_filters_setup_patient_segment')

#Enhancement made during ctfoh1 integration
f_rpt_filters_setup_all = spark.sql("""
select ctfo_trial_id,
INITCAP(therapeutic_area) as therapeutic_area,
INITCAP(disease) as disease,
patient_segment,
trial_phase,nct_id,
replace(protocol_ids,",","|") as protocol_ids,
study_name
from
(select
    ctfo_trial_id, coalesce(therapeutic_area, 'Other') as therapeutic_area,
    case when disease in ("(n/a)") THEN 'Other' ELSE coalesce(disease, 'Other') END AS 
    disease,
    case when lower(trim(patient_segment)) in ( "(n/a)" , "n/a")  THEN 'Other' ELSE coalesce(patient_segment, 'Other') END AS patient_segment,
    coalesce(trial_phase, 'Other') as trial_phase,
    coalesce(nct_exploded, 'Other') as nct_id, coalesce(protocol_ids, 'Other') as protocol_ids,
    coalesce(study_name, 'Other') as study_name
from f_rpt_filters_setup_patient_segment
lateral view outer explode(split(nct_id,"\\\|")) nct as nct_exploded
order by ctfo_trial_id)
""")
f_rpt_filters_setup_all = f_rpt_filters_setup_all.dropDuplicates()
#f_rpt_filters_setup_all.registerTempTable("f_rpt_filters_setup_all")
f_rpt_filters_setup_all.write.mode('overwrite').saveAsTable('f_rpt_filters_setup_all')

write_path = path.replace("table_name", "f_rpt_filters_setup_all")
write_path_csv = path_csv.replace("table_name", "f_rpt_filters_setup_all")
write_path_csv = write_path_csv + "_csv/"
f_rpt_filters_setup_all.write.format("parquet").mode("overwrite").save(path=write_path)

if "$$flag" == "Y":
    f_rpt_filters_setup_all.coalesce(1).write.option("emptyValue", None).option("nullValue", None).option("header",
                                                                                                          "false").option(
        "sep", "|") \
        .option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_setup_all partition(pt_data_dt,pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_filters_setup_all
""")

if f_rpt_filters_setup_all.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_filters_setup_all as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_setup_all")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")