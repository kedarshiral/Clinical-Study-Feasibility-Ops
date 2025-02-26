


################################# Module Information ######################################
#  Module Name         : Site reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_site_study_details_filters
#                           b. f_rpt_site_study_details
#                           c. f_rpt_src_site_details
#                           d. f_rpt_investigator_details
#                           e. f_rpt_site_study_details_non_performance
#  Pre-requisites      : Source table required:
#  Last changed on     : 29-06-2021
#  Last changed by     : Sankarshana Kadambari
#  Reason for change   : NA
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

# data_dt = datetime.datetime.now().strftime('%Y%m%d')
# cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

path = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/" \
       "commons/temp/" \
       "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/" \
           "commons/temp/" \
           "kpi_output_dimension/table_name"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")
########## about trial, d_trial.nct_id not present


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

disease_mapping = spark.read.format('csv').option("header", "true"). \
    option("delimiter", ","). \
    load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/"
         "disease_mapping.csv")
disease_mapping.createOrReplaceTempView('disease_mapping')

ta_mapping = spark.read.format('csv').option("header", "true"). \
    option("delimiter", ","). \
    load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/TA_MAPPING/"
         "ta_mapping.csv")
ta_mapping.createOrReplaceTempView('ta_mapping')

trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/'
          'uploads/trial_status.csv')
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')

temp_drug_info = spark.sql("""
select /*+ broadcast(d_drug) */
    ctfo_trial_id as d_ctfo_trial_id, drug_nm,global_status,cas_num
from
    (select ctfo_trial_id, drug_id
    from sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_drug  group by 1,2) r_trial_drug
left join
    (select drug_id, drug_nm,global_status,cas_num
    from sanofi_ctfo_datastore_app_commons_$$db_env.d_drug group by 1,2,3,4) d_drug
on r_trial_drug.drug_id=d_drug.drug_id group by 1,2,3,4
""")
# temp_drug_info.createOrReplaceTempView("temp_drug_info")
temp_drug_info.write.mode("overwrite").saveAsTable("temp_drug_info")
# write_path = path.replace("table_name", "temp_drug_info")

temp_sponsor_info = spark.sql("""
select
        ctfo_trial_id as s_ctfo_trial_id,
        d_sponsor.sponsor_nm,
        d_sponsor.sponsor_type
from (select ctfo_trial_id, sponsor_id from sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_sponsor
group by 1,2) r_trial_sponsor
left join
        (select sponsor_id, sponsor_name as sponsor_nm,sponsor_type   from
        sanofi_ctfo_datastore_app_commons_$$db_env.d_sponsor group by 1, 2,3) d_sponsor
        on d_sponsor.sponsor_id=r_trial_sponsor.sponsor_id group by 1,2,3
""")
# temp_sponsor_info.registerTempTable("temp_sponsor_info")
temp_sponsor_info.write.mode("overwrite").saveAsTable("temp_sponsor_info")
# write_path = path.replace("table_name", "temp_sponsor_info")

temp_inv_info = spark.sql("""
select
        r_trial_site.ctfo_trial_id as i_ctfo_trial_id, r_trial_site.ctfo_site_id as i_ctfo_site_id,
        ctfo_investigator_id as i_ctfo_investigator_id
from (select ctfo_trial_id, ctfo_site_id from sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site
group by 1,2) r_trial_site
left join
        (select r_site_investigator.ctfo_site_id,r_site_investigator.ctfo_investigator_id,
        r_trial_site.ctfo_trial_id from
        (select ctfo_site_id, ctfo_investigator_id  from
        sanofi_ctfo_datastore_app_commons_$$db_env.r_site_investigator group by 1, 2)
         r_site_investigator
       inner join
        (select ctfo_trial_id, ctfo_investigator_id from
        sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_investigator group by 1,2)
        r_trial_investigator
        on r_site_investigator.ctfo_investigator_id =r_trial_investigator.ctfo_investigator_id
        inner join
        (select ctfo_trial_id, ctfo_site_id from
        sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site group by 1,2) r_trial_site
        on r_trial_site.ctfo_site_id =r_site_investigator.ctfo_site_id   and
        r_trial_site.ctfo_trial_id =r_trial_investigator.ctfo_trial_id
        group by 1,2,3
        ) site_inv_trial
        on
        r_trial_site.ctfo_site_id=site_inv_trial.ctfo_site_id and
        r_trial_site.ctfo_trial_id = site_inv_trial.ctfo_trial_id
        group by 1,2,3
""")
# temp_inv_info.registerTempTable("temp_inv_info")
temp_inv_info = temp_inv_info.dropDuplicates()
temp_inv_info.write.mode("overwrite").saveAsTable("temp_inv_info")
write_path = path.replace("table_name", "temp_inv_info")

# Adding citeline for patient segement

citeline_exp_1 = spark.sql("""SELECT   trial_id,
         Trim(ta)    AS therapeutic_area,
         Trim(dis)    AS disease_name,
         Trim(ps)   AS patient_segment
FROM     sanofi_ctfo_datastore_staging_$$db_env.citeline_trialtrove lateral view
posexplode(split(trial_therapeutic_areas_name,'\\\|'))one AS pos1,
         ta lateral VIEW posexplode (split(disease_name,'\\\|'))two    AS pos2,
         dis lateral VIEW posexplode (split(patient_segment,'\\\|'))three    AS pos3,
         ps
WHERE    pos1=pos2
AND      pos1=pos3
AND      pos2=pos3
GROUP BY 1,
         2,
         3,
         4
ORDER BY therapeutic_area
""")
citeline_exp_1.registerTempTable('citeline_exp_1')

# applying explode on disease for citeline
citeline_exp_2 = spark.sql("""SELECT   trial_id,
         therapeutic_area,
         Trim(dis)    AS disease_name,
         Trim(ps)      AS patient_segment
FROM     citeline_exp_1 lateral view posexplode (split(disease_name,'\\\^'))two    AS pos2,
         dis lateral VIEW posexplode (split(patient_segment,'\\\^'))three AS pos3,
         ps
WHERE    pos2=pos3
GROUP BY 1,
         2,
         3,
         4
""")
citeline_exp_2.write.mode('overwrite').saveAsTable('citeline_exp_2')

temp_disease_info = spark.sql("""
select
    r_trial_disease.ctfo_trial_id as d_ctfo_trial_id,
    (case when trim(d_disease.therapeutic_area) = 'other' then 'Other' else
    d_disease.therapeutic_area end) as therapeutic_area ,
    (case when trim(disease_nm) = 'other' then 'Other' else disease_nm end) as disease_nm,
        (case when trim(citeline.patient_segment) = 'other' then 'Other' else citeline.patient_segment end)
        as patient_segment
from
    (select distinct ctfo_trial_id, disease_id from sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_disease
    group by 1,2) r_trial_disease
left join
    (select distinct disease_id, therapeutic_area, disease_nm
    from sanofi_ctfo_datastore_app_commons_$$db_env.d_disease) d_disease
on r_trial_disease.disease_id=d_disease.disease_id
left join
sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial xref_trial
on r_trial_disease.ctfo_trial_id=xref_trial.ctfo_trial_id
and xref_trial.data_src_nm = 'citeline'
left join
citeline_exp_2 citeline
on xref_trial.src_trial_id=citeline.trial_id
group by 1,2,3,4
""")

temp_disease_info = temp_disease_info.dropDuplicates()
# temp_disease_info.registerTempTable("temp_disease_info")
temp_disease_info.write.mode("overwrite").saveAsTable("temp_disease_info")
# write_path = path.replace("table_name", "temp_disease_info")

temp_site_info = spark.sql("""
select  /*+ broadcast(d_site) */
    ctfo_site_id as s_ctfo_site_id, site_nm, site_address, site_city, site_state, site_country,
    coalesce(d_geo.country,'Others') as country, cm.sanofi_cluster as sanofi_cluster,coalesce(country_code, 'NA') as country_code,
    case when d_geo.region is null then 'Others' else d_geo.region end as region,
    coalesce(d_geo.region_code,'NA') as region_code,site_supporting_urls
from
    (select ctfo_site_id, site_name as site_nm, site_address, site_city, site_state, site_country,
     geo_cd,site_supporting_urls
    from sanofi_ctfo_datastore_app_commons_$$db_env.d_site
    group by 1,2,3,4,5,6,7,8) d_site
left join
    (select geo_cd, standard_country as country, iso2_code as country_code, region, region_code
    from sanofi_ctfo_datastore_app_commons_$$db_env.d_geo group by 1,2,3,4,5) d_geo
on d_site.geo_cd = d_geo.geo_cd
left join country_mapping cm on lower(trim(d_geo.country))=lower(trim(cm.standard_country))
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
temp_site_info = temp_site_info.dropDuplicates()
# temp_site_info.createOrReplaceTempView("temp_site_info")
temp_site_info.write.mode("overwrite").saveAsTable("temp_site_info")
# write_path = path.replace("table_name", "temp_site_info")

union_nct_id = spark.sql("""
select
    'ir' as datasource, member_study_id as trial_no, nct_number as nct_id ,nct_number as
    protocol_ids
    from sanofi_ctfo_datastore_staging_$$db_env.dqs_study
union
select
    'citeline' as datasource, trial_id as trial_no, nctid as nct_id,
    protocol_ids
    from sanofi_ctfo_datastore_staging_$$db_env.citeline_trialtrove LATERAL VIEW outer
    explode(split(nct_id, "\\\|"))nct as nctid
union
    select 'aact' as datasource, nct_id as trial_no, nct_id,nct_id as protocol_ids
    from sanofi_ctfo_datastore_staging_$$db_env.aact_studies
union
    select 'ctms' as datasource, study_code as trial_no,'' as nct_id,study_code as protocol_ids
    from sanofi_ctfo_datastore_staging_$$db_env.ctms_study
""")
union_nct_id = union_nct_id.dropDuplicates()
# union_nct_id.registerTempTable("union_nct_id")
union_nct_id.write.mode("overwrite").saveAsTable("union_nct_id")

temp_nct_id_ctfo_trial_id_map_1 = spark.sql("""
select
    ctfo_trial_id as n_ctfo_trial_id,union_nct_id.datasource, union_nct_id.trial_no,nct_id,
     protocol_ids
from (select datasource, trial_no,nct_id,protocol_id as protocol_ids from union_nct_id lateral view
     outer explode  (split(protocol_ids, "\\\|"))p  as protocol_id) union_nct_id
left join
    (select ctfo_trial_id, src_trial_id, data_src_nm
       from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial
    group by 1, 2, 3) xref_src_trial
on lower(trim(union_nct_id.trial_no)) = lower(trim(xref_src_trial.src_trial_id)) and
union_nct_id.datasource = xref_src_trial.data_src_nm
where ctfo_trial_id is not null
""")
temp_nct_id_ctfo_trial_id_map_1 = temp_nct_id_ctfo_trial_id_map_1.dropDuplicates()
temp_nct_id_ctfo_trial_id_map_1.createOrReplaceTempView("temp_nct_id_ctfo_trial_id_map_1")

temp_nct_id_ctfo_trial_id_map_final = spark.sql("""
select
    A.n_ctfo_trial_id,
    nct_id_combine as nct_id,
    protocol_ids_combine as protocol_ids
    from
    (select
    n_ctfo_trial_id, concat_ws(';', collect_set(datasource)) data_source_combine,
    concat_ws(';', collect_set(trial_no)) trial_no_combine, concat_ws('|', collect_set(nct_id))
     nct_id_combine,
    concat_ws('|', collect_set(protocol_ids)) protocol_ids_combine
    from temp_nct_id_ctfo_trial_id_map_1 where protocol_ids is not null and trim(protocol_ids) <> '' group by 1)
     A
group by 1,2,3
""")
temp_nct_id_ctfo_trial_id_map_final = temp_nct_id_ctfo_trial_id_map_final.dropDuplicates()
temp_nct_id_ctfo_trial_id_map_final.createOrReplaceTempView("temp_nct_id_ctfo_trial_id_map")

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
select n_ctfo_trial_id,protocol_ids, nct_id
    from temp_nct_id_ctfo_trial_id_map
    where protocol_ids is not null and nct_id is not null and trim(protocol_ids) <> ''
    and trim(nct_id) <>''
    group by 1,2,3
""")
exp_nct = exp_nct.dropDuplicates()
# exp_nct.registerTempTable('exp_nct')
exp_nct.write.mode("overwrite").saveAsTable("exp_nct")

######################## creating filter tables #############################

################### Set Up Filters
f_rpt_filters_setup_temp = spark.sql("""
select
    temp_inv_info.ctfo_trial_id, therapeutic_area, disease_nm as disease, patient_segment,
     trial_phase, nct_id,
    protocol_ids, study_name
from
    (select i_ctfo_trial_id as ctfo_trial_id from temp_inv_info group by 1) temp_inv_info
left join temp_disease_info on temp_inv_info.ctfo_trial_id=temp_disease_info.d_ctfo_trial_id
left join
    (select ctfo_trial_id, trial_phase, trial_title as study_name
    from sanofi_ctfo_datastore_app_commons_$$db_env.d_trial group by 1,2,3) d_trial
on d_trial.ctfo_trial_id = temp_inv_info.ctfo_trial_id
left join exp_nct on exp_nct.n_ctfo_trial_id = temp_inv_info.ctfo_trial_id
    group by 1,2,3,4,5,6,7,8
""")
f_rpt_filters_setup_temp = f_rpt_filters_setup_temp.dropDuplicates()
f_rpt_filters_setup_temp.registerTempTable("f_rpt_filters_setup_temp")

f_rpt_filters_setup_patient_segment = spark.sql("""
select ctfo_trial_id, therapeutic_area, disease,
(case when patient_segment_exploded = 'other' then 'Other'
  when patient_segment_exploded ='(n/a)' then 'n/a' else patient_segment_exploded end) as patient_segment,
trial_phase, nct_id,
protocol_ids, study_name
from f_rpt_filters_setup_temp
lateral view outer explode(split(patient_segment,"\#")) one as patient_segment_exploded
""")
f_rpt_filters_setup_patient_segment = f_rpt_filters_setup_patient_segment.dropDuplicates()
# f_rpt_filters_setup_patient_segment.registerTempTable("f_rpt_filters_setup_patient_segment")
f_rpt_filters_setup_patient_segment.write.mode('overwrite').saveAsTable('f_rpt_filters_setup_patient_segment')

f_rpt_filters_setup_all = spark.sql("""
select
    ctfo_trial_id, coalesce(therapeutic_area, 'Other') therapeutic_area, coalesce(disease, 'Other')
     disease,
    coalesce(patient_segment, 'Other') patient_segment, coalesce(trial_phase, 'Other') trial_phase,
    coalesce(nct_exploded, 'Other') as nct_id, coalesce(protocol_ids, 'Other') protocol_ids,
    coalesce(study_name, 'Other') study_name
from f_rpt_filters_setup_patient_segment
lateral view outer explode(split(nct_id,"\\\|")) nct as nct_exploded
order by ctfo_trial_id
""")
f_rpt_filters_setup_all = f_rpt_filters_setup_all.dropDuplicates()
# f_rpt_filters_setup_all.registerTempTable("f_rpt_filters_setup_all")
f_rpt_filters_setup_all.write.mode('overwrite').saveAsTable('f_rpt_filters_setup_all')

write_path = path.replace("table_name", "f_rpt_filters_setup_all")
write_path_csv = path_csv.replace("table_name", "f_rpt_filters_setup_all")
write_path_csv = write_path_csv + "_csv/"
f_rpt_filters_setup_all.write.format("parquet").mode("overwrite").save(path=write_path)

if "$$flag" == "Y":
    f_rpt_filters_setup_all.coalesce(1).write.option("header", "false").option("sep", "|") \
        .option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_setup_all partition(pt_data_dt,pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_filters_setup_all
""")

if f_rpt_filters_setup_all.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_filters_setup_all as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_setup_all")

############# Trial related Filters
f_rpt_filters_trial = spark.sql("""
select
    temp_inv_info.ctfo_trial_id, coalesce(trial_status, 'Other') trial_status,
     coalesce(mesh_exploded, 'Other') mesh_term,
    coalesce(gender, 'Other') gender, coalesce(minimum_age, '0') minimum_age,
     coalesce(maximum_age, '100') maximum_age,
    coalesce(sponsor_nm, 'Other') as sponsor,
     coalesce(sponsor_type,'Other') as sponsor_type,
     coalesce(drug_nm, 'Other') as  drug_names,
    coalesce(global_status, 'Other') as drug_status, coalesce(cas_num, 'Other') cas_num
from
    (select i_ctfo_trial_id as ctfo_trial_id from temp_inv_info group by 1) temp_inv_info
inner join
        (select ctfo_trial_id, trial_status, mesh_terms, patient_gender as gender,
         trial_age_min as minimum_age,
        trial_age_max as maximum_age  from sanofi_ctfo_datastore_app_commons_$$db_env.d_trial
        group by 1,2,3,4,5,6) d_trial
on d_trial.ctfo_trial_id = temp_inv_info.ctfo_trial_id
left join temp_sponsor_info on temp_inv_info.ctfo_trial_id=temp_sponsor_info.s_ctfo_trial_id
left join temp_drug_info on temp_inv_info.ctfo_trial_id=temp_drug_info.d_ctfo_trial_id
lateral view outer explode(split(mesh_terms,"\\\|")) mesh as mesh_exploded
order by temp_inv_info.ctfo_trial_id
""")
f_rpt_filters_trial = f_rpt_filters_trial.dropDuplicates()
# f_rpt_filters_trial.registerTempTable("f_rpt_filters_trial")
f_rpt_filters_trial.write.mode('overwrite').saveAsTable('f_rpt_filters_trial')

write_path = path.replace("table_name", "f_rpt_filters_trial")
write_path_csv = path_csv.replace("table_name", "f_rpt_filters_trial")
write_path_csv = write_path_csv + "_csv/"
f_rpt_filters_trial.write.format("parquet").mode("overwrite").save(path=write_path)

if "$$flag" == "Y":
    f_rpt_filters_trial.coalesce(1).write.option("header", "false").option("sep", "|") \
        .option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_trial partition(pt_data_dt,pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_filters_trial
""")

if f_rpt_filters_trial.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_filters_trial as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_trial")

############# Trial-Site-Investigator Filters
f_rpt_filters_site_inv = spark.sql("""
select
    r_trial_site.ctfo_trial_id, r_trial_site.ctfo_site_id, ctfo_investigator_id, region, country,
    temp_site_info.sanofi_cluster, country_code,
    concat(r_trial_site.ctfo_trial_id, r_trial_site.ctfo_site_id) as trial_site_id,
    concat(r_trial_site.ctfo_trial_id, r_trial_site.ctfo_site_id, ctfo_investigator_id)
    as trial_site_inv_id
from
    (select i_ctfo_trial_id ctfo_trial_id, i_ctfo_site_id ctfo_site_id,
     i_ctfo_investigator_id ctfo_investigator_id
     from temp_inv_info) r_trial_site
left join
    (select distinct s_ctfo_site_id,sanofi_cluster, region, country,country_code from temp_site_info)
    temp_site_info
on r_trial_site.ctfo_site_id=temp_site_info.s_ctfo_site_id
group by 1,2,3,4,5,6,7,8,9
order by trial_site_id, trial_site_inv_id
""")
f_rpt_filters_site_inv = f_rpt_filters_site_inv.dropDuplicates()
# f_rpt_filters_site_inv.registerTempTable("f_rpt_filters_site_inv")
f_rpt_filters_site_inv.write.mode('overwrite').saveAsTable("f_rpt_filters_site_inv")

write_path = path.replace("table_name", "f_rpt_filters_site_inv")
write_path_csv = path_csv.replace("table_name", "f_rpt_filters_site_inv")
write_path_csv = write_path_csv + "_csv/"
f_rpt_filters_site_inv.write.format("parquet").mode("overwrite").save(path=write_path)

if "$$flag" == "Y":
    f_rpt_filters_site_inv.coalesce(1).write.option("header", "false").option("sep", "|") \
        .option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_site_inv partition(pt_data_dt,pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_filters_site_inv
""")

if f_rpt_filters_site_inv.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_filters_site_inv as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_filters_site_inv")

####################################### Site Reporting Table #####################################
######### calculate per site investigator count
site_info_inv_count = spark.sql("""
select
    temp_site_info.*, inv_count
from temp_site_info
left join
    (select ctfo_site_id, count(distinct ctfo_investigator_id) as inv_count
    from f_rpt_filters_site_inv group by 1) ctfo_site_inv_count
on temp_site_info.s_ctfo_site_id=ctfo_site_inv_count.ctfo_site_id

""")
site_info_inv_count = site_info_inv_count.dropDuplicates()
# site_info_inv_count.createOrReplaceTempView("site_info_inv_count")
site_info_inv_count.write.mode("overwrite").saveAsTable("site_info_inv_count")

################## creating table with all dimensions information ################

site_type_tab = spark.sql("""
select ctfo_site_id,site_type from
(select ctfo_site_id,concat_ws(';', collect_set(site_type)) site_type from (select ctfo_site_id,
src_site_id from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site where
data_src_nm = 'citeline' group by 1,2) A
left join ( select site_id,site_type from sanofi_ctfo_datastore_staging_$$db_env.citeline_organization)
B
on A.src_site_id = B.site_id
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
from sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site
""")
mock_psm_scrn_fail.createOrReplaceTempView("mock_psm_scrn_fail")

aact_golden_source_map_site_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.ctfo_site_id,iq.status,iq.status_rnk,ROW_NUMBER() OVER
(PARTITION BY iq.ctfo_trial_id,iq.ctfo_site_id ORDER BY iq.status_rnk ) pred_stat from (SELECT ctfo_trial_id,
       ctfo_site_id,
       data_src_nm,
       tsm.status,
       case when tsm.status='Ongoing' then 1
            when tsm.status='Completed' then 2
            when tsm.status='Planned' then 3
            when tsm.status='Others' then 4 end as status_rnk,
       max(nct_id) as aact_nct_id,
       max(id) as aact_site_id
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
                   FROM   sanofi_ctfo_datastore_staging_$$db_env.aact_facilities
                   GROUP  BY 1,
                             2,3
                             ) B
               ON A.src_trial_id = B.nct_id
                  AND A.src_site_id = B.id
       inner join (select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(B.status))
GROUP  BY 1,
          2,
          3,4,5) iq )ab where pred_stat=1
""")
aact_golden_source_map_site_status = aact_golden_source_map_site_status.dropDuplicates()
aact_golden_source_map_site_status.createOrReplaceTempView("aact_golden_source_map_site_status")

citeline_golden_source_map_site_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.ctfo_site_id,iq.status,iq.status_rnk,ROW_NUMBER() OVER
(PARTITION BY iq.ctfo_trial_id,iq.ctfo_site_id ORDER BY iq.status_rnk ) pred_stat from (SELECT B.ctfo_trial_id,
       A.ctfo_site_id,
       data_src_nm,
       tsm.status,
       case when tsm.status='Ongoing' then 1
            when tsm.status='Completed' then 2
            when tsm.status='Planned' then 3
            when tsm.status='Others' then 4 end as status_rnk
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
       INNER JOIN sanofi_ctfo_datastore_app_commons_$$db_env.d_trial B
               ON A.ctfo_trial_id = B.ctfo_trial_id
       inner join (select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(B.trial_status))
GROUP  BY 1,
          2,
          3,4) iq )ab where pred_stat=1
""")
citeline_golden_source_map_site_status = citeline_golden_source_map_site_status.dropDuplicates()
citeline_golden_source_map_site_status.createOrReplaceTempView("citeline_golden_source_map_site_status")


f_rpt_site_study_details_temp = spark.sql("""
select /*broadcast (site_info_inv_count, site_type_tab, f_trial_site)*/
    r_trial_site.ctfo_trial_id, r_trial_site.ctfo_site_id, nct_id,temp_site_info.region,
    temp_site_info.region_code,temp_site_info.site_country,
    temp_site_info.country_code, inv_count as investigators,COALESCE(f_trial_site.study_status,agsm.status,cgsmss.status,'Others') as trial_status , trial_start_dt,
    trial_end_dt,
    concat_ws('|',collect_set(temp_sponsor_info.sponsor_nm))  as sponsor,
    concat_ws('|',collect_set(temp_sponsor_info.sponsor_type))  as sponsor_type,
    site_type,site_nm, total_site_count,
        coalesce(f_trial_site.enrollment_rate, mock_psm_scrn_fail.mock_enrol_rate) as enrollment_rate,
        first_subject_in_dt, last_subject_in_dt,
    ready_to_enroll_dt,
    coalesce(f_trial_site.lost_opportunity_time, mock_psm_scrn_fail.mock_lost_opportunity_time) as lost_opportunity_time,
    f_trial_site.patients_enrolled as patients_enrolled,
    f_trial_site.total_recruitment_months,
     '' as patients_randomized, randomization_rate,
        coalesce(f_trial_site.screen_fail_rate, mock_psm_scrn_fail.mock_scrn_fail_rate) as screen_fail_rate,
        f_trial_site.average_startup_time as average_startup_time,
    temp_nct_id_ctfo_trial_id_map.protocol_ids, trial_title as study_name,
     completion_dt,temp_site_info.sanofi_cluster,
    temp_site_info.site_address, temp_site_info.site_city, temp_site_info.site_state,
    temp_site_info.site_supporting_urls,primary_endpoints_reported_date
from
        (select ctfo_trial_id, ctfo_site_id from
       sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site) r_trial_site
inner join
    (select ctfo_trial_id, trial_end_dt,trial_phase, trial_start_dt, trial_title,
enrollment_close_date as completion_dt,primary_endpoints_reported_date
from sanofi_ctfo_datastore_app_commons_$$db_env.d_trial dt group by 1,2,3,4,5,6,7) d_trial
    on r_trial_site.ctfo_trial_id=d_trial.ctfo_trial_id
left join (select n_ctfo_trial_id, concat_ws(',',collect_set(nct_id)) as nct_id,concat_ws(',',collect_set(protocol_ids))
as protocol_ids from temp_nct_id_ctfo_trial_id_map
group by 1) temp_nct_id_ctfo_trial_id_map
        on temp_nct_id_ctfo_trial_id_map.n_ctfo_trial_id=r_trial_site.ctfo_trial_id
left join
    (select ctfo_trial_id, ctfo_site_id , enrollment_rate,first_subject_in_dt,last_subject_in_dt,
    ready_to_enroll_dt,
        lost_opportunity_time,randomization_rate,screen_fail_rate,average_startup_time,
        patients_enrolled, total_recruitment_months,study_status
        from sanofi_ctfo_datastore_app_commons_$$db_env.f_trial_site
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13) f_trial_site
on r_trial_site.ctfo_trial_id = f_trial_site.ctfo_trial_id
and r_trial_site.ctfo_site_id = f_trial_site.ctfo_site_id
left join(select s_ctfo_site_id, region, site_country, country, inv_count
    from site_info_inv_count group bY 1,2,3,4,5) site_info_inv_count
on r_trial_site.ctfo_site_id=site_info_inv_count.s_ctfo_site_id
left join temp_site_info on r_trial_site.ctfo_site_id=temp_site_info.s_ctfo_site_id
left join temp_sponsor_info on r_trial_site.ctfo_trial_id=temp_sponsor_info.s_ctfo_trial_id
left join site_type_tab on
r_trial_site.ctfo_site_id=site_type_tab.ctfo_site_id
left join
    (select ctfo_trial_id as c_ctfo_trial_id, count(ctfo_site_id) as total_site_count
    from sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site
    group by 1) site_count
on r_trial_site.ctfo_trial_id=site_count.c_ctfo_trial_id
left join mock_psm_scrn_fail
on mock_psm_scrn_fail.ctfo_trial_id = r_trial_site.ctfo_trial_id
and mock_psm_scrn_fail.ctfo_site_id = r_trial_site.ctfo_site_id
left join aact_golden_source_map_site_status agsm on r_trial_site.ctfo_trial_id = agsm.ctfo_trial_id and r_trial_site.ctfo_site_id = agsm.ctfo_site_id
left join citeline_golden_source_map_site_status cgsmss on r_trial_site.ctfo_trial_id = cgsmss.ctfo_trial_id and r_trial_site.ctfo_site_id = cgsmss.ctfo_site_id
group by 1,2,3,4,5,6,7,8,9,10,11,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36
""")
f_rpt_site_study_details_temp = f_rpt_site_study_details_temp.dropDuplicates()
#f_rpt_site_study_details_temp.createOrReplaceTempView("f_rpt_site_study_details_temp")
f_rpt_site_study_details_temp.write.mode("overwrite").saveAsTable("f_rpt_site_study_details_temp")

f_rpt_site_study_details = spark.sql("""
select
    a.ctfo_trial_id,
    a.ctfo_site_id,
    a.nct_id,
    a.region,
    a.region_code,
    a.site_country,
    a.sanofi_cluster,
    a.country_code,
    a.investigators,
    a.trial_status,
    a.trial_start_dt,
    a.trial_end_dt,
    a.sponsor,
    a.sponsor_type,
    a.site_type as  type,
    a.site_nm as site_nm,
    a.total_site_count as total_no_sites,
    cast(coalesce(a.randomization_rate,'') as double) as randomization_rate,
    cast(coalesce(a.total_recruitment_months,'') as double) as total_recruitment_months ,
    a.first_subject_in_dt,
    a.last_subject_in_dt,
    a.ready_to_enroll_dt,
    cast(lost_opportunity_time as double),
    cast(patients_enrolled as double),
    cast(screen_fail_rate as double),
    cast(average_startup_time as double) as average_startup_time,
    a.protocol_ids,
    a.study_name,
    a.completion_dt as completion_date,
    a.site_address,
    a.site_city,
    a.site_state,
    concat(a.ctfo_trial_id, a.ctfo_site_id) trial_site_id,
    a.site_supporting_urls,
    a.primary_endpoints_reported_date
from f_rpt_site_study_details_temp a
     group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,
     31,32,33,34,35
     order by a.ctfo_site_id, a.ctfo_trial_id, trial_site_id
""")

f_rpt_site_study_details = f_rpt_site_study_details.dropDuplicates()
f_rpt_site_study_details.write.mode('overwrite').saveAsTable("f_rpt_site_study_details")

write_path = path.replace("table_name", "f_rpt_site_study_details")
write_path_csv = path_csv.replace("table_name", "f_rpt_site_study_details")
write_path_csv = write_path_csv + "_csv/"
f_rpt_site_study_details.write.format("parquet").mode("overwrite").save(path=write_path)

'''
f_rpt_site_study_details_final = spark.sql("""
select f_rpt_site_study_details.*, "$$data_dt" as pt_data_dt, "$$cycle_id" as pt_cycle_id from f_rpt_site_study_details
""")
f_rpt_site_study_details_final.createOrReplaceTempView("f_rpt_site_study_details_final")
'''

if "$$flag" == "Y":
    f_rpt_site_study_details.coalesce(1).write.option("header", "false").option("sep", "|") \
        .option('quote', '"').option(
        'escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_site_study_details partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_site_study_details
""")

if f_rpt_site_study_details.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_site_study_details as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_site_study_details")

##############create country reporting table
temp_site_count = spark.sql("""
select d_site.site_country as country,r_trial_site.ctfo_trial_id
from
    (select ctfo_site_id, coalesce(site_country, 'Others') as site_country
    from sanofi_ctfo_datastore_app_commons_$$db_env.d_site) d_site
        inner join (select ctfo_trial_id, ctfo_site_id from sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site) r_trial_site
        on r_trial_site.ctfo_site_id=d_site.ctfo_site_id
group by 1,2""")
temp_site_count.createOrReplaceTempView("temp_site_count")

temp_site_count.write.mode("overwrite").saveAsTable("temp_site_count")

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
golden_id_det.createOrReplaceTempView('golden_id_details')

# Code for DQS Metrics Calculation
dqs_golden_source_map_temp = spark.sql("""
SELECT ctfo_trial_id,
       data_src_nm,
       max(nct_number) as nct_number,
       max(member_study_id) as member_study_id,
       src_site_country as xref_country,
       country as dqs_country
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               data_src_nm,
               src_trial_id,
               src_site_id,
               src_site_country
        FROM   golden_id_details
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


          5,
          6


""")
dqs_golden_source_map_temp = dqs_golden_source_map_temp.dropDuplicates()
dqs_golden_source_map_temp.createOrReplaceTempView("dqs_golden_source_map_temp")

dqs_golden_source_map = spark.sql(""" select a.*, coalesce(b.standard_country,dqs_country) as standard_country from
dqs_golden_source_map_temp a
left join country_mapping b on lower(trim(a.xref_country)) = lower(trim(b.country))
""")
dqs_golden_source_map = dqs_golden_source_map.dropDuplicates()
# dqs_golden_source_map.createOrReplaceTempView("dqs_golden_source_map")
dqs_golden_source_map.write.mode("overwrite").saveAsTable("dqs_golden_source_map")

centre_loc_map = spark.sql("""
select study_id as trial_no,  country_code,  study_site_id,study_code, center_id as centre_loc_id,study_country_id,country
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site where lower(trim(Confirmed)) = 'yes'
group by 1,2,3,4,5,6,7
""")
centre_loc_map = centre_loc_map.dropDuplicates()
centre_loc_map.createOrReplaceTempView("centre_loc_map")

ctms_golden_source_map = spark.sql("""select cgsm_cntry.* from (select ctfo_trial_id,  data_src_nm,  country_code,country_mapping.iso2_code as xref_country_code,
(B.study_code) as study_code,(B.study_country_id) as study_country_id,(trial_no) as trial_no,country_mapping.standard_country as standard_country,
row_number() over (partition by ctfo_trial_id,standard_country order by study_code) as rnk
from
    (select ctfo_trial_id,  data_src_nm, src_trial_id,  src_site_country
    from golden_id_details
    where lower(trim(data_src_nm)) = 'ctms' group by 1,2,3,4) A
inner join
    (select trial_no, country_code,  study_code,study_country_id,country from centre_loc_map group by 1,2,3,4,5) B
on A.src_trial_id = B.study_code and lower(trim(A.src_site_country))=lower(trim(B.country))
left join country_mapping on lower(trim(A.src_site_country)) = lower(trim(country_mapping.country))
where lower(trim(data_src_nm)) = 'ctms'
group by 1,2,3,4,5,6,7,8) cgsm_cntry where rnk=1
""")
ctms_golden_source_map = ctms_golden_source_map.dropDuplicates()
# ctms_golden_source_map.createOrReplaceTempView("ctms_golden_source_map")
ctms_golden_source_map.write.mode("overwrite").saveAsTable("ctms_golden_source_map")

aact_golden_source_map_country_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.xref_country,iq.status,iq.status_rnk,ROW_NUMBER() OVER (PARTITION BY iq.ctfo_trial_id,iq.xref_country ORDER BY iq.status_rnk ) pred_stat from (
SELECT ctfo_trial_id,
       data_src_nm,
       tsm.status,
           src_site_country as xref_country,
       case when tsm.status='Ongoing' then 1
            when tsm.status='Completed' then 2
            when tsm.status='Planned' then 3
            when tsm.status='Others' then 4 end as status_rnk,
       max(nct_id) as aact_nct_id
FROM   (SELECT ctfo_trial_id,
               ctfo_site_id,
               data_src_nm,
               src_trial_id,
               src_site_id,
                           src_site_country
        FROM   golden_id_details
        WHERE  Lower(Trim(data_src_nm)) = 'aact'
        GROUP  BY 1,
                  2,
                  3,
                  4,
                  5,
                                  6) A
       INNER JOIN (SELECT nct_id,
                          id,status
                   FROM   sanofi_ctfo_datastore_staging_$$db_env.aact_facilities
                   GROUP  BY 1,
                             2,3
                             ) B
               ON A.src_trial_id = B.nct_id
                  AND A.src_site_id = B.id
       inner join (select distinct raw_status,status from trial_status_mapping) tsm
       on lower(trim(tsm.raw_status))=lower(trim(B.status))
GROUP  BY 1,
          2,
          3,4,5) iq )ab where pred_stat=1
""")
aact_golden_source_map_country_status = aact_golden_source_map_country_status.dropDuplicates()
aact_golden_source_map_country_status.createOrReplaceTempView("aact_golden_source_map_country_status")

# ctms patient retention


ctms_patient_retention = spark.sql("""
select (((A.sum_entered_treatment)-(A.sum_dropped_treatment))/(A.sum_entered_treatment))*100 as patient_retention,A.country,A.ctfo_trial_id,A.study_code from
(select cgsm.country,cgsm.ctfo_trial_id,(cpr.entered_treatment) as sum_entered_treatment ,(cpr.dropped_treatment) as sum_dropped_treatment,cpr.study_code from
(select ctfo_trial_id, trial_no,study_code,standard_country as country,study_country_id from ctms_golden_source_map group by 1,2,3,4,5) cgsm
left join
(select sum(a.entered_treatment) as entered_treatment,sum(a.dropped_treatment) as dropped_treatment,a.country,d.study_code,a.study_country_id from
(select (entered_treatment) as entered_treatment,(dropped_treatment) as dropped_treatment , picture_as_of,country_name as country,study_code,study_site_id,study_period_number,study_country_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed'  group by 1,2,3,4,5,6,7,8) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and a.picture_as_of=d.picture_as_of

group by 3,4,5) cpr
on cpr.study_code = cgsm.study_code and cpr.study_country_id = cgsm.study_country_id
group by 1,2,3,4,5) A
group by 1,2,3,4
""")
ctms_patient_retention = ctms_patient_retention.dropDuplicates()
# ctms_patient_retention.createOrReplaceTempView("ctms_patient_retention")
ctms_patient_retention.write.mode("overwrite").saveAsTable("ctms_patient_retention")

ir_patient_retention = spark.sql("""select (B.completed/B.enrolled)*100 as patient_retention,A.country,A.ctfo_trial_id from
(select nct_number,ctfo_trial_id,xref_country,standard_country as country from dqs_golden_source_map group by 1,2,3,4) A
left join
(select sum(completed) as completed,sum(enrolled) as enrolled,country ,nct_number
from sanofi_ctfo_datastore_staging_$$db_env.dqs_study group by 3,4) B
on Lower(Trim(A.nct_number)) = Lower(Trim(B.nct_number))
AND Lower(Trim(A.xref_country)) = Lower(Trim(B.country))
group by 1,2,3""")
ir_patient_retention = ir_patient_retention.dropDuplicates()
# ir_patient_retention.createOrReplaceTempView("ir_patient_retention")
ir_patient_retention.write.mode("overwrite").saveAsTable("ir_patient_retention")

# ctms_patients_enrolled
ctms_patients_enrolled = spark.sql("""
select cgsm.country,cgsm.ctfo_trial_id,cntry.study_code,
case when lower(trim(cntry.status)) in ('active', 'initiated', 'last subject enrolled', 'site ready to enrol','active(recruiting)','active(not recruiting)') and mile.lsfv_dt is not null then (cpr.entered_treatment)
when lower(trim(cntry.status)) in ('closed', 'last subject completed', 'discontinued', 'all sites closed','completed') then (cpr.entered_treatment)
else 'null' end
as patients_enrolled
FROM
(select ctfo_trial_id,study_code,standard_country as country,study_country_id from ctms_golden_source_map group by 1,2,3,4) cgsm
left join
(select status,study_country_id,study_code,country
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country where lower(trim(status)) in ( 'active','initiated','last subject enrolled','site ready to enrol','selected','closed', 'last subject completed', 'discontinued') group by 1,2,3,4) cntry
on cntry.study_code = cgsm.study_code and cntry.study_country_id = cgsm.study_country_id
left join (select country,study_code,study_country_id,
case when lower(milestone) = 'last subject first rando/treat' then actual_date END as lsfv_dt
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones where lower(milestone) = 'last subject first rando/treat' group by 1,2,3,4) mile on cntry.study_code = mile.study_code and cntry.study_country_id = mile.study_country_id
left join
(select sum(a.entered_treatment) as entered_treatment,a.country,d.study_code,a.study_country_id from
(select (entered_treatment) as entered_treatment , picture_as_of,country_name as country,study_code,study_period_number,study_country_id,study_site_id from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed'  group by 1,2,3,4,5,6,7) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and a.picture_as_of=d.picture_as_of
group by 2,3,4) cpr
on cpr.study_code = cgsm.study_code and cpr.study_country_id=cgsm.study_country_id
group by 1,2,3,4
""")
ctms_patients_enrolled = ctms_patients_enrolled.dropDuplicates()
# ctms_patients_enrolled.createOrReplaceTempView("ctms_patients_enrolled")
ctms_patients_enrolled.write.mode("overwrite").saveAsTable("ctms_patients_enrolled")

ir_patient_enrolled = spark.sql("""select B.enrolled as patient_enrolled,A.country,A.ctfo_trial_id from
(select nct_number,ctfo_trial_id,xref_country,standard_country as country from dqs_golden_source_map) A
left join
(select sum(enrolled) as enrolled,country,nct_number
from sanofi_ctfo_datastore_staging_$$db_env.dqs_study group by 2,3) B
on Lower(Trim(A.nct_number)) = Lower(Trim(B.nct_number))
AND Lower(Trim(A.xref_country)) = Lower(Trim(B.country))
group by 1,2,3""")
ir_patient_enrolled = ir_patient_enrolled.dropDuplicates()
# ir_patient_enrolled.createOrReplaceTempView("ir_patient_enrolled")
ir_patient_enrolled.write.mode("overwrite").saveAsTable("ir_patient_enrolled")

## need to check

# removed market_launch_flag from the query as it is coming from different source
country_trial_details = spark.sql("""
select  iso2_code,iso3_code,region,region_code,sanofi_cluster,sanofi_csu,post_trial_flag,post_trial_details,
standard_country as country from country_mapping group by 1,2,3,4,5,6,7,8,9""")
# country_trial_details.createOrReplaceTempView("country_trial_details")
country_trial_details = country_trial_details.dropDuplicates()
country_trial_details.write.mode("overwrite").saveAsTable("country_trial_details")

ctms_number_patients_planned = spark.sql("""
select cntry_sbjct_figr.cntry_entered_treatment as number_patients_planned,cgsm.country,cgsm.ctfo_trial_id from
(select ctfo_trial_id, study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4) cgsm
left join
(select sum(cntry_entered_treatment) as cntry_entered_treatment,ppn.study_code,country,study_country_id from
(select (entered_treatment) as cntry_entered_treatment,study_code,country_name as country, study_period_number,study_country_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_cntry_sbject_figures  where lower(trim(figure_type)) = 'planned' group by 1,2,3,4,5) ppn
inner join
(select max(study_period_number) as spn ,study_code from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_cntry_sbject_figures where lower(trim(figure_type)) = 'planned' group by 2) spn on spn.spn =ppn.study_period_number and spn.study_code =ppn.study_code  group by 2,3,4 ) cntry_sbjct_figr
on cgsm.study_code = cntry_sbjct_figr.study_code and cgsm.study_country_id=cntry_sbjct_figr.study_country_id
group by 1,2,3
""")

# ctms_number_patients_planned.createOrReplaceTempView("ctms_number_patients_planned")
ctms_number_patients_planned = ctms_number_patients_planned.dropDuplicates()
ctms_number_patients_planned.write.mode("overwrite").saveAsTable("ctms_number_patients_planned")

ctms_proportion_patient_enrolled = spark.sql("""select B.proportion_patient_enrolled,B.entered_treatment,B.cntry_entered_treatment,B.status,cgsm.country,cgsm.ctfo_trial_id from
(select ctfo_trial_id,  trial_no,study_country_id,study_code,standard_country as country from ctms_golden_source_map group by 1,2,3,4,5) cgsm
left join
(select f.entered_treatment/cntry_sbjct_figr.cntry_entered_treatment as proportion_patient_enrolled,a.study_code,a.study_country_id,f.entered_treatment,a.status,a.country,cntry_sbjct_figr.cntry_entered_treatment
from
(select study_country_id,study_code, status,country from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country where lower(trim(status)) in ('active', 'initiated', 'last subject enrolled', 'site ready to enrol','closed', 'last subject completed', 'discontinued', 'all sites closed','completed','active(recruiting)','active(not recruiting)') group by 1,2,3,4) a
left join
(select sum(cntry_entered_treatment) as cntry_entered_treatment,ppn.study_code,study_country_id,country from
(select (entered_treatment) as cntry_entered_treatment,study_code,study_country_id, study_period_number,country_name as country from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_cntry_sbject_figures  where lower(trim(figure_type)) = 'planned' group by 1,2,3,4,5) ppn
inner join
(select max(study_period_number) as spn ,study_code from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_cntry_sbject_figures where lower(trim(figure_type)) = 'planned' group by 2) spn on spn.spn =ppn.study_period_number and spn.study_code =ppn.study_code  group by 2,3,4) cntry_sbjct_figr
on a.study_country_id = cntry_sbjct_figr.study_country_id and a.study_code = cntry_sbjct_figr.study_code
left join
(select sum(b.entered_treatment) as entered_treatment,b.study_code,max(b.picture_as_of) as picture_as_of,sum(d.spn) as study_period_number,b.study_country_id from
(select (entered_treatment) as entered_treatment ,study_period_number,study_code,picture_as_of,study_country_id,study_site_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed'  group by 1,2,3,4,5,6) b
inner join
(select inq.study_site_id,study_code,study_period_number as spn,picture_as_of from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq ) d
on b.study_site_id = d.study_site_id  and b.study_code = d.study_code and b.study_period_number=d.spn and b.picture_as_of=d.picture_as_of
group by 2,5) f
on f.study_code=a.study_code and f.study_country_id=a.study_country_id group by 1,2,3,4,5,6,7)B
on  B.study_code = cgsm.study_code and B.study_country_id = cgsm.study_country_id  group by 1,2,3,4,5,6""")
# ctms_proportion_patient_enrolled.createOrReplaceTempView("ctms_proportion_patient_enrolled")
ctms_proportion_patient_enrolled = ctms_proportion_patient_enrolled.dropDuplicates()

ctms_proportion_patient_enrolled.write.mode("overwrite").saveAsTable("ctms_proportion_patient_enrolled")

ctms_planned_patient_deviation = spark.sql("""
select country,ctfo_trial_id,
(case when lower(trim(status)) in ( 'active', 'initiated', 'last subject enrolled', 'site ready to enrol','active(recruiting)','active(not recruiting)') and  proportion_patient_enrolled >= 0.5
then (case when (entered_treatment is null or cntry_entered_treatment  is null or entered_treatment=0 or cntry_entered_treatment =0) then null else (entered_treatment/cntry_entered_treatment )  end)
when lower(trim(status)) in ( 'closed', 'last subject completed', 'discontinued', 'all sites closed','completed')
then (case when (entered_treatment is null or cntry_entered_treatment  is null or entered_treatment=0 or cntry_entered_treatment =0) then null else (entered_treatment/cntry_entered_treatment ) end) end) as planned_patient_deviation
from ctms_proportion_patient_enrolled
group by 1,2,3""")
# ctms_planned_patient_deviation.createOrReplaceTempView("ctms_planned_patient_deviation")
ctms_planned_patient_deviation = ctms_planned_patient_deviation.dropDuplicates()
ctms_planned_patient_deviation.write.mode("overwrite").saveAsTable("ctms_planned_patient_deviation")

ctms_planned_fpfv_deviation = spark.sql("""select  s.planned_fpfv_deviation,cgsm.country,cgsm.ctfo_trial_id from
(select ctfo_trial_id,  trial_no,study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4,5) cgsm
left join
(select z.country,z.study_code,z.trial_status,z.study_country_id,
(case when lower(trim(z.trial_status)) in ( 'active','initiated','last subject enrolled','site ready to enrol','selected','active(recruiting)','active(not recruiting)') and  z.fpfv_dt is not null
then datediff(z.fpfv_dt,z.planned_date)
when lower(trim(z.trial_status)) in ( 'closed no subject', 'closed', 'last subject completed', 'discontinued','completed')
then datediff(z.fpfv_dt,z.planned_date) end) as planned_fpfv_deviation
from
(select a.study_country_id,a.study_code,a.planned_date, b.trial_status,a.country,
case when lower(a.milestone) = 'first subject first visit' then a.actual_date end as fpfv_dt
from
(select study_country_id,study_code,milestone,country,current_plan_date as planned_date,actual_date as actual_date
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones where lower(milestone) in ('first subject first visit') group by 1,2,3,4,5,6) a
left join
(select study_country_id,study_code,status as trial_status from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country group by 1,2,3)
b on a.study_code = b.study_code and a.study_country_id= b.study_country_id group by 1,2,3,4,5,6) z group by 1,2,3,4,5)s
on s.study_code = cgsm.study_code and s.study_country_id=cgsm.study_country_id
group by 1,2,3""")
# ctms_planned_fpfv_deviation.createOrReplaceTempView("ctms_planned_fpfv_deviation")
ctms_planned_fpfv_deviation = ctms_planned_fpfv_deviation.dropDuplicates()
ctms_planned_fpfv_deviation.write.mode("overwrite").saveAsTable("ctms_planned_fpfv_deviation")

ctms_planned_fsiv_deviation = spark.sql("""select  s.planned_fsiv_deviation,cgsm.country,cgsm.ctfo_trial_id from
(select ctfo_trial_id,  trial_no,study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4,5) cgsm
left join
(select z.country,z.study_code,z.trial_status,z.study_country_id,
(case when lower(trim(z.trial_status)) in ( 'active','initiated','last subject enrolled','site ready to enrol','selected','active(recruiting)','active(not recruiting)') and  z.fsiv_dt is not null
then datediff(z.fsiv_dt,z.planned_date)
when lower(trim(z.trial_status)) in ( 'closed no subject', 'closed', 'last subject completed', 'discontinued','completed')
then datediff(z.fsiv_dt,z.planned_date) end) as planned_fsiv_deviation

from
(select a.study_country_id,a.study_code,a.planned_date, b.trial_status,a.country,
case when lower(a.milestone) = 'first site initiation visit' then a.actual_date end as fsiv_dt
from
(select study_country_id,study_code,milestone,country,current_plan_date as planned_date,actual_date as actual_date
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones where lower(milestone) in ('first site initiation visit') group by 1,2,3,4,5,6) a
left join
(select study_country_id,study_code,status as trial_status from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country group by 1,2,3)
b on a.study_code = b.study_code and a.study_country_id= b.study_country_id group by 1,2,3,4,5,6) z group by 1,2,3,4,5)s
on s.study_code = cgsm.study_code and s.study_country_id=cgsm.study_country_id
group by 1,2,3""")
# ctms_planned_fsiv_deviation.createOrReplaceTempView("ctms_planned_fsiv_deviation")
ctms_planned_fsiv_deviation = ctms_planned_fsiv_deviation.dropDuplicates()
ctms_planned_fsiv_deviation.write.mode("overwrite").saveAsTable("ctms_planned_fsiv_deviation")

# ctms_screen_fail_rate
ctms_screen_fail_rate = spark.sql("""select cgsm.ctfo_trial_id,cgsm.country,
case when ((lower(trim(cntry.trial_status)) in ( 'active','initiated','last subject enrolled','site ready to enrol','selected','active(recruiting)','active(not recruiting)'))
                         and (lower(trim(mile.milestone)) like '%last subject first visit%') and (mile.actual_date is not null)  )
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
(select ctfo_trial_id,study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4) cgsm
left join
(select status as trial_status,study_country_id,study_code,country
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country where lower(trim(status)) in ( 'active','initiated','last subject enrolled','site ready to enrol','selected','closed', 'last subject completed', 'discontinued','active(recruiting)','active(not recruiting)','completed') group by 1,2,3,4) cntry
on cntry.study_code = cgsm.study_code and cntry.study_country_id=cgsm.study_country_id
left join
(select study_code,study_country_id,milestone,actual_date  from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones where lower(trim(milestone)) like '%last subject first visit%' group by 1,2,3,4) mile
on mile.study_code = cgsm.study_code and mile.study_country_id=cgsm.study_country_id
left join
(select m.entered_study,((m.entered_study)-(m.entered_treatment)) as DROPPED_AT_SCREENING,entered_treatment,m.country,m.study_code,m.study_country_id from (select sum(a.entered_study) as entered_study,sum(a.entered_treatment) as entered_treatment,a.country,d.study_code,d.study_country_id from
(select (entered_study) as entered_study,(entered_treatment) as entered_treatment , picture_as_of,country_name as country,study_code,study_period_number,study_country_id,study_site_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed'
group by 1,2,3,4,5,6,7,8) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of,study_country_id from (select *,row_number() over (partition by study_code,study_site_id,study_country_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and a.picture_as_of=d.picture_as_of
group by 3,4,5)m group by 1,2,3,4,5,6) cpr
on cpr.study_code=cgsm.study_code and cpr.study_country_id=cgsm.study_country_id
group by 1,2,3""")
ctms_screen_fail_rate = ctms_screen_fail_rate.dropDuplicates()
# ctms_screen_fail_rate.registerTempTable("ctms_screen_fail_rate")
ctms_screen_fail_rate.write.mode("overwrite").saveAsTable("ctms_screen_fail_rate")

# ctms - country_startup_time
# june
ctms_cta_siv_temp_new = spark.sql("""
select
case when (A.siv_dt is not null) and (A.cta_date is not null) then datediff(A.siv_dt, A.cta_date)/30.4
     when A.cta_date is null  and (A.siv_dt is not null) and A.irb_iec_date is not null then datediff(A.siv_dt, A.irb_iec_date)/30.4
     when A.siv_dt is null and A.cta_date is not null and A.fsfv_dt is not null then datediff(date_sub(A.fsfv_dt,14),A.cta_date)/30.4
     when A.cta_date is null and A.siv_dt is null and A.irb_iec_date is not null and A.fsfv_dt is not null then datediff(date_sub(A.fsfv_dt,14),A.irb_iec_date)/30.4
     when ( ((A.cta_date is null) and (A.irb_iec_date is null)) or ((A.siv_dt is null) and (A.fsfv_dt is null)) ) then null

end as country_startup_time,A.ctfo_trial_id,A.study_country_id,A.study_code,A.cta_date,A.irb_iec_date,A.siv_dt,A.fsfv_dt,A.country from
(select
    cgsm.ctfo_trial_id,a.study_country_id,a.study_code,cgsm.country,
    min(case when lower(a.milestone) in ('ha/ca submission') then a.actual_date END) as cta_date,
    min(case when lower(a.milestone) in ('first local irb/iec submission') then a.actual_date END) as irb_iec_date,
    min(case when lower(a.milestone) in ('first site initiation visit') then a.actual_date end) as siv_dt,
    min(case when lower(a.milestone) in ('first subject first visit') then a.actual_date end) as fsfv_dt
from
(select ctfo_trial_id,study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4) cgsm
left join
(select study_code,milestone,actual_date,study_country_id,country from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones group by 1,2,3,4,5) a
on a.study_code = cgsm.study_code and  a.study_country_id = cgsm.study_country_id
group by 1,2,3,4) A group by 1,2,3,4,5,6,7,8,9 """)
ctms_cta_siv_temp_new = ctms_cta_siv_temp_new.dropDuplicates()
# ctms_cta_siv_temp_new.registerTempTable('ctms_cta_siv_temp_new')
ctms_cta_siv_temp_new.write.mode("overwrite").saveAsTable("ctms_cta_siv_temp")

ctms_regulatory_iec_time = spark.sql("""
select datediff(A.approval_dt, A.submission_dt)/30.4 as regulatory_iec_time,A.ctfo_trial_id,coalesce(country_mapping.standard_country,A.country) as country from
(select
    cgsm.ctfo_trial_id,a.country,
    min(case when lower(a.milestone) in ('first local irb/iec submission') then a.actual_date END) as submission_dt,
    min(case when lower(a.milestone) in ('first local irb/iec approval') then a.actual_date end) as approval_dt

from
(select ctfo_trial_id,study_code,study_country_id from ctms_golden_source_map group by 1,2,3) cgsm
left join
(select study_code,milestone,actual_date,country,study_country_id from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones group by 1,2,3,4,5
) a
on a.study_code = cgsm.study_code and a.study_country_id=cgsm.study_country_id group by 1,2) A
left join country_mapping on lower(trim(A.country)) = lower(trim(country_mapping.country))
group by 1,2,3 """)
ctms_regulatory_iec_time = ctms_regulatory_iec_time.dropDuplicates()
# ctms_regulatory_iec_time.registerTempTable('ctms_regulatory_iec_time')
ctms_regulatory_iec_time.write.mode("overwrite").saveAsTable("ctms_regulatory_iec_time")

all_calc_dqs = spark.sql("""select ctfo_trial_id,c.standard_country as country,percentile(b.irb_cycle_time, 0.5, 10000) as regulatory_iec_time,percentile(b.site_startup_time,0.5,10000) as country_startup_time
,sum(enrolled) as enrolled_ct ,min(site_open_dt) as rte_dt ,max(last_subject_enrolled_dt) as lsi_dt
,sum(consented) as cnsntd_ct  from
(select ctfo_trial_id, src_trial_id from  sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial
where data_src_nm ='ir') a
left join sanofi_ctfo_datastore_staging_$$db_env.dqs_study b on a.src_trial_id = b.member_study_id
left join standard_country_mapping c on lower(trim(c.country)) = lower(trim(b.country))
group by 1,2
""")
all_calc_dqs = all_calc_dqs.dropDuplicates()
# all_calc_dqs.registerTempTable('all_calc_dqs')
all_calc_dqs.write.mode("overwrite").saveAsTable("all_calc_dqs")

all_calc_ctms = spark.sql("""
select a.ctfo_trial_id,d.standard_country as country,"Null" as regulatory_iec_time,"Null" as country_startup_time,(b.entered_treatment) as enrolled_ct ,
min(c.siv_dt) as rte_dt ,
max(c.lsfv_dt) as lsi_dt ,
sum(b.enrolled) as cnsntd_ct from
(select ctfo_trial_id, src_trial_id from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial
where data_src_nm ='ctms') a
left join
(select sum(a.entered_treatment) as entered_treatment,sum(a.enrolled) as enrolled,a.country,a.study_id from
(select (entered_treatment) as entered_treatment,(enrolled) as enrolled , picture_as_of,country_name as country,study_code,study_site_id,study_period_number,study_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed'  group by 1,2,3,4,5,6,7,8) a
inner join
(select inq.study_site_id,study_code,study_period_number,picture_as_of from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq ) d
on a.study_site_id = d.study_site_id  and a.study_code = d.study_code and a.study_period_number=d.study_period_number and a.picture_as_of=d.picture_as_of
group by 3,4) b on a.src_trial_id = b.study_id
left join
(select country,study_id,
case when lower(milestone) = 'site initiation visit' then actual_date end as siv_dt,
case when lower(milestone) = 'last subject first visit' then actual_date END as lsfv_dt
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones group by 1,2,3,4) c on a.src_trial_id = c.study_id
left join (select center_id, study_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site group by 1,2) study_site on study_site.study_id = c.study_id
left join (select ctfo_site_id, src_site_id from  sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site
where data_src_nm ='ctms') site
on site.src_site_id = study_site.CENTER_ID
left join
standard_country_mapping d on lower(trim(d.country)) = lower(trim(c.country))
group by 1,2,3,4,5
""")
all_calc_ctms = all_calc_ctms.dropDuplicates()
# all_calc_ctms.registerTempTable('all_calc_ctms')
all_calc_ctms.write.mode("overwrite").saveAsTable("all_calc_ctms")

all_calc = spark.sql("""
select * from all_calc_dqs
union
select * from all_calc_ctms""")
# all_calc.createOrReplaceTempView("all_calc")
all_calc = all_calc.dropDuplicates()
# all_calc.registerTempTable('all_calc')
all_calc.write.mode("overwrite").saveAsTable("all_calc")

enrollment_rate_calc = spark.sql("""
select country,ctfo_trial_id,  enrollment_rate as enrollment_rate from (
select ctfo_trial_id, country, (sum(enrolled_ct))/(datediff(max(lsi_dt) , min(rte_dt))/30.4)
as enrollment_rate from all_calc group by 1,2)
group by 1,2,3
""")
enrollment_rate_calc = enrollment_rate_calc.dropDuplicates()
# enrollment_rate_calc.registerTempTable('enrollment_rate_calc')
enrollment_rate_calc.write.mode("overwrite").saveAsTable("enrollment_rate_calc")

no_pt_enr = spark.sql("""select country,ctfo_trial_id,(enrolled_ct) as en from all_calc group by 1,2,3""")
no_pt_enr = no_pt_enr.dropDuplicates()
# no_pt_enr.registerTempTable('no_pt_enr')
no_pt_enr.write.mode("overwrite").saveAsTable("no_pt_enr")

dqs_screen_failure_calc = spark.sql(
    """select country,ctfo_trial_id, (((cnsntd_ct) - (enrolled_ct))/cnsntd_ct)*100 as sc_failure_rt from
               (select ctfo_trial_id,country,cnsntd_ct,case when cnsntd_ct is not null and enrolled_ct is null then 0 else enrolled_ct end as enrolled_ct from all_calc_dqs)
    all_calc_dqs group by 1,2,3""")
dqs_screen_failure_calc = dqs_screen_failure_calc.dropDuplicates()
dqs_screen_failure_calc.write.mode("overwrite").saveAsTable("dqs_screen_failure_calc")
# dqs_screen_failure_calc.registerTempTable('dqs_screen_failure_calc')


# Randomization Rate/Enrollment Rate
ctms_entered_study = spark.sql("""
select  cgsm.ctfo_trial_id , A.entered_treatment,cgsm.country,cgsm.study_code,A.study_country_id
from
(select ctfo_trial_id, trial_no, study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4,5) cgsm
left join
(select sum(a.entered_treatment) as entered_treatment,sum(a.enrolled) as enrolled,a.country,d.study_code,country,a.study_country_id from
(select (entered_treatment) as entered_treatment,(enrolled) as enrolled , picture_as_of,country_name as country,study_code,study_country_id,study_period_number,study_site_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed'  group by 1,2,3,4,5,6,7,8) a
inner join
(select study_code,study_period_number,picture_as_of,study_site_id from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,study_period_number desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' having rnk=1)inq ) d
on  a.study_code = d.study_code and a.study_period_number=d.study_period_number and a.picture_as_of=d.picture_as_of and a.study_site_id=d.study_site_id
group by 3,4,5,6)A
on cgsm.study_code = A.study_code and cgsm.study_country_id=A.study_country_id
group by 1,2,3,4,5
""")
ctms_entered_study = ctms_entered_study.dropDuplicates()
# ctms_entered_study.registerTempTable('ctms_entered_study')
ctms_entered_study.write.mode("overwrite").saveAsTable("ctms_entered_study")

ctms_dates = spark.sql("""select cgsm.ctfo_trial_id,cgsm.country,cgsm.study_code,a.study_country_id,
min(case when lower(a.milestone) = 'first site initiation visit' then a.actual_date end) as fsiv_dt,
min(case when lower(a.milestone) = 'first subject first rando/treat' then a.actual_date end) as fsfr_dt,
min(case when lower(a.milestone) = 'last subject first visit' then a.actual_date END) as lsfv_dt,
min(case when lower(a.milestone) = 'last subject first rando/treat' then a.actual_date end) as lsfr_dt,
min(case when lower(a.milestone) = 'last subject first rando/treat' then a.actual_date end) as cntry_lsfr_dt
from
(select ctfo_trial_id, trial_no,study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4,5) cgsm
left join
(select milestone,actual_date,study_code,country,study_country_id from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country_milestones group by 1,2,3,4,5) a
on cgsm.study_code = a.study_code and  cgsm.study_country_id=a.study_country_id
group by 1,2,3,4""")
ctms_dates = ctms_dates.dropDuplicates()
# ctms_dates.registerTempTable('ctms_dates')
ctms_dates.write.mode("overwrite").saveAsTable("ctms_dates")

ctms_ppm_planned_patients = spark.sql("""select cgsm.ctfo_trial_id,cgsm.study_code,A.ppm as planned_patients,cgsm.country,A.study_country_id
from(select ctfo_trial_id,study_code,study_country_id,standard_country as country from ctms_golden_source_map
group by 1,2,3,4) cgsm
left join
(select a.study_code, entered_treatment as ppm,country,a.study_country_id from
(select study_code,entered_treatment,figure_type,study_period_number,country_name as country,study_country_id
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_cntry_sbject_figures where lower(trim(figure_type)) = 'planned' group by 1,2,3,4,5,6) a
inner join
(select max(study_period_number) as spn,study_code
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_cntry_sbject_figures where lower(trim(figure_type)) = 'planned' group by 2) c
on  a.study_period_number = c.spn and a.study_code = c.study_code
where lower(trim(a.figure_type)) = 'planned' group by 1,2,3,4) A
on cgsm.study_code = A.study_code and cgsm.study_country_id=A.study_country_id group by 1,2,3,4,5
""")
ctms_ppm_planned_patients = ctms_ppm_planned_patients.dropDuplicates()
# ctms_ppm_planned_patients.registerTempTable('ctms_ppm_planned_patients')
ctms_ppm_planned_patients.write.mode("overwrite").saveAsTable("ctms_ppm_planned_patients")

ctms_trial_status = spark.sql("""
select  cgsm.ctfo_trial_id, tsm.status as trial_status,cgsm.country,b.study_code,b.study_country_id from
(select ctfo_trial_id, trial_no,study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4,5) cgsm
left join
(select status as trial_status,study_code,country,study_country_id
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_country  group by 1,2,3,4) b
on b.study_code = cgsm.study_code and cgsm.study_country_id=b.study_country_id
left join (select distinct raw_status,status from trial_status_mapping) tsm  on lower(trim(b.trial_status))=lower(trim(tsm.raw_status))
group by 1,2,3,4,5
""")
ctms_trial_status = ctms_trial_status.dropDuplicates()
# ctms_trial_status.registerTempTable('ctms_trial_status')
ctms_trial_status.write.mode("overwrite").saveAsTable("ctms_trial_status")

ctms_percentage_randomized = spark.sql(""" select a.ctfo_trial_id,a.study_code,a.study_country_id ,case when (a.entered_treatment is null or b.planned_patients  is null or a.entered_treatment=0 or b.planned_patients =0) then null else (a.entered_treatment/b.planned_patients )  end as percentage_randomized,a.country
from
ctms_entered_study a
left join
ctms_ppm_planned_patients b on  a.study_code = b.study_code and a.study_country_id=b.study_country_id
group by 1,2,3,4,5
""")

ctms_percentage_randomized = ctms_percentage_randomized.dropDuplicates()
# ctms_percentage_randomized.registerTempTable('ctms_percentage_randomized')
ctms_percentage_randomized.write.mode("overwrite").saveAsTable("ctms_percentage_randomized")

ctms_elapsed_duration = spark.sql("""
select B.ctfo_trial_id,B.country,B.study_code, B.study_country_id,(B.num/B.den) as elapsed_duration from
(select A.ctfo_trial_id,A.country, A.study_country_id,A.study_code,(datediff(A.fsiv_dt,A.lsfr_curr_dt)) as den,(datediff(A.fsiv_dt,CURRENT_DATE())) as num
from
(select cgsm.ctfo_trial_id,cgsm.country,cgsm.study_code,cgsm.study_country_id,
min(case when lower(a.milestone) = 'last subject first rando/treat' then a.current_plan_date end) as lsfr_curr_dt,
min(case when lower(a.milestone) = 'first site initiation visit' then a.current_plan_date end) as fsiv_dt
from
(select ctfo_trial_id, trial_no, study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4,5) cgsm
left join
(select milestone,actual_date,study_code,current_plan_date from
sanofi_ctfo_datastore_staging_$$db_env.ctms_study_milestones group by 1,2,3,4) a
on cgsm.study_code = a.study_code
group by 1,2,3,4) A group by 1,2,3,4,5,6) B group by 1,2,3,4,5""")
ctms_elapsed_duration = ctms_elapsed_duration.dropDuplicates()
# ctms_elapsed_duration.registerTempTable('ctms_elapsed_duration')
ctms_elapsed_duration.write.mode("overwrite").saveAsTable("ctms_elapsed_duration")

ctms_randomiztion_duration = spark.sql("""
select cgsm.ctfo_trial_id,cgsm.country,cgsm.study_code,A.study_country_id,
case
when lower(trim(A.trial_status)) in ('active', 'initiated', 'last subject enrolled', 'site ready to enrol','active(recruiting)','active(non recruiting)','ongoing', 'recruiting')
then
case when A.fsiv_dt is not null and A.lsfr_dt is not null then datediff(A.lsfr_dt,A.fsiv_dt)/30.4
when A.fsiv_dt is null and A.fsfr_dt is not null and A.lsfr_dt is not null then datediff(A.lsfr_dt,date_sub(A.fsfr_dt,14))/30.4
when A.lsfr_dt is null and A.percentage_randomized>0.5 and (A.fsiv_dt is not null or A.fsfr_dt is not null) then datediff(current_date(),COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,14)))/30.4
when A.lsfr_dt is null and A.percentage_randomized<0.5 and (A.fsiv_dt is not null or A.fsfr_dt is not null) and A.elapsed_duration > 0.5 then (datediff(current_date(),COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,14))))/30.4
else null
end
when lower(trim(A.trial_status)) in ( 'closed', 'last subject completed', 'discontinued', 'all sites closed','completed')
then  case when A.fsiv_dt is null then datediff(A.lsfr_dt,COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,14)))/30.4
when A.fsiv_dt is not null and A.lsfr_dt is not null then datediff(A.lsfr_dt,A.fsiv_dt)/30.4
when A.fsiv_dt is null and A.fsfr_dt is not null and A.lsfr_dt is not null then datediff(A.lsfr_dt,date_sub(A.fsfr_dt,14))/30.4
when A.lsfr_dt is null and (A.fsiv_dt is not null or A.fsfr_dt is not null) and A.cntry_lsfr_dt is not null then datediff(A.cntry_lsfr_dt,COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,14)))/30.4
end
else null
end as duration,
A.trial_status,A.fsfr_dt,A.lsfr_dt,A.fsiv_dt
from
(select ctfo_trial_id, study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4) cgsm
left join
( select b.study_code,b.ctfo_trial_id,b.country,b.trial_status,d.fsfr_dt ,d.fsiv_dt,d.cntry_lsfr_dt,d.lsfr_dt,ced.elapsed_duration,e.percentage_randomized, b.study_country_id from
( select trial_status, study_code,country,study_country_id,ctfo_trial_id from ctms_trial_status group by 1,2,3,4,5) b
left join (select cntry_lsfr_dt,fsfr_dt ,fsiv_dt,lsfr_dt,study_code,country,study_country_id from ctms_dates group by 1,2,3,4,5,6,7) d on b.study_code=d.study_code and b.study_country_id=d.study_country_id
left join
(select ctfo_trial_id, country,elapsed_duration,study_code,study_country_id from
ctms_elapsed_duration group by 1,2,3,4,5) ced on b.study_code=ced.study_code and b.study_country_id=ced.study_country_id
left join
(select percentage_randomized, study_code,study_country_id from ctms_percentage_randomized group by 1,2,3) e on b.study_code=e.study_code and b.study_country_id=e.study_country_id group by 1,2,3,4,5,6,7,8,9,10,11) A
on A.ctfo_trial_id=cgsm.ctfo_trial_id and A.study_code=cgsm.study_code and cgsm.study_country_id=A.study_country_id""")
ctms_randomiztion_duration = ctms_randomiztion_duration.dropDuplicates()
# ctms_randomiztion_duration.registerTempTable('ctms_randomiztion_duration')
ctms_randomiztion_duration.write.mode("overwrite").saveAsTable("ctms_randomiztion_duration")

ctms_randomization_rate = spark.sql("""
select cgsm.ctfo_trial_id,cgsm.country, A.entered_treatment/A.duration as randomization_rate from
(select ctfo_trial_id, study_code,study_country_id,standard_country as country from ctms_golden_source_map group by 1,2,3,4) cgsm
left join
( select a.ctfo_trial_id,a.country,a.study_code,a.duration,b.entered_treatment,b.study_country_id from
( select country, ctfo_trial_id, study_code, study_country_id, duration from
ctms_randomiztion_duration group by 1,2,3,4,5) a
left join
(select country, ctfo_trial_id, study_code, study_country_id, entered_treatment from ctms_entered_study )b on b.study_code=a.study_code and b.study_country_id=a.study_country_id group by 1,2,3,4,5,6) A
on A.ctfo_trial_id=cgsm.ctfo_trial_id and A.study_code=cgsm.study_code and cgsm.study_country_id=A.study_country_id
group by 1,2,3
""")
ctms_randomization_rate = ctms_randomization_rate.dropDuplicates()
# ctms_randomization_rate.registerTempTable('ctms_randomization_rate')
ctms_randomization_rate.write.mode("overwrite").saveAsTable("ctms_randomization_rate_cntry")

dqs_status_value = spark.sql("""select member_study_id,country,site_status,
        case when lower(trim(site_status)) in ('active(not recruiting)','active (recruiting)','ongoing','recruiting') then 'ongoing'
             when lower(trim(site_status)) in ('completed','closed') then 'completed'
        end as status_value from  sanofi_ctfo_datastore_staging_$$db_env.dqs_study where lower(trim(site_status)) in ('completed','closed','ongoing', 'recruiting', 'active(not recruiting)','active (recruiting)') group by 1,2,3,4""")
dqs_status_value.registerTempTable("dqs_status_value")

trial_country_status = spark.sql("""select member_study_id,country,site_status,status_value,
        case when lower(trim(status_value))='ongoing' then 1
        when lower(trim(status_value))='completed' then 2 end as final_rnk from  dqs_status_value""")
trial_country_status.registerTempTable("trial_country_status")

dqs_country_status = spark.sql("""select ab.* from (select iq.ctfo_trial_id,iq.standard_country as country,iq.status,iq.status_rnk,
ROW_NUMBER() OVER (PARTITION BY iq.ctfo_trial_id,iq.standard_country ORDER BY iq.status_rnk ) pred_stat 
from (select A.ctfo_trial_id,B.country,tsm.status,A.standard_country,
case when tsm.status='Ongoing' then 1
when tsm.status='Completed' then 2
when tsm.status='Planned' then 3
when tsm.status='Others' then 4 end as status_rnk
FROM
(select ctfo_trial_id,member_study_id,standard_country,dqs_country from dqs_golden_source_map) A
left join (
select member_study_id,country,site_status from sanofi_ctfo_datastore_staging_$$db_env.dqs_study   group by 1,2,3) B
ON (trim(A.member_study_id)) = (trim(B.member_study_id)) and (trim(A.dqs_country)) = (trim(B.country))
left join (select distinct raw_status,status from trial_status_mapping) tsm  on lower(trim(B.site_status))=lower(trim(tsm.raw_status)))iq
)ab where pred_stat=1
""")
dqs_country_status = dqs_country_status.dropDuplicates()
dqs_country_status.registerTempTable('dqs_country_status')

dqs_site_details = spark.sql("""select inner_q.* from  (SELECT A.ctfo_trial_id,A.standard_country as country,B.patients_randomized,B.first_subject_enrolled_dt,
B.first_subject_randomized_dt,B.last_subject_enrolled_dt,B.last_subject_randomized_dt,B.last_subject_last_visit_dt,B.site_ready_to_enroll_dt,tis.site_status,tis.final_rnk,ROW_NUMBER() OVER (PARTITION BY A.ctfo_trial_id,B.country ORDER BY tis.final_rnk ) pred_stat
FROM
(select ctfo_trial_id,member_study_id,dqs_country,standard_country from dqs_golden_source_map) A
left join (select member_study_id,country,sum(enrolled) as patients_randomized,
Min(first_subject_consented_dt)           AS first_subject_enrolled_dt,
Min(first_subject_enrolled_dt)            AS first_subject_randomized_dt,
Max(last_subject_consented_dt)            AS last_subject_enrolled_dt,
Max(last_subject_enrolled_dt)             AS last_subject_randomized_dt,
Max(last_subject_last_visit_dt)           AS last_subject_last_visit_dt,
Min(site_open_dt)                         AS site_ready_to_enroll_dt from sanofi_ctfo_datastore_staging_$$db_env.dqs_study where
site_status in ('Completed','Closed','Ongoing', 'Recruiting', 'Active(not recruiting)','Active (Recruiting)')  group by 1,2) B
ON (trim(A.member_study_id)) = (trim(B.member_study_id)) and (trim(A.dqs_country)) = (trim(B.country))
LEFT JOIN trial_country_status tis
       ON Lower(Trim(B.member_study_id)) = Lower(Trim(tis.member_study_id))
        AND lower(trim(B.country)) = lower(trim(tis.country))) as inner_q where pred_stat=1 """)
dqs_site_details = dqs_site_details.dropDuplicates()
dqs_site_details.registerTempTable('dqs_site_details')

dqs_randomization_rate_temp = spark.sql("""select A.ctfo_trial_id, b.nct_number,A.standard_country as country,Max(b.last_subject_enrolled_dt) as cntry_last_subject_randomized_dt 
from dqs_golden_source_map A
left join sanofi_ctfo_datastore_staging_$$db_env.dqs_study B ON Lower(Trim(A.nct_number)) = Lower(Trim(B.nct_number))
AND Lower(Trim(A.dqs_country)) = Lower(Trim(B.country)) group by 1,2,3""")
dqs_randomization_rate_temp = dqs_randomization_rate_temp.dropDuplicates()
dqs_randomization_rate_temp.registerTempTable('dqs_randomization_rate_temp')

dqs_randomization_rate = spark.sql("""
select C.ctfo_trial_id,C.recruitment_duration, C.patients_randomized/C.recruitment_duration as dqs_randomization_rate, 
C.country,C.site_ready_to_enroll_dt,C.first_subject_randomized_dt from
(select A.ctfo_trial_id, A.patients_randomized,A.site_ready_to_enroll_dt,A.first_subject_randomized_dt,
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
else null end as recruitment_duration,
A.country as country
from
(select ctfo_trial_id, patients_randomized, country,site_ready_to_enroll_dt,first_subject_randomized_dt,last_subject_randomized_dt, site_status as trial_status from
dqs_site_details where pred_stat=1 group by 1,2,3,4,5,6,7) A
left join dqs_randomization_rate_temp B ON Lower(Trim(A.ctfo_trial_id)) = Lower(Trim(B.ctfo_trial_id))
AND Lower(Trim(A.country)) = Lower(Trim(B.country)) group by 1,2,3,4,5,6) C
group by 1,2,3,4,5,6""")
dqs_randomization_rate = dqs_randomization_rate.dropDuplicates()
# dqs_randomization_rate.registerTempTable('dqs_randomization_rate')
dqs_randomization_rate.write.mode("overwrite").saveAsTable("dqs_randomization_rate_cntry")

f_rpt_country_temp = spark.sql("""
select
    a.country,
    a.ctfo_trial_id,
    coalesce(cast(b.patients_enrolled as bigint),cast(t.patient_enrolled as bigint)) as patients_enrolled,
   coalesce(ctms_sfr.ctms_screen_fail_rate, cast(c.sc_failure_rt as double))  as screen_failure_rate,
coalesce (cast(e.randomization_rate as double),cast(u.dqs_randomization_rate as double)) AS enrollment_rate,
    coalesce(g.patient_retention,m.patient_retention) as patient_retention,
    p.planned_patient_deviation,
        j.number_patients_planned,
    cast(l.planned_fsiv_deviation as double),
        k.planned_fpfv_deviation,
        h.post_trial_flag,
        h.post_trial_details,
        h.iso2_code,
        h.iso3_code,
        h.region,
        h.region_code,
        h.sanofi_cluster,
        h.sanofi_csu,
        cast(coalesce(ctms_cst.country_startup_time,dqs_reg.country_startup_time) as double) as country_startup_time,
        cast(coalesce(ctms_reg.regulatory_iec_time,dqs_reg.regulatory_iec_time) as double) as regulatory_iec_time,
        coalesce(cts.trial_status,dts.status,agsm.status,citeline_status.status,'Others') as trial_status
from
temp_site_count a
left join ctms_patients_enrolled b on lower(trim(a.country)) = lower(trim(b.country)) and trim(a.ctfo_trial_id) = trim(b.ctfo_trial_id)
left join ir_patient_enrolled t on lower(trim(a.country)) = lower(trim(t.country)) and trim(a.ctfo_trial_id) = trim(t.ctfo_trial_id)
left join dqs_screen_failure_calc c on lower(trim(a.country)) = lower(trim(c.country)) and trim(a.ctfo_trial_id) = trim(c.ctfo_trial_id)
left join default.ctms_randomization_rate_cntry e on lower(trim(a.country)) = lower(trim(e.country)) and trim(a.ctfo_trial_id) = trim(e.ctfo_trial_id)
left join default.dqs_randomization_rate_cntry u on lower(trim(a.country)) = lower(trim(u.country)) and trim(a.ctfo_trial_id) = trim(u.ctfo_trial_id)
left join ctms_patient_retention g on lower(trim(a.country)) = lower(trim(g.country)) and trim(a.ctfo_trial_id) = trim(g.ctfo_trial_id)
left join country_trial_details h on lower(trim(a.country)) = lower(trim(h.country))
left join ctms_number_patients_planned j on lower(trim(a.country)) = lower(trim(j.country)) and  trim(a.ctfo_trial_id)=trim(j.ctfo_trial_id)
left join ctms_planned_patient_deviation p on lower(trim(a.country)) = lower(trim(p.country)) and  trim(a.ctfo_trial_id)=trim(p.ctfo_trial_id)
left join ctms_planned_fpfv_deviation k on lower(trim(a.country)) = lower(trim(k.country)) and  trim(a.ctfo_trial_id)=trim(k.ctfo_trial_id)
left join ctms_planned_fsiv_deviation l on lower(trim(a.country)) = lower(trim(l.country)) and  trim(a.ctfo_trial_id)=trim(l.ctfo_trial_id)
left join ir_patient_retention m on lower(trim(a.country)) = lower(trim(m.country)) and trim(a.ctfo_trial_id)=trim(m.ctfo_trial_id)
left join ctms_screen_fail_rate ctms_sfr on lower(trim(a.country)) = lower(trim(ctms_sfr.country)) and  trim(a.ctfo_trial_id)=trim(ctms_sfr.ctfo_trial_id)
left join ctms_cta_siv_temp ctms_cst on lower(trim(a.country)) = lower(trim(ctms_cst.country)) and  trim(a.ctfo_trial_id)=trim(ctms_cst.ctfo_trial_id)
left join ctms_regulatory_iec_time ctms_reg on lower(trim(a.country)) = lower(trim(ctms_reg.country)) and  trim(a.ctfo_trial_id)=trim(ctms_reg.ctfo_trial_id)
left join all_calc_dqs dqs_reg on lower(trim(a.country)) = lower(trim(dqs_reg.country)) and  trim(a.ctfo_trial_id)=trim(dqs_reg.ctfo_trial_id)
left join ctms_trial_status cts on lower(trim(a.country)) = lower(trim(cts.country)) and trim(a.ctfo_trial_id)=trim(cts.ctfo_trial_id)
left join dqs_country_status dts on lower(trim(a.country)) = lower(trim(dts.country)) and trim(a.ctfo_trial_id)=trim(dts.ctfo_trial_id)
left join aact_golden_source_map_country_status agsm on lower(trim(a.country)) = lower(trim(agsm.xref_country)) and trim(a.ctfo_trial_id)=trim(agsm.ctfo_trial_id)
left join citeline_golden_source_map_site_status citeline_status on trim(a.ctfo_trial_id)=trim(citeline_status.ctfo_trial_id)
""")

f_rpt_country_temp = f_rpt_country_temp.dropDuplicates()
f_rpt_country_temp.write.mode("overwrite").saveAsTable("f_rpt_country_temp")

f_rpt_country = spark.sql("""Select country,
ctfo_trial_id,
patients_enrolled,
CASE
               WHEN cast(screen_failure_rate as double) < 0 THEN null
               ELSE cast(screen_failure_rate as double)
END  AS screen_failure_rate,
CASE
               WHEN cast(enrollment_rate as double) < 0 THEN null
               ELSE cast(enrollment_rate as double)
END  AS enrollment_rate,
CASE
               WHEN cast(patient_retention as double) >100  THEN 100
               WHEN cast(patient_retention as double) <0    THEN 0
               ELSE cast(patient_retention as double)
END  AS patient_retention,
planned_patient_deviation,
number_patients_planned,
planned_fsiv_deviation,
planned_fpfv_deviation,
post_trial_flag,
post_trial_details,
iso2_code,
iso3_code,
region,
region_code,
sanofi_cluster,
sanofi_csu,
CASE
               WHEN cast(country_startup_time as double) < 0 THEN null
               ELSE cast(country_startup_time as double)
END AS country_startup_time,
CASE
               WHEN cast(regulatory_iec_time as double) < 0 THEN null
               ELSE cast(regulatory_iec_time as double)
END  AS regulatory_iec_time,
trial_status
from f_rpt_country_temp """)
f_rpt_country = f_rpt_country.dropDuplicates()
f_rpt_country.write.mode("overwrite").saveAsTable("f_rpt_country")

write_path = path.replace("table_name", "f_rpt_country")
write_path_csv = path_csv.replace("table_name", "f_rpt_country")
write_path_csv = write_path_csv + "_csv/"
f_rpt_country.write.format("parquet").mode("overwrite").save(path=write_path)

f_rpt_study_country = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/sanofi_cntryhead_ml_mapping_v0.2.csv")
f_rpt_study_country.createOrReplaceTempView("f_rpt_study_country")


f_rpt_study_country_final = spark.sql("""
select f_rpt_study_country.*, country_mapping.sanofi_cluster from f_rpt_study_country
left join country_mapping on lower(trim(f_rpt_study_country.standard_country)) = lower(trim(country_mapping.standard_country))
""")
f_rpt_study_country_final.createOrReplaceTempView("f_rpt_study_country_final")

'''
f_rpt_country_final = spark.sql("""
select f_rpt_country.*, "$$data_dt" as pt_data_dt, "$$cycle_id" as pt_cycle_id from f_rpt_country
""")
f_rpt_country_final.createOrReplaceTempView("f_rpt_country_final")
'''
if "$$flag" == "Y":
    f_rpt_country.coalesce(1).write.option("header", "false").option("sep", "|").option('quote',
                                                                                        '"') \
        .option('escape', '"').option(
        "multiLine", "true").mode('overwrite').csv(write_path_csv)

    write_path_csv = path_csv.replace("table_name", "f_rpt_study_country_details")
    write_path_csv = write_path_csv + "_csv/"
    f_rpt_study_country_final.coalesce(1).write.option("header", "false").option("sep", "|").option('quote',
                                                                                                    '"') \
        .option('escape', '"').option(
        "multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_country partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_country
""")

if f_rpt_country.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_country as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_country")

##############create drug reporting table

drug_country_status = spark.sql("""select distinct drug_id,drug_nm, country_name1 as country_name,
country_status1 as
country_status from sanofi_ctfo_datastore_app_commons_$$db_env.d_drug lateral view
posexplode(split(country_nm,"\\\|"))one
as pos1,country_name1 lateral view posexplode(split(country_status,"\\\|"))two as pos2,
country_status1 where pos1=pos2""")

drug_country_status.write.mode("overwrite").saveAsTable("drug_country_status")

### creating temp disease mapping table having only distinct disease name and standard disease names

temp_disease_mapping_df = spark.sql("""
SELECT
disease, standard_disease
FROM disease_mapping
GROUP BY 1,2
""")
temp_disease_mapping_df.createOrReplaceTempView("temp_disease_mapping")

drug_disease_status = spark.sql("""
select distinct  drug_id,drug_nm, coalesce(dis_map2.standard_disease, dis_map.standard_disease, 'Other') as disease_name, disease_status
from
(select distinct  drug_id,drug_nm, disease_name1 as disease_name,
disease_status1 as
disease_status from sanofi_ctfo_datastore_app_commons_$$db_env.d_drug lateral view
posexplode(split(regexp_replace(disease_nm,"\;","\\\|"),"\\\|"))one
as pos1,disease_name1 lateral view
  posexplode(split(regexp_replace(disease_status,"\;","\\\|"),"\\\|"))two as pos2,disease_status1
  where pos1=pos2
  ) temp1
left join temp_disease_mapping dis_map on trim(lower(temp1.disease_name)) = trim(lower(dis_map.disease))
left join temp_disease_mapping dis_map2 on trim(lower(temp1.disease_name)) = trim(lower(dis_map2.standard_disease))
  """)

drug_disease_status.write.mode("overwrite").saveAsTable("drug_disease_status")

f_rpt_drug_details = spark.sql(""" select distinct a.drug_id,
a.drug_nm,
a.global_status,
a.originator,
a.originator_status,
a.originator_country,
c.disease_name,
c.disease_status,
map_1.standard_country as country,
map_1.sanofi_cluster as sanofi_cluster,
b.country_status,
map_1.iso2_code as country_code,
Case when (map_2.therapeutic_area is null or map_2.therapeutic_area = 'Other')
        and (map_3.therapeutic_area is not null or map_3.therapeutic_area != 'Other')
        then map_3.therapeutic_area
        else coalesce(map_2.therapeutic_area, 'Other')
        end as therapeutic_class_nm,
a.drug_name_synonyms,
a.delivery_route,
map_1.region
from sanofi_ctfo_datastore_app_commons_$$db_env.d_drug a
left outer join drug_country_status b on a.drug_id =b.drug_id
left outer join drug_disease_status c on a.drug_id =c.drug_id
left join country_mapping map_1 on lower(trim(b.country_name)) = lower(trim(map_1.standard_country))
left join ta_mapping map_2 on lower(trim(map_2.standard_disease)) = lower(trim(c.disease_name))
left join ta_mapping map_3 on lower(trim(map_3.disease_name)) = lower(trim(c.disease_name))
""")

f_rpt_drug_details = f_rpt_drug_details.dropDuplicates()
f_rpt_drug_details.write.mode("overwrite").saveAsTable("f_rpt_drug_details")

write_path = path.replace("table_name", "f_rpt_drug_details")
write_path_csv = path_csv.replace("table_name", "f_rpt_drug_details")
write_path_csv = write_path_csv + "_csv/"
f_rpt_drug_details.write.format("parquet").mode("overwrite").save(path=write_path)

'''
f_rpt_drug_details_final = spark.sql("""
select f_rpt_drug_details.*, "$$data_dt" as pt_data_dt, "$$cycle_id" as pt_cycle_id from f_rpt_drug_details
""")
f_rpt_drug_details_final.createOrReplaceTempView("f_rpt_drug_details_final")
'''
if "$$flag" == "Y":
    f_rpt_drug_details.coalesce(1).write.option("header", "false").option("sep", "|") \
        .option('quote', '"').option(
        'escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_drug_details partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_drug_details
""")

if f_rpt_drug_details.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_drug_details as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_fa_$$db_env.f_rpt_drug_details")




