################################# Module Information ######################################
#  Module Name         : Investigator records combine
#  Purpose             : This will execute queries for creating xref table
#  Pre-requisites      : Source table required: temp_all_inv_dedupe_results_final
#  Last changed on     : 16-06-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# Prepare data for xref tables
############################################################################################
import pyspark
import sys
import unidecode
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
import re

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + '/applications/commons/temp/mastering/investigator/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

temp_all_inv_data = spark.sql \
    (""" select * from $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_data """).registerTempTable \
    ('temp_all_inv_data')

sys.path.insert(1, CommonConstants.EMR_CODE_PATH + '/code')
from DedupeUtility import maintainGoldenID
from DedupeUtility import KMValidate
from MySQLConnectionManager import MySQLConnectionManager
from pyspark.sql.functions import *
from ConfigUtility import JsonConfigUtility
import MySQLdb

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/'
                                  + CommonConstants.ENVIRONMENT_CONFIG_FILE)
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, 'mysql_db'])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

from pyjarowinkler import distance

standard_country_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/standard_country_mapping.csv".format(bucket_path=bucket_path))
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")
standard_country_mapping_temp = spark.sql("""
select distinct * from (select   country, standard_country from standard_country_mapping 
union 
select standard_country, standard_country from standard_country_mapping)
""")
standard_country_mapping_temp.createOrReplaceTempView("standard_country_mapping_temp")

cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "{bucket_path}/uploads/$$client_name_cluster_pta_mapping.csv".format(bucket_path=bucket_path))
cluster_pta_mapping.createOrReplaceTempView("cluster_pta_mapping")

country_standardization = spark.sql("""select coalesce(cs.standard_country,'Other') as standard_country, cs.country,
scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.region_code,scpm.post_trial_flag,
"Not Available" as iec_timeline,
"Not Available" as regulatory_timeline,
scpm.post_trial_details
from (select distinct *
	from standard_country_mapping_temp)  cs
left join
		cluster_pta_mapping scpm
		on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
country_standardization.createOrReplaceTempView("country_standardization")


def fuzzy_match(str1, str2):
    return distance.get_jaro_distance(str1, str2)


spark.udf.register("fuzzy_match", fuzzy_match)


def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


spark.udf.register("get_ascii_value", get_ascii_value)


def str_split_reg_filter(str_val, sep, reg_exp, flag):
    if not str_val:
        return str_val
    arr = str_val.split(sep)
    pattern = re.compile(reg_exp)
    arr_final = [i for i in arr if pattern.match(i)]
    if flag == 'N':
        arr_final = list(set(arr) - set(arr_final))
    str_final_val = sep.join(i for i in arr_final)
    return str_final_val


spark.udf.register("str_split_reg_filter", str_split_reg_filter)


def standardize_names_all(name_value, para):
    try:
        alutations = ['Dr.', 'Dr', 'DR.', 'DR', 'dr.', 'dr', 'prof.', 'prof', 'PROF.', 'PROF', 'Prof.', 'Prof', 'med.',
                      'med', 'MED.', 'MED', 'Med.', 'Med', 'Dr./Prof.', 'Study Coordinator', 'Frau Dr.', 'Herr Dr.',
                      'Herr Priv.-Doz. Dr.', 'Herr Prof. Dr.', 'Frau']
        saltutations_present = ""
        column_name_list_0 = name_value.split(" ")
        column_name_list_1 = name_value.split(" ")
        for col_val in column_name_list_1:
            if col_val in salutations:
                saltutations_present = saltutations_present + col_val + ' '
                column_name_list_0.remove(col_val)
        if len(column_name_list_0) > 0:
            column_name_list = column_name_list_0
            first_name = column_name_list[0]
            first_name_final = saltutations_present + '' + first_name
            column_name_list.remove(first_name)
            if len(column_name_list) > 0:
                middle_name_final = column_name_list[0]
                column_name_list.remove(middle_name_final)
                if len(column_name_list) > 0:
                    last_name_final = ' '.join(column_name_list)
                else:
                    last_name_final = middle_name_final
                    middle_name_final = None
        if para == 'first':
            return first_name_final
        elif para == 'middle':
            return middle_name_final
        elif para == 'last':
            return last_name_final
    except:
        return None


spark.udf.register('standardize_names_all', standardize_names_all)

# citeline data preparation for investigator

temp_tascan_inv_data = spark.sql("""select
    'tascan' as data_src_nm,
    concat_ws(' ', NULLIF(trim(inv.investigator_first_name),''), NULLIF(trim(inv.investigator_middle_initial),''), 
    NULLIF(trim(inv.investigator_last_name),'')) as investigator_name,
    inv.investigator_phone_numbers as investigator_number,
    inv.investigator_emails as investigator_email,
    inv.investigator_speciality as investigator_primary_specialty,
    CAST(CAST(null as string) as string) as investigator_secondary_specialty,
    inv.investigator_degree as investigator_title,
    inv.investigator_p_organization_name as investigator_affiliation,
    CAST(null as string) as investigator_debarred_flag,
    inv.investigator_location_city as investigator_city,
    inv.investigator_location_state as investigator_state,
    inv.investigator_location_post_code as investigator_zip,
    inv.investigator_location_country as investigator_country,
    concat_ws(' ',NULLIF(trim(inv.investigator_location_street_address),''), NULLIF(trim(inv.investigator_location_city),''),
    NULLIF(trim(inv.investigator_location_state),''),NULLIF(trim(inv.investigator_location_country),''),
    NULLIF(trim(inv.investigator_location_post_code),'')) as investigator_address,
    inv.investigator_id as src_investigator_id
from $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_investigator inv
left join temp_all_inv_data investigator 
on lower(trim(inv.investigator_id))=lower(trim(investigator.src_inv_id))  where lower(trim(investigator.data_src_nm))='tascan'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
""")
temp_tascan_inv_data.registerTempTable('temp_tascan_inv_data')

temp_citeline_inv_data = spark.sql("""
select
    'citeline' as data_src_nm,
    concat_ws(' ', NULLIF(trim(inv.investigator_first_name),''), NULLIF(trim(inv.investigator_middle_initial),''), 
    NULLIF(trim(inv.investigator_last_name),'')) as investigator_name,
    regexp_replace(inv.investigator_phone_numbers,'\;','\\\|') as investigator_phone,
    inv.investigator_emails as investigator_email,
    inv.investigator_specialties as investigator_primary_specialty,
    CAST(CAST(null as string) as string) as investigator_secondary_specialty,
    inv.investigator_degrees as investigator_title,
    inv.investigator_p_organization_name as investigator_affiliation,
    CAST(null as string) as investigator_debarred_flag,
    inv.investigator_location_city as investigator_city,
    inv.investigator_location_state as investigator_state,
    inv.investigator_location_post_code as investigator_zip,
    inv.investigator_location_country as investigator_country,
    concat_ws(' ',NULLIF(trim(inv.investigator_location_street_address),''), NULLIF(trim(inv.investigator_location_city),''),
    NULLIF(trim(inv.investigator_location_state),''),NULLIF(trim(inv.investigator_location_country),''),
    NULLIF(trim(inv.investigator_location_post_code),'')) as investigator_address,
    inv.investigator_id as src_investigator_id
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_investigator inv
left join temp_all_inv_data investigator 
on lower(trim(inv.investigator_id))=lower(trim(investigator.src_inv_id))  where lower(trim(investigator.data_src_nm))='citeline'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
""")
temp_citeline_inv_data.registerTempTable('temp_citeline_inv_data')

# AACT data preparation for investigator

inv_aact_all_data = spark.sql("""
select
        'aact' as data_src_nm,
        trim(split(inv.name,',')[0]) as investigator_name,
        if(cont.phone='null',null,cont.phone) as investigator_phone,
        if(cont.email='null',null,cont.email) as investigator_email,
        CAST(null as string) as investigator_primary_specialty,
        trim(split(inv.name,",")[1]) as investigator_title,
        CAST(null as string) as investigator_city,
        CAST(null as string) as investigator_state,
        CAST(null as string) as investigator_zip,
        CAST(null as string) as investigator_country,
        CAST(null as string) as investigator_address,
        inv.id as investigator_id,
        nct_id
from (
select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_facility_investigators
where lower(trim(role)) in ('principal investigator','sub-investigator')) inv
left outer join (
        select
        name,facility_id,
        concat_ws('\|',collect_set(phone)) as phone,
        concat_ws('\|',collect_set(email)) as email
from $$client_name_ctfo_datastore_staging_$$db_env.aact_facility_contacts
group by 1,2) cont
on lower(trim(inv.name))=lower(trim(cont.name))
and lower(trim(inv.facility_id))=lower(trim(cont.facility_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
inv_aact_all_data.registerTempTable('inv_aact_all_data')

temp_aact_inv_master_data_affl_0 = spark.sql("""
select /*+ broadcast(aff) */
    inv.data_src_nm,
    inv.investigator_name,
    inv.investigator_phone,
    inv.investigator_email,
    inv.investigator_primary_specialty,
    inv.investigator_title,
    inv.investigator_city,
    inv.investigator_state,
    inv.investigator_zip,
    inv.investigator_country,
    inv.investigator_address,
    inv.investigator_id,
    inv.nct_id,
    aff.affiliation
from inv_aact_all_data inv
inner join (select nct_id, name, affiliation from
$$client_name_ctfo_datastore_staging_$$db_env.aact_responsible_parties where
lower(trim(responsible_party_type)) <> "sponsor" and name <> "" and name is not null group by 1,2,3)
 aff
on lower(trim(inv.nct_id))=lower(trim(aff.nct_id))
where fuzzy_match(inv.investigator_name,aff.name)>(8/10)
and inv.investigator_name is not null  and inv.investigator_name <> ''
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
""")
temp_aact_inv_master_data_affl_0.registerTempTable("temp_aact_inv_master_data_affl_0")

temp_aact_inv_master_data_affl = spark.sql("""
select
    a.data_src_nm,
    a.investigator_name,
    a.investigator_phone,
    a.investigator_email,
    a.investigator_primary_specialty,
    a.investigator_title,
    a.investigator_city,
    a.investigator_state,
    a.investigator_zip,
    a.investigator_country,
    a.investigator_address,
    a.investigator_id,
    a.nct_id,
    CAST(null as string) as affiliation
from inv_aact_all_data a
left outer join temp_aact_inv_master_data_affl_0 b
on lower(trim(a.investigator_id)) = lower(trim(b.investigator_id))
where b.investigator_id is null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
union
select *
from temp_aact_inv_master_data_affl_0
""")
temp_aact_inv_master_data_affl.registerTempTable("temp_aact_inv_master_data_affl")

temp_aact_inv_data = spark.sql("""
select
    inv.data_src_nm,
    inv.investigator_name,
    regexp_replace(inv.investigator_phone,'\;','\\\|') investigator_phone,
    inv.investigator_email,
    inv.investigator_primary_specialty,
    CAST(null as string) as investigator_secondary_specialty,
    inv.investigator_title as investigator_title,
    inv.affiliation as investigator_affiliation,
    'N' as investigator_debarred_flag,
    inv.investigator_city,
    inv.investigator_state,
    inv.investigator_zip,
    inv.investigator_country,
    inv.investigator_address,
    inv.investigator_id as src_investigator_id
from temp_aact_inv_master_data_affl inv
left join  temp_all_inv_data investigator 
on lower(trim(inv.investigator_id))=lower(trim(investigator.src_inv_id))  where lower(trim(investigator.data_src_nm))='aact'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
""")
temp_aact_inv_data.registerTempTable('temp_aact_inv_data')

temp_dqs_inv_data_rnk = spark.sql("""select
'ir' as data_src_nm,
max(concat_ws(' ',NULLIF(trim(person.first_name),''),NULLIF(trim(person.last_name),'')))  as investigator_name,
concat_ws('\|',sort_array(collect_set(case when lower(trim(person.email)) like '%dd%proxy%' then null else NULLIF(trim(person.email),'') end),true)) as investigator_email,
CAST(null as string) as investigator_primary_specialty,
CAST(null as string) as investigator_secondary_specialty,
CAST(null as string) as investigator_title,
person.facility_name as investigator_affiliation,
debarred as investigator_debarred_flag,
person_city as investigator_city,
person_state as investigator_state,
CAST(null as string) as investigator_zip,
person_country as investigator_country,
CAST(null as string) as investigator_address,
trim(person.person_golden_id) as src_investigator_id,
First(trim(person.person_phone)) as investigator_phone,
person.person_golden_id,
person.site_open_dt,
row_number() over (partition by person.person_golden_id order by person.site_open_dt desc ) as rnk
from  $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir person
group  by 1,4,5,6,7,8,9,10,11,12,13,14,16,17""")
temp_dqs_inv_data_rnk = temp_dqs_inv_data_rnk.dropDuplicates()
temp_dqs_inv_data_rnk.write.mode('overwrite').saveAsTable('temp_dqs_inv_data_rnk')
# temp_dqs_inv_data_rnk.registerTempTable('temp_dqs_inv_data_rnk')

temp_dqs_inv_data = spark.sql("""
select
inv.data_src_nm,
inv.investigator_name,
regexp_replace(inv.investigator_phone,'\;','\\\|') investigator_phone,
inv.investigator_email,
inv.investigator_primary_specialty,
inv.investigator_secondary_specialty,
inv.investigator_title,
inv.investigator_affiliation,
inv.investigator_debarred_flag,
inv.investigator_city,
inv.investigator_state,
inv.investigator_zip,
inv.investigator_country,
inv.investigator_address,
inv.src_investigator_id
from temp_dqs_inv_data_rnk inv 
left join temp_all_inv_data investigator 
on lower(trim(inv.src_investigator_id))=lower(trim(investigator.src_inv_id))  where lower(trim(investigator.data_src_nm))='ir'
and inv.rnk=1
 """)
temp_dqs_inv_data = temp_dqs_inv_data.dropDuplicates()
temp_dqs_inv_data.registerTempTable("temp_dqs_inv_data")

# ctms data preparation for investigator meta_is_current is not in ctms_site_staff_specialty
ctms_person_spec = spark.sql("""SELECT src_investigator_id FROM $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator 
group by 1 having count(distinct(investigator_specialty))<=2""")
ctms_person_spec = ctms_person_spec.dropDuplicates()
ctms_person_spec.registerTempTable('ctms_person_spec')

ctms_person_spec_final = spark.sql("""select src_investigator_id,
concat_ws('\|',sort_array(collect_set(NULLIF(trim(investigator_specialty),'')),true)) as investigator_specialty
from (select a.src_investigator_id,b.investigator_specialty from ctms_person_spec a left join 
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator b on lower(trim(a.src_investigator_id))=lower(trim(b.src_investigator_id)))
group by 1  """)
ctms_person_spec_final = ctms_person_spec_final.dropDuplicates()
ctms_person_spec_final.registerTempTable('ctms_person_spec_final')

temp_ctms_inv_data = spark.sql("""
select
    'ctms' as data_src_nm,
    a.src_investigator_name as investigator_name,
    regexp_replace(a.investigator_phone,'\;','\\\|') investigator_phone,
    a.investigator_email as investigator_email,
    b.investigator_specialty as investigator_primary_specialty,
    CAST(null as string) as investigator_secondary_specialty,
    CAST(null as string) as investigator_title,
    CAST(null as string) as investigator_affiliation,
    a.investigator_debarred_flag as investigator_debarred_flag,
    a.investigator_city as investigator_city,
    a.investigator_state as investigator_state,
    a.investigator_zip as investigator_zip,
    a.investigator_country as investigator_country,
    a.investigator_address as investigator_address,
    a.src_investigator_id as src_investigator_id
from (select src_investigator_name,investigator_email,src_investigator_id,investigator_debarred_flag,investigator_address,
investigator_phone,investigator_city, investigator_state,investigator_zip,investigator_country
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator group by 1,2,3,4,5,6,7,8,9,10) a
left join
(select src_investigator_id,investigator_specialty from ctms_person_spec_final  group by 1,2) b
on lower(trim(a.src_investigator_id))=lower(trim(b.src_investigator_id))
left join temp_all_inv_data investigator 
on lower(trim(a.src_investigator_id))=lower(trim(investigator.src_inv_id))  where lower(trim(investigator.data_src_nm))='ctms'
group  by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
""")
# temp_ctms_inv_data.registerTempTable('temp_ctms_inv_data')
temp_ctms_inv_data.registerTempTable('temp_ctms_inv_data')

temp_all_inv_data_final_temp = spark.sql("""
select * from temp_citeline_inv_data
union
select * from temp_aact_inv_data
union
select * from temp_dqs_inv_data
union
select * from temp_ctms_inv_data
union
select * from temp_tascan_inv_data
""")
temp_all_inv_data_final_temp.registerTempTable('temp_all_inv_data_final_temp')

temp_all_inv_data_final = spark.sql("""
select data_src_nm,
investigator_name,
case when trim(investigator_phone) =''  then null else 
trim(investigator_phone) end as  investigator_phone,
case when trim(investigator_email) =''  then null else 
trim(investigator_email) end as  investigator_email,
case when trim(investigator_primary_specialty) =''  then null else 
trim(investigator_primary_specialty) end as  investigator_primary_specialty,
case when trim(investigator_secondary_specialty) =''  then null else 
trim(investigator_secondary_specialty) end as  investigator_secondary_specialty,
case when trim(investigator_title) =''  then null else 
trim(investigator_title) end as  investigator_title,
case when trim(investigator_affiliation) =''  then null else 
trim(investigator_affiliation) end as  investigator_affiliation,
case when trim(investigator_debarred_flag) =''  then null else 
trim(investigator_debarred_flag) end as  investigator_debarred_flag,
case when trim(investigator_city) =''  then null else 
trim(investigator_city) end as  investigator_city,
case when trim(investigator_state) =''  then null else 
trim(investigator_state) end as  investigator_state,
case when trim(investigator_zip) =''  then null else 
trim(investigator_zip) end as  investigator_zip,
case when trim(investigator_country) =''  then null else 
trim(investigator_country) end as  investigator_country,
case when trim(investigator_address) =''  then null else 
trim(investigator_address) end as  investigator_address,
src_investigator_id from
(select 
data_src_nm,
investigator_name,
case when (investigator_phone like '%?%' or investigator_phone like '-%') then trim(LEADING '-' from regexp_replace(investigator_phone,'\\\?','')) 
else regexp_replace(investigator_phone,"['`~&=]",'')
end as investigator_phone,
case when investigator_email like '%"%' then regexp_replace(investigator_email,'"','')
when investigator_email like '%]%' then regexp_replace(investigator_email,']','')
else regexp_replace(investigator_email,"['*(){}:?=]",'')
end as investigator_email,
investigator_primary_specialty,
investigator_secondary_specialty,
investigator_title,
investigator_affiliation,
investigator_debarred_flag,
investigator_city,
investigator_state,
investigator_zip,
investigator_country,
investigator_address,
src_investigator_id
 from temp_all_inv_data_final_temp)
""")
temp_all_inv_data_final.write.mode('overwrite').saveAsTable('temp_all_inv_data_final')
write_path = path.replace('table_name', 'temp_all_inv_data_final')
temp_all_inv_data_final.repartition(100).write.mode('overwrite').parquet(write_path)

cursor.execute(
    """select cycle_id ,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = 1000 order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db))

fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
print(latest_stable_data_date, latest_stable_cycle_id)
temp_all_inv_dedupe_results_final_0 = spark.read.parquet(
    "{bucket_path}/applications/commons/temp/mastering/investigator/temp_all_inv_dedupe_results_final/pt_data_dt={}/pt_cycle_id={}/".format(
        latest_stable_data_date, latest_stable_cycle_id, bucket_path=bucket_path))
temp_all_inv_dedupe_results_final_0.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results_final_0')

# Modified code to use KMValidate function for incorporating KM input

# Writing base table on HDFS to make it available for the function to fetch data
temp_all_inv_dedupe_results_final_1 = spark.sql("""
select * from
temp_all_inv_dedupe_results_final_0
""")

temp_all_inv_dedupe_results_final_1.write.mode('overwrite'). \
    saveAsTable('temp_all_inv_dedupe_results_final_1')

final_table = KMValidate('temp_all_inv_dedupe_results_final_1', 'investigator', '$$data_dt', '$$cycle_id', '$$s3_env')

cursor.execute(
    """select cycle_id ,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = 2000 order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db))

fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
print(latest_stable_data_date, latest_stable_cycle_id)
prev_xref_src_inv= spark.read.parquet(
    "{bucket_path}/applications/commons/dimensions/xref_src_inv/pt_data_dt={}/pt_cycle_id={}/".format(
        latest_stable_data_date, latest_stable_cycle_id, bucket_path=bucket_path))
prev_xref_src_inv.write.mode('overwrite').saveAsTable('prev_xref_src_inv')


# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("Create Empty DataFrame") \
#     .getOrCreate()
#
# # Create the schema based on the existing DataFrame xref_src_inv1
# schema = StructType([
#     StructField("data_src_nm", StringType(), True),
#     StructField("src_investigator_id", StringType(), True),
#     StructField("ctfo_investigator_id", StringType(), True),
#     StructField("investigator_name", StringType(), True),
#     StructField("investigator_phone", StringType(), True),
#     StructField("investigator_email", StringType(), True),
#     StructField("investigator_specialty", StringType(), True),
#     StructField("investigator_city", StringType(), True),
#     StructField("investigator_state", StringType(), True),
#     StructField("investigator_zip", StringType(), True),
#     StructField("investigator_country", StringType(), True),
#     StructField("investigator_address", StringType(), True)
# ])
#
# # Create an empty DataFrame with the specified schema
# prev_xref_src_inv = spark.createDataFrame([], schema)
#
#
# # Save the empty DataFrame as a table if needed
# prev_xref_src_inv.write.mode('overwrite').saveAsTable('prev_xref_src_inv')
#

temp_inv_id_clusterid = spark.sql("""
        select
            a.final_cluster_id as cluster_id,
            concat("ctfo_inv_", row_number() over(order by null) + coalesce(temp.max_id,0)) as investigator_id
        from
        (select distinct final_cluster_id from """ + final_table + """ where round(final_score,2) >=$$threshold) a cross join
        (select max(cast(split(ctfo_investigator_id,'_')[2] as bigint))  as max_id from prev_xref_src_inv) temp""")
temp_inv_id_clusterid = temp_inv_id_clusterid.dropDuplicates()
temp_inv_id_clusterid.write.mode('overwrite').saveAsTable('temp_inv_id_clusterid')
# temp_inv_id_clusterid.registerTempTable('temp_inv_id_clusterid')
write_path = path.replace('table_name', 'temp_inv_id_clusterid')
temp_inv_id_clusterid.repartition(100).write.mode('overwrite').parquet(write_path)

'''
temp_inv_id_clusterid = spark.sql("""
select concat('ctfo_inv_',row_number() over(order by null)) as investigator_id,
       temp.final_cluster_id as cluster_id
from  (select distinct final_cluster_id
from   """ + final_table + """
where  final_cluster_id is not null
and round(cast(final_score as double), 2) >=$$threshold)temp
""")
temp_inv_id_clusterid = temp_inv_id_clusterid.dropDuplicates()
temp_inv_id_clusterid.write.mode('overwrite').saveAsTable('temp_inv_id_clusterid')
'''



temp_xref_inv_1 = spark.sql("""	
select	
    inv.uid,	
    inv.hash_uid,	
    inv.ds_inv_id,	
    ctoid.investigator_id as inv_id,	
    inv.data_src_nm,	
    inv.name,	
    inv.phone,	
    inv.email,	
    inv.score,
    inv.final_KM_comment as final_KM_comment	
from 
(select distinct uid, final_cluster_id as cluster_id, hash_uid, final_score as score, name, phone, email, ds_inv_id, data_src_nm ,final_KM_comment
from """ + final_table + """ where round(coalesce(final_score,0),2) >= $$threshold) inv
inner join
temp_inv_id_clusterid ctoid
on lower(trim(ctoid.cluster_id))=lower(trim(inv.cluster_id))
""")
# temp_xref_inv_1.registerTempTable('temp_xref_inv_1')
temp_xref_inv_1.write.mode('overwrite').saveAsTable('temp_xref_inv_1')
write_path = path.replace('table_name', 'temp_xref_inv_1')
temp_xref_inv_1.repartition(100).write.mode('overwrite').parquet(write_path)


temp_xref_inv_2 = spark.sql("""
select /*+ broadcast(temp) */
    inv.uid,
    inv.hash_uid,
    inv.ds_inv_id,
    concat('ctfo_inv_', row_number() over(order by null) + coalesce(temp.max_id,0)) as inv_id,
    inv.data_src_nm,
    inv.name,
    inv.phone,
    inv.email,
    inv.score,
    inv.final_KM_comment as final_KM_comment
from 
(select distinct uid,final_cluster_id as cluster_id, hash_uid, final_score as score, name, phone, email, ds_inv_id, data_src_nm,final_KM_comment
    from """ + final_table + """ where round(coalesce(final_score,0),2) < $$threshold) inv
cross join
    (select max(cast(split(inv_id,"inv_")[1] as bigint)) as max_id from temp_xref_inv_1) temp
""")
temp_xref_inv_2 = temp_xref_inv_2.dropDuplicates()
# temp_xref_inv_2.registerTempTable('temp_xref_inv_2')
temp_xref_inv_2.write.mode('overwrite').saveAsTable('temp_xref_inv_2')
write_path = path.replace('table_name', 'temp_xref_inv_2')
temp_xref_inv_2.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_investigator_final_0 = spark.sql("""
select *
from temp_xref_inv_1
union
select *
from temp_xref_inv_2
""")
temp_xref_investigator_final_0 = temp_xref_investigator_final_0.dropDuplicates()
# temp_xref_investigator_final_0.registerTempTable('temp_xref_investigator_final_0')
temp_xref_investigator_final_0.write.mode('overwrite').saveAsTable('temp_xref_investigator_final_0')

# Calling maintainGoldenID function to preserve IDs across runs

if '$$Prev_Data' == 'N':
    temp_xref_inv = spark.sql("""
    select *
    from temp_xref_investigator_final_0
    """)
else:
    final_table = maintainGoldenID('temp_xref_investigator_final_0', 'inv', '$$process_id', '$$Prev_Data', '$$s3_env')
    temp_xref_inv = spark.sql("""
    select
        base.uid,
        base.hash_uid,
        base.data_src_nm,
        base.ds_inv_id as ds_inv_id,
        case when (lower(trim(final_KM_comment)) is null or lower(trim(final_KM_comment))  ='' or lower(trim(final_KM_comment))  ='null'
        ) then func_opt.ctfo_inv_id
        else base.inv_id end as inv_id,
        base.name,
        base.phone,
        base.email,
        base.score
    from
    temp_xref_investigator_final_0 base
    left outer join
    """ + final_table + """ func_opt
    on lower(trim(base.hash_uid)) = lower(trim(func_opt.hash_uid))
    """)
temp_xref_inv = temp_xref_inv.dropDuplicates()
# temp_xref_inv.registerTempTable('temp_xref_inv')
temp_xref_inv.write.mode('overwrite').saveAsTable('temp_xref_inv')
write_path = path.replace('table_name', 'temp_xref_inv')
temp_xref_inv.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_inv1=spark.sql("""
select e.uid, 
e.hash_uid,
e.data_src_nm,
f.ds_inv_id,
e.inv_id, 
e.name,
e.phone,
e.email,
e.score
from temp_xref_inv e left outer join
(select distinct hash_uid, concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_inv_ids),'')),true)) AS ds_inv_id from temp_xref_inv
lateral view outer explode(split(ds_inv_id,'\;'))one as ds_inv_ids group by 1) f
on lower(trim(e.hash_uid))=lower(trim(f.hash_uid))
""")
temp_xref_inv1.write.mode('overwrite').saveAsTable('temp_xref_inv1')


xref_src_temp_inv1=spark.sql("""select a.*, b.cnt_inv_id from temp_xref_inv1 a left join 
(select distinct 
hash_uid, count(distinct inv_id) as cnt_inv_id from temp_xref_inv1 group by 1 having count(distinct inv_id)>1) b
on lower(trim(a.hash_uid))=lower(trim(b.hash_uid))
""")
xref_src_temp_inv1.write.mode('overwrite').saveAsTable("xref_src_temp_inv1")

xref_src_temp1_inv1=spark.sql("""select c.*, d.cnt_hash from xref_src_temp_inv1 c left join
(select distinct inv_id, count(distinct hash_uid) cnt_hash from temp_xref_inv1 where inv_id in (select inv_id from xref_src_temp_inv1)
group by 1) d
on c.inv_id=d.inv_id
""")
xref_src_temp1_inv1.write.mode('overwrite').saveAsTable("xref_src_temp1_inv1")

xref_src_inv_abc1 = spark.sql("""
select data_src_nm, ds_inv_id, hash_uid, uid, score,
inv_id,name,
phone,
email from
(select *, row_number() over (partition by hash_uid order by cnt_hash desc, inv_id desc) as rnk
from xref_src_temp1_inv1) where rnk=1""")
xref_src_inv_abc1.write.mode('overwrite').saveAsTable('xref_src_inv_abc1')


temp_xref_inv_int = spark.sql("""
select
        split(ds_inv_id_new,'_')[0] AS data_src_nm,
        SUBSTRING(ds_inv_id_new,(instr(ds_inv_id_new, '_')+1)) AS src_investigator_id,
        inv_id as investigator_id,
        uid,
        hash_uid,
        name,
        phone,
        email,
        score
from xref_src_inv_abc1 lateral view outer explode(split(ds_inv_id,'\;'))one as ds_inv_id_new
where lower(split(ds_inv_id_new,'_')[0]) <> 'who'
""")
temp_xref_inv_int = temp_xref_inv_int.dropDuplicates()
# temp_xref_inv_int.registerTempTable('temp_xref_inv_int')
temp_xref_inv_int.write.mode('overwrite').saveAsTable('temp_xref_inv_int')
write_path = path.replace('table_name', 'temp_xref_inv_int')
temp_xref_inv_int.repartition(100).write.mode('overwrite').parquet(write_path)
# temp_xref_inv_int.registerTempTable("temp_xref_inv_int")

temp_xref_inv_int_temp = spark.sql("""
select
b.data_src_nm,
b.src_investigator_id,
a.investigator_id,
a.uid,
a.hash_uid,
a.name,
a.phone,
a.email,
a.score
from temp_xref_inv_int a
inner join
(select
src_investigator_id,data_src_nm
from  temp_xref_inv_int group by 1,2  having count(distinct uid)<2) b 
on lower(trim(a.src_investigator_id))=lower(trim(b.src_investigator_id)) AND
lower(trim(a.data_src_nm))=lower(trim(b.data_src_nm))
group by 1,2,3,4,5,6,7,8,9
""")
temp_xref_inv_int_temp = temp_xref_inv_int_temp.dropDuplicates()
temp_xref_inv_int_temp.write.mode('overwrite').saveAsTable('temp_xref_inv_int_temp')
# temp_xref_inv_int_temp.registerTempTable("temp_xref_inv_int_temp")


temp_xref_inv_int_1 = spark.sql("""
select
data_src_nm,
src_investigator_id,
investigator_id,
uid,
hash_uid,
name,
phone,
email,
score
from
temp_xref_inv_int_temp
group by 1,2,3,4,5,6,7,8,9
""")
temp_xref_inv_int_1 = temp_xref_inv_int_1.dropDuplicates()
# temp_xref_inv_int_1.registerTempTable("temp_xref_inv_int_1")
temp_xref_inv_int_1.write.mode('overwrite').saveAsTable('temp_xref_inv_int_1')

temp_xref_inv_int_data_src_rnk = spark.sql("""
select temp_xref_inv_int.*,
        case when data_src_nm like '%ctms%' then 1
        when data_src_nm like '%ir%' then 2
        when data_src_nm like '%citeline%' then 3
        when data_src_nm like '%tascan%' then 4
    when data_src_nm like '%aact%' then 5
else 6 end as datasource_rnk from  temp_xref_inv_int""")
temp_xref_inv_int_data_src_rnk.write.mode('overwrite').saveAsTable('temp_xref_inv_int_data_src_rnk')
# temp_xref_inv_int_data_src_rnk.registerTempTable("temp_xref_inv_int_data_src_rnk")


# Giving rank to records having multiple distinct UID
temp_xref_inv_int_2 = spark.sql("""
select data_src_nm,
src_investigator_id,
investigator_id,
uid,
hash_uid,
name,
phone,
email,
score,
datasource_rnk,
ROW_NUMBER() over (partition by src_investigator_id,data_src_nm order by datasource_rnk asc) as rnk
from
temp_xref_inv_int_data_src_rnk where src_investigator_id in
(select src_investigator_id from (select src_investigator_id,data_src_nm
from  temp_xref_inv_int_data_src_rnk group by 1,2  having count(distinct uid)>1))
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_inv_int_2 = temp_xref_inv_int_2.dropDuplicates()
temp_xref_inv_int_2.write.mode('overwrite').saveAsTable('temp_xref_inv_int_2')
# temp_xref_inv_int_2.registerTempTable('temp_xref_inv_int_2')
write_path = path.replace('table_name', 'temp_xref_inv_int_2')
temp_xref_inv_int_2.repartition(100).write.mode('overwrite').parquet(write_path)

# temp_xref_inv_int_2.registerTempTable("temp_xref_inv_int_2")

temp_xref_inv_int_3 = spark.sql("""
select
data_src_nm,
src_investigator_id,
investigator_id,
uid,
hash_uid,
name,
phone,
email,
score
from
temp_xref_inv_int_2  where rnk = 1
group by 1,2,3,4,5,6,7,8,9
""")
temp_xref_inv_int_3 = temp_xref_inv_int_3.dropDuplicates()
temp_xref_inv_int_3.registerTempTable('temp_xref_inv_int_3')
write_path = path.replace('table_name', 'temp_xref_inv_int_3')
temp_xref_inv_int_3.repartition(100).write.mode('overwrite').parquet(write_path)

# Union of records with unique UID and prec records having distinct multi. UID
temp_xref_inv_int_4 = spark.sql("""
select * from temp_xref_inv_int_3
union
select * from temp_xref_inv_int_1
""")
temp_xref_inv_int_4 = temp_xref_inv_int_4.dropDuplicates()
temp_xref_inv_int_4.write.mode('overwrite').saveAsTable('temp_xref_inv_int_4')
# temp_xref_inv_int_4.registerTempTable('temp_xref_inv_int_4')
write_path = path.replace('table_name', 'temp_xref_inv_int_4')
temp_xref_inv_int_4.repartition(100).write.mode('overwrite').parquet(write_path)

# joining data with golden IDs
xref_src_inv_temp_1 = spark.sql("""
select
    xref_inv.data_src_nm,
    cast(xref_inv.uid as bigint) as uid,
    xref_inv.hash_uid,
    xref_inv.src_investigator_id,
    xref_inv.investigator_id as ctfo_investigator_id,
    cast(xref_inv.score as double) as score,
    investigator_name,
    str_split_reg_filter(all_inv_data.investigator_phone, '|', ".*[0-9].*", "Y") as investigator_phone,
    str_split_reg_filter(all_inv_data.investigator_email, '|', ".*@.*", "Y") as investigator_email,
    all_inv_data.investigator_primary_specialty as investigator_specialty,
    case when all_inv_data.investigator_city in ('null', '', 'na', 'NA', 'N/A')
        then null else all_inv_data.investigator_city end as investigator_city,
    case when all_inv_data.investigator_state in ('null', '', 'na', 'NA', 'N/A')
        then null else all_inv_data.investigator_state end as investigator_state,
    case when all_inv_data.investigator_zip in ('null', '', 'na', 'NA', 'N/A')
        then null else str_split_reg_filter(all_inv_data.investigator_zip, '|', ".*[a-zA-Z].*", "N") end as investigator_zip ,
    case when all_inv_data.investigator_country in ('null', '', 'na', 'NA', 'N/A')
        then null else trim(get_ascii_value(coalesce(country_std.standard_country,all_inv_data.investigator_country))) end as investigator_country,
    case when all_inv_data.investigator_address in ('null', '', 'na', 'NA', 'N/A')
        then null else all_inv_data.investigator_address end as investigator_address
from temp_xref_inv_int_4 xref_inv
left outer join temp_all_inv_data_final all_inv_data
on lower(trim(xref_inv.data_src_nm)) = lower(trim(all_inv_data.data_src_nm))
and lower(trim(xref_inv.src_investigator_id)) = lower(trim(all_inv_data.src_investigator_id))
left outer join country_standardization country_std
on regexp_replace(lower(trim(all_inv_data.investigator_country )),"[^0-9A-Za-z_ -,']",'') = regexp_replace(lower(trim(country_std.country)),"[^0-9A-Za-z_ -,']",'')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
""")
xref_src_inv_temp_1.write.mode('overwrite').saveAsTable('xref_src_inv_temp_1')




xref_src_inv1=spark.sql("""select 
data_src_nm, 
uid, 
hash_uid, 
src_investigator_id,
ctfo_investigator_id,
score,
investigator_name,
investigator_phone,
investigator_email,
investigator_specialty,
investigator_city,
investigator_state,
investigator_zip,
investigator_country,
investigator_address from
(select *, ROW_NUMBER() over (partition by src_investigator_id,data_src_nm order by null asc) as rnk from xref_src_inv_temp_1) a where rnk=1""")

xref_src_inv1.write.mode('overwrite').saveAsTable('xref_src_inv1')
xref_src_inv_precedence_int_1 = xref_src_inv1
xref_src_inv_precedence_int_1.write.mode('overwrite').saveAsTable('xref_src_inv_precedence_int_1')
# To remive pipe char at the end of phone number
xref_src_inv_precedence_int_1 = xref_src_inv_precedence_int_1.withColumn("investigator_phone",
                                                                     regexp_replace("investigator_phone", '[|]$', ''))
xref_src_inv_precedence_int_1 = xref_src_inv_precedence_int_1.withColumn("investigator_phone",
                                                                     regexp_replace("investigator_phone", "\'", ""))

for col in xref_src_inv_precedence_int_1.columns:
    xref_src_inv_precedence_int_1 = xref_src_inv_precedence_int_1.withColumn(col, regexp_replace(col, ' ', '<>')) \
        .withColumn(col, regexp_replace(col, '><', '')) \
        .withColumn(col, regexp_replace(col, '<>', ' ')) \
        .withColumn(col, regexp_replace(col, '[|]+', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@ ', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@', '|')) \
        .withColumn(col, regexp_replace(col, '[|]+', '|')) \
        .withColumn(col, regexp_replace(col, '[|]$', '')) \
        .withColumn(col, regexp_replace(col, '^[|]', ''))

xref_src_inv_precedence_int=spark.sql("""
select 
data_src_nm, 
cast(uid as bigint) as uid,
hash_uid, 
src_investigator_id,
ctfo_investigator_id,
cast(score as double) as score,
investigator_name,
investigator_phone,
investigator_email,
investigator_specialty,
investigator_city,
investigator_state,
investigator_zip,
investigator_country,
investigator_address from xref_src_inv_precedence_int_1
""")

xref_src_inv_precedence_int.write.mode('overwrite').saveAsTable('xref_src_inv_precedence_int')
commons_path = bucket_path + '/applications/commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'
write_path = commons_path.replace('table_name', 'xref_src_inv_precedence_int')
xref_src_inv_precedence_int.repartition(100).write.mode('overwrite').parquet(write_path)

xref_src_inv = xref_src_inv1.drop('score').drop('uid').drop('hash_uid')

for col in xref_src_inv.columns:
    xref_src_inv = xref_src_inv.withColumn(col, regexp_replace(col, ' ', '<>')) \
        .withColumn(col, regexp_replace(col, '><', '')) \
        .withColumn(col, regexp_replace(col, '<>', ' ')) \
        .withColumn(col, regexp_replace(col, '[|]+', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@ ', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@', '|')) \
        .withColumn(col, regexp_replace(col, '[|]+', '|')) \
        .withColumn(col, regexp_replace(col, '[|]$', '')) \
        .withColumn(col, regexp_replace(col, '^[|]', ''))

xref_src_inv.write.mode('overwrite').saveAsTable('xref_src_inv')

spark.sql("""
insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_inv partition (
        pt_data_dt='$$data_dt',
        pt_cycle_id='$$cycle_id'
        )
select *
from default.xref_src_inv
""")

CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_inv')
