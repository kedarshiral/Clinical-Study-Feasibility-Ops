



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
from pyspark.sql.functions import regexp_replace, col
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")


path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/mastering/investigator/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'


sys.path.insert(1, '/clinical_design_center/data_management/sanofi_ctfo/$$env/code')
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


def fuzzy_match(str1, str2):
    return distance.get_jaro_distance(str1, str2)


spark.udf.register("fuzzy_match", fuzzy_match)

#citeline data preparation for investigator
temp_citeline_inv_data = spark.sql("""
select
    'citeline' as data_src_nm,
    concat(inv.investigator_first_name,' ', inv.investigator_middle_initial,' ',
    inv.investigator_last_name) as investigator_name,
    regexp_replace(inv.investigator_phone_numbers,'\;','\\\|') as investigator_phone,
    inv.investigator_emails as investigator_email,
    inv.investigator_specialties as investigator_primary_specialty,
    '' as investigator_secondary_specialty,
    inv.investigator_degrees as investigator_title,
    inv.investigator_p_organization_name as investigator_affiliation,
    '' as investigator_debarred_flag,
    inv.investigator_location_city as investigator_city,
    inv.investigator_location_state as investigator_state,
    inv.investigator_location_post_code as investigator_zip,
    inv.investigator_location_country as investigator_country,
    concat(coalesce(inv.investigator_location_street_address, ''), ' ',
    coalesce(inv.investigator_location_city, ''),
     ' ',coalesce(inv.investigator_location_state, ''), ' ',
     coalesce(inv.investigator_location_country, ''),
     ' ',coalesce(inv.investigator_location_post_code, ''))  as investigator_address,
    inv.investigator_id as src_investigator_id
from sanofi_ctfo_datastore_staging_$$db_env.citeline_investigator inv
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
""")
temp_citeline_inv_data.registerTempTable('temp_citeline_inv_data')

#AACT data preparation for investigator

inv_aact_all_data = spark.sql("""
select
        'aact' as data_src_nm,
        trim(split(inv.name,',')[0]) as investigator_name,
        if(cont.phone='null',null,cont.phone) as investigator_phone,
        if(cont.email='null',null,cont.email) as investigator_email,
        '' as investigator_primary_specialty,
        trim(split(inv.name,",")[1]) as investigator_title,
        '' as investigator_city,
        '' as investigator_state,
        '' as investigator_zip,
        '' as investigator_country,
        '' as investigator_address,
        inv.id as investigator_id,
        nct_id
from (
select * from sanofi_ctfo_datastore_staging_$$db_env.aact_facility_investigators
where lower(trim(role)) in ('principal investigator','sub-investigator')) inv
left outer join (
        select
        name,facility_id,
        concat_ws('\|',collect_set(phone)) as phone,
        concat_ws('\|',collect_set(email)) as email
from sanofi_ctfo_datastore_staging_$$db_env.aact_facility_contacts
group by 1,2) cont
on inv.name=cont.name
and inv.facility_id=cont.facility_id
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
sanofi_ctfo_datastore_staging_$$db_env.aact_responsible_parties where
lower(trim(responsible_party_type)) <> "sponsor" and name <> "" and name is not null group by 1,2,3)
 aff
on inv.nct_id=aff.nct_id
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
    '' as affiliation
from inv_aact_all_data a
left outer join temp_aact_inv_master_data_affl_0 b
on a.investigator_id = b.investigator_id
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
    '' as investigator_primary_specialty,
    '' as investigator_secondary_specialty,
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
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
""")

temp_aact_inv_data.registerTempTable('temp_aact_inv_data')

#who data preparation for investigator

#temp_who_inv_data = spark.sql("""
#select
#    'who' as data_src_nm,
#    trim(concat(inv.contact_firstname,' ',trim(split(inv.contact_lastname,',')[0])))
#    as investigator_name,
 #   inv.CONTACT_TEL as investigator_phone,
#    inv.contact_email as investigator_email,
#    null as investigator_primary_specialty,
#    null as investigator_secondary_specialty,
#    inv.contact_lastname as investigator_title,
#    inv.contact_affiliation as investigator_affiliation,
#    null as investigator_debarred_flag,
#    null as investigator_city,
#    null as investigator_state,
#    null as investigator_zip,
#    null as investigator_country,
#    contact_address as investigator_address,
#    inv.id as src_investigator_id
#from sanofi_ctfo_datastore_staging_$$db_env.who_investigators inv
#group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
#""")

temp_dqs_inv_data_rnk = spark.sql("""select
'ir' as data_src_nm,
max( concat( coalesce(trim(person.first_name), ''), ' ', coalesce(trim(person.last_name), '') ) )  as investigator_name,
concat_ws( '\|', collect_set( case when lower(trim(person.email)) like '%dd%proxy%' then null else person.email end ) ) as investigator_email,
'' as investigator_primary_specialty,
'' as investigator_secondary_specialty,
'' as investigator_title,
person.facility_name as investigator_affiliation,
'' as investigator_debarred_flag,
'' as investigator_city,
'' as investigator_state,
'' as investigator_zip,
'' as investigator_country,
'' as investigator_address,
trim(person.person_golden_id) as src_investigator_id,
First(trim(person.phone1)) as investigator_phone,
person.person_golden_id,
person.site_open_dt,
row_number() over (partition by person.person_golden_id order by person.site_open_dt desc ) as rnk
from  sanofi_ctfo_datastore_staging_$$db_env.dqs_study person
group  by 1,4,5,6,7,8,9,10,11,12,13,14,16,17""")
temp_dqs_inv_data_rnk=temp_dqs_inv_data_rnk.dropDuplicates()
temp_dqs_inv_data_rnk.registerTempTable('temp_dqs_inv_data_rnk')

temp_dqs_inv_data = spark.sql("""
select
data_src_nm,
investigator_name,
investigator_phone,
investigator_email,
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
from temp_dqs_inv_data_rnk where rnk=1
""")
temp_dqs_inv_data=temp_dqs_inv_data.dropDuplicates()
temp_dqs_inv_data.registerTempTable("temp_dqs_inv_data")


#ctms data preparation for investigator

temp_ctms_inv_data = spark.sql("""
select
    'ctms' as data_src_nm,
    concat(a.FIRST_NAME,' ',a.LAST_NAME) as investigator_name,
    '' as investigator_phone,
    a.PRINCIPAL_EMAIL_ADDRESS as investigator_email,
    '' as investigator_primary_specialty,
    '' as investigator_secondary_specialty,
    '' as investigator_title,
    '' as investigator_affiliation,
    '' as investigator_debarred_flag,
    '' as investigator_city,
    '' as investigator_state,
    '' as investigator_zip,
    '' as investigator_country,
    '' as investigator_address,
   a.PERSON_ID as src_investigator_id
from  sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff a
group  by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
""")
temp_ctms_inv_data.registerTempTable('temp_ctms_inv_data')


# taking union of all tables to move forward for cleanup activity
'''
temp_all_inv_data_final = spark.sql("""
select * from temp_citeline_inv_data
union
select * from temp_aact_inv_data
#union
#select * from temp_who_inv_data
union
select * from temp_dqs_inv_data
union
select * from temp_ctms_inv_data
""")
'''

temp_all_inv_data_final = spark.sql("""
select * from temp_citeline_inv_data
union
select * from temp_aact_inv_data
union
select * from temp_dqs_inv_data
union
select * from temp_ctms_inv_data
""")

temp_all_inv_data_final.write.mode('overwrite').saveAsTable('temp_all_inv_data_final')
write_path = path.replace('table_name', 'temp_all_inv_data_final')
temp_all_inv_data_final.repartition(100).write.mode('overwrite').parquet(write_path)


# Modified code to use KMValidate function for incorporating KM input

# Writing base table on HDFS to make it available for the function to fetch data
temp_all_inv_dedupe_results_final = spark.sql("""
select *
from sanofi_ctfo_datastore_app_commons_$$db_env.temp_all_inv_dedupe_results_final
""")

temp_all_inv_dedupe_results_final.write.mode('overwrite').\
    saveAsTable('temp_all_inv_dedupe_results_final')

final_table = KMValidate('temp_all_inv_dedupe_results_final', 'inv', '$$data_dt', '$$cycle_id', '$$s3_env')

temp_inv_id_clusterid = spark.sql("""
select concat('ctfo_inv_',row_number() over(order by null)) as investigator_id,
       temp.final_cluster_id as cluster_id
from  (select distinct final_cluster_id
from   """+ final_table + """
where  final_cluster_id is not null
and round(cast(final_score as double), 2) >= 0.65)temp

""")
temp_inv_id_clusterid = temp_inv_id_clusterid.dropDuplicates()
temp_inv_id_clusterid.write.mode('overwrite').saveAsTable('temp_inv_id_clusterid')

write_path = path.replace('table_name', 'temp_inv_id_clusterid')
temp_inv_id_clusterid.repartition(100).write.mode('overwrite').parquet(write_path)


temp_xref_inv_1 = spark.sql("""
select
    inv.uid as uid,
    inv.hash_uid as hash_uid,
    inv.ds_inv_id as ds_inv_id,
    ctoid.investigator_id as inv_id,
    inv.data_src_nm,
    inv.name as name,
    inv.phone as phone,
    inv.email as email,
    inv.final_score as score
from """+final_table+""" inv left outer join temp_inv_id_clusterid ctoid on ctoid.cluster_id=
inv.final_cluster_id
where inv.final_cluster_id is not null and round(coalesce(inv.final_score,0),2) >= 0.65
group by 1,2,3,4,5,6,7,8,9
""")

temp_xref_inv_1.write.mode('overwrite').saveAsTable('temp_xref_inv_1')
write_path = path.replace('table_name', 'temp_xref_inv_1')
temp_xref_inv_1.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_inv_2 = spark.sql("""
select /*+ broadcast(temp) */
    inv.uid as uid,
    inv.hash_uid as hash_uid,
    inv.ds_inv_id as ds_inv_id,
    concat('ctfo_inv_', row_number() over(order by null) + coalesce(temp.max_id,0))
    as inv_id,
    inv.data_src_nm,
    inv.name as name,
    inv.phone as phone,
    inv.email as email,
    inv.final_score as score
from """+final_table+""" inv join (select max(cast(split(inv_id,'inv_')[1] as bigint))
as max_id from temp_xref_inv_1) temp
where  round(coalesce(inv.final_score,0),2) < 0.65 or ((inv.final_cluster_id is null or
 inv.final_cluster_id = '')
and round(cast(inv.final_score as double),2) >=0.65)

""")
temp_xref_inv_2 = temp_xref_inv_2.dropDuplicates()

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
temp_xref_investigator_final_0.write.mode('overwrite').saveAsTable('temp_xref_investigator_final_0')


# Calling maintainGoldenID function to preserve IDs across runs
final_table = maintainGoldenID('temp_xref_investigator_final_0', 'inv', '$$process_id',
                               '$$Prev_Data', '$$s3_env')

if final_table == 'temp_xref_investigator_final_0':
    temp_xref_inv = spark.sql("""
    select *
    from temp_xref_investigator_final_0
    """)
else:
    temp_xref_inv = spark.sql("""
    select
        base.uid,
        base.hash_uid,
        base.data_src_nm,
        base.ds_inv_id as ds_inv_id,
        func_opt.ctfo_inv_id as inv_id,
        base.name,
        base.phone,
        base.email,
        base.score
    from
    temp_xref_investigator_final_0 base
    left outer join
    """+final_table+""" func_opt
    on base.hash_uid = func_opt.hash_uid
    """)

temp_xref_inv = temp_xref_inv.dropDuplicates()
temp_xref_inv.write.mode('overwrite').saveAsTable('temp_xref_inv')
write_path = path.replace('table_name', 'temp_xref_inv')
temp_xref_inv.repartition(100).write.mode('overwrite').parquet(write_path)


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
from temp_xref_inv lateral view explode(split(ds_inv_id,'\;'))one as ds_inv_id_new
where lower(split(ds_inv_id_new,'_')[0]) <> 'who'

""")

temp_xref_inv_int = temp_xref_inv_int.dropDuplicates()
temp_xref_inv_int.write.mode('overwrite').saveAsTable('temp_xref_inv_int')
write_path = path.replace('table_name', 'temp_xref_inv_int')
temp_xref_inv_int.repartition(100).write.mode('overwrite').parquet(write_path)
#temp_xref_inv_int.registerTempTable("temp_xref_inv_int")



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
from  temp_xref_inv_int group by 1,2 having count(distinct uid)<2) b on a.src_investigator_id=b.src_investigator_id AND a.data_src_nm=b.data_src_nm
group by 1,2,3,4,5,6,7,8,9
""")
temp_xref_inv_int_temp = temp_xref_inv_int_temp.dropDuplicates()
temp_xref_inv_int_temp.write.mode('overwrite').\
    saveAsTable('temp_xref_inv_int_temp')
#temp_xref_inv_int_temp.registerTempTable("temp_xref_inv_int_temp")


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
#temp_xref_inv_int_1.registerTempTable("temp_xref_inv_int_1")
temp_xref_inv_int_1.write.mode('overwrite').\
    saveAsTable('temp_xref_inv_int_1')




temp_xref_inv_int_data_src_rnk = spark.sql ("""
select temp_xref_inv_int.*,
        case when data_src_nm like '%ctms%' then 1
        when data_src_nm like '%ir%' then 2
        when data_src_nm like '%citeline%' then 3
    when data_src_nm like '%aact%' then 4
else 5 end as datasource_rnk from  temp_xref_inv_int""")
temp_xref_inv_int_data_src_rnk.write.mode('overwrite').\
    saveAsTable('temp_xref_inv_int_data_src_rnk')

#temp_xref_inv_int_data_src_rnk.registerTempTable("temp_xref_inv_int_data_src_rnk")


# Giving rank to records having multiple distinct UID
temp_xref_inv_int_2= spark.sql("""
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
ROW_NUMBER() over (partition by src_investigator_id order by datasource_rnk asc) as rnk
from
temp_xref_inv_int_data_src_rnk where src_investigator_id in
(select src_investigator_id from (select
src_investigator_id,data_src_nm
from  temp_xref_inv_int_data_src_rnk group by 1,2 having count(distinct uid)>1))
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_inv_int_2 = temp_xref_inv_int_2.dropDuplicates()
temp_xref_inv_int_2.write.mode('overwrite').saveAsTable('temp_xref_inv_int_2')
write_path = path.replace('table_name', 'temp_xref_inv_int_2')
temp_xref_inv_int_2.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_inv_int_2.registerTempTable("temp_xref_inv_int_2")



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
temp_xref_inv_int_3.write.mode('overwrite').saveAsTable('temp_xref_inv_int_3')
write_path = path.replace('table_name', 'temp_xref_inv_int_3')
temp_xref_inv_int_3.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_inv_int_3.registerTempTable("temp_xref_inv_int_3")


# Union of records with unique UID and prec records having distinct multi. UID

temp_xref_inv_int_4 = spark.sql("""
select
* from temp_xref_inv_int_3
union
select * from temp_xref_inv_int_1
""")
temp_xref_inv_int_4 = temp_xref_inv_int_4.dropDuplicates()
temp_xref_inv_int_4.write.mode('overwrite').saveAsTable('temp_xref_inv_int_4')
write_path = path.replace('table_name', 'temp_xref_inv_int_4')
temp_xref_inv_int_4.repartition(100).write.mode('overwrite').parquet(write_path)

#temp_xref_inv_int_4.registerTempTable("temp_xref_inv_int_4")








'''
############3
temp_xref_inv_int_1= spark.sql("""
select data_src_nm,
src_investigator_id,
investigator_id,
uid,
hash_uid,
name,
phone,
email,
score,
ROW_NUMBER() over (partition by investigator_id,data_src_nm order by score desc) as rnk
from temp_xref_inv_int
group by 1,2,3,4,5,6,7,8,9
""")
temp_xref_inv_int_1 = temp_xref_inv_int_1.dropDuplicates()
temp_xref_inv_int_1.write.mode('overwrite').saveAsTable('temp_xref_inv_int_1')
write_path = path.replace('table_name', 'temp_xref_inv_int_1')
temp_xref_inv_int_1.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_inv_int_2 = spark.sql("""
select
trim(concat_ws('\|',collect_set(src_investigator_id))) as collect_src_investigator_id,
investigator_id
from temp_xref_inv_int_1
group by 2
""")
temp_xref_inv_int_2 = temp_xref_inv_int_2.dropDuplicates()
temp_xref_inv_int_2.write.mode('overwrite').saveAsTable('temp_xref_inv_int_2')
write_path = path.replace('table_name', 'temp_xref_inv_int_2')
temp_xref_inv_int_2.repartition(100).write.mode('overwrite').parquet(write_path)


temp_xref_inv_int_3 = spark.sql("""
select
a.data_src_nm,
a.src_investigator_id,
b.collect_src_investigator_id,
a.investigator_id,
a.uid,
a.hash_uid,
a.name,
a.phone,
a.email,
a.score
from
temp_xref_inv_int_1 a
inner join
temp_xref_inv_int_2 b
on a.investigator_id = b.investigator_id where a.rnk = 1
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_inv_int_3 = temp_xref_inv_int_3.dropDuplicates()
temp_xref_inv_int_3.write.mode('overwrite').saveAsTable('temp_xref_inv_int_3')

write_path = path.replace('table_name', 'temp_xref_inv_int_3')
temp_xref_inv_int_3.repartition(100).write.mode('overwrite').parquet(write_path)
#####################
'''


#joining data with golden IDs
xref_src_inv = spark.sql("""
select
    xref_inv.data_src_nm,
    xref_inv.src_investigator_id,
    xref_inv.investigator_id as ctfo_investigator_id,
    xref_inv.score,
    investigator_name,
    all_inv_data.investigator_phone,
    all_inv_data.investigator_email,
    all_inv_data.investigator_primary_specialty as investigator_specialty,
    case when all_inv_data.investigator_city in ('null', '', 'na', 'NA', 'N/A')
        then null else all_inv_data.investigator_city end as investigator_city,
    case when all_inv_data.investigator_state in ('null', '', 'na', 'NA', 'N/A')
        then null else all_inv_data.investigator_state end as investigator_state,
    case when all_inv_data.investigator_zip in ('null', '', 'na', 'NA', 'N/A')
        then null else all_inv_data.investigator_zip end as investigator_zip ,
    case when all_inv_data.investigator_country in ('null', '', 'na', 'NA', 'N/A')
        then null else all_inv_data.investigator_country end as investigator_country,
    case when all_inv_data.investigator_address in ('null', '', 'na', 'NA', 'N/A')
        then null else all_inv_data.investigator_address end as investigator_address

from temp_xref_inv_int_4 xref_inv
left outer join temp_all_inv_data_final all_inv_data
on xref_inv.data_src_nm = all_inv_data.data_src_nm
and xref_inv.src_investigator_id = all_inv_data.src_investigator_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
""")
xref_src_inv.registerTempTable('xref_src_inv')
xref_src_inv_precedence_int = xref_src_inv
xref_src_inv_precedence_int.registerTempTable('xref_src_inv_precedence_int')
#To remive pipe char at the end of phone number
xref_src_inv_precedence_int=xref_src_inv_precedence_int.withColumn("investigator_phone", regexp_replace("investigator_phone", '[|]$', ''))
xref_src_inv_precedence_int.write.mode('overwrite').saveAsTable('xref_src_inv_precedence_int')
xref_src_inv = xref_src_inv.drop('score')
for col in xref_src_inv.columns:
    xref_src_inv = xref_src_inv.withColumn(col, regexp_replace(col, ' ', '<>'))\
       .withColumn(col, regexp_replace(col, '><', ''))\
       .withColumn(col, regexp_replace(col, '<>', ' '))\
       .withColumn(col, regexp_replace(col, '[|]+', '@#@'))\
       .withColumn(col, regexp_replace(col, '@#@ ', '@#@'))\
       .withColumn(col, regexp_replace(col, '@#@', '|'))\
       .withColumn(col, regexp_replace(col, '[|]+', '|'))\
       .withColumn(col, regexp_replace(col, '[|]$', ''))\
       .withColumn(col, regexp_replace(col, '^[|]', ''))


xref_src_inv.registerTempTable('xref_src_inv')
xref_src_inv.write.mode('overwrite').saveAsTable('xref_src_inv')


spark.sql("""
insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_inv partition (
        pt_data_dt='$$data_dt',
        pt_cycle_id='$$cycle_id'
        )
select *
from xref_src_inv
""")

CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_inv')



