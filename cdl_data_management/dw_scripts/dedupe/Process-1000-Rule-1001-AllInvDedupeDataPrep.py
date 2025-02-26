################################# Module Information ######################################
#  Module Name         : Investigator data preparation
#  Purpose             : This will execute queries for cleaning and standardized data
#  Pre-requisites      : Source table required: citeline_investigator,aact_facility_investigators
#                        aact_facility_contacts, aact_facilities, aact_studies, dqs_person
#  Last changed on     : 22-03-2020
#  Last changed by     : Archit/Kuldeep
#  Reason for change   : Updated with TA
#  Return Values       : NA
############################################################################################
import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType
from CommonUtils import *
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
import re

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

sqlContext = SQLContext(spark)

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])




def str_split_reg_filter(str_val, sep, reg_exp, flag):
    if not str_val:
        return str_val
    arr = str_val.split(sep)
    print(arr)
    pattern = re.compile(reg_exp)

    arr_final = [i for i in arr if pattern.match(i)]
    print(arr_final)
    if len(arr_final) != 0:
        if flag == 'N':
            s = set(arr_final)
            arr_final = [x for x in arr if x not in s]
            #arr_final = list(set(arr) - set(arr_final))
            print(arr_final)
        str_final_val = sep.join(i for i in arr_final)
    elif flag == 'Y':
        str_final_val = sep.join(i for i in arr_final)
    else:
        str_final_val = sep.join(i for i in arr)
    str_final_val = str_final_val.lstrip('|')
    str_final_val = str_final_val.rstrip('|')
    return str_final_val

spark.udf.register("str_split_reg_filter", str_split_reg_filter)


def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


sqlContext.udf.register("is_numeric_type", is_numeric, BooleanType())

sqlContext = SQLContext(spark)



def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


sqlContext.udf.register("get_ascii_value", get_ascii_value)

path = bucket_path + '/applications/commons/temp/mastering/investigator/table_name/pt_data_dt=$$data_dt/' \
                     'pt_cycle_id=$$cycle_id'


json_file = open(
    CommonConstants.EMR_CODE_PATH+'/configs/job_executor/precedence_params.json',
    'r').read()
config_data = json.loads(json_file)
print(config_data)
data_source_order_dict = config_data['data_source_ordering']
a_cMap = [(k,)+(v,) for k,v in data_source_order_dict.items()]
data_precedence = spark.createDataFrame(a_cMap, ['Data_Src_Nm','Precedence'])
data_precedence.write.mode('overwrite').saveAsTable("data_precedence")

#Preparing data from Citeline, started including NPI column & few of the values in NPI which are not expected are treated as NULL.
temp_citeline_inv_data = spark.sql("""
select 
    'citeline' as data_source,
    trim(investigator_id) as src_inv_id,
    concat_ws(' ',NULLIF(trim(investigator_first_name),''),NULLIF(trim(investigator_middle_initial),''),
    NULLIF(trim(investigator_last_name),'')) as name,
    trim(investigator_phone_numbers) as phone,
    trim(investigator_emails) as email,
    case when investigator_npi in ('','null','n/a','s://health') then null
    else trim(investigator_npi) end as npi
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_investigator

""")
temp_citeline_inv_data = temp_citeline_inv_data.dropDuplicates()
temp_citeline_inv_data.registerTempTable('temp_citeline_inv_data')
temp_citeline_inv_data.write.mode('overwrite').saveAsTable('temp_citeline_inv_data')

temp_tascan_inv_data = spark.sql("""
select 
    'tascan' as data_source,
    trim(investigator_id) as src_inv_id,
    concat_ws(' ',NULLIF(trim(investigator_first_name),''),NULLIF(trim(investigator_middle_initial),''),
    NULLIF(trim(investigator_last_name),'')) as name,
    trim(investigator_phone_numbers) as phone,
    trim(investigator_emails) as email,
    case when investigator_npi in ('','null','n/a','s://health') then null
    else trim(investigator_npi) end as npi
from $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_investigator
group by 1,2,3,4,5,6
""")
temp_tascan_inv_data = temp_tascan_inv_data.dropDuplicates()
temp_tascan_inv_data.registerTempTable('temp_tascan_inv_data')
temp_tascan_inv_data.write.mode('overwrite').saveAsTable('temp_tascan_inv_data')


#Preparing data from AACT, started including NPI column (which is flowing as NULL for AACT) and using phone & email from aact_facility_contacts table.
temp_aact_inv_data = spark.sql("""
SELECT 'aact' AS data_source,
       inv.id AS src_inv_id,
       trim(split(inv.NAME, ',')[0]) AS NAME,
       IF(cont.phone = 'null', NULL, trim(cont.phone)) as phone,
       IF(cont.email = 'null', NULL, trim(cont.email)) AS email,
       cast(null as string) as npi
FROM $$client_name_ctfo_datastore_staging_$$db_env.aact_facility_investigators inv 
LEFT OUTER JOIN
(SELECT NAME,
        facility_id,
        concat_ws('\|',sort_array(collect_set(NULLIF(trim(phone),'')),true)) AS phone,
        concat_ws('\|',sort_array(collect_set(NULLIF(trim(email),'')),true)) AS email
        FROM  $$client_name_ctfo_datastore_staging_$$db_env.aact_facility_contacts
        GROUP BY 1,2) cont ON lower(trim(inv.facility_id)) = lower(trim(cont.facility_id)) AND lower(trim(inv.NAME)) = lower(trim(cont.NAME))
WHERE lower(trim(inv.role)) IN ('principal investigator','sub-investigator') AND inv.id IS NOT NULL
GROUP BY 1,2,3,4,5,6
""")
temp_aact_inv_data = temp_aact_inv_data.dropDuplicates()
temp_aact_inv_data.registerTempTable('temp_aact_inv_data')
temp_aact_inv_data.write.mode('overwrite').saveAsTable('temp_aact_inv_data')

#Preparing data from DQS, started including NPI column (which is flowing as NULL for DQS)
dqs_exploded = spark.sql("""
select trim(person_golden_id) as person_golden_id,
       trim(first_name) as first_name,
       trim(last_name) as last_name,
       trim(person_phone) as person_phone,
       trim(email) as email,
       cast(null as string) as npi,
       trim(site_open_dt) as site_open_dt
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir where person_golden_id is not null
group by 1,2,3,4,5,6,7""")
dqs_exploded = dqs_exploded.dropDuplicates()
dqs_exploded.registerTempTable('dqs_exploded')


#Due to duplicate data in DQS, one of the record is getting picked up based upon the latest site open date.
temp_dqs_inv_data_rank = spark.sql("""
select "ir" as data_source,
a.person_golden_id as src_inv_id,
max(concat_ws(' ',coalesce(a.first_name,''),coalesce(a.last_name,''))) as name,
concat_ws('\|',sort_array(collect_set(NULLIF(trim(a.person_phone),'')),true)) as phone,
concat_ws('\|',sort_array(collect_set(case when lower(trim(a.email)) like '%dd%proxy%' then null 
else NULLIF(trim(a.email),'') end),true)) as email,
concat_ws('\|',sort_array(collect_set(NULLIF(trim(a.npi),'')),true)) as npi,
row_number() over (partition by a.person_golden_id order by a.site_open_dt desc) as rnk
from dqs_exploded a
group by a.person_golden_id, a.site_open_dt""")
temp_dqs_inv_data_rank = temp_dqs_inv_data_rank.dropDuplicates()
temp_dqs_inv_data_rank.write.mode('overwrite').saveAsTable('temp_dqs_inv_data_rank')

#Picking one record from DQS based upon the rank created as per the DF above.
temp_dqs_inv_data = spark.sql("""
select distinct trim(data_source) as data_source,
trim(src_inv_id) as src_inv_id,
trim(name) as name,
trim(phone) as phone,
trim(email) as email,
trim(npi) as npi
from temp_dqs_inv_data_rank where rnk=1
""")
temp_dqs_inv_data = temp_dqs_inv_data.dropDuplicates()
temp_dqs_inv_data.write.mode('overwrite').saveAsTable('temp_dqs_inv_data')


#Preparing data from CTMS, started including NPI column (which is flowing as NULL for CTMS)
temp_ctms_inv_data = spark.sql("""
select a.source as data_source,
trim(a.src_investigator_id) as src_inv_id,
trim(a.src_investigator_name) as name,
trim(a.investigator_phone) as phone, 
case when lower(trim(a.investigator_email)) like '%dd%proxy%' then null 
else NULLIF(trim(a.investigator_email),'') end as email,
cast(null as string) as npi
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator a
where a.src_investigator_id is not null
group by 1,2,3,4,5,6
""")
temp_ctms_inv_data = temp_ctms_inv_data.dropDuplicates()
temp_ctms_inv_data.write.mode('overwrite').saveAsTable('temp_ctms_inv_data')

#Performing union of all the data sources
temp_all_inv_data = spark.sql("""
select *
from temp_citeline_inv_data
union
select *
from temp_aact_inv_data
union
select *
from temp_dqs_inv_data
union
select *
from temp_ctms_inv_data
union
select *
from temp_tascan_inv_data
""")
temp_all_inv_data = temp_all_inv_data.dropDuplicates()
temp_all_inv_data.write.mode('overwrite').saveAsTable('temp_all_inv_data')

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_data PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_inv_data
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_data')

#Seperating out the first name & last name for investigator
temp_all_inv_data_names = spark.sql("""
    select data_source, 
       src_inv_id, 
       regexp_replace(substring_index(trim(lower(name)), ' ', 1),'[^0-9A-Za-z_ ]','') as first_name,
       regexp_replace(substring_index(trim(lower(name)), ' ', -1),'[^0-9A-Za-z_ ]','') as last_name,
       name,
       phone, 
       email,
       npi
       from temp_all_inv_data
""")
temp_all_inv_data_names.createOrReplaceTempView('temp_all_inv_data_names')
temp_all_inv_data_names.write.mode('overwrite').saveAsTable('temp_all_inv_data_names')

#Apply cleaning rules for making '', 'null','na' values as null for further processing
temp_all_inv_data_names_temp=spark.sql("""
select data_source,
src_inv_id,
case when lower(trim(first_name)) in ('', 'null','na') then null
else first_name end as first_name,
case when lower(trim(last_name)) in ('', 'null','na') then null
else last_name end as last_name,
case when lower(trim(name)) in ('', 'null','na') then null
else name end as name,
case when lower(trim(phone)) in ('', 'null','na') then null
else phone end as phone,
case when lower(trim(email)) in ('', 'null','na') then null
else email end as email,
npi from temp_all_inv_data_names
""")
temp_all_inv_data_names_temp.createOrReplaceTempView('temp_all_inv_data_names_temp')
temp_all_inv_data_names_temp.write.mode('overwrite').saveAsTable('temp_all_inv_data_names_temp')

#Dropping the records where below mentioned cases take place
#1. First Name is available but Last Name, Phone, Email are Null
#2. Last Name is available but First Name, Phone, Email are Null
#3. First Name & Last Name both are Null
#The reason for dropping these records is that such records wouldn't hold much value to us.
temp_all_inv_data_0 = spark.sql("""
select a.data_source, 
   a.src_inv_id,
   a.name,
   a.phone, 
   a.email,
   npi
   from 
   (
select data_source, 
       src_inv_id,
       case when first_name is not null and last_name is null and phone is null and email is null then 1 else 0 end as rnk1, 
       case when first_name is null and last_name is not null and phone is null and email is null then 1 else 0 end as rnk2,
       case when first_name is null and last_name is null then 1 else 0 end as rnk3,
       name,
       phone, 
       email,
       npi
from temp_all_inv_data_names_temp ) a
where a.rnk1 != 1 and a.rnk2 != 1 and a.rnk3 != 1
""")
temp_all_inv_data_0 = temp_all_inv_data_0.dropDuplicates()
temp_all_inv_data_0.write.mode('overwrite').saveAsTable('temp_all_inv_data_0')

# cleaning data and investigators with names consisting of certain keywords will be dropped
#The reason for dropping these records is that such records wouldn't hold much value to us.
temp_all_inv_data_cleaned = spark.sql("""
select
    data_source,
    concat_ws('_',NULLIF(trim(data_source),''), NULLIF(trim(src_inv_id),'')) as ds_inv_ids,
    case when trim(name) in (' ','','null','NA','na') then null
    else regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    regexp_replace(regexp_replace(lower(trim(get_ascii_value(name))),'[^0-9A-Za-z]',' '),' ','<>'),'><',''),'<>',' ')
    ,'dr ',''),'md ','') end as name,
    case when trim(phone) in (' ','','null','NA','na') then null
    else regexp_replace(lower(trim(phone)),'[^0-9|]','') end as phone,
    case when trim(email) in (' ','','null','NA','na') then null when trim(email) like '%<%' then
    regexp_replace(regexp_replace(split(lower(email),'<')[1],'>',''),',','\\\|') else
    regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(email),',','\\\|'),';','|'),'/','|'),'[^0-9A-Za-z@|_]', '')
    end as email,
    npi
from temp_all_inv_data_0
where regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%research%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%clinical%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%investigator%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%medical%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%site pi%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%hospital%'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not in ('heart and diabetes center nrw georgstrae 11','03.22.45.59.30 03.22.45.59.30','principal investigatoi designated by the sponsor')
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') is not null
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') != ''
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') != ' '
and name != 'MD.'
and lower(name) !='md'
and lower(name) !='dr'
and trim(lower(name)) not like '%dummy%'
and regexp_replace(lower(trim(name)),"[^A-Za-z]",'') != 'na na'
and lower(trim(name)) != ''
and is_numeric_type(regexp_replace(trim(name),"[^0-9A-Za-z_ -,\.']",'')) != 'true'
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') not in ('null', '', 'na')
and regexp_replace(lower(trim(name)),"[^0-9A-Za-z_ -,']",'') is not null
""")
temp_all_inv_data_cleaned = temp_all_inv_data_cleaned.dropDuplicates()
temp_all_inv_data_cleaned.write.mode('overwrite').saveAsTable('temp_all_inv_data_cleaned')

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_data_cleaned PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_inv_data_cleaned
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_data_cleaned')


#Performing the self-clustering of records where the NPI ID is same.
#Ideally such records shouldn't prevail in the source data itself, but this piece of code has been added for the safer side since NPI is always a key identifier for an investigator.
#Here, first we concat the source IDs at NPI-level.
temp_inv_data_common_id = spark.sql("""
select
    npi,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_inv_ids),'')),true)) as ds_inv_id
from (select * from temp_all_inv_data_cleaned where npi is not null and trim(npi) <> '')
group by 1
having size(collect_set(ds_inv_ids)) > 1
""")

temp_inv_data_common_id.write.mode('overwrite').saveAsTable("temp_inv_data_common_id")

#Creating a comn_cluster_id for the records with same NPI
temp_inv_comnid_clusterid = spark.sql("""
select
    concat_ws('','common_',row_number() over(order by null)) as comn_cluster_id,
    temp.npi
from
(select distinct npi from temp_inv_data_common_id where npi is not null and npi <> '') temp
""")
temp_inv_comnid_clusterid.write.mode('overwrite').saveAsTable('temp_inv_comnid_clusterid')
write_path = path.replace('table_name', 'temp_inv_comnid_clusterid')
temp_inv_comnid_clusterid.repartition(100).write.mode('overwrite').parquet(write_path)

# Fetching all the data again and attaching the comn_cluster_id & precedence.
temp_inv_data_comnid_map = spark.sql("""
select /*+ broadcast(clstr_data) */
distinct
    base.*,
    clstr_data.comn_cluster_id, prec.precedence as precedence 
from
temp_all_inv_data_cleaned base
left outer join temp_inv_comnid_clusterid clstr_data
on lower(trim(base.npi)) = lower(trim(clstr_data.npi))
left outer join data_precedence prec on lower(trim(base.data_source)) = lower(trim(prec.data_src_nm))
""")
temp_inv_data_comnid_map.write.mode('overwrite').saveAsTable('temp_inv_data_comnid_map')
write_path = path.replace('table_name', 'temp_inv_data_comnid_map')
temp_inv_data_comnid_map.repartition(100).write.mode('overwrite').parquet(write_path)


#Concatenating data source & source IDs at the level of comn_cluster_id
temp_inv_comnid_clustered = spark.sql("""
select
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_source),'')),true)) as data_source,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_inv_ids),'')),true)) as ds_inv_id,
    comn_cluster_id
from temp_inv_data_comnid_map where comn_cluster_id is not null
group by comn_cluster_id
""")
temp_inv_comnid_clustered.write.mode('overwrite').saveAsTable('temp_inv_comnid_clustered')
write_path = path.replace('table_name', 'temp_inv_comnid_clustered')
temp_inv_comnid_clustered.repartition(100).write.mode('overwrite').parquet(write_path)

#Creating a rank to pick only one record due to duplicate NPI
temp_inv_comnid_precedence_1 = spark.sql("""
select
comn_cluster_id,
name,
phone,
email,
precedence,
ds_inv_ids as ds_inv_id,
row_number () over (partition by comn_cluster_id order by precedence asc, ds_inv_ids desc) as rnk
from
(select * from temp_inv_data_comnid_map where comn_cluster_id is not null)
""")
temp_inv_comnid_precedence_1.write.mode('overwrite').saveAsTable('temp_inv_comnid_precedence_1')


#Picking only 1 record where NPI has duplicated
temp_inv_comnid_precedence = spark.sql("""
select
comn_cluster_id,
max(name) as name,
max(phone) as phone,
max(email) as email
from

temp_inv_comnid_precedence_1
where rnk =1

group by 1
""")
temp_inv_comnid_precedence.write.mode('overwrite').saveAsTable('temp_inv_comnid_precedence')
write_path = path.replace('table_name', 'temp_inv_comnid_precedence')
temp_inv_comnid_precedence.repartition(100).write.mode('overwrite').parquet(write_path)


#Finally, taking the union of the data which has duplicate NPIs with the rest of the data.
temp_inv_common_id_final = spark.sql("""
select
    base.comn_cluster_id,
    base.data_source,
    base.ds_inv_id,
    prec_data.name,
    prec_data.phone,
    prec_data.email
from
temp_inv_comnid_clustered base
left outer join
temp_inv_comnid_precedence prec_data
on lower(trim(base.comn_cluster_id)) = lower(trim(prec_data.comn_cluster_id))
group by 1,2,3,4,5,6
union
select 
    comn_cluster_id,
    data_source,
    ds_inv_ids as ds_inv_id,
    name,
    phone,
    email
from temp_inv_data_comnid_map where comn_cluster_id is null
group by 1,2,3,4,5,6
""")

temp_inv_common_id_final.write.mode('overwrite').saveAsTable('temp_inv_common_id_final')
write_path = path.replace('table_name', 'temp_inv_common_id_final')
temp_inv_common_id_final.repartition(100).write.mode('overwrite').parquet(write_path)

#At the grain of name, phone, email rolling the data up in order to self-cluster & cluster across sources
temp_all_inv_data_prep0 = spark.sql("""
select
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_source),'')),true)) as data_source,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_inv_id),'')),true)) as ds_inv_id,
    regexp_replace(lower(trim(max(name))),'  ',' ') as name,
    regexp_replace(trim(max(phone)),'  ',' ') as phone,
    regexp_replace(lower(trim(max(email))),'  ',' ') as email
from temp_inv_common_id_final
group by regexp_replace(lower(trim(name)),'[^a-z]','') , split(email,'\\\|')[0],
split(phone,'\\\|')[0]
""")
temp_all_inv_data_prep0 = temp_all_inv_data_prep0.dropDuplicates()
temp_all_inv_data_prep01 = temp_all_inv_data_prep0.sort("name","phone","email")
temp_all_inv_data_prep01.write.mode('overwrite').saveAsTable('temp_all_inv_data_prep01')
write_path = path.replace('table_name', 'temp_all_inv_data_prep01')
temp_all_inv_data_prep01.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_inv_data_prep02=spark.sql("""
select 
concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_source),'')),true)) as data_source,
concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_inv_id),'')),true)) as ds_inv_id,
case when lower(trim(name)) in ('', 'null','na') then null
else name end as name,
case when lower(trim(phone)) in ('', 'null','na') then null
else phone end as phone,
case when lower(trim(email)) in ('', 'null','na') then null
else email end as email
from temp_all_inv_data_prep01 group by 3,4,5
""")
temp_all_inv_data_prep02.registerTempTable("temp_all_inv_data_prep02")
temp_all_inv_data_prep02.write.mode('overwrite').saveAsTable('temp_all_inv_data_prep02')
write_path = path.replace('table_name', 'temp_all_inv_data_prep02')
temp_all_inv_data_prep02.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_inv_data_prep03=spark.sql("""
select 
concat_ws('\;',sort_array(split(data_source,'\;'),true)) as data_source,
concat_ws('\;',sort_array(split(ds_inv_id,'\;'),true)) as ds_inv_id,
name, email, phone from temp_all_inv_data_prep02
""")
temp_all_inv_data_prep03.registerTempTable("temp_all_inv_data_prep03")
temp_all_inv_data_prep03.write.mode('overwrite').saveAsTable('temp_all_inv_data_prep03')
write_path = path.replace('table_name', 'temp_all_inv_data_prep03')
temp_all_inv_data_prep03.repartition(100).write.mode('overwrite').parquet(write_path)


def remove_spaces(string):
    string_f = str(string)
    pattern = re.compile(r'\s+')
    return re.sub(pattern, ' ', string_f)


sqlContext.udf.register("remove_spaces", remove_spaces)


# Assigning unique row number for each record for dedupeio algorithm and sha2 hash id to identify same record in multiple runs
temp_all_inv_data_prep9 = spark.sql("""
select
    cast(DENSE_RANK() over( order by coalesce(max(name), 'NA'), coalesce(max(phone), 'NA'), coalesce(max(email), 'NA')) as bigint) as uid,
    sha2(concat_ws(coalesce(max(name), 'NA'), coalesce(max(phone), 'NA'), coalesce(max(email), 'NA')), 256) as hash_uid,
    data_source,
    ds_inv_id,
    regexp_replace(name,'  ',' ') as name,
    regexp_replace(phone,'  ',' ') as phone,
    regexp_replace(email,'  ',' ') as email
from temp_all_inv_data_prep03
group by 3,4,5,6,7
""")
temp_all_inv_data_prep9 = temp_all_inv_data_prep9.dropDuplicates()
temp_all_inv_data_prep9.write.mode('overwrite').saveAsTable('temp_all_inv_data_prep9')
write_path = path.replace('table_name', 'temp_all_inv_data_prep9')
temp_all_inv_data_prep9.repartition(100).write.mode('overwrite').parquet(write_path)

#Standardizing the phone & email using the created UDF & publishing the final data prep table.
temp_all_inv_data_prep = spark.sql("""
select uid,hash_uid,data_source,ds_inv_id,name,phone,email from (
select
    temp_all_inv_data_prep9.uid,
    temp_all_inv_data_prep9.hash_uid,
    temp_all_inv_data_prep9.data_source,
    temp_all_inv_data_prep9.ds_inv_id,
    temp_all_inv_data_prep9.name,
    str_split_reg_filter(temp_all_inv_data_prep9.phone,'|', ".*[a-zA-Z].*", "N") as phone,
    str_split_reg_filter(temp_all_inv_data_prep9.email, '|', ".*@.*", "Y") as email,
    regexp_replace(lower(trim(temp_all_inv_data_prep9.name)),"[^A-Za-z]",'') != '' as flag
from temp_all_inv_data_prep9) where flag = 'true'
""")
temp_all_inv_data_prep=temp_all_inv_data_prep.dropDuplicates()
temp_all_inv_data_prep.write.mode('overwrite').saveAsTable('temp_all_inv_data_prep')

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_data_prep PARTITION (
            pt_data_dt='$$data_dt',
            pt_cycle_id='$$cycle_id'
            )
    SELECT *
    FROM temp_all_inv_data_prep
    """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_data_prep')
