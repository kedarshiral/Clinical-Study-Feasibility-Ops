



################################# Module Information ######################################
#  Module Name         : Investigator data preparation
#  Purpose             : This will execute queries for cleaning and standardized data
#  Pre-requisites      : Source table required: citeline_investigator,aact_facility_investigators
#                        aact_facility_contacts, aact_facilities, aact_studies, dqs_person
#  Last changed on     : 22-03-2020
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################
import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

sqlContext = SQLContext(spark)


def is_numeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


sqlContext.udf.register("is_numeric_type", is_numeric, BooleanType())


def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)


sqlContext.udf.register("get_ascii_value", get_ascii_value)

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/' \
       'applications/commons/temp/mastering/investigator/table_name/pt_data_dt=$$data_dt/' \
       'pt_cycle_id=$$cycle_id'

lookup_inv_phonenumber1 = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/study_staff_address.csv")
lookup_inv_phonenumber1.createOrReplaceTempView("lookup_inv_phonenumber1")

lookup_inv_phonenumber = spark.sql("""select person_id,PHONE_NUMBER_1 from (
select PERSON_ID as PERSON_ID,trim(PHONE_NUMBER_1) as PHONE_NUMBER_1,
row_number() over (partition by trim(PERSON_ID),trim(PHONE_NUMBER_1) order by trim(PERSON_ID),trim(PHONE_NUMBER_1) ) as rnk
    from lookup_inv_phonenumber1 where PHONE_NUMBER_1 is not null ) where rnk=1
""")
lookup_inv_phonenumber.createOrReplaceTempView("lookup_inv_phonenumber")

lookup_inv_phonenumber2 = spark.sql("""select  person_id,concat_ws(';',collect_set(PHONE_NUMBER_1)) as PHONE_NUMBER from(select person_id, PHONE_NUMBER_1 from lookup_inv_phonenumber )a group by person_id
""")
lookup_inv_phonenumber2.createOrReplaceTempView("lookup_inv_phonenumber2")


# preparing table to get explodead trial ids from citeline investigator which will be used to get
# nct_id from citeline trialtrove
trials_table_union = spark.sql("""
select
    investigator_id as src_inv_id,
    concat(investigator_first_name,' ',investigator_middle_initial,' ',investigator_last_name)
    as name,
    investigator_phone_numbers as phone,
    investigator_emails as email,
        new_past_trials_id as trial_id
from sanofi_ctfo_datastore_staging_$$db_env.citeline_investigator
lateral view explode(split(past_trials_id,'\\\|'))one as new_past_trials_id
union
select
    investigator_id as src_inv_id,
    concat(investigator_first_name,' ',investigator_middle_initial,' ',investigator_last_name)
    as name,
    investigator_phone_numbers as phone,
    investigator_emails as email,
        new_ongoing_trials_id as trial_id
from sanofi_ctfo_datastore_staging_$$db_env.citeline_investigator
lateral view explode(split(ongoing_trials_id,'\\\|'))one as new_ongoing_trials_id
""")

trials_table_union.registerTempTable('citeline_investigator_trial')

# taking non-duplicate data for a given grain for aact data source.
temp_aact_inv_data = spark.sql("""
select
    'aact' as data_src_nm,
    inv.id as src_inv_id,
    trim(split(inv.name,',')[0]) as name,
    if(cont.phone='null',NULL,cont.phone) as phone,
    if(cont.email='null',NULL,cont.email) as email,
        concat_ws('\;', collect_set(inv.nct_id)) as nct_id
from sanofi_ctfo_datastore_staging_$$db_env.aact_facility_investigators inv
left outer join (select name, facility_id, concat_ws('\|',collect_set(phone)) as phone,
concat_ws('\|',collect_set(email)) as email from
sanofi_ctfo_datastore_staging_$$db_env.aact_facility_contacts group by 1,2) cont
on inv.facility_id=cont.facility_id
and inv.name=cont.name
where lower(trim(role)) in ('principal investigator','sub-investigator')
group by 1,2,3,4,5
""")
temp_aact_inv_data.dropDuplicates()

temp_aact_inv_data.registerTempTable('temp_aact_inv_data')

# taking non-duplicate data for a given grain for citeline_investigator data source.
temp_citeline_inv_data = spark.sql("""
select
    'citeline' as data_src_nm,
    inv.src_inv_id,
    inv.name,
    inv.phone,
    inv.email,
        concat_ws('\;', collect_set(cite_trialtrove.nct_id)) as nct_id
from citeline_investigator_trial inv
left outer join
(select protocol_ids as nct_id,trial_id as trial_id
from sanofi_ctfo_datastore_staging_$$db_env.citeline_trialtrove
group by 1,2) cite_trialtrove
on inv.trial_id = cite_trialtrove.trial_id
group by 1,2,3,4,5
""")
temp_citeline_inv_data.dropDuplicates()
temp_citeline_inv_data.registerTempTable('temp_citeline_inv_data')

# dqs data
# Taking non-duplicate data for a given grain for dqs data source
# Removing emails having dd%proxy in it
temp_dqs_inv_data_rank = spark.sql("""
select "ir" as data_source,
a.person_golden_id as src_inv_id,
max( concat( coalesce(a.first_name, ''), ' ', coalesce(a.last_name, '') ) ) as name,
concat_ws('\|', collect_set(a.phone1)) as phone,
concat_ws( '\|', collect_set( case when lower(trim(a.email)) like '%dd%proxy%' then null else a.email end ) ) as email,
concat_ws('\;', collect_set(a.nct_number)) as nct_id,
row_number() over (partition by a.person_golden_id order by a.site_open_dt desc ) as rnk
from sanofi_ctfo_datastore_staging_$$db_env.dqs_study a
group by a.person_golden_id, a.site_open_dt""")
temp_dqs_inv_data_rank.dropDuplicates()
temp_dqs_inv_data_rank.registerTempTable("temp_dqs_inv_data_rank")
temp_dqs_inv_data_rank.write.mode('overwrite').saveAsTable('temp_dqs_inv_data_rank')
temp_dqs_inv_data = spark.sql("""
select
data_source,
src_inv_id,
name,
phone,
email,
nct_id
from temp_dqs_inv_data_rank where rnk=1
""")
temp_dqs_inv_data.dropDuplicates()
temp_dqs_inv_data.registerTempTable("temp_dqs_inv_data")
temp_dqs_inv_data.write.mode('overwrite').saveAsTable('temp_dqs_inv_data')
# ctms data
# Removing emails having dd%proxy in it
temp_ctms_inv_data_0 = spark.sql("""
select
    "ctms" as data_source,
        a.meta_modification_date,
     a.PERSON_ID as src_inv_id,
     concat(a.FIRST_NAME," ", a.LAST_NAME) as name,
     '' as phone,
     concat_ws('\|',collect_set(case when lower(trim(a.PRINCIPAL_EMAIL_ADDRESS)) like '%dd%proxy%' then null else a.PRINCIPAL_EMAIL_ADDRESS end )) as email,
     '' as nct_id
from
sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff a
where trim(a.INVESTIGATOR) = "Yes"
group by 1,2,3,4
""")
temp_ctms_inv_data_0.registerTempTable("temp_ctms_inv_data_0")
temp_ctms_inv_data_rank = spark.sql("""
select
data_source,
src_inv_id,
name,
phone,
email,
nct_id,
row_number() over (partition by src_inv_id order by meta_modification_date desc) as rnk
from temp_ctms_inv_data_0
""")
temp_ctms_inv_data_rank.dropDuplicates()
temp_ctms_inv_data_rank.registerTempTable("temp_ctms_inv_data_rank")
temp_ctms_inv_data_rank.write.mode('overwrite').saveAsTable('temp_ctms_inv_data_rank')

temp_ctms_inv_data=spark.sql("""
select
data_source,
src_inv_id,
name,
phone,
email,
nct_id
from temp_ctms_inv_data_rank where rnk=1
""")
temp_ctms_inv_data.registerTempTable("temp_ctms_inv_data")
temp_ctms_inv_data.write.mode('overwrite').saveAsTable('temp_ctms_inv_data')
# taking union of all tables to move forward for cleanup activity
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
""")
temp_all_inv_data.write.mode('overwrite').saveAsTable('temp_all_inv_data')
write_path = path.replace('table_name', 'temp_all_inv_data')
temp_all_inv_data.repartition(100).write.mode('overwrite').parquet(write_path)

# cleaning data and investigators with names consisting of certain keywords will be dropped
temp_all_inv_data_cleaned_0 = spark.sql("""
select
    data_src_nm,
    concat(data_src_nm, '_', src_inv_id) as ds_inv_id,
    case when name in (' ','','null','NA','na') then null
    else regexp_replace(regexp_replace(regexp_replace(regexp_replace(
    regexp_replace(regexp_replace(lower(trim(get_ascii_value(name))),'[^0-9A-Za-z]',' '),' ','<>'),'><',''),'<>',' ')
    ,'dr ',''),'md ','') end as name,
    case when phone in (' ','','null','NA','na') then null
    else regexp_replace(regexp_replace(phone,'[,/;extor]','|'),'[^0-9|]','') end as phone,
    case when email in (' ','','null','NA','na') then null when email like '%<%' then
    regexp_replace(regexp_replace(split(lower(email),'<')[1],'>',''),',','\\\|') else
     regexp_replace(regexp_replace(regexp_replace(lower(email),',','\\\|'),';','|'),'/','|') end
     as email,
        nct_id
from temp_all_inv_data
where regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%administrator%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%admin%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%representative%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%research%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like 'bms%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%services%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%clinical%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%investigator%'
and regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') not  like '%unassigned%'
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

#temp_all_inv_data_cleaned_0.registerTempTable('temp_all_inv_data_cleaned_0')
temp_all_inv_data_cleaned_0.write.mode('overwrite').saveAsTable('temp_all_inv_data_cleaned_0')


# standardized phone column and seperated all pipe delimited
temp_all_inv_data_cleaned = spark.sql("""
select
    data_src_nm,
    ds_inv_id,
    name,
    concat_ws('|',sort_array(collect_set(exploded_phone))) as phone,
    regexp_replace(email,'//','') as email,
        nct_id
from (select data_src_nm,ds_inv_id,name,exploded_phone,email,nct_id from temp_all_inv_data_cleaned_0
lateral view outer explode(split(phone, '\\\|'))one as exploded_phone
)
group by data_src_nm,ds_inv_id,name,email,nct_id
""")

temp_all_inv_data_cleaned.write.mode('overwrite').saveAsTable('temp_all_inv_data_cleaned')
write_path = path.replace('table_name', 'temp_all_inv_data_cleaned')
temp_all_inv_data_cleaned.repartition(100).write.mode('overwrite').parquet(write_path)

# treaming columns to avoid space
temp_all_inv_data_prep0 = spark.sql("""
select
    cast(row_number() over( order by coalesce(max(name), 'NA'), coalesce(max(phone), 'NA'),
     coalesce(max(email), 'NA')) as bigint ) as uid,
    substr(sha2(Concat(coalesce(max(name), 'NA'), coalesce(max(phone), 'NA'),
    coalesce(max(email), 'NA')), 256), 1, 16) as hash_uid,
    concat_ws('\;', collect_set(data_src_nm)) as data_src_nm,
    concat_ws('\;', collect_set(ds_inv_id)) as ds_inv_id,
    lower(trim(max(name))) as name,
    trim(max(phone)) as phone,
    lower(trim(max(email))) as email,
        concat_ws('\;', collect_set(nct_id)) as nct_id
from temp_all_inv_data_cleaned
group by regexp_replace(lower(trim(name)),'[^a-z]','') , split(email,'\\\|')[0],
split(phone,'\\\|')[0]
""")

temp_all_inv_data_prep0.dropDuplicates()

temp_all_inv_data_prep0.write.mode('overwrite').saveAsTable('temp_all_inv_data_prep0')
write_path = path.replace('table_name', 'temp_all_inv_data_prep0')
temp_all_inv_data_prep0.repartition(100).write.mode('overwrite').parquet(write_path)

# Assigning unique row number for each record for dedupeio algorithm and sha2 hash id to identify same record in multiple runs
# added this block
temp_all_inv_data_prep9 = spark.sql("""
select
    cast(row_number() over( order by coalesce(max(name), 'NA'), coalesce(max(phone), 'NA'), coalesce(max(email), 'NA')) as bigint ) as uid,
    substr(sha2(Concat(coalesce(max(name), 'NA'), coalesce(max(phone), 'NA'), coalesce(max(email), 'NA')), 256), 1, 16) as hash_uid,
    concat_ws('\;', collect_set(data_src_nm)) as data_src_nm,
    concat_ws('\;', collect_set(ds_inv_id)) as ds_inv_id,
    lower(trim(max(name))) as name,
    concat_ws('\|', collect_set(phone)) as phone,
    lower(trim(max(email))) as email,
    concat_ws('\;', collect_set(nct_id)) as nct_id,
        split(ds_inv_id,'\\\_')[1] as person_id
from temp_all_inv_data_prep0
group by regexp_replace(lower(trim(name)),'[^a-z]',''), split(email,'\\\|')[0],split(ds_inv_id,'\\\_')[1]
""")
temp_all_inv_data_prep9.dropDuplicates()

#temp_all_inv_data_prep9.registerTempTable('temp_all_inv_data_prep9')
temp_all_inv_data_prep9.write.mode('overwrite').saveAsTable('temp_all_inv_data_prep9')



#final table joining to get phone number from lookup file

temp_all_inv_data_prep = spark.sql("""
select
     temp_all_inv_data_prep9.uid,
     temp_all_inv_data_prep9.hash_uid,
    temp_all_inv_data_prep9.data_src_nm,
    temp_all_inv_data_prep9.ds_inv_id,
     temp_all_inv_data_prep9.name,
    coalesce(lookup_inv_phonenumber2.PHONE_NUMBER,temp_all_inv_data_prep9.phone) as phone,
     temp_all_inv_data_prep9.email,
    temp_all_inv_data_prep9.nct_id
from temp_all_inv_data_prep9
left join lookup_inv_phonenumber2
on trim(lookup_inv_phonenumber2.PERSON_ID)=trim(temp_all_inv_data_prep9.person_id)
""")


temp_all_inv_data_prep.dropDuplicates()
temp_all_inv_data_prep.write.mode('overwrite').saveAsTable('temp_all_inv_data_prep')
write_path = path.replace('table_name', 'temp_all_inv_data_prep')
temp_all_inv_data_prep.repartition(100).write.mode('overwrite').parquet(write_path)



