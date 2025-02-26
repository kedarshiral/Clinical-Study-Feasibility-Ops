
################################# Module Information ######################################
#  Module Name         : All Trial Dedupe Data Prep
#  Purpose             : This will prepare input data for Trial Dedupe for all data sources
#  Pre-requisites      : L1 source table required: aact_studies, citeline_trialtrove, who_trials
#  Last changed on     : 16-07-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Input data is prepared for dedupe using L1 tables
# 2. Required column values are standardised and cleaned
############################################################################################

import pandas as pd
import unidecode
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf, col, lit

# Parameter set up for the processing
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

sqlContext = SQLContext(spark)

def get_ascii_value(text):
    if text is None:
        return text
    return unidecode.unidecode(text)

sqlContext.udf.register("get_ascii_value", get_ascii_value)

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/mastering/trial/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

# Fetching trial phase mapping file for phase standardization
trial_phase_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/trial_phase.csv')
#trial_phase_mapping.registerTempTable('trial_phase_mapping')
trial_phase_mapping.write.mode('overwrite').saveAsTable('trial_phase_mapping')


# Fetching trial indication mapping file for disease standardization
trial_disease_mapping = spark.read.format('csv').option("header", "true").option("delimiter", "^") \
    .option("multiLine", "true").load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/"
                                      "clinical-data-lake/uploads/study_indication_ta_mapping.csv")
#trial_disease_mapping.registerTempTable('trial_disease_mapping')
trial_disease_mapping.write.mode('overwrite').saveAsTable('trial_disease_mapping')


trial_disease_mapping_final = spark.sql("""
select
    data_source,
    trial_id,
    concat_ws('\\|',collect_set(coalesce(standardized_disease,final_disease))) standardized_disease
from trial_disease_mapping
group by 1,2
""")

#trial_disease_mapping_final.registerTempTable('trial_disease_mapping_final')
trial_disease_mapping_final.write.mode('overwrite').saveAsTable('trial_disease_mapping_final')


# Get AACT obselete ID information
id_info = spark.sql("""
select
    nct_id,
    id_value
from sanofi_ctfo_datastore_staging_$$db_env.aact_id_information
where id_type = 'nct_alias'
group by 1,2
""")

#id_info.registerTempTable('id_info')
id_info.write.mode('overwrite').saveAsTable('id_info')


# Get trial information from AACT
# SK-HH Added the where clause for overall_status
temp_aact_trial_data = spark.sql("""
select
    'aact' as data_src_nm,
    study.nct_id as src_trial_id,
    coalesce(study.brief_title, study.official_title) as trial_title,
    study.phase as trial_phase,
    drug.name as trial_drug,
    disease.condition as trial_indication,
    study.nct_id as nct_id,
    study.nct_id as protocol_ids
from sanofi_ctfo_datastore_staging_$$db_env.aact_studies study
left outer join
(select d.nct_id, concat_ws('\|',collect_set(d.name)) as name from
sanofi_ctfo_datastore_staging_$$db_env.aact_interventions d where
lower(trim(d.intervention_type))='drug' group by d.nct_id) drug
on study.nct_id = drug.nct_id
left outer join
(select e.nct_id, concat_ws('\|',collect_set(name)) as condition from
sanofi_ctfo_datastore_staging_$$db_env.aact_conditions e group by e.nct_id) disease
on study.nct_id = disease.nct_id
where coalesce(trim(lower(study.overall_status)),'') not in ('approved for marketing','available','no longer available','temporarily not available','withheld')
group by 1,2,3,4,5,6,7,8
""")

#temp_aact_trial_data.registerTempTable('temp_aact_trial_data')
temp_aact_trial_data.write.mode('overwrite').saveAsTable('temp_aact_trial_data')


# Get trial information from CITELINE
# added lower and trin in the like condition
temp_citeline_trial_data = spark.sql("""
select
    'citeline' as data_src_nm,
    ct.trial_id as src_trial_id,
    ct.trial_title as trial_title ,
    ct.trial_phase as trial_phase,
    ct.drug_name as trial_drug,
    ct.disease_name as trial_indication,
    case when trim(lower(src_protocol_ids)) like '%nct%' then src_protocol_ids else null end
    as nct_id,
    case when src_protocol_ids = 'NA' then null else src_protocol_ids end as protocol_ids
from sanofi_ctfo_datastore_staging_$$db_env.citeline_trialtrove as ct
lateral view explode(split(coalesce(protocol_ids,'NA'),'\\\|'))one as src_protocol_ids
group by 1,2,3,4,5,6,7,8
""")
#temp_citeline_trial_data.registerTempTable('temp_citeline_trial_data')
temp_citeline_trial_data.write.mode('overwrite').saveAsTable('temp_citeline_trial_data')


# Get trial information for DQS
# DQS is code different compare to janssen bcz od study_site table

temp_dqs_trial_data = spark.sql("""
select
        data_src_nm,
        src_trial_id,
        trial_title,
        trial_phase,
        trial_drug,
        trial_indication,
        nct_id,
         protocol_ids
from
        (select
                'ir' as data_src_nm,
                member_study_id as src_trial_id,
                study_title as trial_title ,
                phase as trial_phase,
                '' as trial_drug,
                primary_indication as trial_indication,
                nct_number as nct_id,
                case when nct_number is not null then  concat(member_study_id,'_', nct_number)
                else member_study_id end as protocol_ids
                from sanofi_ctfo_datastore_staging_$$db_env.dqs_study
        group by 1,2,3,4,5,6,7,8) tmp
lateral view explode(split(protocol_ids, '_'))one as protocol_id
group by 1,2,3,4,5,6,7,8
""")
#temp_dqs_trial_data.registerTempTable('temp_dqs_trial_data')



temp_dqs_trial_data.write.mode('overwrite').saveAsTable('temp_dqs_trial_data')


# Get trial information from CTMS
temp_ctms_trial_data_rnk = spark.sql("""
select
    'ctms' as data_src_nm,
    a.STUDY_CODE as src_trial_id,
    a.PROTOCOL_TITLE as trial_title ,
    a.PHASE_CODE as trial_phase,
    concat_ws('\;',collect_set(b.PRODUCT_NAME)) as trial_drug,
    concat_ws('\;',collect_set(c.INDICATION)) as trial_indication,
    '' as nct_id,
    a.STUDY_CODE as protocol_ids
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study as a
left outer join sanofi_ctfo_datastore_staging_$$db_env.ctms_product b
on lower(trim(a.INVESTIGATIONAL_PRODUCT)) = lower(trim(b.PRODUCT_CODE))
left outer join sanofi_ctfo_datastore_staging_$$db_env.ctms_study_indication c
on lower(trim(a.STUDY_CODE)) = lower(trim(c.STUDY_CODE))
group by 1,2,3,4,7,8
""")
#temp_ctms_trial_data_rnk.registerTempTable('temp_ctms_trial_data_rnk')
temp_ctms_trial_data_rnk.write.mode('overwrite').saveAsTable('temp_ctms_trial_data_rnk')


temp_ctms_trial_data = spark.sql("""
select
data_src_nm,
src_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication,
nct_id,
protocol_ids
from temp_ctms_trial_data_rnk
""")
temp_ctms_trial_data.dropDuplicates()
#temp_ctms_trial_data.registerTempTable("temp_ctms_trial_data")
temp_ctms_trial_data.write.mode('overwrite').saveAsTable('temp_ctms_trial_data')

# Union all data sources

temp_all_trial_data = spark.sql("""
select *
from temp_aact_trial_data
union
select *
from temp_citeline_trial_data
union
select *
from temp_dqs_trial_data
Union
select *
from temp_ctms_trial_data
""")
#temp_all_trial_data.registerTempTable("temp_all_trial_data")
temp_all_trial_data.write.mode('overwrite').saveAsTable('temp_all_trial_data')


# Save on HDFS
temp_all_trial_data.write.mode('overwrite').saveAsTable('temp_all_trial_data')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data')
temp_all_trial_data.repartition(100).write.mode('overwrite').parquet(write_path)



# Perform clean up for all data sources
temp_all_trial_data_cleaned_aact_cite = spark.sql("""
select /*+ broadcast(phase, disease) */
    data_src_nm,
    concat(data_src_nm,'_',src_trial_id) as src_trial_id,
    case when trim(trial_title) in (' ','','null','NA', 'na') then null
    else trim(lower(regexp_replace(trial_title,'[^0-9A-Za-z]',' '))) end as trial_title,
    case when phase.standard_value is null or phase.standard_value = '' then 'Other'
    else phase.standard_value end as trial_phase,
    case when trim(trial_drug) in (' ','','null','NA', 'na') then null
    else trim(lower(regexp_replace(trial_drug,'[^0-9A-Za-z]',' '))) end as trial_drug,
    coalesce((case when trim(trial_indication) in (' ','','null','NA', 'na') then null
    else disease.standardized_disease end) ,base.trial_indication) as trial_indication,
    protocol_ids
from
temp_all_trial_data base
left outer join trial_phase_mapping phase
on lower(trim(base.trial_phase)) = lower(trim(phase.trial_phase))
left outer join trial_disease_mapping_final disease
on lower(trim(base.data_src_nm)) = lower(trim(disease.data_source))
and base.src_trial_id = disease.trial_id
where base.data_src_nm in ('citeline','aact')
group by 1,2,3,4,5,6,7
""")
#temp_all_trial_data_cleaned_aact_cite.registerTempTable('temp_all_trial_data_cleaned_aact_cite')
temp_all_trial_data_cleaned_aact_cite.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_aact_cite')


# Save on HDFS
temp_all_trial_data_cleaned_aact_cite.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_aact_cite')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_cleaned_aact_cite')
temp_all_trial_data_cleaned_aact_cite.repartition(100).write.mode('overwrite').parquet(write_path)

latest_disease_mapping = spark.read.format('csv').option("header", "true").\
    option("delimiter", ",").\
    load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/disease_mapping.csv")
latest_disease_mapping.registerTempTable('latest_disease_mapping')

disease_mapping_1 = spark.sql("select * from latest_disease_mapping")
#disease_mapping_1.registerTempTable('disease_mapping_1')
disease_mapping_1.write.mode('overwrite').saveAsTable('disease_mapping_1')




temp_all_trial_data_cleaned_dqs_ctms = spark.sql("""
select
data_src_nm,
src_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication_exp as trial_indication,
nct_id,
protocol_ids
from
temp_all_trial_data
lateral view explode(split(trial_indication, '\\\;'))one as trial_indication_exp
where data_src_nm in ('ctms','ir')
group by 1,2,3,4,5,6,7,8
""")
#temp_all_trial_data_cleaned_dqs_ctms.registerTempTable('temp_all_trial_data_cleaned_dqs_ctms')
temp_all_trial_data_cleaned_dqs_ctms.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_dqs_ctms')




temp_all_trial_data_cleaned_dqs_ctms_1 = spark.sql("""
select /*+ broadcast(phase, disease) */
    data_src_nm,
    concat(data_src_nm,'_',src_trial_id) as src_trial_id,
    case when trim(trial_title) in (' ','','null','NA', 'na') then null
    else trim(lower(regexp_replace(trial_title,'[^0-9A-Za-z]',' '))) end as trial_title,
    case when phase.standard_value is null or phase.standard_value = '' then 'Other'
    else phase.standard_value end as trial_phase,
    case when trim(trial_drug) in (' ','','null','NA', 'na') then null
    else trim(lower(regexp_replace(trial_drug,'[^0-9A-Za-z]',' '))) end as trial_drug,
    coalesce((case when trim(trial_indication) in (' ','','null','NA', 'na') then null
    else disease.standard_disease end) ,base.trial_indication) as trial_indication,
    protocol_ids
from
temp_all_trial_data_cleaned_dqs_ctms base
left outer join trial_phase_mapping phase
on lower(trim(base.trial_phase)) = lower(trim(phase.trial_phase))
left outer join disease_mapping_1 disease
on lower(trim(base.trial_indication)) = lower(trim(disease.disease))
group by 1,2,3,4,5,6,7
""")
#temp_all_trial_data_cleaned_dqs_ctms_1.registerTempTable('temp_all_trial_data_cleaned_dqs_ctms_1')
temp_all_trial_data_cleaned_dqs_ctms_1.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_dqs_ctms_1')


temp_all_trial_data_cleaned_dqs_ctms_2 = spark.sql("""
select
   data_src_nm,
    src_trial_id,
   trial_title ,
    trial_phase,
    trial_drug,
    concat_ws('\;',collect_set(trial_indication)) as trial_indication,
protocol_ids
from temp_all_trial_data_cleaned_dqs_ctms_1
group by 1,2,3,4,5,7
""")
#temp_all_trial_data_cleaned_dqs_ctms_2.registerTempTable('temp_all_trial_data_cleaned_dqs_ctms_2')
temp_all_trial_data_cleaned_dqs_ctms_2.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_dqs_ctms_2')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_cleaned_dqs_ctms_2')
temp_all_trial_data_cleaned_dqs_ctms_2.repartition(100).write.mode('overwrite').parquet(write_path)


temp_all_trial_data_cleaned = spark.sql("""
select * from temp_all_trial_data_cleaned_dqs_ctms_2
union
select * from temp_all_trial_data_cleaned_aact_cite
""")

#temp_all_trial_data_cleaned.registerTempTable('temp_all_trial_data_cleaned')
temp_all_trial_data_cleaned.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_cleaned')
temp_all_trial_data_cleaned.repartition(100).write.mode('overwrite').parquet(write_path)





# Replace obselete NCT ID with latest ID in protocol ID column
#Sk-HH Added the where condition
temp_all_trial_data_cleaned_obs_remove = spark.sql("""
select /*+ broadcast(info) */
    data_src_nm,
    src_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    case when trim(lower(trial_indication)) in ('ct gov','ct. gov', 'ct.gov') then null
    else trial_indication end as trial_indication,
    coalesce(info.nct_id,base.protocol_ids) as protocol_ids
from temp_all_trial_data_cleaned base
left outer join id_info info
on base.protocol_ids = info.id_value
where (data_src_nm in ('citeline','aact') and regexp_replace(trim(lower(trial_title)),'[^0-9A-Za-z]','')
is not null and regexp_replace(trim(lower(trial_title)),'[^0-9A-Za-z]','') != '') or data_src_nm in ('ctms','ir')
group by 1,2,3,4,5,6,7
""")
#temp_all_trial_data_cleaned_obs_remove.registerTempTable('temp_all_trial_data_cleaned_obs_remove')
# Save on HDFS
temp_all_trial_data_cleaned_obs_remove.write.mode('overwrite').saveAsTable(
    'temp_all_trial_data_cleaned_obs_remove')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_cleaned_obs_remove')
temp_all_trial_data_cleaned_obs_remove.repartition(100).write.mode('overwrite').parquet(write_path)

# Code to resolve duplicate protocol_ids in Citeline
# 2 more where condition addtional to janssen
#removed a where condition
common_id_citeline = spark.sql("""
select
    data_src_nm,
    protocol_ids as common_id,
    concat_ws('\#',collect_set(src_trial_id)) as ds_trial_id
from temp_all_trial_data_cleaned_obs_remove
where data_src_nm = 'citeline'
and (trim(lower(protocol_ids)) like 'nct%' or trim(lower(protocol_ids)) like 'eudract%')
group by 1,2
having size(collect_set(src_trial_id)) > 1
""")
#common_id_citeline.registerTempTable('common_id_citeline')
common_id_citeline.write.mode('overwrite').saveAsTable('common_id_citeline')

# Preparing data for recursive match code
trial_df = spark.sql("""
select
    base.src_trial_id,
    concat_ws('\|',collect_set(base.protocol_ids)) as protocol_ids_list
from temp_all_trial_data_cleaned_obs_remove base
inner join (select distinct explode(split(ds_trial_id,'\\\#')) as src_trial_id
from common_id_citeline) ctl
on lower(trim(base.src_trial_id)) = lower(trim(ctl.src_trial_id))
group by 1
""")
#trial_df.registerTempTable('trial_df')
trial_df.write.mode('overwrite').saveAsTable('trial_df')

# Parameter set for recursive match function

pandas_df = trial_df.toPandas()
trial_id_dict = {}
for index, row in pandas_df.iterrows():
    key = row['src_trial_id']
    protocol_ids_list = row['protocol_ids_list'].split('|')
    if key in trial_id_dict:
        trial_id_dict[key].extend(protocol_ids_list)
    else:
        trial_id_dict[key] = protocol_ids_list


# Intermediate function
def common_data(list1, list2):
    result = False
    for x in list1:
        for y in list2:
            if x == y:
                result = True
                return result
    return result


# Actual function to match src trials based on protocol ids
def recursive_match(src_trial_id_mapping):
    processed_list = []
    src_trial_id_combination = {}
    # golden_id = 'golden_id'
    golden_id = 1000
    for key in src_trial_id_mapping:
        current_protocol_ids_list = []
        mapping_list = []
        golden_id = golden_id + 1
        golden_id_key = str(golden_id)
        if key not in processed_list:
            processed_list.append(key)
            mapping_list.append(key)
            current_protocol_ids_list = src_trial_id_mapping[key]
            flag = True
            while flag == True:
                flag = False
                for inner_key in src_trial_id_mapping:
                    if inner_key not in processed_list:
                        inner_key_protocol_ids_list = src_trial_id_mapping[inner_key]
                        result = common_data(inner_key_protocol_ids_list, current_protocol_ids_list)
                        if result:
                            flag = True
                            current_protocol_ids_list.extend(inner_key_protocol_ids_list)
                            processed_list.append(inner_key)
                            mapping_list.append(inner_key)
                src_trial_id_combination[golden_id_key] = mapping_list
    final_output = []
    print('Creating final_output ----------------')
    for key in src_trial_id_combination:
        for src_trial_id in src_trial_id_combination[key]:
            final_output.append([key, src_trial_id])
    print('Creating DF ----------------')
    df = pd.DataFrame(final_output, columns=['golden_id', 'src_trial_id'])
    return df


df = recursive_match(trial_id_dict)
protocol_ids_match = spark.createDataFrame(df)
protocol_ids_match.write.mode('overwrite').saveAsTable('protocol_ids_match')

protocol_ids_match.registerTempTable('protocol_ids_match')

# Generating final data to combine with all records
match_with_src_ids = spark.sql("""
select
    src_trial_id,
    clt_trial_id,
    max_src_id
from protocol_ids_match base
inner join
(select
    golden_id,
    concat_ws('\#',collect_set(src_trial_id)) as clt_trial_id,
    max(src_trial_id) as max_src_id
from protocol_ids_match
group by 1) id_match
on base.golden_id = id_match.golden_id
""")
#match_with_src_ids.registerTempTable('match_with_src_ids')
match_with_src_ids.write.mode('overwrite').saveAsTable('match_with_src_ids')


# Final data after combining citeline similar trials based on protocol id match
temp_all_trial_data_cleaned_citeline_match = spark.sql("""
select /*+ broadcast(id_match) */
    base.data_src_nm,
    coalesce(id_match.clt_trial_id,base.src_trial_id) as src_trial_id,
    coalesce(clean_data.trial_title,base.trial_title) as trial_title,
    coalesce(clean_data.trial_phase,base.trial_phase) as trial_phase,
    coalesce(clean_data.trial_drug,base.trial_drug) as trial_drug,
    coalesce(clean_data.trial_indication,base.trial_indication) as trial_indication,
    base.protocol_ids
from temp_all_trial_data_cleaned_obs_remove base
left outer join match_with_src_ids id_match
on base.src_trial_id = id_match.src_trial_id
left outer join temp_all_trial_data_cleaned clean_data
on id_match.max_src_id = clean_data.src_trial_id
group by 1,2,3,4,5,6,7
""")
temp_all_trial_data_cleaned_citeline_match.write.mode('overwrite').saveAsTable(
    'temp_all_trial_data_cleaned_citeline_match')

#Finding common country specific ID across data sources, there are records with same src site id and all details but different protocol id , this will removie those #specific ids but there will be case where same src ids will have protocol ids who are start with the terms which are in where filter
temp_trial_data_common_id = spark.sql("""
select
    protocol_ids,
    concat_ws('\;',collect_set(src_trial_id)) as ds_trial_id
from temp_all_trial_data_cleaned_citeline_match
where trim(lower(protocol_ids)) like 'nct%' or trim(lower(protocol_ids)) like 'eudract%'
or trim(lower(protocol_ids)) like 'euctr%' or trim(lower(protocol_ids)) like 'jprn%'
or trim(lower(protocol_ids)) like 'chictr%'  or trim(lower(protocol_ids)) like 'irct%'
or trim(lower(protocol_ids)) like 'ctri%' or trim(lower(protocol_ids)) like 'actrn%'
or trim(lower(protocol_ids)) like 'isrctn%' or trim(lower(protocol_ids)) like 'drks%'
or trim(lower(protocol_ids)) like 'ntr%' or trim(lower(protocol_ids)) like 'kct%'
or trim(lower(protocol_ids)) like 'tctr%' or trim(lower(protocol_ids)) like 'rbr-%'
or trim(lower(protocol_ids)) like 'pactr%' or trim(lower(protocol_ids)) like 'supergen%'
or trim(lower(protocol_ids)) like 'slctr%' or trim(lower(protocol_ids)) like 'rpcec%'
or trim(lower(protocol_ids)) like 'lbctr%' or trim(lower(protocol_ids)) in (select distinct trim(lower(src_trial_id)) from temp_ctms_trial_data)
group by 1
having size(collect_set(src_trial_id)) > 1
""")
#temp_trial_data_common_id.registerTempTable('temp_trial_data_common_id')
temp_trial_data_common_id.write.mode('overwrite').saveAsTable('temp_trial_data_common_id')



#Finding common country specific ID across data sources, there are records with same src site id and all details but different protocol id , this will removie those #specific ids but there will be case where same src ids will have protocol ids who are start with the terms which are in where filter




# Assigning temporary cluster IDs to protocol_ids for merging records
temp_trial_comnid_clusterid = spark.sql("""
select
    concat('common_',row_number() over(order by null)) as comn_cluster_id,
    temp.protocol_ids
from
(select distinct protocol_ids from temp_trial_data_common_id where protocol_ids is not null and protocol_ids <> '') temp
""")

#temp_trial_comnid_clusterid.registerTempTable('temp_trial_comnid_clusterid')
temp_trial_comnid_clusterid.write.mode('overwrite').saveAsTable('temp_trial_comnid_clusterid')

temp_trial_comnid_clusterid.repartition(1).write.mode('overwrite').option("header","true").option("delimiter", '^').save('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/trial/$$data_dt/')

temp_trial_comnid_clusterid_as = spark.read.format("parquet").option("header", "true").load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/trial/$$data_dt/")




# SK-HH changed the precedence according to the required rules
#In case DQS studies get merged with external data sources, assign a cluster. DQS study to be considered as golden record
temp_trial_data_comnid_map = spark.sql("""
select /*+ broadcast(clstr_data) */
distinct
    base.*,
    clstr_data.comn_cluster_id,
    case when data_src_nm = 'ctms' then '1'
    when data_src_nm = 'ir' then '2'
    when data_src_nm = 'citeline' then '3'
    when data_src_nm = 'aact' then '4'
    else '5' end as precedence
from
temp_all_trial_data_cleaned_citeline_match base
inner join temp_trial_comnid_clusterid clstr_data
on base.protocol_ids = clstr_data.protocol_ids
""")

temp_trial_data_comnid_map.write.mode('overwrite').saveAsTable('temp_trial_data_comnid_map')

temp_trial_comnid_clustered = spark.sql("""
select
    concat_ws('\;',collect_set(data_src_nm)) as data_src_nm,
    concat_ws('\;',collect_set(src_trial_id)) as src_trial_id,
    comn_cluster_id
from temp_trial_data_comnid_map
group by comn_cluster_id
""")
#temp_trial_comnid_clustered.registerTempTable('temp_trial_comnid_clustered')
temp_trial_comnid_clustered.write.mode('overwrite').saveAsTable('temp_trial_comnid_clustered')


temp_trial_comnid_precedence_1 = spark.sql("""

select
comn_cluster_id,
trial_title,
trial_phase,
trial_drug,
trial_indication,
precedence,
src_trial_id,
Rank () over (partition by comn_cluster_id order by precedence asc, src_trial_id desc) rnk
from
temp_trial_data_comnid_map
group by 1,2,3,4,5,6,7
""")
#temp_trial_comnid_precedence_1.registerTempTable('temp_trial_comnid_precedence_1')
temp_trial_comnid_precedence_1.write.mode('overwrite').saveAsTable('temp_trial_comnid_precedence_1')

temp_trial_comnid_precedence = spark.sql("""
select
comn_cluster_id,
max(trial_title) as trial_title,
max(trial_phase) as trial_phase,
max(trial_drug) as trial_drug,
max(trial_indication) as trial_indication
from

temp_trial_comnid_precedence_1
where rnk =1

group by 1
""")
#temp_trial_comnid_precedence.registerTempTable('temp_trial_comnid_precedence')
temp_trial_comnid_precedence.write.mode('overwrite').saveAsTable('temp_trial_comnid_precedence')




temp_trial_common_id_final = spark.sql("""
select
    base.comn_cluster_id,
    base.data_src_nm,
    base.src_trial_id,
    prec_data.trial_title,
    prec_data.trial_phase,
    prec_data.trial_drug,
    prec_data.trial_indication
from
temp_trial_comnid_clustered base
left outer join
temp_trial_comnid_precedence prec_data
on base.comn_cluster_id = prec_data.comn_cluster_id
group by 1,2,3,4,5,6,7
""")

temp_trial_common_id_final.write.mode('overwrite').saveAsTable('temp_trial_common_id_final')

#Sk-HH added this block
to_group_by_clusters = spark.sql("""
select
    comn_cluster_id
from
(select
    ds_trial_ids,
    count(distinct comn_cluster_id)
from
    temp_trial_common_id_final
lateral view explode(split(src_trial_id,'\\\;'))one as ds_trial_ids
group by 1
having count(distinct comn_cluster_id)>1) base0
inner join
(select
    ds_trial_ids,
    comn_cluster_id
from
    temp_trial_common_id_final
lateral view explode(split(src_trial_id,'\\\;'))one as ds_trial_ids) prep
on base0.ds_trial_ids = prep.ds_trial_ids
group by 1
""")

#to_group_by_clusters.registerTempTable("to_group_by_clusters")
to_group_by_clusters.write.mode('overwrite').saveAsTable('to_group_by_clusters')


#added where condition in group by cluster table
temp_trial_data_common_id_result = spark.sql("""
select
    concat_ws('\;',collect_set(data_src_nm)) as data_src_nm,
    concat_ws('\;',collect_set(src_trial_id)) as src_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from temp_trial_common_id_final
where comn_cluster_id in (select  comn_cluster_id from to_group_by_clusters)
group by trial_title,trial_phase,trial_drug,trial_indication
union
select
    data_src_nm,
    src_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from temp_trial_common_id_final
where comn_cluster_id not in (select  comn_cluster_id from to_group_by_clusters)
group by 1,2,3,4,5,6
""")

# Save on HDFS
temp_trial_data_common_id_result.write.mode('overwrite').saveAsTable(
    'temp_trial_data_common_id_result')

# Push to S3
write_path = path.replace('table_name', 'temp_trial_data_common_id_result')
temp_trial_data_common_id_result.repartition(100).write.mode('overwrite').parquet(write_path)

# Preparing the remaining data for Dedupe
temp_trial_data_prep = spark.sql("""
select /*+ broadcast(temp) */
    base.data_src_nm,
    base.src_trial_id,
    base.trial_title,
    base.trial_phase,
    base.trial_drug,
    base.trial_indication
from temp_all_trial_data_cleaned_citeline_match base
left outer join
    (select src_trial_ids from temp_trial_common_id_final
    lateral view explode(split(src_trial_id,'\\\;'))one as src_trial_ids group by 1) temp
on lower(trim(base.src_trial_id)) = lower(trim(temp.src_trial_ids))
where temp.src_trial_ids is null
""")
temp_trial_data_prep = temp_trial_data_prep.dropDuplicates()
#temp_trial_data_prep.registerTempTable('temp_trial_data_prep')
temp_trial_data_prep.write.mode('overwrite').saveAsTable('temp_trial_data_prep')


temp_trial_master_data_prep_intd = spark.sql("""
select *
from temp_trial_data_common_id_result
union
select *
from temp_trial_data_prep
""")
temp_trial_master_data_prep_intd = temp_trial_master_data_prep_intd.dropDuplicates()
#temp_trial_master_data_prep_intd.registerTempTable('temp_trial_master_data_prep_intd')
temp_trial_master_data_prep_intd.write.mode('overwrite').saveAsTable('temp_trial_master_data_prep_intd')


#added this function
def sort_collected_data_colon(text=''):
    try:
        text=text.replace('#',';')
    except:
        text=''
    try:
        collected_data = text.split(';')
    except:
        collected_data=['']
    collected_data.sort()
    text = u';'.join(collected_data).encode('utf-8').strip()
    return text.decode('utf-8')

spark.udf.register('sort_collected_data_colon', sort_collected_data_colon)

# Final Data Prep Output for Dedupe
#updated this block
temp_all_trial_data_prep_intd1 = spark.sql("""
select
    uid,
    hash_uid,
    data_src_nm,
    concat_ws('\;',collect_set(exp_ds_trial_id)) as ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from
(select
    uid,
    hash_uid,
    concat_ws('\;',collect_set(exp_data_source)) as data_src_nm,
    ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from
(select
cast(row_number() over(order by coalesce(src_trial_id, 'NA'),coalesce(trial_phase, 'NA'),coalesce(trial_title, 'NA'),coalesce(trial_drug, 'NA'),coalesce(trial_indication, 'NA')) as bigint) as uid,
substr(sha2(concat(coalesce(sort_collected_data_colon(src_trial_id), 'NA'),coalesce(trial_phase, 'NA'),coalesce(trial_title, 'NA'),coalesce(trial_drug, 'NA'),coalesce(trial_indication, 'NA')),256),1,16) as hash_uid,
data_src_nm,
regexp_replace(src_trial_id,'#',';') as ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication
from temp_trial_master_data_prep_intd
)
lateral view explode(split(coalesce(data_src_nm,'NA'),'\\\;'))one as exp_data_source
group by uid,hash_uid,ds_trial_id,trial_title, trial_phase, trial_drug, trial_indication)
lateral view explode(split(coalesce(ds_trial_id,'NA'),'\\\;'))one as exp_ds_trial_id
group by uid,hash_uid,data_src_nm,trial_title, trial_phase, trial_drug, trial_indication
""")
#temp_all_trial_data_prep_intd1.registerTempTable("temp_all_trial_data_prep_intd1")

# Save on HDFS
temp_all_trial_data_prep_intd1.write.mode('overwrite').\
    saveAsTable('temp_all_trial_data_prep_intd1')

#logic to give datasource rank

temp_all_trial_data_prep_intd2_2 = spark.sql ("""
select temp_all_trial_data_prep_intd1.*,
case when data_src_nm like '%ctms%' then 1
when data_src_nm like '%ir%' then 2
when data_src_nm like '%citeline%' then 3
when data_src_nm like '%aact%' then 4
else 5 end as datasource_rnk from  temp_all_trial_data_prep_intd1""")
#temp_all_trial_data_prep_intd2_2.registerTempTable("temp_all_trial_data_prep_intd2_2")
temp_all_trial_data_prep_intd2_2.write.mode('overwrite').\
    saveAsTable('temp_all_trial_data_prep_intd2_2')


# Logic to resolve issue of src ID getting tagged to multiple clusters because of
# multiple protocol IDs

multi_cluster_df = spark.sql("""
select
        base1.ds_trial_ids as cluster_id,
		prep.uid as muid,
		prep.hash_uid,
		prep.data_src_nm,
		prep.ds_trial_id,
		prep.trial_title,
		prep.trial_phase,
		prep.trial_drug,
		prep.trial_indication,
		prep.datasource_rnk,
		row_number() over (partition by base1.ds_trial_ids order by prep.datasource_rnk, prep.hash_uid desc) as rnk
from
(select
base0.ds_trial_ids,
uid
from
(select
        ds_trial_ids,
        count(distinct uid)
from
        temp_all_trial_data_prep_intd2_2
lateral view explode(split(ds_trial_id,'\\\;'))one as ds_trial_ids
group by 1
having count(distinct uid)>1) base0
inner join
(select
        ds_trial_ids,
        uid
from
        temp_all_trial_data_prep_intd2_2
lateral view explode(split(ds_trial_id,'\\\;'))one as ds_trial_ids) prep
on base0.ds_trial_ids = prep.ds_trial_ids
group by 1,2) base1
inner join
(select *
from temp_all_trial_data_prep_intd2_2 prep) prep
on trim(lower(base1.uid)) = trim(lower(prep.uid))
""")
#multi_cluster_df.registerTempTable("multi_cluster_df")
multi_cluster_df.write.mode('overwrite').saveAsTable('multi_cluster_df')


clustered_src_ids = spark.sql("""
select
cluster_id,
concat_ws('\;',collect_set(ds_trial_id)) as ds_trial_id,
concat_ws('\;',collect_set(data_src_nm)) as data_src_nm
from
multi_cluster_df
group by 1
""")

clustered_src_ids.registerTempTable("clustered_src_ids")
clustered_src_ids.write.mode('overwrite').saveAsTable('clustered_src_ids')

# block added to remove duplicate
#capture all incorrect records
temp_all_trial_data_prep_1 = spark.sql("""
select
        base.muid as uid,
        base.hash_uid,
        clstr.data_src_nm,
        clstr.ds_trial_id,
        base.trial_title,
        base.trial_phase,
        base.trial_drug,
        base.trial_indication
from
multi_cluster_df base
inner join
clustered_src_ids clstr
on base.cluster_id = clstr.cluster_id
where base.rnk = 1""")
#temp_all_trial_data_prep_1.registerTempTable("temp_all_trial_data_prep_1")
temp_all_trial_data_prep_1.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_1')


temp_all_trial_data_prep_2_temp = spark.sql("""
select uid, count(distinct ds_trial_id) as cnt_ds_trial_id from temp_all_trial_data_prep_1 group by 1
""")
#temp_all_trial_data_prep_2_temp.registerTempTable("temp_all_trial_data_prep_2_temp")
temp_all_trial_data_prep_2_temp.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_2_temp')


temp_all_trial_data_prep_2 = spark.sql("""
select uid from temp_all_trial_data_prep_2_temp where cnt_ds_trial_id>1 group by 1
""")
#temp_all_trial_data_prep_2.registerTempTable("temp_all_trial_data_prep_2")
temp_all_trial_data_prep_2.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_2')


#apply rank
temp_all_trial_data_prep_3 = spark.sql("""
select uid, hash_uid,data_src_nm, trial_title, trial_phase, trial_drug, trial_indication, row_number() over (partition by trial_title order by hash_uid desc) as rnk from temp_all_trial_data_prep_1 where uid in (select distinct uid from temp_all_trial_data_prep_2) group by 1,2,3,4,5,6,7
""")
#temp_all_trial_data_prep_3.registerTempTable("temp_all_trial_data_prep_3")
temp_all_trial_data_prep_3.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_3')


temp_all_trial_data_prep_4 = spark.sql("""
select uid,
concat_ws('\;',collect_set(ds_trial_id_1)) as ds_trial_id
from (select distinct uid,
ds_trial_id_1
from temp_all_trial_data_prep_1
lateral view explode(split(ds_trial_id,'\\\;'))one as ds_trial_id_1
where uid in (select distinct uid from temp_all_trial_data_prep_2))
group by 1
""")
#temp_all_trial_data_prep_4.registerTempTable("temp_all_trial_data_prep_4")
temp_all_trial_data_prep_4.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_4')


temp_all_trial_data_prep_5 = spark.sql("""
select uid, hash_uid, data_src_nm, ds_trial_id, trial_title, trial_phase,trial_drug,trial_indication
from temp_all_trial_data_prep_1 where uid not in (select distinct uid from temp_all_trial_data_prep_2)
union
select a.uid,
a.hash_uid,
a.data_src_nm,
b.ds_trial_id,
a.trial_title,
a.trial_phase,
a.trial_drug,
a.trial_indication
from temp_all_trial_data_prep_3 a
inner join temp_all_trial_data_prep_4 b
on a.uid = b.uid
where a.rnk = 1
""")
#temp_all_trial_data_prep_5.registerTempTable("temp_all_trial_data_prep_5")
temp_all_trial_data_prep_5.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_5')


# Assigning unique row number for each record for dedupeio algorithm and sha2 hash id to identify same record in multiple runs

temp_all_trial_data_prep_6 = spark.sql(""" select
distinct
base.uid,
base.hash_uid,
base.data_src_nm,
base.ds_trial_id,
base.trial_title,
base.trial_phase,
base.trial_drug,
base.trial_indication,
base.datasource_rnk
from
temp_all_trial_data_prep_intd2_2 base
left outer join
( select muid from
multi_cluster_df clstr group by 1)
on base.uid = clstr.muid
where clstr.muid is null
union
select
uid,
hash_uid,
data_src_nm,
ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication,
1 as datasource_rnk
from
temp_all_trial_data_prep_5
""")
#temp_all_trial_data_prep_6.registerTempTable("temp_all_trial_data_prep_6")
temp_all_trial_data_prep_6.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_6')


temp_all_trial_data_prep = spark.sql("""
select
uid,
hash_uid,
concat_ws('\;',collect_set(data_src_nm_1)) as data_src_nm,
ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication
from (select
uid,
hash_uid,
data_src_nm_1,
ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication
from temp_all_trial_data_prep_6
lateral view explode(split(data_src_nm,'\\\;'))one as data_src_nm_1
) a
group by uid,
hash_uid,
ds_trial_id,
trial_title,
trial_phase,
trial_drug,
trial_indication
""")
#temp_all_trial_data_prep.registerTempTable("temp_all_trial_data_prep")


'''
temp_all_trial_data_prep = spark.sql("""
select base.*
from
        temp_all_trial_data_prep_intd1 base
left outer join
        multi_cluster_df clstr
on base.uid = clstr.uid
where clstr.uid is null
union
select
        base.uid,
        base.hash_uid,
        base.data_src_nm,
        clstr.ds_trial_id,
        base.trial_title,
        base.trial_phase,
        base.trial_drug,
        base.trial_indication
from
multi_cluster_df base
inner join
clustered_src_ids clstr
on base.cluster_id = clstr.cluster_id
where base.rnk = 1
""")
temp_all_trial_data_prep.registerTempTable("temp_all_trial_data_prep")
'''
temp_all_trial_data_prep.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_prep')
temp_all_trial_data_prep.repartition(100).write.mode('overwrite').parquet(write_path)



