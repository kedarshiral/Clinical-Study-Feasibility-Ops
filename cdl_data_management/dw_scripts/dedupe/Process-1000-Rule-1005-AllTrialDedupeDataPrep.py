################################# Module Information ######################################
#  Module Name         : All Trial Dedupe Data Prep
#  Purpose             : This will prepare input data for Trial Dedupe for all data sources
#  Pre-requisites      : L1 source table required: aact_studies, citeline_trialtrove, who_trials
#  Last changed on     : 20-03-2023
#  Last changed by     : Kashish
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
import json
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf, col, lit
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

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

path = bucket_path + '/applications/commons/temp/mastering/trial/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

print("I'm fetching code from dedupe test branch!!!!!")

# Fetching trial phase mapping file for phase standardization
trial_phase_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/uploads/trial_phase.csv'.format(bucket_path=bucket_path))
# trial_phase_mapping_temp.registerTempTable('trial_phase_mapping_temp')
trial_phase_mapping_temp.write.mode('overwrite').saveAsTable('trial_phase_mapping_temp')

trial_phase_mapping = spark.sql(""" select distinct * from (select trial_phase, standard_value from trial_phase_mapping_temp 
union select standard_value, standard_value from trial_phase_mapping_temp )  """)
trial_phase_mapping.registerTempTable('trial_phase_mapping')

latest_disease_mapping = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping """)
latest_disease_mapping.registerTempTable('latest_disease_mapping')

mesh_mapping_1 = spark.sql("""select distinct * from (select mesh, standard_mesh from latest_disease_mapping
union select standard_mesh, standard_mesh from latest_disease_mapping) """)
# disease_mapping_1.registerTempTable('disease_mapping_1')
mesh_mapping_1.write.mode('overwrite').saveAsTable('mesh_mapping_1')

drug_mapping_file = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/DRUG/Drug_Mapping_File/Drug_Mapping.xlsx".format(bucket_path=bucket_path))
drug_mapping_file.dropDuplicates()
drug_mapping_file.write.mode('overwrite').saveAsTable('drug_mapping_file')


json_file = open(
    CommonConstants.EMR_CODE_PATH + '/configs/job_executor/precedence_params.json',
    'r').read()
config_data = json.loads(json_file)
print(config_data)
data_source_order_dict = config_data['data_source_ordering']
a_cMap = [(k,) + (v,) for k, v in data_source_order_dict.items()]
data_precedence = spark.createDataFrame(a_cMap, ['Data_Src_Nm', 'Precedence'])
data_precedence.write.mode('overwrite').saveAsTable("data_precedence")

# Get AACT obselete ID information
id_info = spark.sql("""
select
    nct_id,
    id_value
from $$client_name_ctfo_datastore_staging_$$db_env.aact_id_information
where id_type = 'nct_alias'
group by 1,2
""")

# id_info.registerTempTable('id_info')
id_info.write.mode('overwrite').saveAsTable('id_info')

df_aact = spark.sql("""
select distinct nct_id ,mesh_term as mesh_name from 
(select mesh_term,nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_browse_conditions
        union 
     select mesh_term,nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_browse_interventions  )
""")
df_aact.createOrReplaceTempView("aact_data")

# Get trial information from AACT
# SK-HH Added the where clause for overall_status
temp_aact_trial_data = spark.sql("""
select
    'aact' as data_src_nm,
    trim(study.nct_id) as src_trial_id,
    trim(coalesce(study.official_title,study.brief_title)) as trial_title,
    trim(study.phase) as trial_phase,
    trim(drug.name) as trial_drug,
    coalesce(disease_1.mesh,disease.condition) as trial_indication,
    trim(study.nct_id) as nct_id,
    trim(study.nct_id) as protocol_ids
from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies study
left outer join
(select d.nct_id, concat_ws('\|',sort_array(collect_set(NULLIF(trim(d.name),'')),true)) as name from
$$client_name_ctfo_datastore_staging_$$db_env.aact_interventions d where
lower(trim(d.intervention_type))='drug' group by d.nct_id) drug
on lower(trim(study.nct_id)) = lower(trim(drug.nct_id))
left outer join
(select e.nct_id, concat_ws('\|',sort_array(collect_set(NULLIF(trim(name),'')),true)) as condition from
$$client_name_ctfo_datastore_staging_$$db_env.aact_conditions e group by e.nct_id) disease
on lower(trim(study.nct_id)) = lower(trim(disease.nct_id))

left outer join
(select e.nct_id, concat_ws('\|',collect_set(mesh_name)) as mesh from
aact_data e group by e.nct_id) disease_1
on lower(trim(study.nct_id)) = lower(trim(disease_1.nct_id))

group by 1,2,3,4,5,6,7,8

""")

# temp_aact_trial_data.registerTempTable('temp_aact_trial_data')
temp_aact_trial_data.write.mode('overwrite').saveAsTable('temp_aact_trial_data')

# Get trial information from CITELINE
# added lower and trim in the like condition

cite_trial_exp = spark.sql("""select distinct trial_id, trial_mesh_term_name_exp from 
$$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove 
                            lateral view outer explode (split(trial_mesh_term_name,'\\\|')) as trial_mesh_term_name_exp 
""")
cite_trial_exp.registerTempTable('cite_trial_exp')

cite_trial_exp1 = spark.sql("""select distinct trial_id, trial_mesh_term_name_exp1 from cite_trial_exp
lateral view outer explode (split(trial_mesh_term_name_exp,'\\\^')) as trial_mesh_term_name_exp1 

""").registerTempTable('cite_trial_exp1')

cite_trial_exp2 = spark.sql("""select distinct trial_id, trial_mesh_term_name_exp2 as mesh from cite_trial_exp1
    lateral view outer explode (split(trial_mesh_term_name_exp1,'\\\#')) as trial_mesh_term_name_exp2 
""").registerTempTable('cite_trial_exp2')

temp_citeline_trial_data = spark.sql("""
select
    'citeline' as data_src_nm,
    trim(ct.trial_id) as src_trial_id,
    trim(coalesce(ct.trial_title,ct.objective)) as trial_title,
    trim(ct.trial_phase) as trial_phase,
    trim(ct.drug_name) as trial_drug,
    coalesce(cm.mesh,ct.disease_name) as trial_indication,
    trim(nct_id) as nct_id,
    case when src_protocol_ids = 'NA' then null else trim(src_protocol_ids) end as protocol_ids
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove as ct
inner join cite_trial_exp2 cm on ct.trial_id=cm.trial_id
lateral view outer explode(split(coalesce(protocol_ids,'NA'),'\\\|'))one as src_protocol_ids
group by 1,2,3,4,5,6,7,8
""")
# temp_citeline_trial_data.registerTempTable('temp_citeline_trial_data')
temp_citeline_trial_data.write.mode('overwrite').saveAsTable('temp_citeline_trial_data')

# Get trial information for DQS

temp_dqs_trial_data = spark.sql("""
select
        data_src_nm,
        src_trial_id,
        trial_title,
        trial_phase,
        trial_drug,
        coalesce(trim(mesh_heading),trim(trial_indication)) as trial_indication,
        nct_id,
        protocol_id
from
        (select
                'ir' as data_src_nm,
                trim(member_study_id) as src_trial_id,
                trim(coalesce(study_title,description)) as trial_title,
                trim(phase) as trial_phase,
                '' as trial_drug,
                mesh_heading_explode2 as mesh_heading,
                trim(primary_indication) as trial_indication,
                trim(nct_number) as nct_id,
                case when nct_number is not null and trim (nct_number)!='' then trim(nct_number)
                else trim(member_study_id) end as protocol_ids
                from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir 
                lateral view outer explode(split(mesh_heading, ';'))one as mesh_heading_explode
                lateral view outer explode(split(mesh_heading_explode, '\\\|'))one as mesh_heading_explode2
        group by 1,2,3,4,5,6,7,8,9) tmp
lateral view outer explode(split(protocol_ids, '_'))one as protocol_id
group by 1,2,3,4,5,6,7,8
""")
# temp_dqs_trial_data.registerTempTable('temp_dqs_trial_data')
temp_dqs_trial_data.write.mode('overwrite').saveAsTable('temp_dqs_trial_data')

# Get trial information from CTMS
temp_ctms_trial_data_rnk = spark.sql("""
select
    trim(source) as data_src_nm,
    trim(src_trial_id) as src_trial_id,
    trim(trial_title) as trial_title,
    trim(trial_phase) as trial_phase,
    trim(drug) as trial_drug,
    trim(disease) as trial_indication,
    trim(nct_id) as nct_id,
    case when nct_id is not null and trim (nct_id)!='' then trim(nct_id)
    else trim(src_trial_id) end as protocol_ids
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study
group by 1,2,3,4,5,6,7,8
""")
# temp_ctms_trial_data_rnk.registerTempTable('temp_ctms_trial_data')
temp_ctms_trial_data_rnk.write.mode('overwrite').saveAsTable('temp_ctms_trial_data')

temp_tascan_trial_data_rnk = spark.sql("""
SELECT
    'tascan' AS data_src_nm,
    TRIM(trial_id) AS src_trial_id,
    TRIM(trial_title) AS trial_title,
    TRIM(trial_phase) AS trial_phase,
    TRIM(agg_drug_names.drug_names) AS trial_drug,  
    TRIM(agg_mesh_terms.trial_mesh_term_names) AS trial_indication,
    TRIM(nct_id) AS nct_id,
    CASE 
        WHEN nct_id IS NOT NULL AND TRIM(nct_id) != '' THEN TRIM(nct_id) 
        ELSE TRIM(trial_id) 
    END AS protocol_ids
FROM $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial
LATERAL VIEW OUTER EXPLODE(SPLIT(drug_names, ';')) agg_drug_names AS drug_names   
LATERAL VIEW OUTER EXPLODE(SPLIT(mesh_term_names, ';')) agg_mesh_terms AS trial_mesh_term_names
GROUP BY trial_id, trial_title, trial_phase, agg_drug_names.drug_names, agg_mesh_terms.trial_mesh_term_names, nct_id

""")
# temp_ctms_trial_data_rnk.registerTempTable('temp_ctms_trial_data')
temp_tascan_trial_data_rnk.write.mode('overwrite').saveAsTable('temp_tascan_trial_data')

# Union all data sources

temp_all_trial_data = spark.sql("""
select distinct *
from temp_aact_trial_data
union
select distinct *
from temp_citeline_trial_data
union
select distinct *
from temp_dqs_trial_data
Union
select distinct *
from temp_ctms_trial_data
Union
select distinct *
from temp_tascan_trial_data
""")
# temp_all_trial_data.registerTempTable("temp_all_trial_data")
temp_all_trial_data.write.mode('overwrite').saveAsTable('temp_all_trial_data')

# Save on HDFS
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data')
temp_all_trial_data.repartition(100).write.mode('overwrite').parquet(write_path)


spark.sql("""
   INSERT OVERWRITE TABLE
   $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_data PARTITION (
           pt_data_dt='$$data_dt',
           pt_cycle_id='$$cycle_id'
           )
   SELECT *
   FROM temp_all_trial_data
   """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_data')

temp_all_trial_data_cleaned_aact_cite_temp_0 = spark.sql(""" 
select
data_src_nm,
src_trial_id,
trim(trial_title) as trial_title,
trim(trial_phase) as trial_phase,
trim(trial_drug_exp)as trial_drug,
trim(trial_indication_exp) as trial_indication,
nct_id,
protocol_ids
from 
temp_all_trial_data
lateral view outer explode(split(trial_indication, '\\\|'))one as trial_indication_exp
lateral view outer explode(split(trial_drug, '\\\|'))one as trial_drug_exp
where lower(trim(data_src_nm)) in ('citeline','aact')
group by 1,2,3,4,5,6,7,8
""")
temp_all_trial_data_cleaned_aact_cite_temp_0.write.mode('overwrite').saveAsTable(
    "temp_all_trial_data_cleaned_aact_cite_temp_0")

temp_all_trial_data_cleaned_aact_cite_temp_1 = spark.sql(""" 
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
        temp_all_trial_data_cleaned_aact_cite_temp_0
        lateral view outer explode(split(trial_indication, '\\\^'))one as trial_indication_exp
        where lower(trim(data_src_nm)) in ('citeline','aact')
        group by 1,2,3,4,5,6,7,8
        """)
temp_all_trial_data_cleaned_aact_cite_temp_1.write.mode('overwrite').saveAsTable(
    "temp_all_trial_data_cleaned_aact_cite_temp_1")

temp_all_trial_data_cleaned_aact_cite_temp_2_citeline = spark.sql(""" 
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
temp_all_trial_data_cleaned_aact_cite_temp_1
lateral view outer explode(split(trial_indication, '\\\;'))one as trial_indication_exp
where lower(trim(data_src_nm)) in ('citeline')
group by 1,2,3,4,5,6,7,8
""")
temp_all_trial_data_cleaned_aact_cite_temp_2_citeline.write.mode('overwrite').saveAsTable(
    "temp_all_trial_data_cleaned_aact_cite_temp_2_citeline")

temp_all_trial_data_cleaned_aact_cite_temp_2_aact = spark.sql(""" 
select
data_src_nm,
src_trial_id,
trial_title,
trial_phase,
trial_drug_exp as trial_drug,
trial_indication,
nct_id,
protocol_ids
from 
temp_all_trial_data_cleaned_aact_cite_temp_1
lateral view outer explode(split(trial_drug, '\\\;'))one as trial_drug_exp
where lower(trim(data_src_nm)) in ('aact')
group by 1,2,3,4,5,6,7,8
""")
temp_all_trial_data_cleaned_aact_cite_temp_2_aact.write.mode('overwrite').saveAsTable(
    "temp_all_trial_data_cleaned_aact_cite_temp_2_aact")

temp_all_trial_data_cleaned_aact_cite_temp_2 = spark.sql("""
select * from temp_all_trial_data_cleaned_aact_cite_temp_2_citeline
UNION
select * from temp_all_trial_data_cleaned_aact_cite_temp_2_aact
""")

temp_all_trial_data_cleaned_aact_cite_temp_2.write.mode('overwrite').saveAsTable(
    "temp_all_trial_data_cleaned_aact_cite_temp_2")

# Perform clean up for all data sources
temp_all_trial_data_cleaned_aact_cite_1 = spark.sql("""
select /*+ broadcast(phase, disease) */
    data_src_nm,
    concat_ws('_',NULLIF(trim(data_src_nm),''),NULLIF(trim(src_trial_id),'')) as src_trial_id,
    case when trim(trial_title) in (' ','','null','NA', 'na','N/A','(N/A)') then null
    else trim(lower(regexp_replace(trial_title,'[^0-9A-Za-z ]',''))) end as trial_title,
    case when phase.standard_value is null or trim(phase.standard_value) = '' then 'other'
    else lower(trim(phase.standard_value)) end as trial_phase,
    case when trim(trial_drug) in (' ','','null','NA', 'na') then null
    else lower(trim(coalesce(drug.drugprimaryname,trial_drug))) end as trial_drug,
    case when trim(trial_indication) in (' ','','null','NA', 'na') then 'Other'
    else lower(trim(coalesce(disease.standard_mesh,'Unassigned'))) end as trial_indication,
    protocol_ids
from
temp_all_trial_data_cleaned_aact_cite_temp_2 base
left outer join trial_phase_mapping phase
on lower(trim(base.trial_phase)) = lower(trim(phase.trial_phase))


left outer join (select * from mesh_mapping_1 where lower(trim(standard_mesh))!='other' 
and lower(trim(standard_mesh))!='others' ) disease
on lower(trim(base.trial_indication)) = lower(trim(disease.mesh))
left outer join (select * from drug_mapping_file where lower(trim(drugprimaryname))!='other' and lower(trim(drugprimaryname))!='others' )drug
on lower(trim(base.trial_drug)) = lower(trim(drug.drugnamesynonyms))
where lower(trim(base.data_src_nm)) in ('citeline','aact')
group by 1,2,3,4,5,6,7
""")
# temp_all_trial_data_cleaned_aact_cite.registerTempTable('temp_all_trial_data_cleaned_aact_cite')
temp_all_trial_data_cleaned_aact_cite_1.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_aact_cite_1')

temp_all_trial_data_cleaned_aact_cite = spark.sql("""
select
   data_src_nm,
    src_trial_id,
   trial_title ,
    trial_phase,
        concat_ws('\;',sort_array(collect_set(NULLIF(trim(trial_drug),'')),true)) as trial_drug,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(trial_indication),'')),true)) as trial_indication,
protocol_ids
from temp_all_trial_data_cleaned_aact_cite_1
group by 1,2,3,4,7
""")
# Save on HDFS
temp_all_trial_data_cleaned_aact_cite.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_aact_cite')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_cleaned_aact_cite')
temp_all_trial_data_cleaned_aact_cite.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_trial_data_cleaned_dqs_ctms = spark.sql(""" 
select
data_src_nm,
src_trial_id,
trim(trial_title) as trial_title,
trim(trial_phase) as trial_phase,
trim(trial_drug_exp) as trial_drug,
trim(trial_indication_exp) as trial_indication,
nct_id,
protocol_ids
from 
temp_all_trial_data
lateral view outer explode(split(trial_indication, '\\\;'))one as trial_indication_exp
lateral view outer explode(split(trial_drug, '\\\;'))one as trial_drug_exp
where lower(trim(data_src_nm)) in ('ctms','ir')
group by 1,2,3,4,5,6,7,8
""")
# temp_all_trial_data_cleaned_dqs_ctms.registerTempTable('temp_all_trial_data_cleaned_dqs_ctms')
temp_all_trial_data_cleaned_dqs_ctms.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_dqs_ctms')

temp_all_trial_data_cleaned_dqs_ctms_1 = spark.sql("""
select /*+ broadcast(phase, disease) */
    data_src_nm,
    concat_ws('_',NULLIF(trim(data_src_nm),''),NULLIF(trim(src_trial_id),'')) as src_trial_id,
    case when trim(trial_title) in (' ','','null','NA', 'na','(N/A)','N/A') then null
    else trim(lower(regexp_replace(trial_title,'[^0-9A-Za-z ]',''))) end as trial_title,
    case when phase.standard_value is null or trim(phase.standard_value) = '' then 'other'
    else lower(trim(phase.standard_value)) end as trial_phase,
    case when trim(trial_drug) in (' ','','null','NA', 'na') then null
    else lower(trim(coalesce(drug.drugprimaryname,trial_drug))) end as trial_drug,
    case when trim(trial_indication) in (' ','','null','NA', 'na') then 'Other'
    else lower(trim(coalesce(disease.standard_mesh,'Unassigned'))) end as trial_indication,
    protocol_ids
from
temp_all_trial_data_cleaned_dqs_ctms base
left outer join trial_phase_mapping phase
on lower(trim(base.trial_phase)) = lower(trim(phase.trial_phase))
left outer join (select * from mesh_mapping_1 where lower(trim(standard_mesh))!='other' 
and lower(trim(standard_mesh))!='others' ) disease
on lower(trim(base.trial_indication)) = lower(trim(disease.mesh))
left outer join (select * from drug_mapping_file where lower(trim(drugprimaryname))!='other' and lower(trim(drugprimaryname))!='others' ) drug
on lower(trim(base.trial_drug)) = lower(trim(drug.drugnamesynonyms))
group by 1,2,3,4,5,6,7
""")
# temp_all_trial_data_cleaned_dqs_ctms_1.registerTempTable('temp_all_trial_data_cleaned_dqs_ctms_1')
temp_all_trial_data_cleaned_dqs_ctms_1.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_dqs_ctms_1')

temp_all_trial_data_cleaned_dqs_ctms_2 = spark.sql("""
select
   data_src_nm,
    src_trial_id,
   trial_title ,
    trial_phase,
            concat_ws('\;',sort_array(collect_set(NULLIF(trim(trial_drug),'')),true)) as trial_drug,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(trial_indication),'')),true)) as trial_indication,
protocol_ids
from temp_all_trial_data_cleaned_dqs_ctms_1
group by 1,2,3,4,7
""")
# temp_all_trial_data_cleaned_dqs_ctms_2.registerTempTable('temp_all_trial_data_cleaned_dqs_ctms_2')
temp_all_trial_data_cleaned_dqs_ctms_2.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_dqs_ctms_2')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_cleaned_dqs_ctms_2')
temp_all_trial_data_cleaned_dqs_ctms_2.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_trial_data_cleaned_tascan = spark.sql("""
select
   data_src_nm,
    concat_ws('_',NULLIF(trim(data_src_nm),''),NULLIF(trim(src_trial_id),'')) as src_trial_id,
   trial_title ,
    trial_phase,
            concat_ws('\;',sort_array(collect_set(NULLIF(trim(trial_drug),'')),true)) as trial_drug,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(trial_indication),'')),true)) as trial_indication,
protocol_ids
from temp_tascan_trial_data
group by 1,2,3,4,7
""")
# temp_all_trial_data_cleaned_dqs_ctms_2.registerTempTable('temp_all_trial_data_cleaned_dqs_ctms_2')
temp_all_trial_data_cleaned_tascan.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned_tascan')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_cleaned_tascan')
temp_all_trial_data_cleaned_tascan.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_trial_data_cleaned = spark.sql("""
select distinct * from temp_all_trial_data_cleaned_dqs_ctms_2 
union 
select distinct * from temp_all_trial_data_cleaned_aact_cite
union
select distinct * from temp_all_trial_data_cleaned_tascan
""")

# temp_all_trial_data_cleaned.registerTempTable('temp_all_trial_data_cleaned')
temp_all_trial_data_cleaned.write.mode('overwrite').saveAsTable('temp_all_trial_data_cleaned')
# Push to S3


# Replace obselete NCT ID with latest ID in protocol ID column
# Sk-HH Added the where condition
temp_all_trial_data_cleaned_obs_remove = spark.sql("""
select /*+ broadcast(info) */ distinct
    data_src_nm,
    src_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
   trial_indication,
    coalesce(info.nct_id,base.protocol_ids) as protocol_ids
from temp_all_trial_data_cleaned base
left outer join id_info info
on regexp_replace(trim(lower(base.protocol_ids)),'[^0-9A-Za-z]','') = regexp_replace(trim(lower(info.id_value)),'[^0-9A-Za-z]','')
where 
(lower(trim(data_src_nm)) in ('citeline','aact','tascan') and regexp_replace(trim(lower(trial_title)),'[^0-9A-Za-z]','') is not null and 
regexp_replace(trim(lower(trial_title)),'[^0-9A-Za-z]','') != '') or lower(trim(data_src_nm)) in ('ctms','ir')
group by 1,2,3,4,5,6,7
""")
# temp_all_trial_data_cleaned_obs_remove.registerTempTable('temp_all_trial_data_cleaned_obs_remove')
# Save on HDFS
temp_all_trial_data_cleaned_obs_remove.write.mode('overwrite').saveAsTable(
    'temp_all_trial_data_cleaned_obs_remove')
#write_path = path.replace('table_name', 'temp_all_trial_data_cleaned_obs_remove')
temp_all_trial_data_cleaned_obs_remove.repartition(100).write.mode('overwrite').parquet(write_path)


spark.sql("""
   INSERT OVERWRITE TABLE
   $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_data_cleaned_obs_remove PARTITION (
           pt_data_dt='$$data_dt',
           pt_cycle_id='$$cycle_id'
           )
   SELECT *
   FROM temp_all_trial_data_cleaned_obs_remove
   """)
CommonUtils().copy_hdfs_to_s3(
    '$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_data_cleaned_obs_remove')

# give rank to citeline records to identify multiple records having same protocol_id
# In case multiple records have same protocol id, then we will only take one record to merge acress data sources
Citeline_multiple_protocol = spark.sql("""select *, row_number() over (partition by trim(protocol_ids) order by trial_title nulls last , trial_indication nulls last , trial_drug nulls last , trial_phase nulls last ,src_trial_id desc) as rnk 
from (select * from temp_all_trial_data_cleaned_obs_remove where data_src_nm = 'citeline')""")
Citeline_multiple_protocol.registerTempTable('Citeline_multiple_protocol')

temp_trial_data_common_id_temp = spark.sql("""
select *, null as rnk from temp_all_trial_data_cleaned_obs_remove where lower(trim(data_src_nm)) not in ('citeline')
union 
select * from Citeline_multiple_protocol where rnk=1
""")
temp_trial_data_common_id_temp.registerTempTable('temp_trial_data_common_id_temp')

temp_trial_data_common_id = spark.sql("""
select
    protocol_ids,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(src_trial_id),'')),true)) as ds_trial_id
from temp_trial_data_common_id_temp
where trim(lower(protocol_ids)) like 'nct%' or trim(lower(protocol_ids)) like 'eudract%'
or trim(lower(protocol_ids)) like 'euctr%' or trim(lower(protocol_ids)) like 'jprn%'
or trim(lower(protocol_ids)) like 'chictr%'  or trim(lower(protocol_ids)) like 'irct%'
or trim(lower(protocol_ids)) like 'ctri%' or trim(lower(protocol_ids)) like 'actrn%'
or trim(lower(protocol_ids)) like 'isrctn%' or trim(lower(protocol_ids)) like 'drks%'
or trim(lower(protocol_ids)) like 'ntr%' or trim(lower(protocol_ids)) like 'kct%'
or trim(lower(protocol_ids)) like 'tctr%' or trim(lower(protocol_ids)) like 'rbr-%'
or trim(lower(protocol_ids)) like 'pactr%' or trim(lower(protocol_ids)) like 'supergen%'
or trim(lower(protocol_ids)) like 'slctr%' or trim(lower(protocol_ids)) like 'rpcec%'
or trim(lower(protocol_ids)) like 'lbctr%' or trim(lower(protocol_ids)) in (select distinct trim(lower(protocol_ids)) from temp_trial_data_common_id_temp where trim(lower(data_src_nm)) in ('ctms','ir'))
group by 1
having size(collect_set(src_trial_id)) > 1
""")
temp_trial_data_common_id.registerTempTable('temp_trial_data_common_id')

# temp_trial_data_common_id.write.mode('overwrite').saveAsTable("temp_trial_data_common_id")
write_path = path.replace('table_name', 'temp_trial_data_common_id')
temp_trial_data_common_id.repartition(100).write.mode('overwrite').parquet(write_path)

# Assigning temporary cluster IDs to protocol_ids for merging records
temp_trial_comnid_clusterid = spark.sql("""
select
    concat_ws('','common_',row_number() over(order by null)) as comn_cluster_id,
    temp.protocol_ids
from
(select distinct protocol_ids from temp_trial_data_common_id where protocol_ids is not null and protocol_ids <> '') temp
""")

# temp_trial_comnid_clusterid.registerTempTable('temp_trial_comnid_clusterid')
temp_trial_comnid_clusterid.write.mode('overwrite').saveAsTable('temp_trial_comnid_clusterid')

# Assigning the temporary cluster IDs to the base data
temp_trial_data_comnid_map = spark.sql("""
select /*+ broadcast(clstr_data) */
distinct
    base.*,
    clstr_data.comn_cluster_id, prec.precedence as precedence 
from
temp_trial_data_common_id_temp base
inner join default.temp_trial_comnid_clusterid clstr_data
on lower(trim(base.protocol_ids)) = lower(trim(clstr_data.protocol_ids))
inner join data_precedence prec on lower(trim(base.data_src_nm)) = lower(trim(prec.data_src_nm))
""")
# temp_trial_data_comnid_map.registerTempTable('temp_trial_data_comnid_map')
temp_trial_data_comnid_map.write.mode('overwrite').saveAsTable('temp_trial_data_comnid_map')

temp_trial_comnid_clustered = spark.sql("""
select
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_src_nm),'')),true)) as data_src_nm,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(src_trial_id),'')),true)) as src_trial_id,
    comn_cluster_id
from temp_trial_data_comnid_map
group by comn_cluster_id
""")
temp_trial_comnid_clustered.registerTempTable('temp_trial_comnid_clustered')
# temp_trial_comnid_clustered.write.mode('overwrite').saveAsTable('temp_trial_comnid_clustered')


temp_trial_comnid_precedence_1 = spark.sql("""

select
comn_cluster_id,
trial_title,
trial_phase,
trial_drug,
trial_indication,
precedence,
src_trial_id,
row_number () over (partition by comn_cluster_id order by precedence asc, src_trial_id desc) rnk
from
temp_trial_data_comnid_map
group by 1,2,3,4,5,6,7
""")
# temp_trial_comnid_precedence_1.registerTempTable('temp_trial_comnid_precedence_1')
temp_trial_comnid_precedence_1.write.mode('overwrite').saveAsTable('temp_trial_comnid_precedence_1')

# give the highest precedence attrubuted to all the clsutered records
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
# temp_trial_comnid_precedence.registerTempTable('temp_trial_comnid_precedence')
temp_trial_comnid_precedence.write.mode('overwrite').saveAsTable('temp_trial_comnid_precedence')

# to bring the precedene data on the level of concatenated src id and data src
temp_trial_common_id_final_temp = spark.sql("""
select
    base.comn_cluster_id,
    base.data_src_nm,
    base.src_trial_id,
    regexp_replace(prec_data.trial_title,'  ',' ') as trial_title,
    regexp_replace(prec_data.trial_phase,'  ',' ') as trial_phase,
    regexp_replace(prec_data.trial_drug,'  ',' ') as trial_drug,
    regexp_replace(prec_data.trial_indication,'  ',' ') as trial_indication
from
temp_trial_comnid_clustered base
left outer join
temp_trial_comnid_precedence prec_data
on lower(trim(base.comn_cluster_id)) = lower(trim(prec_data.comn_cluster_id))
group by 1,2,3,4,5,6,7
""")
# temp_trial_common_id_final_temp.registerTempTable('temp_trial_common_id_final_temp')
temp_trial_common_id_final = temp_trial_common_id_final_temp.drop('comn_cluster_id')
temp_trial_common_id_final.write.mode('overwrite').saveAsTable('temp_trial_common_id_final')

# Preparing the remaining data for Dedupe
temp_trial_data_prep = spark.sql("""
select /*+ broadcast(temp) */
    base.data_src_nm,
    base.src_trial_id,
    regexp_replace(base.trial_title,'  ',' ') as trial_title,
    regexp_replace(base.trial_phase,'  ',' ') as trial_phase,
    regexp_replace(base.trial_drug,'  ',' ') as trial_drug,
    regexp_replace(base.trial_indication,'  ',' ') as trial_indication
from default.temp_all_trial_data_cleaned_obs_remove base
left outer join
    (select distinct src_trial_ids from default.temp_trial_common_id_final
    lateral view outer explode(split(src_trial_id,'\\\;'))one as src_trial_ids group by 1) temp
on lower(trim(base.src_trial_id)) = lower(trim(temp.src_trial_ids))
where temp.src_trial_ids is null 
""")
temp_trial_data_prep = temp_trial_data_prep.dropDuplicates()
# temp_trial_data_prep.registerTempTable('temp_trial_data_prep')
temp_trial_data_prep.write.mode('overwrite').saveAsTable('temp_trial_data_prep')

temp_trial_master_data_prep_intd = spark.sql("""
select distinct *
from temp_trial_common_id_final
union
select distinct *
from temp_trial_data_prep
""")
temp_trial_master_data_prep_intd = temp_trial_master_data_prep_intd.dropDuplicates()
# temp_trial_master_data_prep_intd.registerTempTable('temp_trial_master_data_prep_intd')
temp_trial_master_data_prep_intd.write.mode('overwrite').saveAsTable('temp_trial_master_data_prep_intd')

temp_trial_master_data_prep_intd_1 = temp_trial_master_data_prep_intd.sort("src_trial_id", "trial_phase", "trial_title",
                                                                           "trial_drug", "trial_indication")
temp_trial_master_data_prep_intd_1.registerTempTable('temp_trial_master_data_prep_intd_1')


# added this function
def sort_collected_data_colon(text=''):
    try:
        text = text.replace('#', ';')
    except:
        text = ''
    try:
        collected_data = text.split(';')
    except:
        collected_data = ['']
    collected_data.sort()
    text = u';'.join(collected_data).encode('utf-8').strip()
    return text.decode('utf-8')


spark.udf.register('sort_collected_data_colon', sort_collected_data_colon)

# Final Data Prep Output for Dedupe
# updated this block
temp_all_trial_data_prep_intd1 = spark.sql("""
select
    cast(DENSE_RANK() over(order by coalesce(lower(trim(trial_phase)), 'NA'),coalesce(lower(trim(trial_title)), 'NA'),
    coalesce(lower(trim(trial_drug)), 'NA'),coalesce(lower(trim(trial_indication)), 'NA')) as bigint) as uid,
    sha2(concat_ws('',coalesce(lower(trim(trial_phase)), 'NA'),coalesce(lower(trim(trial_title)), 'NA'),coalesce(lower(trim(trial_drug)), 'NA'),coalesce(lower(trim(trial_indication)), 'NA')),256) as hash_uid,
    data_src_nm,
    ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from
(select
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(exp_data_source),'')),true)) as data_src_nm,
    concat_ws('\;',sort_array(collect_set(NULLIF(trim(exp_ds_trial_id),'')),true)) as ds_trial_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication
from
(select data_src_nm,
        regexp_replace(src_trial_id,'#',';') as ds_trial_id,
        trial_title,
        trial_phase,
        trial_drug,
        trial_indication
from temp_trial_master_data_prep_intd_1)
lateral view outer explode(split(coalesce(data_src_nm,'NA'),'\\\;'))one as exp_data_source
lateral view outer explode(split(coalesce(ds_trial_id,'NA'),'\\\;'))one as exp_ds_trial_id
group by trial_title,trial_phase,trial_drug,trial_indication)
""")
# temp_all_trial_data_prep_intd1.registerTempTable("temp_all_trial_data_prep_intd1")

# Save on HDFS
temp_all_trial_data_prep_intd1.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_intd1')

# logic to give datasource rank

temp_all_trial_data_prep_intd2_2 = spark.sql("""
select temp_all_trial_data_prep_intd1.*,
case when lower(trim(data_src_nm)) like '%ctms%' then 1
when lower(trim(data_src_nm)) like '%ir%' then 2
when lower(trim(data_src_nm)) like '%citeline%' then 3
when lower(trim(data_src_nm)) like '%tascan%' then 4
when lower(trim(data_src_nm)) like '%aact%' then 5
else 6 end as datasource_rnk from  default.temp_all_trial_data_prep_intd1""")
# temp_all_trial_data_prep_intd2_2.registerTempTable("temp_all_trial_data_prep_intd2_2")
temp_all_trial_data_prep_intd2_2.write.mode('overwrite'). \
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
lateral view outer explode(split(ds_trial_id,'\\\;'))one as ds_trial_ids
group by 1
having count(distinct uid)>1) base0
inner join
(select
        ds_trial_ids,
        uid
from
        temp_all_trial_data_prep_intd2_2
lateral view outer explode(split(ds_trial_id,'\\\;'))one as ds_trial_ids) prep
on lower(trim(base0.ds_trial_ids)) = lower(trim(prep.ds_trial_ids))
group by 1,2) base1
inner join
(select *
from temp_all_trial_data_prep_intd2_2 prep) prep
on trim(lower(base1.uid)) = trim(lower(prep.uid))
""")
# multi_cluster_df.registerTempTable("multi_cluster_df")
multi_cluster_df.write.mode('overwrite').saveAsTable('multi_cluster_df')

clustered_src_ids = spark.sql("""
select
cluster_id,
concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_trial_id),'')),true)) as ds_trial_id,
concat_ws('\;',sort_array(collect_set(NULLIF(trim(data_src_nm),'')),true)) as data_src_nm
from
multi_cluster_df
group by 1
""")

clustered_src_ids.registerTempTable("clustered_src_ids")
# clustered_src_ids.write.mode('overwrite').saveAsTable('clustered_src_ids')

# bring all the trial id for the common cluster id
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
on lower(trim(base.cluster_id)) = lower(trim(clstr.cluster_id))
where base.rnk = 1""")
# temp_all_trial_data_prep_1.registerTempTable("temp_all_trial_data_prep_1")
temp_all_trial_data_prep_1.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_1')

# to resolve duplicate on UID
temp_all_trial_data_prep_2 = spark.sql("""
select uid  
from temp_all_trial_data_prep_1 
group by 1 
having count(distinct ds_trial_id) >1
""")
# temp_all_trial_data_prep_2.registerTempTable("temp_all_trial_data_prep_2")
temp_all_trial_data_prep_2.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_2')

# apply rank
temp_all_trial_data_prep_3 = spark.sql(""" 
select uid, hash_uid,data_src_nm, trial_title, 
trial_phase, trial_drug,
 trial_indication, 
 row_number() over (partition by hash_uid order by uid desc) as rnk 
from temp_all_trial_data_prep_1 where uid in (select distinct uid from temp_all_trial_data_prep_2) group by 1,2,3,4,5,6,7
""")
# temp_all_trial_data_prep_3.registerTempTable("temp_all_trial_data_prep_3")
temp_all_trial_data_prep_3.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_3')

# collect all the trail id for corresponding UID
temp_all_trial_data_prep_4 = spark.sql(""" 
select uid,
concat_ws('\;',sort_array(collect_set(NULLIF(trim(ds_trial_id_1),'')),true)) as ds_trial_id
from (select distinct uid,
ds_trial_id_1
from temp_all_trial_data_prep_1 
lateral view outer explode(split(ds_trial_id,'\\\;'))one as ds_trial_id_1
where uid in (select distinct uid from temp_all_trial_data_prep_2))
group by 1
""")
# temp_all_trial_data_prep_4.registerTempTable("temp_all_trial_data_prep_4")
temp_all_trial_data_prep_4.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_4')

# union the dataset of unique uid (having multiple clsuter id) and and uid where duplicates records are resolved

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
on lower(trim(a.uid)) = lower(trim(b.uid))
where a.rnk = 1
""")
# temp_all_trial_data_prep_5.registerTempTable("temp_all_trial_data_prep_5")
temp_all_trial_data_prep_5.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_5')

# union with complete data of unique clauer ud and resolved data
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
multi_cluster_df group by 1) clstr
on lower(trim(base.uid)) = lower(trim(clstr.muid))
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
# temp_all_trial_data_prep_6.registerTempTable("temp_all_trial_data_prep_6")
temp_all_trial_data_prep_6.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep_6')

temp_all_trial_data_prep = spark.sql("""
select distinct
uid,
hash_uid,
data_src_nm,
ds_trial_id,
regexp_replace(trial_title,'  ',' ') as trial_title,
regexp_replace(trial_phase,'  ',' ') as trial_phase,
regexp_replace(trial_drug,'  ',' ') as trial_drug,
regexp_replace(trial_indication,'  ',' ') as trial_indication
from temp_all_trial_data_prep_6
group by 1,2,3,4,5,6,7,8
""")

temp_all_trial_data_prep.write.mode('overwrite').saveAsTable('temp_all_trial_data_prep')
# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_prep')

# temp_all_trial_data_prep.repartition(100).write.mode('overwrite').parquet(write_path)
spark.sql("""
   INSERT OVERWRITE TABLE
   $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_data_prep PARTITION (
           pt_data_dt='$$data_dt',
           pt_cycle_id='$$cycle_id'
           )
   SELECT *
   FROM temp_all_trial_data_prep
   """)
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_data_prep')


