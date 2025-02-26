################################# Module Information ################################################################
#     Module Name         : D_DRUG
#     Purpose             : This will create target table d_drug
#     Pre-requisites      : L1 source table required: CITELINE_PHARMAPROJECTS,CITELINE_TRIALTROVE,AACT_INTERVENTIONS
#     Last changed on     : 12-1-2023
#     Last changed by     : Dhruvika|Ujjwal
#     Reason for change   : NA
#     Return Values       : NA
#####################################################################################################################

################################### High level Process ################################################################
# 1. Fetch all relevant information from L1 layer table
# 2. Pass through the L1 table on key columns to create final target table
#######################################################################################################################
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import os
import sys
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import initcap
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import traceback

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')

path = "{bucket_path}/applications/commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id".format(
    bucket_path=bucket_path)

##Drugs from citeline_pharmaprojects
final_mapping = spark.read.format("com.crealytics.spark.excel").option("header", "true")\
    .load("{bucket_path}/uploads/DRUG/Drug_Mapping_File/Drug_Mapping.xlsx".format(bucket_path=bucket_path))
final_mapping = final_mapping.dropDuplicates()
final_mapping.createOrReplaceTempView('final_mapping')

##To update a unique primary name for synonyms (if any changes done in master file before post dedupe run)
master_drug_file = spark.read.format("com.crealytics.spark.excel").option("header", "true")\
    .load("{bucket_path}/uploads/DRUG/Master_File/Drug_Master_File.xlsx".format(bucket_path=bucket_path))
master_drug_file = master_drug_file.dropDuplicates()
master_drug_file.createOrReplaceTempView('master_drug_file')

final_mapping_all=spark.sql("""
select distinct coalesce(b.drugprimaryname, a.drugprimaryname) as drugprimaryname,  a.drugnamesynonyms
from final_mapping a left join master_drug_file b 
on lower(trim(a.drugnamesynonyms)) = lower(trim(b.drugnamesynonyms))
""")
final_mapping_all = final_mapping_all.dropDuplicates()
#final_mapping_all.registerTempTable('final_mapping_all')
final_mapping_all.write.mode('overwrite').saveAsTable('final_mapping_all')

temp_d_drug_citeline_pharma = spark.sql("""
select distinct
    A.drugprimaryname as drug_nm,
    trim(exp_src_trial_id) as src_trial_id,
    'citeline' as data_src_nm
    from 
    (select distinct coalesce(b.drugprimaryname, a.drugprimaryname) as drugprimaryname, trialids from
    (select distinct lower(trim(drugprimaryname)) as drugprimaryname, lower(trim(synonyms)) as drugnamesynonyms,trialids
    from $$client_name_ctfo_datastore_staging_$$db_env.citeline_pharmaprojects
    lateral view outer explode(split(drugnamesynonyms, '\\\|')) as synonyms
    where lower(ispharmaprojectsdrug) ='true' and trialids !='' and drugprimaryname != '') a
    LEFT OUTER JOIN final_mapping_all b on lower(trim(a.drugnamesynonyms)) = lower(trim(b.drugnamesynonyms)))A 
lateral view outer explode(split(trialids, '\\\|')) as exp_src_trial_id
group by 1,2,3
""")
temp_d_drug_citeline_pharma = temp_d_drug_citeline_pharma.dropDuplicates()
temp_d_drug_citeline_pharma.registerTempTable('temp_d_drug_citeline_pharma')
#temp_d_drug_citeline_pharma.write.mode('overwrite').saveAsTable('temp_d_drug_citeline_pharma')

temp_d_drug_citeline = spark.sql("""	
select distinct
    initcap(trim(drug_nm)) as drug_nm,
    concat_ws("\|",collect_set(distinct concat('citeline_', src_trial_id))) as src_trial_id,	
    data_src_nm,
    concat_ws("\|",sort_array(collect_set(distinct initcap(trim(mapping.drugnamesynonyms))),true)) as drug_name_synonyms
from temp_d_drug_citeline_pharma citeline
left join final_mapping_all mapping on lower(trim(citeline.drug_nm)) = lower(trim(mapping.drugprimaryname))
group by 1,3
""")
temp_d_drug_citeline = temp_d_drug_citeline.dropDuplicates()
#temp_d_drug_citeline.registerTempTable('temp_d_drug_citeline')
temp_d_drug_citeline.write.mode('overwrite').saveAsTable('temp_d_drug_citeline')

temp_d_drug_aact = spark.sql("""	
select distinct
    initcap(trim(coalesce(mapping.drugprimaryname,'Other'))) as drug_nm,
    concat_ws("\|",collect_set(distinct concat('aact_', trim(inv.nct_id)))) as src_trial_id,
    'aact' as data_src_nm,       
    concat_ws("\|",sort_array(collect_set(distinct coalesce(initcap(trim(mapping.drugnamesynonyms)),'Other')),true)) as drug_name_synonyms
from (select distinct nct_id, regexp_replace(drug_name,'\\\"','\\\'') as name 
from $$client_name_ctfo_datastore_staging_$$db_env.aact_interventions
lateral view outer explode(split(name, '\\\;')) as drug_name
where lower(intervention_type) = 'drug' and trim(drug_name) <> '' and name is not null and nct_id is not null) inv
left join final_mapping_all mapping on lower(trim(inv.name)) = lower(trim(mapping.drugnamesynonyms))
group by 1,3
""")
temp_d_drug_aact = temp_d_drug_aact.dropDuplicates()
#temp_d_drug_aact.registerTempTable('temp_d_drug_aact')
temp_d_drug_aact.write.mode('overwrite').saveAsTable('temp_d_drug_aact')

temp_d_drug_ctms = spark.sql("""	
select distinct
    initcap(trim(coalesce(mapping.drugprimaryname,'Other'))) as drug_nm,
    concat_ws("\|",collect_set(distinct concat('ctms_', trim(ctms.src_trial_id)))) as src_trial_id,
    'ctms' as data_src_nm,       
    concat_ws("\|",sort_array(collect_set(distinct coalesce(initcap(trim(mapping.drugnamesynonyms)),'Other')),true)) as drug_name_synonyms
from (select distinct src_trial_id, drug from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study where drug is not null or drug != '') ctms
left join final_mapping_all mapping
on lower(trim(ctms.drug)) = lower(trim(mapping.drugnamesynonyms))
group by 1,3
""")
temp_d_drug_ctms = temp_d_drug_ctms.dropDuplicates()
#temp_d_drug_ctms.registerTempTable('temp_d_drug_ctms')
temp_d_drug_ctms.write.mode('overwrite').saveAsTable('temp_d_drug_ctms')

temp_d_drug_tascan = spark.sql("""	
SELECT DISTINCT
    initcap(trim(drug_name)) AS drug_nm,
    concat_ws("\|", collect_set(distinct concat('tascan_', trial_id))) AS src_trial_id,    
    'tascan' AS data_src_nm,
    '' AS drug_name_synonyms
FROM
    $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial tatrial
LEFT JOIN 
    final_mapping_all mapping 
    ON lower(trim(tatrial.drug_names)) = lower(trim(mapping.drugprimaryname))
LATERAL VIEW 
    explode(split(tatrial.drug_names, ';')) AS drug_name
GROUP BY 
    1, 3
""")
temp_d_drug_tascan = temp_d_drug_tascan.dropDuplicates()
temp_d_drug_tascan.write.mode('overwrite').saveAsTable('temp_d_drug_tascan')

union_d_drug = spark.sql("""
select * from temp_d_drug_citeline
union
select * from temp_d_drug_aact
union
select * from temp_d_drug_ctms
union
select * from temp_d_drug_tascan
""")
union_d_drug = union_d_drug.dropDuplicates()
#union_d_drug.registerTempTable('union_d_drug')
union_d_drug.write.mode('overwrite').saveAsTable('union_d_drug')

union_d_drug_final = spark.sql(""" select replace(drug_nm,'?', '') as drug_nm,
src_trial_id,
data_src_nm ,
replace(drug_name_synonyms,'?', '') as drug_name_synonyms
from union_d_drug """)
union_d_drug_final = union_d_drug_final.dropDuplicates()
#union_d_drug_final.registerTempTable('union_d_drug_final')
union_d_drug_final.write.mode('overwrite').saveAsTable('union_d_drug_final')

temp_drug_id = spark.sql("""
select row_number() over(order by drug_nm) as drug_id, drug_nm
from (select distinct lower(trim(drug_nm)) as drug_nm from default.union_d_drug_final )
""")
temp_drug_id.write.mode('overwrite').saveAsTable('temp_drug_id')

temp_d_drug = spark.sql("""
select distinct
    coalesce(A.drug_id,'') as drug_id,
    initcap(A.drug_nm) as drug_nm,
    concat_ws("\;",sort_array(collect_set(distinct B.src_trial_id),true)) as src_trial_id,
    concat_ws("\;",sort_array(collect_set(distinct B.data_src_nm),true)) as data_src_nm,
    concat_ws("\||",sort_array(collect_set(distinct initcap(exploded_drug_name_synonyms)),true)) as drug_name_synonyms, 
    coalesce(max(C.globalstatus) ,'') as global_status,
    coalesce(max(C.originatorname) ,'') as originator,
    coalesce(max(C.originatorstatus) ,'') as originator_status,
    coalesce(max(C.originatorcountry) ,'') as originator_country,
    coalesce(max(C.originatorcompanykey) ,'') as originator_company_key,
    coalesce(max(C.originatorcompanykey) ,'') as originator_parent_nm,
    coalesce(max(C.originatorparentkey) ,'') as originator_parent_key,
    coalesce(max(C.casnumbers) ,'') as cas_num,
    coalesce(max(C.overview) ,'') as overview,
    coalesce(max(C.marketing) ,'') as marketing,
    coalesce(max(C.licensing) ,'') as licensing,
    coalesce(max(C.licensee_licenseename) ,'') as licensee_nm,
    coalesce(max(C.licensee_licenseestatus) ,'') as licensee_status,
    coalesce(max(C.licensee_licenseecompanykey) ,'') as licensee_company_id,
    coalesce(max(C.licensee_licenseecountry) ,'') as licensee_country,
    coalesce(max(C.indicationgroups_id) ,'') as indication_group_id,
    coalesce(max(C.indicationgroups_name) ,'') as indication_group_nm,
    coalesce(max(C.indications_diseasename) ,'') as disease_nm,
    coalesce(max(C.indications_diseasestatus) ,'') as disease_status,
    coalesce(max(C.phasei) ,'') as phase_1,
    coalesce(max(C.phaseii) ,'') as phase_2,
    coalesce(max(C.phaseiii) ,'') as phase_3,
    coalesce(max(C.preclinical) ,'') as pre_clinical,
    coalesce(max(C.drugmeshterms_meshid) ,'') as mesh_id,
    coalesce(max(C.drugmeshterms_name) ,'') as mesh_term,
    coalesce(max(C.drugicd9_icd9id) ,'') as icd9_id,
    coalesce(max(C.drugicd9_name) ,'') as icd9_nm,
    coalesce(max(C.keyevents_keyeventdate) ,'') as key_event_dt,
    coalesce(max(C.keyevents_keyeventhistory) ,'') as key_event_history,
    coalesce(max(C.keyevents_keyeventdetail) ,'') as key_event_detail,
    coalesce(max(C.therapeuticclasses_therapeuticclassname) ,'') as therapeutic_class_nm,
    coalesce(max(C.therapeuticclasses_therapeuticclassstatus) ,'') as therapeutic_class_status,
    coalesce(max(C.mechanismsofaction) ,'') as mechanism_of_action,
    coalesce(max(C.deliveryroutes) ,'') as delivery_route,
    coalesce(max(C.deliverymediums) ,'') as delivery_medium,
    coalesce(max(C.deliverytechnologies) ,'') as delivery_technology,
    coalesce(max(C.origin) ,'') as origin,
    coalesce(max(C.molecularformula) ,'') as molecular_formula,
    coalesce(max(C.molecularweight) ,'') as molecular_weight,
    coalesce(max(C.nce) ,'') as nce,
    coalesce(max(C.chemicalname) ,'') as chemical_nm,
    coalesce(max(C.chemicalstructure) ,'') as chemical_structure,
    coalesce(max(C.pharmacokinetics_model) ,'') as model,
    coalesce(max(C.pharmacokinetics_unit) ,'') as unit,
    coalesce(max(C.pharmacokinetics_parameter) ,'') as parameter,
    coalesce(max(C.drugcountries_name) ,'') as country_nm,
    coalesce(max(C.drugcountries_status) ,'') as country_status,
    coalesce(max(C.drugcountries_yearlaunched) ,'') as country_year_launched,
    coalesce(max(C.drugcountries_licensingopportunity) ,'') as country_licensing_opportunity,
    coalesce(max(C.patents_patentnumber) ,'') as patent_num,
    coalesce(max(C.patents_patentprioritycountrydate) ,'') as patent_priority_country_dt,
    coalesce(max(C.targets_targetname) ,'') as target_nm,
    coalesce(max(C.targets_targetsynonyms) ,'') as target_synonym,
    coalesce(max(C.targets_targetfamilies) ,'') as target_family,
    coalesce(max(C.targets_targetecnumbers) ,'') as target_ec_num,
    coalesce(max(C.targets_entrezgeneid ) ,'') as entrezgene_id,
    coalesce(max(C.ispharmaprojectsdrug) ,'') as is_pharma_project,
    coalesce(max(C.latestchangedate) ,'') as latest_change_dt,
    coalesce(max(C.latestchange) ,'') as latest_change
from temp_drug_id A
left join default.union_d_drug_final B on lower(trim(A.drug_nm)) = lower(trim(B.drug_nm))
left join (select * from $$client_name_ctfo_datastore_staging_$$db_env.citeline_pharmaprojects where lower(ispharmaprojectsdrug) ='true' and trialids !='' ) C on lower(trim(A.drug_nm)) = lower(trim(C.drugprimaryname))
lateral view outer explode(split(B.drug_name_synonyms, '\\\|')) as exploded_drug_name_synonyms
group by 1,2
""")
temp_d_drug = temp_d_drug.dropDuplicates()
temp_d_drug.write.mode('overwrite').saveAsTable('temp_d_drug')
write_path = path.replace('table_name', 'temp_d_drug')
temp_d_drug.repartition(100).write.mode('overwrite').parquet(write_path)

d_drug = spark.sql("""
select distinct
drug_id,
drug_nm,
drug_name_synonyms,
global_status,
originator,
originator_status,
originator_country,
originator_company_key,
originator_parent_nm,
originator_parent_key,
cas_num,
overview,
marketing,
licensing,
licensee_nm,
licensee_status,
licensee_company_id,
licensee_country,
indication_group_id,
indication_group_nm,
disease_nm,
trim(disease_status) as disease_status,
phase_1,
phase_2,
phase_3,
pre_clinical,
mesh_id,
mesh_term,
icd9_id,
icd9_nm,
key_event_dt,
latest_change,
key_event_history,
key_event_detail,
therapeutic_class_nm,
therapeutic_class_status,
mechanism_of_action,
delivery_route,
delivery_medium,
delivery_technology,
origin,
molecular_formula,
molecular_weight,
nce,
chemical_nm,
chemical_structure,
model,
unit,
parameter,
country_nm,
trim(country_status) as country_status,
country_year_launched,
country_licensing_opportunity,
patent_num,
patent_priority_country_dt,
target_nm,
target_synonym,
target_family,
target_ec_num,
entrezgene_id,
is_pharma_project,
latest_change_dt
   from temp_d_drug
""")
d_drug = d_drug.dropDuplicates()
d_drug.write.mode('overwrite').saveAsTable('d_drug')

# Insert data into hdfs
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.d_drug partition(
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id')
select * from d_drug
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.d_drug')