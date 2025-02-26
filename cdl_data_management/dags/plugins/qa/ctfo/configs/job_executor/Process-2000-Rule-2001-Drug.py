


################################# Module Information ######################################
#  Module Name         : D_DRUG
#  Purpose             : This will create target table d_drug
#  Pre-requisites      : L1 source table required: CITELINE_PHARMAPROJECTS
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from L1 layer table
# 2. Pass through the L1 table on key columns to create final target table
############################################################################################
from DrugStandardization import DrugStandardization
from NotificationUtility import NotificationUtility

email_ses_region = 'us-east-1'
email_subject = 'Unmapped Standardized Data'
email_sender = 'atul.manchanda@zs.com'
email_template_path = 'drug_templ.html'
email_recipient_list = ["deeksha.deeksha@zs.com"]

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

# Fetching CITELINE_PHARMAPROJECTS data
temp_d_drug_citeline = spark.sql("""
select
    lower(citeline_pharma.drugprimaryname) as drug_nm,
    max(trialids) as src_trial_id,
    'citeline' as data_src_nm,
    max(drugnamesynonyms)  as drug_name_synonyms,
    max(globalstatus) as global_status,
    max(originatorname) as originator,
    max(originatorstatus) as originator_status,
    max(originatorcountry) as originator_country,
    max(originatorcompanykey) as originator_company_key,
    max(originatorcompanykey) as originator_parent_nm,
    max(originatorparentkey) as originator_parent_key,
    max(casnumbers) as cas_num,
    max(overview) as overview,
    max(marketing) as marketing,
    max(licensing) as licensing,
    max(licensee_licenseename) as licensee_nm,
    max(licensee_licenseestatus) as licensee_status,
    max(licensee_licenseecompanykey) as licensee_company_id,
    max(licensee_licenseecountry) as licensee_country,
    max(indicationgroups_id) as indication_group_id,
    max(indicationgroups_name) as indication_group_nm,
    max(indications_diseasename) as disease_nm,
    max(indications_diseasestatus) as disease_status,
    max(phasei) as phase_1,
    max(phaseii) as phase_2,
    max(phaseiii) as phase_3,
    max(preclinical) as pre_clinical,
    max(drugmeshterms_meshid) as mesh_id,
    max(drugmeshterms_name) as mesh_term,
    max(drugicd9_icd9id) as icd9_id,
    max(drugicd9_name) as icd9_nm,
    max(keyevents_keyeventdate) as key_event_dt,
    max(keyevents_keyeventhistory) as key_event_history,
    max(keyevents_keyeventdetail) as key_event_detail,
    max(therapeuticclasses_therapeuticclassname) as therapeutic_class_nm,
    max(therapeuticclasses_therapeuticclassstatus) as therapeutic_class_status,
    max(mechanismsofaction) as mechanism_of_action,
    max(deliveryroutes) as delivery_route,
    max(deliverymediums) as delivery_medium,
    max(deliverytechnologies) as delivery_technology,
    max(origin) as origin,
    max(molecularformula) as molecular_formula,
    max(molecularweight) as molecular_weight,
    max(nce) as nce,
    max(chemicalname) as chemical_nm,
    max(chemicalstructure) as chemical_structure,
    max(pharmacokinetics_model) as model,
    max(pharmacokinetics_unit) as unit,
    max(pharmacokinetics_parameter) as parameter,
    max(drugcountries_name) as country_nm,
    max(drugcountries_status) as country_status,
    max(drugcountries_yearlaunched) as country_year_launched,
    max(drugcountries_licensingopportunity) as country_licensing_opportunity,
    max(patents_patentnumber) as patent_num,
    max(patents_patentprioritycountrydate) as patent_priority_country_dt,
    max(targets_targetname) as target_nm,
    max(targets_targetsynonyms) as target_synonym,
    max(targets_targetfamilies) as target_family,
    max(targets_targetecnumbers) as target_ec_num,
    max(targets_entrezgeneid ) as entrezgene_id,
    max(ispharmaprojectsdrug) as is_pharma_project,
    max(latestchangedate) as latest_change_dt,
    max(latestchange) as latest_change
    from sanofi_ctfo_datastore_staging_$$db_env.citeline_pharmaprojects citeline_pharma
where lower(citeline_pharma.ispharmaprojectsdrug) ='true'
group by 1
""")
temp_d_drug_citeline = temp_d_drug_citeline.dropDuplicates()
temp_d_drug_citeline.write.mode('overwrite').saveAsTable('temp_d_drug_citeline')


# union of drug data from sources
union_d_drug = spark.sql("""
select  * from temp_d_drug_citeline
""")
union_d_drug.write.mode('overwrite').saveAsTable('union_d_drug')

# creating temp drug id on the basis of drug name
temp_drug_id = spark.sql("""
select row_number() over(order by 'null') as drug_id, drug_nm, data_src_nm
from (select lower(trim(drug_nm)) as drug_nm,concat_ws("\;",collect_set(distinct data_src_nm))
as data_src_nm from union_d_drug group by 1)
""")
temp_drug_id.write.mode('overwrite').saveAsTable('temp_drug_id')

# getting temp d drug columns for unique drug id and drug names
temp_d_drug = spark.sql("""
select A.drug_id, A.drug_nm,coalesce(C.src_trial_id,'') as src_trial_id,
coalesce(C.data_src_nm,'') as data_src_nm,
coalesce(C.drug_name_synonyms) as drug_name_synonyms,
C.global_status,C.originator,C.originator_status,C.originator_country,
C.originator_company_key,C.originator_parent_nm,C.originator_parent_key,C.cas_num,C.overview,
C.marketing,C.licensing,
C.licensee_nm,C.licensee_status,C.licensee_company_id,C.licensee_country,C.indication_group_id,
C.indication_group_nm,
C.disease_nm,C.disease_status,C.phase_1,C.phase_2,C.phase_3,C.pre_clinical,C.mesh_id,C.mesh_term,
C.icd9_id,C.icd9_nm,
C.key_event_dt,C.key_event_history,C.key_event_detail,C.therapeutic_class_nm,
C.therapeutic_class_status,C.mechanism_of_action,
C.delivery_route,C.delivery_medium,C.delivery_technology,C.origin,C.molecular_formula,
C.molecular_weight,C.nce,C.chemical_nm,
C.chemical_structure,C.model,C.unit,C.parameter,C.country_nm,C.country_status,
C.country_year_launched,C.country_licensing_opportunity,
C.patent_num,C.patent_priority_country_dt,C.target_nm,C.target_synonym,C.target_family,
C.target_ec_num,C.entrezgene_id,
C.is_pharma_project,C.latest_change_dt,C.latest_change
from temp_drug_id A
left OUTER join temp_d_drug_citeline  C
on lower(trim(A.drug_nm)) = lower(trim(C.drug_nm))
""")
temp_d_drug = temp_d_drug.dropDuplicates()
temp_d_drug.write.mode('overwrite').saveAsTable('temp_d_drug')


d_drug = spark.sql("""
select distinct
   drug_id, drug_nm, drug_name_synonyms, global_status, originator, originator_status,
   originator_country,
   originator_company_key, originator_parent_nm, originator_parent_key, cas_num, overview,
   marketing, licensing,
   licensee_nm, licensee_status, licensee_company_id, licensee_country, indication_group_id,
   indication_group_nm,
   disease_nm, disease_status, phase_1, phase_2, phase_3, pre_clinical, mesh_id, mesh_term,
   icd9_id, icd9_nm,
   key_event_dt, key_event_history, key_event_detail, therapeutic_class_nm,
    therapeutic_class_status,
   mechanism_of_action, delivery_route, delivery_medium, delivery_technology, origin,
   molecular_formula,
   molecular_weight, nce, chemical_nm, chemical_structure, model, unit, parameter, country_nm,
   country_status,
   country_year_launched, country_licensing_opportunity, patent_num, patent_priority_country_dt,
   target_nm,
   target_synonym, target_family, target_ec_num, entrezgene_id, is_pharma_project, latest_change_dt,
    latest_change
   from temp_d_drug
""")
d_drug = d_drug.dropDuplicates()
d_drug.registerTempTable('d_drug')
d_drug.write.mode('overwrite').saveAsTable('d_drug')

# Insert data into hdfs
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.d_drug partition(
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
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.d_drug')





