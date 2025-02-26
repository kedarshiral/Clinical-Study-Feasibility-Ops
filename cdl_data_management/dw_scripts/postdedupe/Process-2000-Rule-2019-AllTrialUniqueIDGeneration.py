
################################# Module Information ######################################

#  Module Name         : All Trial Unique ID Generation
#  Purpose             : This will execute queries to prepare data for xref tables to create
#                        cross reference mapping of source IDs with newly generated
#                        golden IDs based on dedupe clustering
#  Pre-requisites      : Source table required: temp_all_trial_dedupe_results_union_final
#  Last changed on     : 13-04-2023
#  Last changed by     : Himanshi
#  USL changes         : Kuldeep/Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# Prepare data for xref tables
############################################################################################
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from DedupeUtility import maintainGoldenID
from DedupeUtility import KMValidate
from pyspark.sql.functions import *
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
import MySQLdb
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

sys.path.insert(1, CommonConstants.EMR_CODE_PATH + '/code')

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = bucket_path + '/applications/commons/temp/mastering/trial/' \
                     'table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/'
                                  + CommonConstants.ENVIRONMENT_CONFIG_FILE)
audit_db = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, 'mysql_db'])
mysql_connection = MySQLConnectionManager().get_my_sql_connection()
cursor = mysql_connection.cursor(MySQLdb.cursors.DictCursor)

age_temp_path = bucket_path + '/uploads/age_mapping_devisor.csv'
age_standardization_temp = spark.read.csv(age_temp_path, sep=',', header=True, inferSchema=True)
age_standardization_temp.createOrReplaceTempView('age_standardization_temp')

age_standardization = spark.sql(""" select distinct * from (select age,value from age_standardization_temp 
union select value, value from age_standardization_temp )  """)
age_standardization.registerTempTable('age_standardization')

##Parameter to handle Age Min/Max Condition
min_age = 0
max_age = 100

gender_temp_path = bucket_path + '/uploads/gender_mapping.csv'
gender_standardization_temp = spark.read.csv(gender_temp_path, sep=',', header=True, inferSchema=True)
gender_standardization_temp.createOrReplaceTempView('gender_standardization_temp')

gender_standardization = spark.sql(""" select distinct * from (select gender,std_gender from gender_standardization_temp 
union select std_gender, std_gender from gender_standardization_temp )  """)
gender_standardization.registerTempTable('gender_standardization')

trial_phase_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/uploads/trial_phase.csv'.format(bucket_path=bucket_path))
# trial_phase_mapping_temp.registerTempTable('trial_phase_mapping_temp')
trial_phase_mapping_temp.write.mode('overwrite').saveAsTable('trial_phase_mapping_temp')

trial_phase_mapping = spark.sql(""" select distinct * from (select trial_phase, standard_value from trial_phase_mapping_temp 
union select standard_value, standard_value from trial_phase_mapping_temp )  """)
trial_phase_mapping.registerTempTable('trial_phase_mapping')

trial_status_mapping_temp = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping_temp.createOrReplaceTempView('trial_status_mapping_temp')

trial_status_mapping = spark.sql(""" select distinct * from (select raw_status, status from trial_status_mapping_temp 
union select status, status from trial_status_mapping_temp )  """)
trial_status_mapping.registerTempTable('trial_status_mapping')

###Dedupe enhancement

fuzzy_file_temp = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/FINAL_MESH_MAPPING/temp_mesh_mapping_folder/delt_mesh_mapping_temp.xlsx".format(
    bucket_path=bucket_path))
fuzzy_file_temp.dropDuplicates()
fuzzy_file_temp.registerTempTable('fuzzy_file_temp')
fuzzy_file = spark.sql("""select * from fuzzy_file_temp where similarity >= 0.8""")
fuzzy_file.write.mode('overwrite').saveAsTable('fuzzy_file')

mesh_standard_mapping = spark.sql(
    """ select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping """)
mesh_standard_mapping.createOrReplaceTempView("mesh_standard_mapping")

mesh_mapping_1 = spark.sql("""select distinct * from (select mesh, standard_mesh from mesh_standard_mapping
union select standard_mesh, standard_mesh from mesh_standard_mapping) """)
# disease_mapping_1.registerTempTable('disease_mapping_1')
mesh_mapping_1.write.mode('overwrite').saveAsTable('mesh_mapping_1')

disease_ta_mapping = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_to_ta_mapping """)
disease_ta_mapping.createOrReplaceTempView("disease_ta_mapping")


final_disease_ta_mapping = spark.sql("""
select distinct a.mesh,a.standard_mesh,b.therapeutic_area from mesh_mapping_1 a left join disease_ta_mapping b
on lower(trim(a.standard_mesh))=lower(trim(b.mesh_name)) """).registerTempTable('final_disease_ta_mapping')

src_to_std_ta_mapping = spark.read.format("csv").option("header", "true").option('delimiter', '^').load(
    "{bucket_path}/uploads/TA_MAPPING/src_to_std_ta_mapping.csv".format(bucket_path=bucket_path))
src_to_std_ta_mapping.registerTempTable('src_to_std_ta_mapping')

src_ta_value_in_df = spark.sql(""" select distinct src_ta_value from src_to_std_ta_mapping """)
src_ta_value_list = src_ta_value_in_df.select('src_ta_value').rdd.flatMap(lambda x: x).collect()
all_src_ta_values_trial = tuple(src_ta_value_list)


drug_mapping_file = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/DRUG/Drug_Mapping_File/Drug_Mapping.xlsx".format(bucket_path=bucket_path))
drug_mapping_file.dropDuplicates()
drug_mapping_file.write.mode('overwrite').saveAsTable('drug_mapping_file')

# Preparing union of all source data
# Get trial information from AACT
def sort_collected_data(text=''):
    try:
        text = text.replace('^', '|')
        text = text.replace('#', '|')
    except:
        return text
    if text.replace('|', '').strip() == '':
        return None
    try:
        collected_data = text.split('|')
    except:
        collected_data = ['']
    temp = list()
    for i in collected_data:
        temp.append(i.strip())
    collected_data = temp
    collected_data.sort()
    text = u'|'.join(collected_data).encode('utf-8').strip()
    return text.decode('utf-8')


spark.udf.register('sort_collected_data', sort_collected_data)


###Added for sorting protocol because '^' is coming as source
def sort_collected_data_protocol(text=''):
    if text.replace('|', '').strip() == '':
        return None
    try:
        collected_data = text.split('|')
    except:
        collected_data = ['']
    temp = list()
    for i in collected_data:
        temp.append(i.strip())
    collected_data = temp
    collected_data.sort()
    text = u'|'.join(collected_data).encode('utf-8').strip()
    return text.decode('utf-8')


spark.udf.register('sort_collected_data_protocol', sort_collected_data_protocol)


###Added for sorting nct because ',' is delimeter
def sort_collected_data_nct(text=''):
    if text.replace(',', '').strip() == '':
        return None
    try:
        collected_data = text.split(',')
    except:
        collected_data = ['']
    temp = list()
    for i in collected_data:
        temp.append(i.strip())
    collected_data = temp
    collected_data.sort()
    text = u','.join(collected_data).encode('utf-8').strip()
    return text.decode('utf-8')


spark.udf.register('sort_collected_data_nct', sort_collected_data_nct)


# function for calculating criterias
def sort_criteria(text, para):
    try:
        if text == None:
            return None
        if text.find('Withdrawal Criteria') != -1:
            text = text[:text.find('Withdrawal Criteria')]
            print("Text-----", text)
        a = ["Inclusion Criteria", "inclusion criteria", "Inclusion criteria", "inclusion Criteria",
             "INCLUSION CRITERIA"]
        for i in a:
            if text.find(i) != -1:
                text = text.replace(i, 'Inclusion Criteria')
                break

        b = ["Exclusion Criteria", "exclusion criteria", "Exclusion criteria", "exclusion Criteria",
             "EXCLUSION CRITERIA"]
        for i in b:
            if text.find(i) != -1:
                text = text.replace(i, 'Exclusion Criteria')
                break

        if para == 1:
            collected_data_in = text.split('Exclusion Criteria')

            if len(collected_data_in) > para:
                colon_data = collected_data_in[para]
                if colon_data[0] == ':':
                    ex_text = colon_data[1:]
                    ex_text = ex_text.strip()
                    return ex_text
                else:
                    ex_text = colon_data
                    ex_text = ex_text.strip()
                    return ex_text

            elif len(collected_data_in) == 1:
                if colon_data[0] == ':':
                    ex_text = colon_data[1:]
                    ex_text = ex_text.strip()
                    return ex_text
                else:
                    ex_text = colon_data
                    ex_text = ex_text.strip()
                    return ex_text
        elif para == 0:
            ex_text = text.replace('Inclusion Criteria', '')
            ex_text = ex_text[:ex_text.find('Exclusion Criteria')]
            ex_text = ex_text.strip()
            if ex_text[0] == ':':
                ex_text = ex_text[1:]
                ex_text = ex_text.strip()
            else:
                ex_text = ex_text.strip()

            return ex_text
        else:
            return None
    except Exception as e:
        return None


spark.udf.register('sort_criteria', sort_criteria)

temp_aact_trial_data_temp = spark.sql("""
select
    'aact' as data_src_nm,
    Cast(NULL as string) as patient_segment,
    study.nct_id as nct_id,
    coalesce(study.official_title,study.brief_title) as trial_title,
    study.phase as trial_phase,
    drug.name as trial_drug,
    a.name as trial_indication,
    Case when b.therapeutic_area in """ + str(all_src_ta_values_trial) + """ then std_ta.std_ta_value when 
    (b.therapeutic_area is null  or trim(b.therapeutic_area)='') then 'Other' else b.therapeutic_area end as therapeutic_area,
    study.nct_id as src_trial_id,
    study.nct_id as protocol_id,
    'clinicaltrials.gov' as protocol_type,
    study.overall_status as trial_status,
    study.study_type as trial_type,
    design.description as trial_design,
    study.start_date as trial_start_dt,
    study.start_date_type as trial_start_dt_type,
    coalesce(study.completion_date,study.primary_completion_date) as trial_end_dt,
    coalesce(study.completion_date_type, study.primary_completion_date_type) as trial_end_dt_type,
    trim(concat_ws('\|',collect_set(aact_facilities.country))) as trial_country,
    Cast(NULL as string) as trial_endpoint,
    trim(sort_criteria(eligibilities.criteria,0)) as trial_inclusion_criteria,
    trim(sort_criteria(eligibilities.criteria,1)) as trial_exclusion_criteria,
    regexp_replace(lower(trim(eligibilities.minimum_age)),"[^0-9A-Za-z_ -,']",'') as trial_age_min,
    regexp_replace(lower(trim(eligibilities.maximum_age)),"[^0-9A-Za-z_ -,']",'') as trial_age_max,
    eligibilities.gender as patient_gender,
    eligibilities.criteria as criteria,
    case when mesh.mesh_terms is null or trim(mesh.mesh_terms)='' then a.name
    else mesh.mesh_terms end as mesh_terms,
    study.results_first_submitted_date as results_date,
    Cast(NULL as string) as primary_endpoints_reported_date,
    Cast(NULL as string) as enrollment_close_date
from $$client_name_ctfo_datastore_staging_$$db_env.aact_studies study 
left join 
(select nct_id,
          concat_ws('\;',collect_set(regexp_replace(name, '\\\"', '\\\''))) as name  from $$client_name_ctfo_datastore_staging_$$db_env.aact_conditions group by 1) a
           on lower(trim(study.nct_id))=lower(trim(a.nct_id))
left join final_disease_ta_mapping b on lower(trim(a.name)) = lower(trim(b.mesh))
left join src_to_std_ta_mapping std_ta on lower(trim(b.therapeutic_area))=lower(trim(std_ta.src_ta_value))
left outer join
(select
    d.nct_id,
    concat_ws('\;',collect_set(d.name)) as name
from $$client_name_ctfo_datastore_staging_$$db_env.aact_interventions d
where lower(trim(d.intervention_type))='drug'
group by d.nct_id) drug
on lower(trim(study.nct_id)) = lower(trim(drug.nct_id))
left outer join
    (select nct_id,trim(concat_ws('\;',collect_set(description))) as description
    from $$client_name_ctfo_datastore_staging_$$db_env.aact_design_groups group by nct_id) design
on lower(trim(study.nct_id))=lower(trim(design.nct_id))
left outer join
        (select distinct nct_id ,mesh_term as mesh_terms from 
        (select mesh_term ,nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_browse_conditions
        union 
     select mesh_term ,nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_browse_interventions)
        ) mesh
    on lower(trim(study.nct_id))=lower(trim(mesh.nct_id))

left outer join
(select * from $$client_name_ctfo_datastore_staging_$$db_env.aact_eligibilities) eligibilities
on lower(trim(study.nct_id))=lower(trim(eligibilities.nct_id))
left outer join
(select nct_id,trim(concat_ws('\;',collect_set(country))) as country from
$$client_name_ctfo_datastore_staging_$$db_env.aact_facilities group by 1) aact_facilities
on lower(trim(study.nct_id))=lower(trim(aact_facilities.nct_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20,21,22,23,24,25,26,27,28,29,30
""")

temp_aact_trial_data_temp.registerTempTable('temp_aact_trial_data_temp')

temp_aact_trial_data_temp2 = spark.sql("""
select
    data_src_nm,
    patient_segment,
    nct_id,
    trial_title,
    trial_phase,
    case when trim(trial_drug) in (' ','','null','NA', 'na') then null
    else lower(trim(coalesce(drug.drugprimaryname,trial_drug))) end as trial_drug,
    trial_indication,
    trim(concat_ws('\|',sort_array(collect_set(a.therapeutic_area),true))) as therapeutic_area,
    src_trial_id,
    protocol_id,
    protocol_type,
    trial_status,
    trial_type,
    trial_design,
    trial_start_dt,
    trial_start_dt_type,
    trial_end_dt,
    trial_end_dt_type,
    trial_country,
    trial_endpoint,
    trial_inclusion_criteria,
    trial_exclusion_criteria,
    trial_age_min,
    trial_age_max,
    patient_gender,
    criteria,
    case when trim(mesh_terms) in (' ','','null','NA', 'na') then 'Other'
    else lower(trim(coalesce(b.standard_mesh,'Unassigned'))) end as mesh_terms,
    results_date,
    primary_endpoints_reported_date,
    enrollment_close_date
   from temp_aact_trial_data_temp a
   left outer join final_disease_ta_mapping b on lower(trim(a.mesh_terms)) = lower(trim(b.mesh))
    left outer join (select * from drug_mapping_file where lower(trim(drugprimaryname))!='other' 
    and lower(trim(drugprimaryname))!='others' )drug
    on lower(trim(a.trial_drug)) = lower(trim(drug.drugnamesynonyms))
    left outer join (select * from fuzzy_file ) fuzzy
    on lower(trim(a.mesh_terms)) = lower(trim(fuzzy.mesh_name))

    group by 1,2,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30""")

temp_aact_trial_data_temp2.createOrReplaceTempView('temp_aact_trial_data_temp2')

temp_aact_trial_data = spark.sql("""
select
    data_src_nm,
    patient_segment,
    nct_id,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication,
    therapeutic_area,
    src_trial_id,
    protocol_id,
    protocol_type,
    trial_status,
    trial_type,
    trial_design,
    trial_start_dt,
    trial_start_dt_type,
    trial_end_dt,
    trial_end_dt_type,
    trial_country,
    trial_endpoint,
    trial_inclusion_criteria,
    trial_exclusion_criteria,
    trial_age_min,
    trial_age_max,
    patient_gender,
    criteria,
    trim(concat_ws('\|',sort_array(collect_set(a.mesh_terms),true))) as mesh_terms,
    results_date,
    primary_endpoints_reported_date,
    enrollment_close_date
   from temp_aact_trial_data_temp2 a
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28,29,30""")
temp_aact_trial_data.createOrReplaceTempView('temp_aact_trial_data')


# tascan_exp = spark.sql("""SELECT   nct_id, trial_id,
#          Trim(ta)    AS therapeutic_area,
#          Trim(mesh) AS trial_mesh_term_name
#
# FROM     $$client_name_ctfo_datastore_common_$$db_env.tascan_trial
#         lateral view outer posexplode(split(trial_mesh_term_name,','))one AS pos1,mesh
# GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
# tascan_exp.registerTempTable('tascan_exp')
# # Get trial information from CITELINE
#
# tascan_exp_1 = spark.sql("""SELECT   nct_id, trial_id,
#          therapeutic_area,
#          Trim(mesh) as trial_mesh_term_name
#
# FROM     tascan_exp
#         lateral view outer explode(split(trial_mesh_term_name,'\\\^'))two AS mesh
# GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
# tascan_exp_1.registerTempTable('tascan_exp_1')
#
# tascan_2_ta = spark.sql("""SELECT   nct_id, trial_id, therapeutic_area,
#          Trim(mesh) as trial_mesh_term_name
#
# FROM     tascan_exp_1
#         lateral view outer explode(split(trial_mesh_term_name,'\\\#'))one AS mesh
# GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
#
# tascan_2_ta.registerTempTable('tascan_2_ta')
#
# tascan_explode_3_temp = spark.sql("""
#         select nct_id,trial_id, case when (therapeutic_area is null or trim(therapeutic_area)='') then src_ta else therapeutic_area end  as therapeutic_area,mesh_terms from (select
#               nct_id,
#                          a.trial_id,
#                     a.therapeutic_area as src_ta,
#                          b.therapeutic_area as therapeutic_area,
#                          case when trim(trial_mesh_term_name) in (' ','','null','NA', 'na') then 'Other'
#     else lower(trim(coalesce(b.standard_mesh,'Unassigned'))) end as mesh_terms
#                          from tascan_2_ta a
#               left join final_disease_ta_mapping b on lower(trim(a.trial_mesh_term_name)) = lower(trim(b.mesh))
#     left outer join (select * from fuzzy_file ) fuzzy
#     on lower(trim(a.trial_mesh_term_name)) = lower(trim(fuzzy.mesh_name))
#                          group by 1,2,3,4,5)group by 1,2,3,4
#     """).registerTempTable('tascan_explode_3_temp')
#
# tascan_explode_3 = spark.sql("""
#         select
#         nct_id,
#         trial_id,
#         Case when all_trial_data.therapeutic_area in """ + str(all_src_ta_values_trial) + """ then std_ta.std_ta_value
#         when (all_trial_data.therapeutic_area is null  or trim(all_trial_data.therapeutic_area)='') then NULL else all_trial_data.therapeutic_area
#         end as therapeutic_area,mesh_terms
#         from tascan_explode_3_temp all_trial_data
#                         left join src_to_std_ta_mapping std_ta
#                         on lower(trim(all_trial_data.therapeutic_area))=lower(trim(std_ta.src_ta_value))
#                          group by 1,2,3,4
#     """).registerTempTable('tascan_explode_3')
#
# temp_tascan_trial_data_exploaded = spark.sql("""
# select
#     'tascan' as data_src_nm,
#     ct.disease_name as disease_name,
#     ct.patient_segment as patient_segment,
#     ct.nct_id,
#     ct.trial_title as trial_title ,
#     ct.trial_phase as trial_phase,
#     case when trim(ct.drug_name) in (' ','','null','NA', 'na') then null
#     else lower(trim(coalesce(drug.drugprimaryname,ct.drug_name))) end as trial_drug,
#     ct.disease_name as trial_indication,
#     trim(concat_ws('\|',sort_array(collect_set(a.therapeutic_area),true))) as therapeutic_area,
#     ct.trial_id as src_trial_id,
#     '' as protocol_ids,
#     ct.protocol_type,
#     ct.trial_state_change as trial_status,
#     Cast(NULL as string) as trial_type,
#     ct.trial_design,
#     colaesce(ct.trial_start_date,ct.tral_actual_start_date) as trial_start_date,
#     Cast(NULL as string) as trial_start_dt_type,
#     ct.trial_end_date,
#     Cast(NULL as string) as trial_end_dt_type,
#     ct.trial_countries as trial_country,#
#     ct.primary_endpoint as trial_endpoint,
#     ct.inclusion_criteria as trial_inclusion_criteria,
#     ct.exclusion_criteria as trial_exclusion_criteria,
#     ct.min_patient_age as trial_age_min,
#     ct.max_patient_age as trial_age_max,
#     ct.patient_gender as patient_gender,
#     concat_ws(" ",ct.inclusion_criteria, ct.exclusion_criteria) as criteria,
#     trim(concat_ws('\|',sort_array(collect_set(a.trial_mesh_term_name),true))) as mesh_terms,
#     trim(two.results_date) as results_date,# trial first results submitted on
#
#     ct.trial_primary_endpoints_reported as primary_endpoints_reported_date,#
#     ct.trial_recruitment_period_end_date as enrollment_close_date
# from $$client_name_ctfo_datastore_common_$$db_env.tascan_trial ct
# left join tascan_explode_3 a on lower(trim(ct.trial_id))=lower(trim(a.trial_id))
# left outer join (select * from drug_mapping_file where lower(trim(drugprimaryname))!='other'
#     and lower(trim(drugprimaryname))!='others' )drug
#     on lower(trim(ct.drug_name)) = lower(trim(drug.drugnamesynonyms))
# group by 1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,29,30,31 #
# """)
#
# temp_tascan_trial_data_exploaded.createOrReplaceTempView('temp_tascan_trial_data_exploaded')


############## final select satement for TASCAN

citeline_exp = spark.sql("""SELECT   protocol_ids, trial_id,
         Trim(ta)    AS therapeutic_area,
         case when mesh is null or trim(mesh) = '' then disease
else mesh end as trial_mesh_term_name 

FROM     $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove 
        lateral view outer posexplode(split(trial_therapeutic_areas_name,'\\\|'))one AS pos1,ta
        lateral view outer posexplode(split(trial_mesh_term_name,'\\\|'))two AS pos2,mesh
        lateral view outer posexplode(split(disease_name,'\\\|'))three AS pos3,disease
WHERE     pos1=pos2 and pos2=pos3 and pos3=pos1
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
citeline_exp.registerTempTable('citeline_exp')

citeline_exp_1 = spark.sql("""SELECT   protocol_ids, trial_id,
         therapeutic_area,
         Trim(mesh) as trial_mesh_term_name

FROM     citeline_exp 
        lateral view outer explode(split(trial_mesh_term_name,'\\\^'))two AS mesh
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
citeline_exp_1.registerTempTable('citeline_exp_1')

citeline_explode_2_ta = spark.sql("""SELECT   protocol_ids, trial_id, therapeutic_area,
         Trim(mesh) as trial_mesh_term_name 

FROM     citeline_exp_1
        lateral view outer explode(split(trial_mesh_term_name,'\\\#'))one AS mesh
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)

citeline_explode_2_ta.registerTempTable('citeline_explode_2_ta')

citeline_explode_3_temp = spark.sql("""
        select protocol_ids,trial_id, case when (therapeutic_area is null or trim(therapeutic_area)='') then src_ta else therapeutic_area end  as therapeutic_area,mesh_terms from (select
              protocol_ids,
                         a.trial_id,
                    a.therapeutic_area as src_ta,
                         b.therapeutic_area as therapeutic_area,
                         case when trim(trial_mesh_term_name) in (' ','','null','NA', 'na') then 'Other'
    else lower(trim(coalesce(b.standard_mesh,'Unassigned'))) end as mesh_terms
                         from citeline_explode_2_ta a
              left join final_disease_ta_mapping b on lower(trim(a.trial_mesh_term_name)) = lower(trim(b.mesh))              
    left outer join (select * from fuzzy_file ) fuzzy
    on lower(trim(a.trial_mesh_term_name)) = lower(trim(fuzzy.mesh_name))
                         group by 1,2,3,4,5)group by 1,2,3,4
    """).registerTempTable('citeline_explode_3_temp')

citeline_explode_3 = spark.sql("""
        select 
        protocol_ids,
        trial_id,
        Case when all_trial_data.therapeutic_area in """ + str(all_src_ta_values_trial) + """ then std_ta.std_ta_value 
        when (all_trial_data.therapeutic_area is null  or trim(all_trial_data.therapeutic_area)='') then NULL else all_trial_data.therapeutic_area 
        end as therapeutic_area,mesh_terms
        from citeline_explode_3_temp all_trial_data
                        left join src_to_std_ta_mapping std_ta
                        on lower(trim(all_trial_data.therapeutic_area))=lower(trim(std_ta.src_ta_value))
                         group by 1,2,3,4
    """).registerTempTable('citeline_explode_3')

temp_citeline_trial_data_exploaded = spark.sql("""
select
    'citeline' as data_src_nm,
    ct.disease_name as disease_name,
    ct.patient_segment as patient_segment,
    case when lower(trim(src_protocol_ids)) like 'nct%' and lower(trim(src_protocol_ids)) not like 'nct#%'
        then src_protocol_ids else null end as nct_id,
    ct.trial_title as trial_title ,
    ct.trial_phase as trial_phase,
    case when trim(ct.drug_name) in (' ','','null','NA', 'na') then null
    else lower(trim(coalesce(drug.drugprimaryname,ct.drug_name))) end as trial_drug,
    ct.disease_name as trial_indication,
    trim(concat_ws('\|',sort_array(collect_set(a.therapeutic_area),true))) as therapeutic_area,
    ct.trial_id as src_trial_id,
    regexp_replace(src_protocol_ids,"\;","\\\,") as protocol_ids,
    ct.trial_source as protocol_type,
    ct.trial_status as trial_status,
    Cast(NULL as string) as trial_type,
    ct.study_design as trial_design,
    coalesce(ct.actual_trial_start_date,ct.start_date) as trial_start_dt,
    Cast(NULL as string) as trial_start_dt_type,
    ct.primary_completion_date as trial_end_dt,
    Cast(NULL as string) as trial_end_dt_type,
    ct.trial_countries as trial_country,
    concat_ws('',ct.primary_endpoint,ct.other_endpoint) as trial_endpoint,
    ct.inclusion_criteria as trial_inclusion_criteria,
    ct.exclusion_criteria as trial_exclusion_criteria,
    concat_ws(" ",ct.min_patient_age, ct.min_age_units) as trial_age_min,
    concat_ws(" ",ct.max_patient_age, ct.max_age_units) as trial_age_max,
    ct.patient_gender as patient_gender,
    concat_ws(" ",ct.inclusion_criteria, ct.exclusion_criteria) as criteria,
    trim(concat_ws('\|',sort_array(collect_set(a.mesh_terms),true))) as mesh_terms,
    trim(two.results_date) as results_date,
    ct.trial_primary_endpoints_reported as primary_endpoints_reported_date,
    ct.enrollment_close_date as enrollment_close_date
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove ct
left join citeline_explode_3 a on lower(trim(ct.trial_id))=lower(trim(a.trial_id))
left outer join (select * from drug_mapping_file where lower(trim(drugprimaryname))!='other' 
    and lower(trim(drugprimaryname))!='others' )drug
    on lower(trim(ct.drug_name)) = lower(trim(drug.drugnamesynonyms))
lateral view outer explode(split(ct.protocol_ids,'\\\|'))one as src_protocol_ids
lateral view outer explode(split(trim(ct.results_date),'\\\|'))two as results_date
group by 1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,29,30,31
""")

temp_citeline_trial_data_exploaded.createOrReplaceTempView('temp_citeline_trial_data_exploaded')

###changed delimeter of nct id to ',' from '|'
temp_citeline_trial_data = spark.sql("""
select
    data_src_nm,
    sort_collected_data(trim(ct.patient_segment)) as patient_segment,
    sort_collected_data_nct(ct.nct_id) as nct_id,
    ct.trial_title,
    ct.trial_phase,
    ct.trial_drug,
    case when ct.trial_indication in (' ','','null','na') then null else sort_collected_data(ct.trial_indication) end
    as trial_indication,
    ct.therapeutic_area,
    ct.src_trial_id,
    sort_collected_data_protocol(ct.protocol_ids) as protocol_id,
    ct.protocol_type,
    ct.trial_status,
    ct.trial_type,
    ct.trial_design,
    ct.trial_start_dt,
    ct.trial_start_dt_type,
    ct.trial_end_dt,
    ct.trial_end_dt_type,
    sort_collected_data(ct.trial_country) as trial_country,
    ct.trial_endpoint,
    ct.trial_inclusion_criteria,
    ct.trial_exclusion_criteria,
    regexp_replace(lower(trim(ct.trial_age_min)),"[^0-9A-Za-z_ -,']",'') as trial_age_min,
    regexp_replace(lower(trim(ct.trial_age_max)),"[^0-9A-Za-z_ -,']",'') as trial_age_max,
    ct.patient_gender,
    ct.criteria,
    ct.mesh_terms,
    cast(ct.results_date as date) as results_date,
    cast(ct.primary_endpoints_reported_date as date) as primary_endpoints_reported_date,
    ct.enrollment_close_date
from
(
select
    data_src_nm,
    concat_ws('\|',collect_set(trial_data_exploaded.patient_segment)) as patient_segment,
    concat_ws('\,',collect_set(trial_data_exploaded.nct_id)) as nct_id,
    concat_ws('\|',collect_set(trial_data_exploaded.protocol_ids)) as protocol_ids,
    trial_data_exploaded.trial_title as trial_title,
    trial_data_exploaded.trial_phase as trial_phase,
    trial_data_exploaded.trial_drug as trial_drug,
    concat_ws('\|',collect_set(trial_data_exploaded.disease_name)) as trial_indication,
    trial_data_exploaded.src_trial_id as src_trial_id,
    trial_data_exploaded.protocol_type,
    trial_data_exploaded.trial_status,
    trial_data_exploaded.trial_type,
    trial_data_exploaded.trial_design,
    trial_data_exploaded.trial_start_dt,
    trial_data_exploaded.trial_start_dt_type,
    trial_data_exploaded.trial_end_dt,
    trial_data_exploaded.trial_end_dt_type,
    trim(concat_ws('\|',collect_set(trial_data_exploaded.trial_country))) as trial_country,
    trial_data_exploaded.trial_endpoint,
    trial_data_exploaded.trial_inclusion_criteria,
    trial_data_exploaded.trial_exclusion_criteria,
    trial_data_exploaded.trial_age_min,
    trial_data_exploaded.trial_age_max,
    trial_data_exploaded.patient_gender,
    trial_data_exploaded.criteria,
    trial_data_exploaded.mesh_terms,
    trial_data_exploaded.results_date,
    trial_data_exploaded.primary_endpoints_reported_date,
    trial_data_exploaded.enrollment_close_date,
    sort_collected_data(therapeutic_area) as therapeutic_area
from temp_citeline_trial_data_exploaded trial_data_exploaded
group by 1,5,6,7,9,10,11,12,13,14,15,16,17,19,20,21,22,23,24,25,26,27,28,29,30) ct
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
""")
temp_citeline_trial_data.createOrReplaceTempView('temp_citeline_trial_data')

# Get trial information for DQS
dqs_status = spark.sql("""select * from (select d.member_study_id,d.status,d.trial_status_rnk,ROW_NUMBER() OVER (PARTITION BY d.member_study_id ORDER BY coalesce(d.trial_status_rnk, 5) ) as prec_rnk from     
 (select c.member_study_id, c.status,      
case when lower(trim(c.status)) ='ongoing' then 1             
when lower(trim(c.status))='completed' then 2  
when lower(trim(c.status))='planned' then 3       
when lower(trim(c.status))='others' then 4
end as trial_status_rnk from (select a.member_study_id,case when a.site_status like '%ongoing%' then 'ongoing'             
      else b.status end as status from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir a left join trial_status_mapping b on 
                 lower(trim(a.site_status))=lower(trim(b.raw_status)) ) as c) as d group by 1,2,3) cc where cc.prec_rnk=1""")
dqs_status.createOrReplaceTempView("dqs_status")

###exploding DQS diseases####
temp_dqs_exploded_disease = spark.sql("""select
    member_study_id,
               primary_indication,coalesce(mesh_heading,primary_indication) as mesh_heading
    from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir

group by 1,2,3""").registerTempTable('temp_dqs_exploded_disease')
###exploding DQS diseases####
temp_dqs_exploded_disease = spark.sql("""select
    member_study_id,
               trial_indication_exp as primary_indication,mesh_heading_explode2 as mesh_term
    from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir
lateral view outer explode(split(primary_indication, '\\\;'))one as trial_indication_exp
lateral view outer explode(split(mesh_heading, ';'))one as mesh_heading_explode
lateral view outer explode(split(mesh_heading_explode, '\\\|'))one as mesh_heading_explode2

group by 1,2,3""").registerTempTable('temp_dqs_exploded_disease')
######code update during dedupe enhancement


temp_dqs_site_data_0_temp = spark.sql("""
select
    a.member_study_id,    
    Case when b.therapeutic_area in """ + str(all_src_ta_values_trial) + """ then std_ta.std_ta_value when 
    (b.therapeutic_area is null  or trim(b.therapeutic_area)='') then 'Other' else b.therapeutic_area end as therapeutic_area,
    case when trim(mesh_term) in (' ','','null','NA', 'na') then 'Other'
    else lower(trim(coalesce(b.standard_mesh,'Unassigned'))) end as mesh_terms
from temp_dqs_exploded_disease a
left join final_disease_ta_mapping b on lower(trim(a.primary_indication))=lower(trim(b.mesh))
left outer join (select * from fuzzy_file ) fuzzy
    on lower(trim(a.primary_indication)) = lower(trim(fuzzy.mesh_name))
left join src_to_std_ta_mapping std_ta
on lower(trim(b.therapeutic_area))=lower(trim(std_ta.src_ta_value))
group by 1,2,3
""")
temp_dqs_site_data_0_temp.registerTempTable('temp_dqs_site_data_0_temp')

temp_dqs_site_data_0 = spark.sql("""
select
    member_study_id,
    trim(concat_ws('\|',sort_array(collect_set(therapeutic_area),true))) as therapeutic_area,
        trim(concat_ws('\|',sort_array(collect_set(mesh_terms),true))) as mesh_terms
from temp_dqs_site_data_0_temp
group by 1
""")
temp_dqs_site_data_0.registerTempTable('temp_dqs_site_data_0')
### Updated protocol id column from nct_number to member_study_id as that is the key trial id from dqs
temp_dqs_trial_data = spark.sql("""
select
    trials.data_src_nm,
    trials.patient_segment,
    trials.nct_id,
    trials.trial_title ,
    trials.trial_phase,
    trials.trial_drug,
    case when trim(lower(trials.trial_indication)) in (' ','','null','na') then null
    else trials.trial_indication end as trial_indication,
    therapeutic_area,
    src_trial_id,
    trials.protocol_id,
    trials.protocol_type,
    coalesce(trials.trial_status,'Others') as trial_status,
    trials.trial_type,
    trials.trial_design,
    trials.trial_start_dt,
    trials.trial_start_dt_type,
    trials.trial_end_dt,
    trials.trial_end_dt_type,
    trials.trial_country,
    trials.trial_endpoint,
    trials.trial_inclusion_criteria,
    trials.trial_exclusion_criteria,
    regexp_replace(lower(trim(trials.trial_age_min)),"[^0-9A-Za-z_ -,']",'') as trial_age_min,
    regexp_replace(lower(trim(trials.trial_age_max)),"[^0-9A-Za-z_ -,']",'') as trial_age_max,
    trials.patient_gender,
    trials.criteria,
    trials.mesh_terms,
    trials.results_date,
    trials.primary_endpoints_reported_date,
    trials.enrollment_close_date
from
(select
    'ir' as data_src_nm,
    Cast(NULL as string) as patient_segment,
    case when base.nct_number like 'NCT%' then base.nct_number else null end as nct_id,
    base.study_title as trial_title ,
    base.phase as trial_phase,
    Cast(NULL as string) as trial_drug,
    base.primary_indication as trial_indication,
    base.member_study_id as src_trial_id,
    base.member_study_id as protocol_id,
    Cast(NULL as string) as protocol_type,
st.status as trial_status,
    Cast(NULL as string) as trial_type,
    Cast(NULL as string) as trial_design,
    min(base.site_open_dt) as trial_start_dt,
    Cast(NULL as string) as trial_start_dt_type,
    max(base.last_subject_last_visit_dt) as trial_end_dt,
    Cast(NULL as string) as trial_end_dt_type,
    trim(concat_ws('\|',collect_set(base.facility_country))) as trial_country,
    Cast(NULL as string) as trial_endpoint,
    Cast(NULL as string) as trial_inclusion_criteria,
    Cast(NULL as string) as trial_exclusion_criteria,
    Cast(NULL as string) as trial_age_min,
    Cast(NULL as string) as trial_age_max,
    Cast(NULL as string) as patient_gender,
    Cast(NULL as string) as criteria,
    b.mesh_terms,
    Cast(NULL as string) as results_date,
    Cast(NULL as string) as primary_endpoints_reported_date,
    max(last_subject_enrolled_dt) as enrollment_close_date,
    b.therapeutic_area
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir base
left join temp_dqs_site_data_0 b on lower(trim(base.member_study_id))=lower(trim(b.member_study_id))
left join dqs_status st on lower(trim(base.member_study_id)) = lower(trim(st.member_study_id)) 
left join trial_status_mapping std on lower(trim(base.site_status))=lower(trim(std.raw_status))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,15,17,19,20,21,22,23,24,25,26,27,28,30) trials
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30
""")
temp_dqs_trial_data.createOrReplaceTempView('temp_dqs_trial_data')

# Get trial information from CTMS

temp_ctms_indication_exploded = spark.sql("""
select src_trial_id,
        therapeutic_area,
        trial_disease_exp as disease,drug as trial_drug
               from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study
               lateral view outer explode(split(disease, '\\\|'))one as trial_disease_exp
               group by 1,2,3,4 """).registerTempTable('temp_ctms_indication_exploded')
temp_ctms_ta_temp = spark.sql("""
    select src_trial_id,case when (therapeutic_area is null or trim(therapeutic_area)='') then src_ta else therapeutic_area end as therapeutic_area,
    mesh_terms,trial_drug from
    (select src_trial_id,
           a.therapeutic_area as src_ta,
           b.therapeutic_area  as therapeutic_area,
           case when trim(disease) in (' ','','null','NA', 'na') then 'Other'
    else lower(trim(coalesce(b.standard_mesh,'Unassigned'))) end as mesh_terms,
    case when trim(trial_drug) in (' ','','null','NA', 'na') then null
    else lower(trim(coalesce(drug.drugprimaryname,trial_drug))) end as trial_drug
    from  temp_ctms_indication_exploded a 
    left join final_disease_ta_mapping b on lower(trim(a.disease))=lower(trim(b.mesh))
    left outer join (select * from fuzzy_file ) fuzzy
    on lower(trim(a.disease)) = lower(trim(fuzzy.mesh_name))
    left outer join (select * from drug_mapping_file where lower(trim(drugprimaryname))!='other' 
    and lower(trim(drugprimaryname))!='others' )drug
    on lower(trim(a.trial_drug)) = lower(trim(drug.drugnamesynonyms))
    group by 1,2,3,4,5) group by 1,2,3,4 """).registerTempTable('temp_ctms_ta_temp')

temp_ctms_ta = spark.sql("""
select src_trial_id,
        Case when a.therapeutic_area in """ + str(all_src_ta_values_trial) + """ then std_ta.std_ta_value 
        when (a.therapeutic_area is null  or trim(a.therapeutic_area)='') then NULL else a.therapeutic_area end as therapeutic_area,
        mesh_terms,trial_drug
from  temp_ctms_ta_temp a 
left join src_to_std_ta_mapping std_ta
on lower(trim(a.therapeutic_area))=lower(trim(std_ta.src_ta_value))
group by 1,2,3,4 """).registerTempTable('temp_ctms_ta')

### Updated protocol id column from blank to src_trial_id as that is the key trial id from ctms
temp_ctms_trial_data = spark.sql("""select
  source as data_src_nm,
  patient_segment,
  nct_id,
  trim(concat_ws('\|', collect_set(trial_title))) as trial_title,
  trim(concat_ws('\|', collect_set(trial_phase))) as trial_phase,
  trim(concat_ws('\|', collect_set(b.trial_drug))) as trial_drug,
  trim(concat_ws('\|', collect_set(disease))) as trial_indication,
trim(concat_ws('\|',sort_array(collect_set(b.therapeutic_area),true))) as therapeutic_area,
  a.src_trial_id,
  a.src_trial_id as protocol_id,
  protocol_type,
  trim(concat_ws('\|', collect_set(trial_status))) as trial_status,
  trial_type,
  trial_design,
  trial_start_dt,
  Cast(NULL as string) as trial_start_dt_type,
  trial_end_dt,
  Cast(NULL as string) as trial_end_dt_type,
  Cast(NULL as string) as trial_country,
  Cast(NULL as string) as trial_endpoint,
  inclusion_criteria as trial_inclusion_criteria,
  exclusion_criteria as trial_exclusion_criteria,
  trial_age_min,
  trial_age_max,
  patient_gender,
  Cast(NULL as string) as criteria,
  trim(concat_ws('\|', sort_array(collect_set(b.mesh_terms),true))) as mesh_terms,
  Cast(NULL as Date) as results_date,
  Cast(NULL as Date) as primary_endpoints_reported_date,
  last_subject_first_treatment_actual_dt as  enrollment_close_date
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study a
left join temp_ctms_ta b on lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id))
group by 1,2,3,9,10,11,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28,29,30
""")

temp_ctms_trial_data.createOrReplaceTempView('temp_ctms_trial_data')

temp_tascan_indication_exploded = spark.sql("""
select trial_id as src_trial_id,
        trial_therapeutic_area_name as therapeutic_area,
        trial_disease_exp as disease,trial_drug_exp as trial_drug
               from $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial
               lateral view outer explode(split(mesh_term_names, '\\\;'))one as trial_disease_exp
               lateral view outer explode(split(drug_names, '\\\;'))two as trial_drug_exp
               group by 1,2,3,4 """).registerTempTable('temp_tascan_indication_exploded')
temp_tascan_ta_temp = spark.sql("""
    select src_trial_id,case when (therapeutic_area is null or trim(therapeutic_area)='') then src_ta else therapeutic_area end as therapeutic_area,
    mesh_terms,trial_drug from
    (select src_trial_id,
           a.therapeutic_area as src_ta,
           b.therapeutic_area  as therapeutic_area,
           case when trim(disease) in (' ','','null','NA', 'na') then 'Other'
    else lower(trim(coalesce(b.standard_mesh,'Unassigned'))) end as mesh_terms,
    case when trim(trial_drug) in (' ','','null','NA', 'na') then null
    else lower(trim(coalesce(drug.drugprimaryname,trial_drug))) end as trial_drug
    from  temp_tascan_indication_exploded a 
    left join final_disease_ta_mapping b on lower(trim(a.disease))=lower(trim(b.mesh))
    left outer join (select * from fuzzy_file ) fuzzy
    on lower(trim(a.disease)) = lower(trim(fuzzy.mesh_name))
    left outer join (select * from drug_mapping_file where lower(trim(drugprimaryname))!='other' 
    and lower(trim(drugprimaryname))!='others' )drug
    on lower(trim(a.trial_drug)) = lower(trim(drug.drugnamesynonyms))
    group by 1,2,3,4,5) group by 1,2,3,4 """).registerTempTable('temp_tascan_ta_temp')

temp_tascan_ta = spark.sql("""
select src_trial_id,
        Case when a.therapeutic_area in """ + str(all_src_ta_values_trial) + """ then std_ta.std_ta_value 
        when (a.therapeutic_area is null  or trim(a.therapeutic_area)='') then NULL else a.therapeutic_area end as therapeutic_area,
        mesh_terms,trial_drug
from  temp_tascan_ta_temp a 
left join src_to_std_ta_mapping std_ta
on lower(trim(a.therapeutic_area))=lower(trim(std_ta.src_ta_value))
group by 1,2,3,4 """).registerTempTable('temp_tascan_ta')

### Updated protocol id column from blank to src_trial_id as that is the key trial id from tascan
temp_tascan_trial_data = spark.sql("""select
  'tascan' as data_src_nm,
  '' patient_segment,
  nct_id,
  trim(concat_ws('\|', collect_set(trial_title))) as trial_title,
  trim(concat_ws('\|', collect_set(trial_phase))) as trial_phase,
  trim(concat_ws('\|', collect_set(b.trial_drug))) as trial_drug,
  trim(concat_ws('\|', collect_set(mesh_terms))) as trial_indication,
  trim(concat_ws('\|',sort_array(collect_set(b.therapeutic_area),true))) as therapeutic_area,
  a.trial_id,
  a.trial_id as protocol_id,
  '' protocol_type,
  trim(concat_ws('\|', collect_set(state))) as trial_status,
  '' trial_type,
  trial_design,
  trial_start_date as trial_start_dt,
  Cast(NULL as string) as trial_start_dt_type,
  trial_end_date as trial_end_dt,
  Cast(NULL as string) as trial_end_dt_type,
  Cast(NULL as string) as trial_country,
  Cast(NULL as string) as trial_endpoint,
  inclusion_criteria as trial_inclusion_criteria,
  exclusion_criteria as trial_exclusion_criteria,
  min_patient_age as trial_age_min,
  max_patient_age as trial_age_max,
  patient_gender,
  Cast(NULL as string) as criteria,
  trim(concat_ws('\|', sort_array(collect_set(b.mesh_terms),true))) as mesh_terms,
  Cast(NULL as Date) as results_date,
  Cast(NULL as Date) as primary_endpoints_reported_date,
  enrollment_close_date as  enrollment_close_date
from $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial a
left join temp_tascan_ta b on lower(trim(a.trial_id))=lower(trim(b.src_trial_id))
group by 1,2,3,9,10,11,13,14,15,16,17,18,19,20,21,22,23,24,25,26,28,29,30
""")

temp_tascan_trial_data.createOrReplaceTempView('temp_tascan_trial_data')

# temp_tascan_trial_data.write.mode("overwrite").saveAsTable("temp_tascan_trial_data")

temp_all_trial_data_final_0 = spark.sql("""
select * from temp_aact_trial_data
union
select * from temp_citeline_trial_data
union
select * from temp_dqs_trial_data
union
select * from temp_ctms_trial_data
union 
select * from temp_tascan_trial_data
""").registerTempTable('temp_all_trial_data_final_0')

temp_all_trial_data_final = spark.sql("""
select data_src_nm,
patient_segment,trim(nct_id) as nct_id,
case when trim(trial_title) =''  then null else 
trim(trial_title) end as  trial_title,
case when trim(trial_phase) =''  then null else 
trim(trial_phase) end as  trial_phase,
case when trim(trial_drug) =''  then null else 
trim(trial_drug) end as  trial_drug,
case when trim(trial_indication) =''  then null else 
trim(trial_indication) end as  trial_indication,
case when trim(therapeutic_area) =''  then null else 
trim(therapeutic_area) end as  therapeutic_area,
src_trial_id,
case when trim(protocol_id) =''  then null else 
trim(protocol_id) end as  protocol_id,
case when trim(protocol_type) =''  then null else 
trim(protocol_type) end as  protocol_type,
case when trim(trial_status) =''  then null else 
trim(trial_status) end as  trial_status,
case when trim(trial_type) =''  then null else 
trim(trial_type) end as  trial_type,
case when trim(trial_design) =''  then null else 
trim(trial_design) end as  trial_design,
case when trim(trial_start_dt) =''  then null else 
trim(trial_start_dt) end as  trial_start_dt,
case when trim(trial_start_dt_type) =''  then null else 
trim(trial_start_dt_type) end as  trial_start_dt_type,
case when trim(trial_end_dt) =''  then null else 
trim(trial_end_dt) end as  trial_end_dt,
case when trim(trial_end_dt_type) =''  then null else 
trim(trial_end_dt_type) end as  trial_end_dt_type,
case when trim(trial_country) =''  then null else 
trim(trial_country) end as  trial_country,
case when trim(trial_endpoint) =''  then null else 
trim(trial_endpoint) end as  trial_endpoint,
case when trim(trial_inclusion_criteria) =''  then null else 
trim(trial_inclusion_criteria) end as  trial_inclusion_criteria,
case when trim(trial_exclusion_criteria) =''  then null else 
trim(trial_exclusion_criteria) end as  trial_exclusion_criteria,
case when trim(trial_age_min) =''  then null else 
trim(trial_age_min) end as  trial_age_min,
case when trim(trial_age_max) =''  then null else 
trim(trial_age_max) end as  trial_age_max,
case when trim(patient_gender) =''  then null else 
trim(patient_gender) end as  patient_gender,
case when trim(criteria) =''  then null else 
trim(criteria) end as  criteria,
case when trim(mesh_terms) =''  then null else 
trim(mesh_terms) end as  mesh_terms,
case when trim(results_date) =''  then null else 
trim(results_date) end as  results_date,
case when trim(primary_endpoints_reported_date) =''  then null else 
trim(primary_endpoints_reported_date) end as  primary_endpoints_reported_date,
case when trim(enrollment_close_date) =''  then null else 
trim(enrollment_close_date) end as  enrollment_close_date from
(select 
    data_src_nm,
    patient_segment,
    nct_id,
    case when lower(trim(trial_title)) in ('',';',';;','`','n/a','null') then null else trial_title
    end as trial_title,
    trial_phase,
    trial_drug,
    trial_indication,
    Case when all_trial_data.therapeutic_area in """ + str(all_src_ta_values_trial) + """ then std_ta.std_ta_value when (all_trial_data.therapeutic_area is null  or trim(all_trial_data.therapeutic_area)='') then 'Other' else all_trial_data.therapeutic_area end as therapeutic_area,
    src_trial_id,
    protocol_id,
    protocol_type,
    trial_status,
    trial_type,
    trial_design,
    trial_start_dt,
    trial_start_dt_type,
    trial_end_dt,
    trial_end_dt_type,
    trial_country,
    trial_endpoint,
    trial_inclusion_criteria,
    trial_exclusion_criteria,
    trial_age_min,
    trial_age_max,
    patient_gender,
    criteria,
    mesh_terms,
    results_date,
    primary_endpoints_reported_date,
    enrollment_close_date
from temp_all_trial_data_final_0 all_trial_data
left join src_to_std_ta_mapping std_ta
on lower(trim(all_trial_data.therapeutic_area))=lower(trim(std_ta.src_ta_value)))
""")

# Save on HDFS
temp_all_trial_data_final.write.mode('overwrite').saveAsTable('temp_all_trial_data_final')

# Push to S3
write_path = path.replace('table_name', 'temp_all_trial_data_final')
temp_all_trial_data_final.repartition(100).write.mode('overwrite').parquet(write_path)

cursor.execute(
    """select cycle_id ,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = 1000 order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db))

fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
print(latest_stable_data_date, latest_stable_cycle_id)
temp_all_trial_dedupe_results_final_0 = spark.read.parquet(
    "{bucket_path}/applications/commons/temp/mastering/trial/temp_all_trial_dedupe_results_final/pt_data_dt={}/pt_cycle_id={}/".format(
        latest_stable_data_date, latest_stable_cycle_id, bucket_path=bucket_path))
#final_table = spark.sql("""select * from temp_all_trial_dedupe_results_final""")
temp_all_trial_dedupe_results_final_0.write.mode('overwrite').saveAsTable('temp_all_trial_dedupe_results_final_0')

# Modified code to use KMValidate function for incorporating KM input
#
# test111=spark.read.parquet('s3://aws-a0339-use1-00-d-s3b-incy-spo-data01/clinical-data-lake/applications/commons/temp/mastering/trial/temp_all_trial_data/pt_data_dt=20240709/pt_cycle_id=2024071013543876214/')
# test111.registerTempTable('test111')
#
# id_info1=spark.read.parquet('s3://aws-a0339-use1-00-d-s3b-incy-spo-data01/clinical-data-lake/staging/AACT/AACT_ID_INFORMATION/pt_batch_id=2024060314452512902/pt_file_id=3219/')
# id_info1.registerTempTable('id_info1')
#
# id_info = spark.sql("""
# select
#     nct_id,
#     id_value
# from id_info1
# where id_type = 'nct_alias'
# group by 1,2
# """)
# id_info.registerTempTable('id_info')
#
# temp_all_trial_data_cleaned_obs_remove = spark.sql("""
# select /*+ broadcast(info) */ distinct
#     data_src_nm,
#     src_trial_id,
#     trial_title,
#     trial_phase,
#     trial_drug,
#    trial_indication,
#     coalesce(info.nct_id,base.protocol_ids) as protocol_ids
# from test111 base
# left outer join id_info info
# on regexp_replace(trim(lower(base.protocol_ids)),'[^0-9A-Za-z]','') = regexp_replace(trim(lower(info.id_value)),'[^0-9A-Za-z]','')
# where (lower(trim(data_src_nm)) in ('citeline','aact') and regexp_replace(trim(lower(trial_title)),'[^0-9A-Za-z]','')
# is not null and regexp_replace(trim(lower(trial_title)),'[^0-9A-Za-z]','') != '') or lower(trim(data_src_nm)) in ('ctms','ir')
# group by 1,2,3,4,5,6,7
# """)
# temp_all_trial_data_cleaned_obs_remove.registerTempTable('temp_all_trial_data_cleaned_obs_remove')
# # Writing base table on HDFS to make it available for the function to fetch data
final_table = spark.sql("""
select a.rule_id,
a.cluster_id,
a.score,
a.uid,
a.hash_uid,
CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(NULLIF(TRIM(exploded_data_src_nm), '')), true)) AS data_src_nm,
CONCAT_WS(';', SORT_ARRAY(COLLECT_SET(NULLIF(TRIM(exploded_trial_id_raw), '')), true)) AS ds_trial_id,
a.trial_title,
a.trial_phase,
a.trial_drug,
a.trial_indication,
a.KM_comment,case when lower(b.protocol_ids) like 'nct%' then b.protocol_ids else null end as nct_id
from
(SELECT
              a.*,
              exploded1.exploded_trial_id_raw,
                SPLIT(exploded1.exploded_trial_id_raw, '_')[0] AS exploded_data_src_nm
          FROM
              temp_all_trial_dedupe_results_final_0 a
          LATERAL VIEW OUTER EXPLODE(SPLIT(a.ds_trial_id, ';')) exploded1 AS exploded_trial_id_raw
         ) a

LEFT JOIN
    (select * from $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_trial_data_cleaned_obs_remove where lower(trim(protocol_ids)) like 'nct%') b
ON
    a.exploded_trial_id_raw = b.src_trial_id
group by 1,2,3,4,5,8,9,10,11,12,13
""")

final_table.write.mode('overwrite'). \
    saveAsTable('final_table')
#
# # final table contains the name of the final table [temp_all_<trial>_dedupe_results_km_opt_union]
# # having columns final_cluster_id, final_score and final_KM_comment to be used for
# #  further processing
# final_table = KMValidate('temp_all_trial_dedupe_results_final_1', 'trial', '$$data_dt', '$$cycle_id', '$$s3_env')

cursor.execute(
    """select cycle_id ,data_date from {orchestration_db_name}.log_cycle_dtl where trim(lower(cycle_status)) = 'succeeded' and process_id = 2000 order by cycle_start_time desc limit 1 """.format(
        orchestration_db_name=audit_db))

fetch_enable_flag_result = cursor.fetchone()
latest_stable_data_date = str(fetch_enable_flag_result['data_date']).replace("-", "")
latest_stable_cycle_id = str(fetch_enable_flag_result['cycle_id'])
print(latest_stable_data_date, latest_stable_cycle_id)
prev_xref_src_trial= spark.read.parquet(
    "{bucket_path}/applications/commons/dimensions/xref_src_trial/pt_data_dt={}/pt_cycle_id={}/".format(
        latest_stable_data_date, latest_stable_cycle_id, bucket_path=bucket_path))
prev_xref_src_trial.write.mode('overwrite').saveAsTable('prev_xref_src_trial')


#
# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("Create Empty DataFrame") \
#     .getOrCreate()
#
# # Create the schema based on the existing DataFrame xref_src_inv1
# schema = StructType([
#     StructField("data_src_nm", StringType(), True),
#     StructField("src_trial_id", StringType(), True),
#     StructField("ctfo_trial_id", StringType(), True),
#     StructField("nct_id", StringType(), True),
#     StructField("trial_title", StringType(), True),
#     StructField("trial_phase", IntegerType(), True),
#     StructField("trial_drug", StringType(), True),
#     StructField("mesh_terms", StringType(), True),
#     StructField("therapeutic_area", StringType(), True)
# ])
#
# # Create an empty DataFrame with the specified schema
# prev_xref_src_trial = spark.createDataFrame([], schema)
#
# # Save the empty DataFrame as a table if needed
# prev_xref_src_trial.write.mode('overwrite').saveAsTable('prev_xref_src_trial')


'''# Assiging Golden ID for records with score greater than $$threshold
temp_trial_id_clusterid = spark.sql("""
select
    concat('ctfo_trial_',row_number() over(order by null)) as trial_id,
    temp.final_cluster_id as cluster_id
from
    (select distinct final_cluster_id from """ + final_table + """ where
    final_cluster_id is not null
    and final_cluster_id <> '' and round(cast(final_score as double),2) >=$$threshold) temp
""")

# Save on HDFS
temp_trial_id_clusterid.write.mode('overwrite').saveAsTable('temp_trial_id_clusterid')
'''

temp_trial_id_clusterid = spark.sql("""
        select
            a.cluster_id as cluster_id,
            concat("ctfo_trial_", row_number() over(order by null) + coalesce(temp.max_id,0)) as trial_id
        from
        (select distinct cluster_id from final_table where round(score,2) >=$$threshold) a cross join
        (select max(cast(split(ctfo_trial_id,'_')[2] as bigint))  as max_id from prev_xref_src_trial) temp""")


temp_trial_id_clusterid.write.mode('overwrite').saveAsTable('temp_trial_id_clusterid')
write_path = path.replace('table_name', 'temp_trial_id_clusterid')
temp_trial_id_clusterid.repartition(100).write.mode('overwrite').parquet(write_path)

# Push to S3
# write_path = path.replace('table_name', 'temp_trial_id_clusterid')
# temp_trial_id_clusterid.repartition(100).write.mode('overwrite').parquet(write_path)

# Generating Cross reference for records with score greater than 0.65
temp_xref_trial_1 = spark.sql("""
select
    res.uid,
    res.hash_uid,
    res.data_src_nm,
    res.ds_trial_id,
    ctoid.trial_id as trial_id,
    res.trial_title,
    res.trial_phase,
    res.trial_drug,
    res.trial_indication,
    res.score,
    res.final_KM_comment as final_KM_comment,res.nct_id
from
    (select distinct uid,cluster_id as cluster_id,hash_uid,score,
    trial_title, trial_phase,
    trial_drug, trial_indication, ds_trial_id, data_src_nm,'' as final_KM_comment,nct_id
    from  final_table where round(coalesce(score,0),2) >= $$threshold) res
left outer join temp_trial_id_clusterid ctoid
on lower(trim(ctoid.cluster_id))=lower(trim(res.cluster_id))
""")

# Save on HDFS
temp_xref_trial_1.write.mode('overwrite').saveAsTable('temp_xref_trial_1')

# Push to S3
write_path = path.replace('table_name', 'temp_xref_trial_1')
temp_xref_trial_1.repartition(100).write.mode('overwrite').parquet(write_path)

# for some records whose score>=0.65 and no cluster id after post dedupe logic
# assign them unique golden_id=max(site_id)+row_number
temp_xref_trial_2 = spark.sql("""
select /*+ broadcast(temp) */
    res.uid,
    res.hash_uid,
    res.data_src_nm,
    res.ds_trial_id,
    concat('ctfo_trial_', row_number() over(order by null) + coalesce(temp.max_id,0)) as trial_id,
    res.trial_title,
    res.trial_phase,
    res.trial_drug,
    res.trial_indication,
    res.score,
    res.final_KM_comment as final_KM_comment,res.nct_id
from
    (select distinct uid,cluster_id,hash_uid,score,
     trial_title, trial_phase,
    trial_drug, trial_indication, ds_trial_id, data_src_nm,'' as final_KM_comment,nct_id
    from final_table  where round(coalesce(score,0),2) < $$threshold) res
cross join
    (select max(cast(split(trial_id,'trial_')[1] as bigint)) as max_id from temp_xref_trial_1) temp
""")

# Save on HDFS
temp_xref_trial_2.write.mode('overwrite').saveAsTable('temp_xref_trial_2')

# Push to S3
write_path = path.replace('table_name', 'temp_xref_trial_2')
temp_xref_trial_2.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_trial_final_0 = spark.sql("""
select *
from temp_xref_trial_1
union
select *
from temp_xref_trial_2
""")

temp_xref_trial_final_0.write.mode('overwrite').saveAsTable('temp_xref_trial_final_0')


if '$$Prev_Data' == 'N':
    temp_xref_trial = spark.sql("""
    select *
    from temp_xref_trial_final_0
    """)
else:
    # Calling maintainGoldenID function to preserve IDs across runs
    final_table_1 = maintainGoldenID('temp_xref_trial_final_0', 'trial', '$$process_id', '$$Prev_Data', '$$s3_env')
    temp_xref_trial = spark.sql("""
    select
        base.uid,
        base.hash_uid,
        base.data_src_nm,
        base.ds_trial_id,
        coalesce(func_opt.ctfo_trial_id,base.trial_id) as trial_id,
        base.trial_title,
        base.trial_phase,
        base.trial_drug,
        base.trial_indication,
        base.score,base.nct_id
    from
    temp_xref_trial_final_0 base
    left outer join
    """ + final_table_1 + """ func_opt
    on base.trial_id = func_opt.cur_run_id
    """)

temp_xref_trial.write.mode('overwrite').saveAsTable('temp_xref_trial')
write_path = path.replace('table_name', 'temp_xref_trial')
temp_xref_trial.repartition(100).write.mode('overwrite').parquet(write_path)

temp_xref_trial_intd = spark.sql("""
select
    split(ds_trial_ids,'_')[0] AS data_src_nm,
        SUBSTRING(ds_trial_ids,(instr(ds_trial_ids, '_')+1)) as src_trial_id,
    trial_id,
    uid,
    hash_uid,
    trial_title,
    trial_phase,
    trial_drug,
    trial_indication,
    score
from temp_xref_trial
lateral view outer explode(split(ds_trial_id,'\\s*[;|]\\s*'))one as ds_trial_ids
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd = temp_xref_trial_intd.dropDuplicates()
temp_xref_trial_intd.write.mode('overwrite').saveAsTable('temp_xref_trial_intd')
write_path = path.replace('table_name', 'temp_xref_trial_intd')
temp_xref_trial_intd.repartition(100).write.mode('overwrite').parquet(write_path)

# temp_xref_trial_intd.registerTempTable("temp_xref_trial_intd")


temp_xref_trial_intd_temp = spark.sql(""" 
select 
b.data_src_nm,
b.src_trial_id,
a.trial_id,
a.uid,
a.hash_uid,
a.trial_title,
a.trial_phase,
a.trial_drug,
a.trial_indication,
a.score
from temp_xref_trial_intd a 
inner join 
(select 
src_trial_id,data_src_nm
from  temp_xref_trial_intd group by 1,2 having count(distinct uid)<2) b on lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id)) AND lower(trim(a.data_src_nm))=lower(trim(b.data_src_nm))
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd_temp = temp_xref_trial_intd_temp.dropDuplicates()
temp_xref_trial_intd_temp.write.mode('overwrite'). \
    saveAsTable('temp_xref_trial_intd_temp')
# temp_xref_trial_intd_temp.registerTempTable("temp_xref_trial_intd_temp")


# partitoning the records having count distinct uid 1
temp_xref_trial_intd_0 = spark.sql("""
select data_src_nm,
src_trial_id,
trial_id,
cast(uid as bigint) as uid,
hash_uid,
trial_title,
trial_phase,
trial_drug,
trial_indication,
score
from
temp_xref_trial_intd_temp
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd_0 = temp_xref_trial_intd_0.dropDuplicates()
# temp_xref_trial_intd_0.registerTempTable("temp_xref_trial_intd_0")
temp_xref_trial_intd_0.write.mode('overwrite'). \
    saveAsTable('temp_xref_trial_intd_0')

temp_xref_trial_intd_data_src_rnk = spark.sql("""
select temp_xref_trial_intd.*,
        case when lower(trim(data_src_nm)) like '%ctms%' then 1
        when lower(trim(data_src_nm)) like '%ir%' then 2
        when lower(trim(data_src_nm)) like '%citeline%' then 3
    when lower(trim(data_src_nm)) like '%aact%' then 5
    when lower(trim(data_src_nm)) like '%tascan%' then 4
else 6 end as datasource_rnk from  temp_xref_trial_intd""")
temp_xref_trial_intd_data_src_rnk.write.mode('overwrite'). \
    saveAsTable('temp_xref_trial_intd_data_src_rnk')

# temp_xref_trial_intd_data_src_rnk.registerTempTable("temp_xref_trial_intd_data_src_rnk")


# Giving rank to records having multiple distinct UID
temp_xref_trial_intd_1 = spark.sql("""
select data_src_nm,
src_trial_id,
trial_id,
uid,
hash_uid,
trial_title,
trial_phase,
trial_drug,
trial_indication,
score,
datasource_rnk,
ROW_NUMBER() over (partition by src_trial_id,data_src_nm order by datasource_rnk asc) as rnk
from
temp_xref_trial_intd_data_src_rnk where src_trial_id in 
(select src_trial_id from (select 
src_trial_id,data_src_nm
from  temp_xref_trial_intd_data_src_rnk group by 1,2 having count(distinct uid)>1))
group by 1,2,3,4,5,6,7,8,9,10,11
""")
temp_xref_trial_intd_1 = temp_xref_trial_intd_1.dropDuplicates()
temp_xref_trial_intd_1.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_1')
write_path = path.replace('table_name', 'temp_xref_trial_intd_1')
temp_xref_trial_intd_1.repartition(100).write.mode('overwrite').parquet(write_path)

# temp_xref_trial_intd_1.registerTempTable("temp_xref_trial_intd_1")


temp_xref_trial_intd_2 = spark.sql("""
select
data_src_nm,
src_trial_id,
trial_id,
cast(uid as bigint) as uid,
hash_uid,
trial_title,
trial_phase,
trial_drug,
trial_indication,
score
from
temp_xref_trial_intd_1  where rnk = 1
group by 1,2,3,4,5,6,7,8,9,10
""")
temp_xref_trial_intd_2 = temp_xref_trial_intd_2.dropDuplicates()
temp_xref_trial_intd_2.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_2')
write_path = path.replace('table_name', 'temp_xref_trial_intd_2')
temp_xref_trial_intd_2.repartition(100).write.mode('overwrite').parquet(write_path)

# temp_xref_trial_intd_2.registerTempTable("temp_xref_trial_intd_2")


# Union of records with unique UID and prec records having distinct multi. UID

temp_xref_trial_intd_3_temp = spark.sql("""
select
* from temp_xref_trial_intd_2
union 
select * from temp_xref_trial_intd_0
""")
temp_xref_trial_intd_3_temp = temp_xref_trial_intd_3_temp.dropDuplicates()
temp_xref_trial_intd_3_temp.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_3_temp')
write_path = path.replace('table_name', 'temp_xref_trial_intd_3')
temp_xref_trial_intd_3_temp.repartition(100).write.mode('overwrite').parquet(write_path)

# temp_xref_trial_intd_3.registerTempTable("temp_xref_trial_intd_3")

temp_xref_trial_intd_3 = spark.sql(
    """select * from (select *,row_number() over(partition by src_trial_id,data_src_nm order by trial_id desc) as rnk 
    from temp_xref_trial_intd_3_temp) where rnk = 1""".format(
    ))
temp_xref_trial_intd_3 = temp_xref_trial_intd_3.drop('rnk')
temp_xref_trial_intd_3.write.mode('overwrite').saveAsTable('temp_xref_trial_intd_3')
write_path = path.replace('table_name', 'temp_xref_trial_intd_3')
temp_xref_trial_intd_3.repartition(100).write.mode('overwrite').parquet(write_path)

# joining golden ids with trial data
xref_src_trial00 = spark.sql("""
    select
        base.data_src_nm,
        base.src_trial_id ,
        base.trial_id as ctfo_trial_id,
        concat_ws('\|',collect_set(src_data.nct_id)) as nct_id,
        trim(src_data.trial_title) as trial_title,
        B.standard_value as trial_phase,
        src_data.trial_drug,
        src_data.mesh_terms,
        src_data.therapeutic_area
    from   temp_xref_trial_intd_3 base
    left outer join temp_all_trial_data_final src_data
    on lower(trim(base.data_src_nm)) = lower(trim(src_data.data_src_nm)) and lower(trim(base.src_trial_id)) = lower(trim(src_data.src_trial_id))
    left outer join trial_phase_mapping B on trim(lower(src_data.trial_phase)) =
    trim(lower(B.trial_phase))

    group by 1,2,3,5,6,7,8,9
    """)
xref_src_trial00 = xref_src_trial00.dropDuplicates()
xref_src_trial00.write.mode('overwrite').saveAsTable('xref_src_trial00')

xref_src_trial = spark.sql("""
select data_src_nm, src_trial_id,
ctfo_trial_id,nct_id,trial_title,trial_phase,trial_drug,mesh_terms,therapeutic_area from
(select *, row_number() over(partition by data_src_nm,src_trial_id,ctfo_trial_id order by trial_title desc nulls last,therapeutic_area desc) as rnk
from xref_src_trial00) where rnk=1""")
xref_src_trial = xref_src_trial.dropDuplicates()
xref_src_trial.write.mode('overwrite').saveAsTable('xref_src_trial')

age_temp_min_value = spark.sql("""
select distinct
    base.data_src_nm,
    src_data.src_trial_id as src_trial_id,
    base.trial_id as ctfo_trial_id,
    case when lower(trim(src_data.trial_age_min)) LIKE "%na%" then 'na'
    when lower(trim(src_data.trial_age_min)) LIKE "%other%" then 'na'
    when src_data.trial_age_min is null then 'na'
    else cast(regexp_replace(src_data.trial_age_min,"[a-zA-Z()]",'') as double) end as trial_age_min,
    coalesce(age_std.value,1) as min_value
from temp_xref_trial_intd_3 base
left outer join temp_all_trial_data_final src_data
on lower(trim(base.data_src_nm)) = lower(trim(src_data.data_src_nm)) and lower(trim(base.src_trial_id)) = lower(trim(src_data.src_trial_id))
left outer join age_standardization age_std 
on regexp_replace(lower(trim(src_data.trial_age_min)),"[^a-zA-Z]",'') = lower(trim(age_std.age))
""")
age_temp_min_value.createOrReplaceTempView('age_temp_min_value')

age_temp_max_value = spark.sql("""
select distinct 
    base.data_src_nm,
    src_data.src_trial_id as src_trial_id,
    base.trial_id as ctfo_trial_id,
    case when lower(trim(src_data.trial_age_max)) LIKE "%na%" then 'na'
    when lower(trim(src_data.trial_age_max)) LIKE "%other%" then 'na'
    when src_data.trial_age_max is null then 'na'
    else cast(regexp_replace(src_data.trial_age_max,"[a-zA-Z()]",'') as double) end as trial_age_max,
    coalesce(age_std.value,1) as max_value
from temp_xref_trial_intd_3 base
left outer join temp_all_trial_data_final src_data
on lower(trim(base.data_src_nm)) = lower(trim(src_data.data_src_nm)) and lower(trim(base.src_trial_id)) = lower(trim(src_data.src_trial_id))
left outer join age_standardization age_std 
on regexp_replace(lower(trim(src_data.trial_age_max)),"[^a-zA-Z]",'') = lower(trim(age_std.age))
""")
age_temp_max_value.createOrReplaceTempView('age_temp_max_value')

standardized_age_temp = spark.sql("""
select distinct
    min.data_src_nm,
    min.src_trial_id,
    min.ctfo_trial_id,
    coalesce(case when min.trial_age_min = 'na' then 'na'
                  when (min.trial_age_min / min_value) > {max_age} then {max_age} 
                  when (min.trial_age_min / min_value)> {min_age} then (min.trial_age_min / min_value) 
                  else {min_age} end, {min_age}) trial_age_min,         
    coalesce(case when max.trial_age_max = 'na' then 'na'
                  when (max.trial_age_max / max_value) < {min_age} then {min_age} 
                  when (max.trial_age_max / max_value) < {max_age} then (max.trial_age_max / max_value) 
                  else {max_age} end, {max_age}) trial_age_max       
from age_temp_min_value min left outer join age_temp_max_value max on lower(trim(min.data_src_nm)) = lower(trim(max.data_src_nm)) and lower(trim(min.ctfo_trial_id)) = lower(trim(max.ctfo_trial_id)) and lower(trim(min.src_trial_id)) = lower(trim(max.src_trial_id))
""".format(max_age=max_age, min_age=min_age))
standardized_age_temp.createOrReplaceTempView('standardized_age_temp')

standardized_age = spark.sql("""
select distinct
    data_src_nm,
    ctfo_trial_id,
    cast(min(floor(trial_age_min)) as int) trial_age_min,
    cast(max(ceiling(trial_age_max)) as int) trial_age_max    
from standardized_age_temp
group by 1,2
""")
standardized_age.createOrReplaceTempView('standardized_age')

xref_src_trial_precedence_temp = spark.sql("""
select distinct
    base.data_src_nm,
    base.trial_id as ctfo_trial_id,
    min(src_data.trial_start_dt) trial_start_dt, max(src_data.trial_end_dt) trial_end_dt, max(src_data.results_date) results_date,
    max(src_data.primary_endpoints_reported_date) primary_endpoints_reported_date,max(src_data.enrollment_close_date) enrollment_close_date  
from temp_xref_trial_intd_3 base
left outer join temp_all_trial_data_final src_data
on lower(trim(base.data_src_nm)) = lower(trim(src_data.data_src_nm)) and lower(trim(base.src_trial_id)) = lower(trim(src_data.src_trial_id))
group by 1,2
""")

xref_src_trial_precedence_temp = xref_src_trial_precedence_temp.dropDuplicates()
# xref_src_trial_precedence_temp.write.mode('overwrite').saveAsTable('xref_src_trial_precedence_temp')
xref_src_trial_precedence_temp.registerTempTable("xref_src_trial_precedence_temp")

xref_src_trial_precedence_int_f1 = spark.sql("""
select
base.uid,base.hash_uid,
    base.data_src_nm,
    base.src_trial_id,
    base.trial_id as ctfo_trial_id,
    cast(base.score as double) as score,
    case when src_data.protocol_id in (null, '', 'NA', 'na') then null else
    src_data.protocol_id end as protocol_id,
    src_data.protocol_type,
    trim(src_data.trial_title) as trial_title,
    coalesce(B.standard_value, 'Other') as trial_phase,
    coalesce(C.status, 'Others') as trial_status,
    src_data.trial_type,
    trim(src_data.trial_design) as trial_design,
    D.trial_start_dt,
    src_data.trial_start_dt_type,
    D.trial_end_dt,
    src_data.trial_end_dt_type,
    case when lower(trim(std_age.trial_age_min)) is null then 0
    else std_age.trial_age_min end as trial_age_min, 
    case when lower(trim(std_age.trial_age_max)) is null then 100
    else std_age.trial_age_max end as trial_age_max,
    trim(coalesce(gender_std.std_gender, 'Other')) as patient_gender,
    src_data.patient_segment,
    INITCAP(src_data.mesh_terms) as mesh_terms,
    trim(D.results_date) as results_date,
    D.primary_endpoints_reported_date,
    D.enrollment_close_date,src_data.therapeutic_area
from temp_xref_trial_intd_3 base
left outer join temp_all_trial_data_final src_data
on lower(trim(base.data_src_nm)) = lower(trim(src_data.data_src_nm)) and lower(trim(base.src_trial_id)) = lower(trim(src_data.src_trial_id))
left outer join standardized_age std_age 
on lower(trim(base.data_src_nm)) = lower(trim(std_age.data_src_nm)) and lower(trim(base.trial_id)) = lower(trim(std_age.ctfo_trial_id))
left outer join gender_standardization gender_std
on regexp_replace(trim(src_data.patient_gender),"[^0-9A-Za-z_ -,']",'') =
regexp_replace(trim(gender_std.gender),"[^0-9A-Za-z_ -,']",'')
left outer join trial_phase_mapping B on trim(lower(src_data.trial_phase)) =
trim(lower(B.trial_phase))
left outer join (select distinct raw_status,status from trial_status_mapping) C 
on trim(lower(src_data.trial_status)) = trim(lower(C.raw_status))
left join xref_src_trial_precedence_temp D on trim(lower(base.data_src_nm)) = trim(lower(D.data_src_nm))
and trim(lower(base.trial_id)) = trim(lower(D.ctfo_trial_id))
""")
xref_src_trial_precedence_int_f1.registerTempTable('xref_src_trial_precedence_int_f1')

xref_src_trial_precedence_int_1 = spark.sql("""select uid,hash_uid,data_src_nm,src_trial_id,ctfo_trial_id,score,protocol_id,
protocol_type,trial_title,trial_phase,trial_status,trial_type,trial_design,trial_start_dt,trial_start_dt_type,trial_end_dt,
trial_end_dt_type,trial_age_min, trial_age_max,patient_gender,patient_segment,mesh_terms,results_date,
primary_endpoints_reported_date,enrollment_close_date
from
(select *, row_number() over(partition by data_src_nm,src_trial_id,ctfo_trial_id order by trial_title desc nulls last,
therapeutic_area desc) as rnk
from xref_src_trial_precedence_int_f1) where rnk=1""")
xref_src_trial_precedence_int_1.registerTempTable('xref_src_trial_precedence_int_1')


xref_src_trial_precedence_int_1 = xref_src_trial_precedence_int_1.dropDuplicates()
for col in xref_src_trial_precedence_int_1.columns:
    xref_src_trial_precedence_int_1 = xref_src_trial_precedence_int_1.withColumn(col, regexp_replace(col, ' ', '<>')) \
        .withColumn(col, regexp_replace(col, '><', '')) \
        .withColumn(col, regexp_replace(col, '<>', ' ')) \
        .withColumn(col, regexp_replace(col, '[|]+', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@ ', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@', '|')) \
        .withColumn(col, regexp_replace(col, '[|]+', '|')) \
        .withColumn(col, regexp_replace(col, '[|]$', '')) \
        .withColumn(col, regexp_replace(col, '^[|]', ''))

xref_src_trial_precedence_int=spark.sql("""
select cast(uid as bigint) as uid,hash_uid,data_src_nm,src_trial_id,ctfo_trial_id,cast(score as double) as score,protocol_id,
protocol_type,trial_title,trial_phase,trial_status,trial_type,trial_design,cast(trial_start_dt as date) as trial_start_dt,trial_start_dt_type,cast(trial_end_dt as date) as trial_end_dt,
trial_end_dt_type,trial_age_min, trial_age_max,patient_gender,patient_segment,mesh_terms,results_date,
primary_endpoints_reported_date,enrollment_close_date
from xref_src_trial_precedence_int_1
""")

xref_src_trial_precedence_int = xref_src_trial_precedence_int.dropDuplicates()
xref_src_trial_precedence_int.write.mode('overwrite').saveAsTable('xref_src_trial_precedence_int')
commons_path = bucket_path + '/applications/commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'
write_path = commons_path.replace('table_name', 'xref_src_trial_precedence_int')
xref_src_trial_precedence_int.repartition(100).write.mode('overwrite').parquet(write_path)

for col in xref_src_trial.columns:
    xref_src_trial = xref_src_trial.withColumn(col, regexp_replace(col, ' ', '<>')) \
        .withColumn(col, regexp_replace(col, '><', '')) \
        .withColumn(col, regexp_replace(col, '<>', ' ')) \
        .withColumn(col, regexp_replace(col, '[|]+', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@ ', '@#@')) \
        .withColumn(col, regexp_replace(col, '@#@', '|')) \
        .withColumn(col, regexp_replace(col, '[|]+', '|')) \
        .withColumn(col, regexp_replace(col, '[|]$', '')) \
        .withColumn(col, regexp_replace(col, '^[|]', ''))

xref_src_trial = xref_src_trial.dropDuplicates()
xref_src_trial.write.mode('overwrite').saveAsTable('xref_src_trial')

spark.sql("""
insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial partition (
        pt_data_dt='$$data_dt',
        pt_cycle_id='$$cycle_id'
        )
select *
from xref_src_trial
""")

CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial')