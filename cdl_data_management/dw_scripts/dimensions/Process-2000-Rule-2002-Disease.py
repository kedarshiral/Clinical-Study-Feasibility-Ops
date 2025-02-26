import json
import traceback
import sys
import os
import datetime
import time
from NotificationUtility import NotificationUtility
import smtplib
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import pyspark.sql.functions as f
import CommonConstants as CommonConstants
from PySparkUtility import PySparkUtility
from CommonUtils import CommonUtils
from pyspark.sql import *
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
import os
from pyspark.sql.types import *
import os
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = "{bucket_path}/applications/commons/dimensions/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id".format(
    bucket_path=bucket_path)

'''
# getting mapping of ta-disease for DQS
dqs_ind_ta = spark.read.format('csv').option("header", "true").option("delimiter", ",").load(
    "s3://aws-a0220-use1-00-$$s3_env-s3b-shrd-cus-cdl01/clinical-data-lake/uploads/dqs_ind_ta.csv")
dqs_ind_ta.registerTempTable('dqs_ind_ta')
'''

json_file = open(
    CommonConstants.EMR_CODE_PATH + '/configs/job_executor/precedence_params.json',
    'r').read()
config_data = json.loads(json_file)
print(config_data)
data_source_order_dict = config_data['data_source_ordering']
a_cMap = [(k,) + (v,) for k, v in data_source_order_dict.items()]
data_precedence = spark.createDataFrame(a_cMap, ['datasource', 'Precedence'])
data_precedence.write.mode('overwrite').saveAsTable("data_precedence")

'''
disease_mapping_file = spark.read.format('csv').option("header", "true").\
    option("delimiter", ",").\
    load("{bucket_path}/uploads/FINAL_DISEASE_MAPPING/disease_mapping.csv".format(bucket_path=bucket_path))
disease_mapping_file=disease_mapping_file.dropDuplicates()
disease_mapping_file.registerTempTable('disease_mapping_file')
'''
mesh_mapping_file = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_mapping """)
mesh_mapping_file = mesh_mapping_file.dropDuplicates()
mesh_mapping_file.registerTempTable('mesh_mapping_file')

delta_records = spark.read.format('csv').option("header", "true"). \
    option("delimiter", ","). \
    load("{bucket_path}/uploads/FINAL_DISEASE_MAPPING/delta.csv".format(bucket_path=bucket_path))
delta_records = delta_records.dropDuplicates()
delta_records.registerTempTable('delta_records')

# disease_mapping=spark.sql("""select
#  Synonyms,
#  datasource,
#  mesh,
#  similarity,
#  standard_mesh,
#  therapeutic_area,
#  trial_id,
#  unique_id from mesh_mapping_file
#  union
#  select
#  Synonyms,
#  datasource,
#  disease,
#  similarity,
#  standard_disease,
#  therapeutic_area,
#  trial_id,
#  unique_id from delta_records """)
# disease_mapping=disease_mapping.dropDuplicates()
# #disease_mapping.registerTempTable('disease_mapping')
# disease_mapping.write.mode('overwrite').saveAsTable("disease_mapping")


'''
ta_mapping = spark.read.format('csv').option("header", "true").\
    option("delimiter", ",").\
    load("{bucket_path}/uploads/TA_MAPPING/ta_mapping.csv".format(bucket_path=bucket_path))
#ta_mapping.registerTempTable('ta_mapping')
ta_mapping.write.mode('overwrite').saveAsTable('ta_mapping')
'''
ta_mapping = spark.sql(""" select * from $$client_name_ctfo_datastore_staging_$$db_env.mesh_to_ta_mapping """)
ta_mapping.registerTempTable('ta_mapping')
# ta_mapping.write.mode('overwrite').saveAsTable('ta_mapping')


# applying explode on TA for citeline

exp_1 = spark.sql("""SELECT   trial_id,
         Trim(ta)    AS therapeutic_area,
         Trim(ps)   AS patient_segment,
         case when mesh is null or trim(mesh) = '' then disease
else mesh end as trial_mesh_term_name 

FROM     $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove 
        lateral view outer posexplode(split(trial_therapeutic_areas_name,'\\\|'))one AS pos1,ta
        lateral view outer posexplode(split(trial_mesh_term_name,'\\\|'))two AS pos2,mesh
        lateral view outer posexplode(split(disease_name,'\\\|'))three AS pos3,disease
        lateral VIEW outer posexplode(split(patient_segment,'\\\|'))four AS pos4,ps
WHERE     pos1=pos2 and pos2=pos3 and pos3=pos4 and pos4=pos1
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
exp_1.registerTempTable('exp_1')
# exp_1.write.mode('overwrite').saveAsTable('exp_1')

exp_2 = spark.sql("""SELECT   trial_id,
         therapeutic_area,
         Trim(ps)   AS patient_segment,
         Trim(mesh) as trial_mesh_term_name

FROM     exp_1 
        lateral view outer posexplode(split(trial_mesh_term_name,'\\\^'))two AS pos1,mesh
        lateral VIEW outer posexplode(split(patient_segment,'\\\^'))three AS pos2,ps
WHERE     pos1=pos2
GROUP BY 1,2,3,4 ORDER BY therapeutic_area """)
exp_2.registerTempTable('exp_2')
# exp_2.write.mode('overwrite').saveAsTable('exp_2')

exp_3 = spark.sql("""SELECT   'citeline' as datasource, trial_id, therapeutic_area,
         Trim(ps)   AS patient_segment,
         Trim(mesh) as trial_mesh_term_name 

FROM     exp_2
        lateral view outer explode(split(trial_mesh_term_name,'\\\#'))one AS mesh
        lateral VIEW outer explode(split(patient_segment,'\\\#'))two AS ps
GROUP BY 1,2,3,4,5 ORDER BY therapeutic_area """)
exp_3.registerTempTable('exp_3')
# exp_3.write.mode('overwrite').saveAsTable('exp_3')


# creating  trial -TA -disease- patient segment mapping for citeline
citeline_pharma_temp = spark.sql("""
select distinct case when drugmeshterms_name is null or trim(drugmeshterms_name) = '' then indications_diseasename
else drugmeshterms_name end as disease_nm
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_pharmaprojects citeline_pharma
where lower(citeline_pharma.ispharmaprojectsdrug) ='true'
group by 1
""")
citeline_pharma_temp.registerTempTable('citeline_pharma_temp')

citeline_pharma_diseases = spark.sql("""
select trim(disease_name1)  as trial_mesh_term_name,CAST(NULL as string) as trial_id,CAST(NULL as string) as therapeutic_area from citeline_pharma_temp  lateral view outer
explode(split(regexp_replace(disease_nm,"\;","\\\|"),"\\\|"))one
as disease_name1
""")
citeline_pharma_diseases.registerTempTable('citeline_pharma_diseases')

citeline_data = spark.sql("""
select trial_id, trial_mesh_term_name as mesh_term,therapeutic_area
from
exp_3 group by 1,2,3
union
select trial_id, trial_mesh_term_name as mesh_term,therapeutic_area
from
citeline_pharma_diseases group by 1,2,3
""")
citeline_data.registerTempTable('citeline_data')
# citeline_data.write.mode('overwrite').saveAsTable('citeline_data')

# getting indication and ta for aact trials
df_aact = spark.sql("""
SELECT DISTINCT a.nct_id as trial_id,
                CAST(NULL as string) as therapeutic_area,
                case when mesh.mesh_terms is null or trim(mesh.mesh_terms)='' then trim(a.name)
    else mesh.mesh_terms end as mesh_term
                from $$client_name_ctfo_datastore_staging_$$db_env.aact_conditions a
left outer join
        (select distinct nct_id ,mesh_term as mesh_terms from 
        (select mesh_term ,nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_browse_conditions
        union 
     select mesh_term ,nct_id from $$client_name_ctfo_datastore_staging_$$db_env.aact_browse_interventions)
        ) mesh
    on lower(trim(a.nct_id))=lower(trim(mesh.nct_id))

""")
df_aact.registerTempTable("aact_data")
# df_aact.write.mode('overwrite').saveAsTable('aact_data')


df_ctms_1 = spark.sql("""
SELECT src_trial_id as trial_id,
trim(therapeutic_area) as therapeutic_area,
trim(disease_nm)  AS mesh_term
from $$client_name_ctfo_datastore_app_commons_$$db_env.usl_study
lateral VIEW posexplode (split(disease,'\\\|'))three AS pos3,disease_nm
group by 1,2,3
""")
df_ctms_1.registerTempTable("df_ctms_1")
# df_ctms_1.write.mode('overwrite').saveAsTable('df_ctms_1')

ctms_data = spark.sql("""
select * from df_ctms_1 where trim(mesh_term)<>''
""")
ctms_data.registerTempTable("ctms_data")
# ctms_data.write.mode('overwrite').saveAsTable('ctms_data')


# getting indication and ta for dqs trials
df_dqs_temp = spark.sql("""
SELECT member_study_id          AS trial_id,
       coalesce(mesh_heading,primary_indication) as mesh_heading,
       CAST(NULL as string) as therapeutic_area FROM   $$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir 

""")
df_dqs_temp.registerTempTable("df_dqs_temp")

dqs_data = spark.sql("""select
    trial_id,
               mesh_heading_explode2 as mesh_term,therapeutic_area
    from df_dqs_temp
lateral view outer explode(split(mesh_heading, ';'))one as mesh_heading_explode
lateral view outer explode(split(mesh_heading_explode, '\\\|'))one as mesh_heading_explode2
group by 1,2,3""").registerTempTable('dqs_data')
# df_dqs.write.mode('overwrite').saveAsTable('dqs_data')


df_data_monitor = spark.sql(""" SELECT CAST(NULL as string) as trial_id,
trim(regexp_replace(trim(lower(disease)),'[+]',' ')) as mesh_term,
CAST(NULL as string) AS therapeutic_area
from $$client_name_ctfo_datastore_staging_$$db_env.data_monitor
""")
df_data_monitor.registerTempTable("data_monitor_data")


df_tascan = spark.sql("""
SELECT DISTINCT a.trial_id trial_id,
                a.trial_therapeutic_area_name therapeutic_area,
                mesh_term_name AS mesh_term
                from $$client_name_ctfo_datastore_app_commons_$$db_env.tascan_trial a
                lateral view outer explode(split(mesh_term_names, ';'))one as mesh_term_name
                
""")
df_tascan.registerTempTable("tascan_data")

# UNION of disease data from all sources
df_union = spark.sql("""
SELECT 'citeline' AS datasource,
       trial_id,
       mesh_term,
       therapeutic_area
FROM   citeline_data
UNION
SELECT 'aact' AS datasource,
       trial_id,
       mesh_term,
       therapeutic_area
FROM   aact_data
UNION
SELECT 'ir' AS datasource,
       trial_id,
       mesh_term,
       therapeutic_area
FROM   dqs_data
UNION
SELECT 'ctms' AS datasource,
       trial_id,
       mesh_term,
       therapeutic_area
FROM   ctms_data
UNION
SELECT 'tascan' AS datasource,
       trial_id,
       mesh_term,
       therapeutic_area
FROM   tascan_data
""")
df_union.createOrReplaceTempView("union_trials")

df_final = spark.sql("select (row_number() over(order by null)) as unique_id, * from union_trials")
# df_final.registerTempTable('input_data')
df_final.write.mode('overwrite').saveAsTable('input_data')

'''
delta = spark.sql("""
select a.* from input_data a left join
(select trial_id,datasource,disease as diseasename,standard_disease from disease_mapping )   b on
lower(trim(a.disease_name)) =lower(trim(b.diseasename)) where
( b.standard_disease  is null )
""")

delta.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('sep', ',') \
    .save('/user/hive/warehouse/Delta')

dis_std = DiseaseStandardization()
dis_std.main()

disease_mapping = spark.read.format('csv').option("header", "true").\
option("delimiter", ",").\
load("s3://aws-a0220-use1-00-$$s3_env-s3b-shrd-cus-cdl01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/disease_mapping.csv")
#disease_mapping.registerTempTable('disease_mapping')
disease_mapping.write.mode('overwrite').saveAsTable("disease_mapping")


delta = spark.read.format('csv').option("header", "true").\
option("delimiter", ",").\
load("s3://aws-a0220-use1-00-$$s3_env-s3b-shrd-cus-cdl01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/delta.csv")
#delta.registerTempTable('delta')
delta.write.mode('overwrite').saveAsTable("delta")
'''

disease_mapping_final = spark.sql("""
select * from mesh_mapping_file
""")
disease_mapping_final.createOrReplaceTempView("disease_mapping_final")
write_path = path.replace('table_name', 'disease_mapping_final')
disease_mapping_final.repartition(100).write.mode('overwrite').parquet(write_path)

combined_mapping = spark.sql("""
select distinct * from (select 
mesh as input_disease, standard_mesh from mesh_mapping_file union
SELECT
standard_mesh as input_disease, standard_mesh 
FROM mesh_mapping_file)
""")
combined_mapping = combined_mapping.dropDuplicates()
combined_mapping.registerTempTable('combined_mapping')

combined_mapping_1 = spark.sql(""" select *, row_number() over (partition by lower(trim(input_disease)) order by null) as rnk
 from combined_mapping
""")
combined_mapping_1.registerTempTable('combined_mapping_1')

combined_mapping_2 = spark.sql(""" select * from combined_mapping_1 where rnk =1
""")
combined_mapping_2.registerTempTable('combined_mapping_2')

delta_diseases = spark.sql("""
select a.* from input_data a left outer join combined_mapping_2 b on
lower(trim(a.mesh_term)) =lower(trim(b.input_disease)) where
(b.input_disease  is null)
""")
delta_diseases = delta_diseases.dropDuplicates()
# delta_diseases.registerTempTable('delta_diseases')
delta_diseases.write.mode('overwrite').saveAsTable("delta_diseases")
delta_diseases.repartition(1).write.mode("overwrite").format("csv").option("header", "true"). \
    option("delimiter", ",").option("quoteAll", "true"). \
    save("{bucket_path}/uploads/DELTA_DISEASES/".format(bucket_path=bucket_path))

# notif_obj = NotificationUtility()
# try:
#    output = notif_obj.send_notification(email_ses_region, email_subject, email_sender,
#                                         email_recipient_list, email_template=email_template_path)
#    print("Output of Email Notification Trigger: " + str(output))
# except Exception as E:
#    print("failed")

delta_env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "env"])
server_host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_server"])
server_port = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_port"])
email_recipient_list = configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_recipient_list"])
print(email_recipient_list)
sender_email = str(configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_sender"]))
recipient_list = ", ".join(email_recipient_list)
try:
    # Create a multipart message and set headers
    msg = MIMEMultipart('alternative')
    msg["From"] = sender_email
    msg["To"] = recipient_list
    msg["Subject"] = "Enviroment : {env} | [URGENT] Update Delta Records In Disease Mapping | Post Dedupe ".format(
        env=delta_env)
    body = "Hello Team ,\n\nBased on our post dedupe run, attached is the list of diseases which are still missing in the mapping file.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n$$client_name DE Team\nClinical Development Excellence"
    msg.attach(MIMEText(body))
    attachment_name = "Delta_disease_Post_Dedupe.csv"
    if (attachment_name):
        part = MIMEApplication(delta_diseases.toPandas().to_csv(escapechar="\\", doublequote=False, encoding='utf-8'),
                               Name=attachment_name)
        content_dis = "delta_diseases; filename=" + attachment_name
        part['Content-Disposition'] = content_dis
        msg.attach(part)
    server = smtplib.SMTP(host=server_host, port=server_port)
    server.connect(server_host, server_port)
    server.starttls()
    server.send_message(msg)
    server.quit()
except Exception as E:
    print("Failed")

final_table = spark.sql("""
select a.trial_id,  a.datasource,a.mesh_term as disease_name,
case when trim(a.mesh_term) in (' ','','null','NA', 'na') then 'Other'
    else lower(trim(coalesce(b.standard_mesh,'Unassigned'))) end as standard_disease,
 b.standard_mesh as sd from
input_data a
left outer join
(select standard_mesh,input_disease  from combined_mapping_2 )
b
on lower(trim(a.mesh_term)) =lower(trim(b.input_disease))
""")
final_table.registerTempTable('final_table')
# final_table.write.mode('overwrite').saveAsTable('final_table')

final_table_1 = spark.sql("""select *,
row_number() over (partition by lower(trim(standard_disease)),trial_id,datasource order by disease_name desc) as rnk from final_table """)
# final_table_1.registerTempTable('final_table_1')
final_table_1.write.mode('overwrite').saveAsTable('final_table_1')


final_table_2 = spark.sql("""select * from final_table_1 where rnk=1""")
# final_table_1.registerTempTable('final_table_1')
final_table_2.write.mode('overwrite').saveAsTable('final_table_2')

ta_mapped_1 = spark.sql("""select a.trial_id,a.datasource, a.standard_disease as disease,
ps.patient_segment as patient_segment
from
final_table_2 a
left outer join (select therapeutic_area,trial_mesh_term_name,patient_segment from exp_3 where therapeutic_area is not null) ps
on lower(trim(ps.trial_mesh_term_name)) = lower(trim(a.disease_name))

group by 1,2,3,4 """)
# ta_mapped.registerTempTable('ta_mapped')
ta_mapped_1.write.mode('overwrite').saveAsTable('ta_mapped_1')

ta_mapped = spark.sql("""select a.trial_id,a.datasource, a.disease,
patient_segment,
max(coalesce(b.therapeutic_area,'Other')) as therapeutic_area
from
ta_mapped_1 a
left outer join ta_mapping b on
lower(trim(a.disease))=lower(trim(b.mesh_name)) group by 1,2,3,4
 """)
# ta_mapped.registerTempTable('ta_mapped')
ta_mapped.write.mode('overwrite').saveAsTable('ta_mapped')


temp_d_disease = spark.sql("""
select distinct ctfo_trial_id,
(case when disease in ('','-','?','Not Applicable', 'Other', 'other', 'NA', 'N/A', 'na', 'n/a')
    then 'Other' else coalesce(disease,'Other') end) as disease_nm,
(case when patient_segment in
('','-','?','Not Applicable', 'Other', 'other', 'NA', 'N/A', 'na', 'n/a')
    then 'Other' else coalesce(patient_segment,'Other') end) as patient_segment,
(case when a.therapeutic_area is null or a.therapeutic_area in
('','-','?','Not Applicable', 'Other', 'other', 'NA', 'N/A', 'na', 'n/a','Unassigned')
    then 'Other' else trim(coalesce(a.therapeutic_area,'Other')) end) as therapeutic_area
from
default.ta_mapped a
inner join  $$client_name_ctfo_datastore_app_commons_$$db_env.xref_src_trial b
on lower(trim(A.trial_id)) = lower(trim(B.src_trial_id)) and lower(trim(A.datasource)) = lower(trim(B.data_src_nm))
group by 1,2,3,4
""")
temp_d_disease.registerTempTable('temp_d_disease')

temp_d_disease.write.mode('overwrite').saveAsTable('temp_d_disease')
write_path = path.replace('table_name', 'temp_d_disease')
temp_d_disease.repartition(100).write.mode('overwrite').parquet(write_path)

temp_d_disease_temp = spark.sql("""
select distinct disease_nm, concat_ws('||',collect_set(patient_segment)) as patient_segment from temp_d_disease group by 1
""")
temp_d_disease_temp.registerTempTable('temp_d_disease_temp')

# creating dimension disease
d_disease = spark.sql("""
select
    rank() over(order by disease_nm) as disease_id,
    initcap(disease_nm) as disease_nm,
    patient_segment,
    initcap(therapeutic_area) as therapeutic_area
from (select lower(trim(a.disease_nm)) as disease_nm, lower(trim(therapeutic_area)) as
therapeutic_area,lower(trim(b.patient_segment)) as patient_segment from temp_d_disease a left join temp_d_disease_temp b on lower(trim(a.disease_nm))=lower(trim(b.disease_nm))
group by 1,2,3)
""")
d_disease.createOrReplaceTempView('d_disease')
d_disease.write.mode('overwrite').saveAsTable('d_disease')

# Insert data in hdfs table
spark.sql("""insert overwrite table $$client_name_ctfo_datastore_app_commons_$$db_env.d_disease
partition(
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id')
select *, 'ctfo' as project_flag from d_disease
""")

# Closing spark context
try:
    print('Closing spark context')
    spark.stop()
except:
    print('Error while closing spark context')

# Insert data on S3
CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.d_disease')
