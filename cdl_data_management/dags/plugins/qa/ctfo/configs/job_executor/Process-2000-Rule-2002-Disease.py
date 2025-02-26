################################# Module Information ######################################
#  Module Name         : D_DISEASE
#  Purpose             : This will create target table d_disease
#  Pre-requisites      : L1 source table required: NA
#  Last changed on     : 20-1-2021
#  Last changed by     : Rakesh D
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Fetch all relevant information from L1 layer table
# 2. Pass through the L1 table on key columns to create final target table
############################################################################################
from DiseaseStandardization import DiseaseStandardization
from NotificationUtility import NotificationUtility

email_ses_region = 'us-east-1'
email_subject = 'Unmapped Standardized Data'
email_sender = 'atul.manchanda@zs.com'
email_template_path = 'disease_templ.html'
email_recipient_list = ["deeksha.deeksha@zs.com"]

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/dimensions/table_name/' \
       'pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

'''
# getting mapping of ta-disease for DQS
dqs_ind_ta = spark.read.format('csv').option("header", "true").option("delimiter", ",").load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/dqs_ind_ta.csv")
dqs_ind_ta.registerTempTable('dqs_ind_ta')
'''

disease_mapping = spark.read.format('csv').option("header", "true").\
    option("delimiter", ",").\
    load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/")
# disease_mapping.registerTempTable('disease_mapping')
disease_mapping.write.mode('overwrite').saveAsTable("disease_mapping")





ta_mapping = spark.read.format('csv').option("header", "true").\
    option("delimiter", ",").\
    load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/TA_MAPPING/"
         "ta_mapping.csv")
#ta_mapping.registerTempTable('ta_mapping')
ta_mapping.write.mode('overwrite').saveAsTable('ta_mapping')


# applying explode on TA for citeline

exp_1 = spark.sql("""SELECT   trial_id,
         Trim(ta)    AS therapeutic_area,
         Trim(dis)    AS disease_name,
         Trim(ps)   AS patient_segment
FROM     sanofi_ctfo_datastore_staging_$$db_env.citeline_trialtrove lateral view
posexplode(split(trial_therapeutic_areas_name,'\\\|'))one AS pos1,
         ta lateral VIEW posexplode (split(disease_name,'\\\|'))two    AS pos2,
         dis lateral VIEW posexplode (split(patient_segment,'\\\|'))three AS pos3,
         ps
WHERE    pos1=pos2 AND pos1=pos3 AND pos2=pos3 and dis <> ''

GROUP BY 1,
         2,
         3,
         4
ORDER BY therapeutic_area
""")
exp_1.registerTempTable('exp_1')

# applying explode on disease for citeline
exp_2 = spark.sql("""SELECT   'citeline' as datasource,trial_id,
         therapeutic_area,
         Trim(dis)    AS disease_name,
         Trim(ps)      AS patient_segment
FROM     exp_1
         lateral view posexplode (split(disease_name,'\\\^'))two    AS pos2,
         dis lateral VIEW posexplode (split(patient_segment,'\\\^'))three AS pos3,
         ps
WHERE    pos2=pos3

GROUP BY 1,
         2,
         3,
         4,5
""")
#exp_2.registerTempTable('exp_2')
exp_2.write.mode('overwrite').saveAsTable('exp_2')



# creating  trial -TA -disease- patient segment mapping for citeline
citeline_data = spark.sql("""
select trial_id, disease_name ,therapeutic_area
from
exp_2 group by 1,2,3
""")
citeline_data.registerTempTable('citeline_data')
#citeline_data.write.mode('overwrite').saveAsTable('citeline_data')


# getting indication and ta for aact trials
df_aact = spark.sql("""
SELECT DISTINCT nct_id as trial_id,
                NULL as therapeutic_area,
                regexp_replace(name,'\\\"','\\\'')  AS disease_name
                from sanofi_ctfo_datastore_staging_$$db_env.aact_conditions
""")
df_aact.registerTempTable("aact_data")
#df_aact.write.mode('overwrite').saveAsTable('aact_data')


df_ctms = spark.sql("""
SELECT DISTINCT a.study_code as trial_id,
                b.therapeutic_area as therapeutic_area,
                a.indication AS disease_name
                from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_indication as a
                left join sanofi_ctfo_datastore_staging_$$db_env.ctms_study as b
                on lower(trim(a.study_code)) = lower(trim(b.study_code))
""")
df_ctms.registerTempTable("ctms_data")
#df_ctms.write.mode('overwrite').saveAsTable('ctms_data')


# getting indication and ta for dqs trials
df_dqs = spark.sql("""
SELECT member_study_id          AS trial_id,
       disease          AS disease_name,
       '' as therapeutic_area
FROM   sanofi_ctfo_datastore_staging_$$db_env.dqs_study
       lateral VIEW posexplode (split(primary_indication,'\\\;'))three AS pos3,disease


""")
df_dqs.registerTempTable("dqs_data")
#df_dqs.write.mode('overwrite').saveAsTable('dqs_data')


# getting indication and ta for ihme trials
df_ihme = spark.sql("""
SELECT DISTINCT NULL  trial_id,
                NULL therapeutic_area,
                a.cause AS disease_name
                from sanofi_ctfo_datastore_staging_$$db_env.ihme_gbd as a
""")
df_ihme.registerTempTable("ihme_data")
#df_ihme.write.mode('overwrite').saveAsTable('ihme_data')


# UNION of disease data from all sources
df_union = spark.sql("""
SELECT 'citeline' AS datasource,
       trial_id,
       disease_name,
       therapeutic_area
FROM   citeline_data
UNION
SELECT 'aact' AS datasource,
       trial_id,
       disease_name,
       therapeutic_area
FROM   aact_data
UNION
SELECT 'ir' AS datasource,
       trial_id,
       disease_name,
       therapeutic_area
FROM   dqs_data
UNION
SELECT 'ctms' AS datasource,
       trial_id,
       disease_name,
       therapeutic_area
FROM   ctms_data
UNION
SELECT 'ihme' AS datasource,
       trial_id,
       disease_name,
       therapeutic_area
FROM   ihme_data
""")
df_union.createOrReplaceTempView("union_trials")

df_final = spark.sql("select (row_number() over(order by null)) as unique_id, * from union_trials")
#df_final.registerTempTable('input_data')
df_final.write.mode('overwrite').saveAsTable('input_data')

delta = spark.sql("""
select a.* from input_data a left join
(select trial_id,datasource,disease as diseasename from disease_mapping )   b on
lower(trim(a.disease_name)) =lower(trim(b.diseasename)) where
( b.diseasename  is null )
""")

delta.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('sep', ',') \
    .save('/user/hive/warehouse/Delta')

dis_std = DiseaseStandardization()
dis_std.main()

disease_mapping = spark.read.format('csv').option("header", "true").\
    option("delimiter", ",").\
    load("s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/FINAL_DISEASE_MAPPING/")
# disease_mapping.registerTempTable('disease_mapping')
disease_mapping.write.mode('overwrite').saveAsTable("disease_mapping")



disease_mapping_final = spark.sql("""
select * from disease_mapping
""")
disease_mapping_final.createOrReplaceTempView("disease_mapping_final")
#Saving disease mapping on s3 to get the delta disease.                 
write_path = path.replace('table_name', 'disease_mapping_final')
disease_mapping_final.repartition(100).write.mode('overwrite').parquet(write_path)


combined_mapping = spark.sql("""
select disease as input_disease, datasource, trial_id, therapeutic_area, standard_disease, Synonyms  from disease_mapping""")
combined_mapping.dropDuplicates()
combined_mapping.registerTempTable('combined_mapping')

combined_mapping_0 = spark.sql("""
select *,
case when datasource = 'ctms' then '4'
            when datasource = 'citeline' then '3'
            when datasource = 'ir' then '2'
            when datasource = 'aact' then '1' end as prec
            from combined_mapping
""")
combined_mapping_0.registerTempTable('combined_mapping_0')


combined_mapping_1 = spark.sql(""" select *, row_number() over (partition by lower(trim(input_disease)) order by prec desc) as rnk from combined_mapping_0
""")
combined_mapping_1.registerTempTable('combined_mapping_1')

combined_mapping_2 = spark.sql(""" select * from combined_mapping_1 where rnk =1
""")
combined_mapping_2.registerTempTable('combined_mapping_2')


delta_diseases = spark.sql("""
select a.* from input_data a left join combined_mapping_2 b on
lower(trim(a.disease_name)) =lower(trim(b.input_disease)) where
(b.input_disease  is null)
""")

delta_diseases.repartition(1).write.mode("overwrite").format("csv").option("header", "true").\
    option("delimiter", ",").option("quoteAll", "true").\
    save('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/uploads/DELTA_DISEASES/')

notif_obj = NotificationUtility()
try:
    output = notif_obj.send_notification(email_ses_region, email_subject, email_sender,
                                         email_recipient_list, email_template=email_template_path)
    print("Output of Email Notification Trigger: " + str(output))
except Exception as E:
    print("failed")



final_table = spark.sql("""
select a.trial_id,  a.datasource,a.disease_name,coalesce(b.standard_disease,a.disease_name) as standard_disease, b.standard_disease as sd from
input_data a
left join
(select datasource,standard_disease,input_disease  from combined_mapping_2 )
b
on lower(trim(a.disease_name)) =lower(trim(b.input_disease))
""")
#final_table.registerTempTable('final_table')
final_table.write.mode('overwrite').saveAsTable('final_table')

final_table_1 = spark.sql("""select *,
row_number() over (partition by lower(trim(standard_disease)),trial_id,datasource order by disease_name desc) as rnk from final_table """)
#final_table_1.registerTempTable('final_table_1')
final_table_1.write.mode('overwrite').saveAsTable('final_table_1')



ta_mapped = spark.sql("""select a.trial_id,a.datasource, a.standard_disease as disease,
max(coalesce(b.therapeutic_area,ps.therapeutic_area)) as therapeutic_area,
max(ps.patient_segment) as patient_segment
from
default.final_table_1 a
left join (select therapeutic_area,disease_name,patient_segment from default.exp_2 where therapeutic_area is not null) ps
on lower(trim(ps.disease_name)) = lower(trim(a.disease_name))
left join default.ta_mapping b on
lower(trim(a.standard_disease))=lower(trim(b.disease_name))
where a.rnk=1
group by 1,2,3 """)
#ta_mapped.registerTempTable('ta_mapped')
ta_mapped.write.mode('overwrite').saveAsTable('ta_mapped')



temp_d_disease = spark.sql("""
select ctfo_trial_id,
(case when disease in ('','-','?','Not Applicable', 'Other', 'other', 'NA', 'N/A', 'na', 'n/a')
    then 'Other' else coalesce(disease,'Other') end) as disease_nm,
(case when patient_segment in
('','-','?','Not Applicable', 'Other', 'other', 'NA', 'N/A', 'na', 'n/a')
    then 'Other' else coalesce(patient_segment,'Other') end) as patient_segment,
(case when therapeutic_area is null or therapeutic_area in
('','-','?','Not Applicable', 'Other', 'other', 'NA', 'N/A', 'na', 'n/a','Unassigned')
    then 'Other' else trim(coalesce(therapeutic_area,'Other')) end) as therapeutic_area
from
default.ta_mapped a
inner join  sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial b
on A.trial_id = B.src_trial_id and A.datasource = B.data_src_nm
group by 1,2,3,4
""")
temp_d_disease.registerTempTable('temp_d_disease')

temp_d_disease.write.mode('overwrite').saveAsTable('temp_d_disease')
write_path = path.replace('table_name', 'temp_d_disease')
temp_d_disease.repartition(100).write.mode('overwrite').parquet(write_path)




# creating dimension disease
d_disease = spark.sql("""
select
    rank() over(order by disease_nm,therapeutic_area) as disease_id,
    disease_nm as disease_nm,
    patient_segment,
    therapeutic_area as therapeutic_area
from (select lower(trim(disease_nm)) as disease_nm, lower(trim(therapeutic_area)) as
therapeutic_area,lower(trim(patient_segment)) as patient_segment from temp_d_disease group by 1,2,3)
""")
d_disease.createOrReplaceTempView('d_disease')
d_disease.write.mode('overwrite').saveAsTable('d_disease')




# Insert data in hdfs table
spark.sql("""insert overwrite table sanofi_ctfo_datastore_app_commons_$$db_env.d_disease
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
CommonUtils().copy_hdfs_to_s3('sanofi_ctfo_datastore_app_commons_$$db_env.d_disease')



