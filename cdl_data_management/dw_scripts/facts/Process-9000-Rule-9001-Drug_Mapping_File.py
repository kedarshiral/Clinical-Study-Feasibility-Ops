import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import os
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import initcap
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from NotificationUtility import NotificationUtility
import smtplib
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import traceback

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')

##Drugs from citeline_pharmaprojects
citeline_pharma = spark.sql("""
select distinct lower(trim(drugprimaryname)) as drugprimaryname, 
    lower(trim(synonyms)) as drugnamesynonyms
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_pharmaprojects
lateral view explode(split(drugnamesynonyms, '\\\|')) as synonyms
where lower(ispharmaprojectsdrug) ='true' and trialids !='' and drugprimaryname != ''
""")
citeline_pharma.registerTempTable("citeline_pharma")

one2one_mapping = spark.sql("""
select distinct drugprimaryname , drugprimaryname as drugnamesynonyms
from citeline_pharma where drugprimaryname in 
(select distinct drugprimaryname 
from citeline_pharma
where lower(trim(drugprimaryname)) != lower(trim(drugnamesynonyms)))
""")
one2one_mapping.registerTempTable("one2one_mapping")

citeline_final_mapping = spark.sql("""
select drugprimaryname, drugnamesynonyms
from citeline_pharma
union
select drugprimaryname, drugnamesynonyms
from one2one_mapping
""")
citeline_final_mapping.dropDuplicates()
#citeline_final_mapping.registerTempTable("citeline_final_mapping")
citeline_final_mapping.write.mode('overwrite').saveAsTable('citeline_final_mapping')

current_Drug_Delta = spark.read.format("com.crealytics.spark.excel").option("header", "true")\
    .load("{bucket_path}/uploads/DRUG/Drug_Delta/Drug_Delta.xlsx".format(bucket_path=bucket_path))
current_Drug_Delta.dropDuplicates()
current_Drug_Delta.createOrReplaceTempView('current_Drug_Delta')

#AACT,CTMS and CITELINE TRIALTROVE
aact_ctms_final_mapping = spark.sql("""
select distinct lower(trim(citeline_primary_drug_name)) as drugprimaryname, lower(trim(raw_drug_name)) as drugnamesynonyms 
from current_Drug_Delta
""")
aact_ctms_final_mapping.registerTempTable("aact_ctms_final_mapping")

drug_mapping_file = spark.read.format("com.crealytics.spark.excel").option("header", "true")\
    .load("{bucket_path}/uploads/DRUG/Drug_Mapping_File/Drug_Mapping.xlsx".format(bucket_path=bucket_path))
drug_mapping_file.dropDuplicates()
drug_mapping_file.write.mode('overwrite').saveAsTable('drug_mapping_file')

##To update a unique primary name for synonyms
master_drug_file = spark.read.format("com.crealytics.spark.excel").option("header", "true")\
    .load("{bucket_path}/uploads/DRUG/Master_File/Drug_Master_File.xlsx".format(bucket_path=bucket_path))
master_drug_file.dropDuplicates()
master_drug_file.createOrReplaceTempView('master_drug_file')

temp_final_mapping_all=spark.sql("""
select distinct coalesce(b.drugprimaryname, a.drugprimaryname) as drugprimaryname,  a.drugnamesynonyms
from
((select case when b.drugprimaryname = 'Other' 
            then coalesce(lower(trim(a.drugprimaryname)), lower(trim(b.drugprimaryname))) 
       else lower(trim(a.drugprimaryname)) end as drugprimaryname,
lower(trim(a.drugnamesynonyms)) as drugnamesynonyms
from citeline_final_mapping a left outer join drug_mapping_file b
on lower(trim(a.drugnamesynonyms)) = lower(trim(b.drugnamesynonyms)))
union
(select lower(trim(a.drugprimaryname)) as drugprimaryname, lower(trim(a.drugnamesynonyms)) as drugnamesynonyms
from drug_mapping_file a left outer join citeline_final_mapping b
on lower(trim(a.drugnamesynonyms)) = lower(trim(b.drugnamesynonyms))
where b.drugnamesynonyms is null)
union
(select lower(trim(drugprimaryname)) as drugprimaryname, lower(trim(drugnamesynonyms)) as drugnamesynonyms from aact_ctms_final_mapping)
) a left join master_drug_file b on lower(trim(a.drugnamesynonyms)) = lower(trim(b.drugnamesynonyms))
""")
temp_final_mapping_all = temp_final_mapping_all.dropDuplicates()
temp_final_mapping_all.registerTempTable('temp_final_mapping_all')

##To identify drug synonyms having multiple drug primary name
multiple_primary_drugs_email = spark.sql(""" 
select distinct drugprimaryname, drugnamesynonyms
from temp_final_mapping_all where drugnamesynonyms in 
(select distinct drugnamesynonyms from temp_final_mapping_all group by 1 having count(distinct drugprimaryname)>1)
order by 2
""")
multiple_primary_drugs_email = multiple_primary_drugs_email.dropDuplicates()
#multiple_primary_drugs_email.createOrReplaceTempView('multiple_primary_drugs_email')
multiple_primary_drugs_email.write.mode('overwrite').saveAsTable('multiple_primary_drugs_email')

final_mapping_all=spark.sql("""
select * from
((select distinct a.drugprimaryname, a.drugnamesynonyms
from temp_final_mapping_all a left join multiple_primary_drugs_email b
on lower(trim(a.drugnamesynonyms)) = lower(trim(b.drugnamesynonyms))
where b.drugnamesynonyms is null)
union
(select distinct 'Other' as drugprimaryname, drugnamesynonyms
from multiple_primary_drugs_email))
""")
final_mapping_all = final_mapping_all.dropDuplicates()
#final_mapping_all.registerTempTable('final_mapping_all')
final_mapping_all.write.mode('overwrite').saveAsTable('final_mapping_all')

##Sent email with data of multiple primary drug names
delta_env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "env"])
server_host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_server"])
server_port = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_port"])
email_recipient_list = configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_recipient_list"])
print("email_recipient_list",email_recipient_list)
sender_email = str(configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_sender"]))
print("sender_email",sender_email)
recipient_list = ", ".join(email_recipient_list)
print("recipient_list",recipient_list)
client_name = str(configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "client_name"]))
try:
    # Create a multipart message and set headers
    msg = MIMEMultipart('alternative')
    msg["From"] = sender_email
    msg["To"] = recipient_list
    msg["Subject"] = "Enviroment : {env} | [URGENT] Update Unique Drug Primary Name for Same Drug Name Synonyms".format(
        env=delta_env)
    body = """Hello Team ,\n\nBased on our Drug run, attached is the list of drug name synonyms which are having multiple primary drug names.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n{client_name} DE Team\nClinical $$db_envelopment Excellence""".format(client_name=client_name)
    msg.attach(MIMEText(body))
    attachment_name = "multiple_primary_drugs_email.csv"
    if (attachment_name):
        part = MIMEApplication(
            multiple_primary_drugs_email.toPandas().to_csv(escapechar="\\", doublequote=False, encoding='utf-8'),
            Name=attachment_name)
        content_dis = "multiple_primary_drugs_email; filename=" + attachment_name
        part['Content-Disposition'] = content_dis
        msg.attach(part)
    server = smtplib.SMTP(host=server_host, port=server_port)
    server.connect(server_host, server_port)
    server.starttls()
    server.send_message(msg)
    server.quit()
except Exception as E:
    print("Email Failed with error : ",E)

##Moving delta file in Archive folder with date
CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
s3_mapping_file_location = "s3://{}/clinical-data-lake/uploads/DRUG/Drug_Mapping_File/Drug_Mapping.xlsx".format(s3_bucket_name)

prev_mapping_file_location = "s3://{}/clinical-data-lake/uploads/DRUG/Drug_Mapping_File/Archive/".format(s3_bucket_name)

# creating backup of existing drug mapping file in archive folder
file_copy_command = "aws s3 cp {} {}".format(
    os.path.join(s3_mapping_file_location),
    os.path.join(prev_mapping_file_location, "Drug_Mapping_" +CURRENT_DATE+ ".xlsx"))

print("Command to create backup of file on s3 - {}".format(file_copy_command))

file_copy_status = os.system(file_copy_command)
if file_copy_status != 0:
    raise Exception("Failed to create backup of drug mapping file on s3")

file_delete_command = "aws s3 rm {} ".format(os.path.join(s3_mapping_file_location))
file_delete_status = os.system(file_delete_command)
if file_delete_status != 0:
   raise Exception("Failed to delete drug mapping file on s3")

## Mapping file to be updated with the Current Citeline runs' data
drug_mapping_file_write = spark.sql("""select * from default.final_mapping_all """)
drug_mapping_file_write = drug_mapping_file_write.dropDuplicates()
drug_mapping_file_write.registerTempTable('drug_mapping_file_write')

drug_mapping_code_path = CommonConstants.AIRFLOW_CODE_PATH + '/Drug_Mapping.xlsx'

if os.path.exists(drug_mapping_code_path):
    os.system('rm ' + drug_mapping_code_path)

# create new mapping file : union of new citeline data and delta from KM
drug_mapping_file_write.toPandas().to_excel("Drug_Mapping.xlsx", index=False)

#COPY new mapping file on s3
os.system("aws s3 cp Drug_Mapping.xlsx {bucket_path}/uploads/DRUG/Drug_Mapping_File/Drug_Mapping.xlsx".format(bucket_path=bucket_path))

##Moving delta file in Archive folder with date 
CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
s3_delta_file_location = "s3://{}/clinical-data-lake/uploads/DRUG/Drug_Delta/Drug_Delta.xlsx".format(s3_bucket_name)

prev_delta_file_location = "s3://{}/clinical-data-lake/uploads/DRUG/Drug_Delta/Archive/".format(s3_bucket_name)

# creating backup of existing drug delta file in archive folder
file_copy_command = "aws s3 cp {} {}".format(
    os.path.join(s3_delta_file_location),
    os.path.join(prev_delta_file_location, "Drug_Delta_" +CURRENT_DATE+ ".xlsx"))

print("Command to create backup of file on s3 - {}".format(file_copy_command))

file_copy_status = os.system(file_copy_command)
if file_copy_status != 0:
    raise Exception("Failed to create backup of drug delta file on s3")

#deleting the old delta file
file_delete_command = "aws s3 rm {} ".format(os.path.join(s3_delta_file_location))
file_delete_status = os.system(file_delete_command)
if file_delete_status != 0:
   raise Exception("Failed to delete drug delta file on s3")

##Creating an empty delta file
schema = StructType([ \
    StructField("raw_drug_name",StringType(),True), \
    StructField("citeline_drug_name_synonyms",StringType(),True), \
    StructField("citeline_primary_drug_name",StringType(),True), \
  ])
Drug_Delta = spark.createDataFrame(data=[],schema=schema)
Drug_Delta.registerTempTable('Drug_Delta')

#creating new delta file
Drug_Delta.toPandas().to_excel("Drug_Delta.xlsx", index=False)
os.system("aws s3 cp Drug_Delta.xlsx {bucket_path}/uploads/DRUG/Drug_Delta/Drug_Delta.xlsx".format(bucket_path=bucket_path))

