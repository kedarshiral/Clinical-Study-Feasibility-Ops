import re
import pandas as pd
import pyspark.sql.functions as f
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, when
import os
import sys
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from NotificationUtility import NotificationUtility
import smtplib
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import traceback

sys.path.insert(0, os.getcwd())

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
s3_bucket_name = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "s3_bucket_name"])

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set('mapreduce.fileoutputcommitter.algorithm.version', '2')
spark.conf.set('spark.sql.crossJoin.enabled', 'True')


# Some sponsor names contains | in them. Identify such sponsors and replace | with ^
# eg. In NCT02295176: The sponsor name "Viatris {Mylan/Meda {Rottapharm|Madaus/Rottapharm}}| Xytis" contains "Rottapharm|Madaus" seperated by |
# for above case this function finds such cases and replace | in value (ie. {Rottapharm|Madaus/Rottapharm})with ^
# Expected Output: "Viatris {Mylan/Meda {Rottapharm^Madaus/Rottapharm}}| Xytis"
def regex_handle_pipe(x):
    result = re.search("({[a-zA-Z\/\-\s]*\|[a-zA-Z\/\-\s]*})", x)
    if result:
        after_match = result.group()
        after_replace = re.sub("\|", "^", after_match)
        final = x.replace(after_match, after_replace)
        return final
    else:
        return x


# if sponsor names contain following punctutation: /, {, ^, |
# we get the list and replace following punctions /, |, {, } by ^. eg. Viatris {Mylan/Meda {Rottapharm^Madaus/Rottapharm}}
# output: Viatris^Mylan^Meda^Rottapharm^Madaus^Rottapharm^
# Save first names as parent sponsor(parent_sponsor) and give remaning list as input to get_sponsor_list()
# We Append parent sponsor name to the subsidiary sponsor list and then return it by joining them on ^
# Note: if any sponsor name is in bracket (), we will not split it and return as it is. eg. (Other Hospital/Academic/Medical Center)
def get_parent_subsidiary_sponsor(x):
    result = re.search(".*\/.*|.*\{.*|.*\^.*|.*\|.*", x)
    bracket_case = re.search("^\([A-Za-z\.\,].*\)$", x)
    if bracket_case:
        return x
    elif result:
        sponsor_combined = result.group()
        sponsor_split = re.sub("\s*\/\s*|\s*\|\s*|\s*\{\s*|\s*}+\s*", "^", sponsor_combined)
        sp_list = sponsor_split.split("^")
        sp_list = clean_sponsor_names(sp_list)
        parent_sponsor = sp_list[0]
        sp_list = "^".join(get_sponsor_list(sp_list[1:]))
        final_list = parent_sponsor.title() + "^" + sp_list.title()

        return final_list.strip("^")
    else:
        return x.title()


# Here we get subsidiary sponsor list as input
# We remove duplicates, empty values, top 20 pharma names and then join by ^
# eg.1 input: Viatris^Mylan^Meda^Rottapharm^Meda^Madaus^Rottapharm^ | output: Viatris^Mylan^Meda^Rottapharm^Madaus
def get_sponsor_list(input_string):
    sponsor_list = []
    [sponsor_list.append(item) for item in input_string if item not in sponsor_list and item != '']
    return sponsor_list


## Revamp the sponsor list
# 1. replace "astellas pharma" by "astellas"
# 2. replace "j&j" by "johnson & johnson"
# 3. replace "merck" by "merck & co."
def clean_sponsor_names(subsidiary_list):
    for i in range(len(subsidiary_list)):
        if re.search("astellas pharma", subsidiary_list[i], re.IGNORECASE):
            pattern = re.compile("astellas pharma", re.IGNORECASE)
            subsidiary_list[i] = re.sub(pattern, "Astellas", subsidiary_list[i], re.IGNORECASE)

        if re.search("j&j", subsidiary_list[i], re.IGNORECASE):
            pattern = re.compile("j&j", re.IGNORECASE)
            subsidiary_list[i] = re.sub(pattern, "Johnson & Johnson", subsidiary_list[i], re.IGNORECASE)

        if subsidiary_list[i].lower() == "merck":
            subsidiary_list[i] = "Merck & Co."

    return subsidiary_list


# we get deduped and clean subsidiary sponsor list subsidiary_list as input, which is used in iteration
# Here, create a copy of list as final_list, from which we will remove the unwanted elements
# We will remove the element if:
# i. If the list contains top 20 pharma company name as substring (eg. Pfizer Pharma Ltd.)
#    and does not contain "joint venture" in it, then we will remove that element from the final_list.
# ii.We will sort this list (alphabetically), join the list of ^ and return this final_list
def get_subsidiary_sponsor_list(input_string):
    subsidiary_list = input_string.split("^")
    # create copy of the list, one of iteration and one of making changes
    final_list = subsidiary_list.copy()

    # iterating subsidiary list
    for item in subsidiary_list:
        # iterating top pharma names list and checking in each item
        for sp_name in top_pharma:
            # check if the name does not contain: joint venture| check if the item is in final list
            if sp_name.lower() in item.lower() and "joint venture" not in item.lower():
                if item in final_list:
                    final_list.remove(item)

    final_list = [item.title() for item in final_list]

    final_list.sort()
    sub_sponsor_list = "^".join(final_list)
    return sub_sponsor_list.strip("^")


# UDF
# Using the functions we create UDF(User Defined Functions) to execute in pyspark
handle_pipe_UDF = udf(lambda x: regex_handle_pipe(x))
get_parent_subsidiary_UDF = udf(lambda x: get_parent_subsidiary_sponsor(x))
get_subsidiary_sponsor_UDF = udf(lambda x: get_subsidiary_sponsor_list(x))

splitUDF = udf(lambda x: x.split("^")[0] if "^" in x else x.title())
concatUDF = udf(lambda x: "^".join(x.split("^")[1:]) if "^" in x else x.title())

# Get the complete data set from citeline_trialtrove
# Here in below query, we replace Astellas Pharma by Astellas to maintain sponsor name consistency
# And replace double quotes (") by empty '', new line (\n) by space ' '
citeline_initial_sponsor_data = spark.sql("""
select distinct 
       regexp_replace(trim(sponsor_name), '\\\"', '') as sponsor_name,
       sponsor_type
from $$client_name_ctfo_datastore_staging_$$db_env.citeline_trialtrove
where sponsor_name is not null or sponsor_name != ''
""")
citeline_initial_sponsor_data.createOrReplaceTempView('citeline_initial_sponsor_data')

# We handle the special pipe cases using handle_pipe_UDF here and save df as citeline_sponsor_data_df
citeline_sponsor_data_df = citeline_initial_sponsor_data.withColumn("sponsor_name", handle_pipe_UDF("sponsor_name"))
citeline_sponsor_data_df.createOrReplaceTempView('citeline_sponsor_data_df')

# The citeline_sponsor_exploded df sponsor name and type are exploded on pipe |
citeline_sponsor_exploded = spark.sql("""
select trim(sn) as sponsor_name, trim(st) as sponsor_type
from citeline_sponsor_data_df 
lateral view posexplode(split(sponsor_name, '\\\|')) one AS pos1, sn 
lateral view posexplode(split(sponsor_type, '\\\|')) two AS pos2, st 
WHERE pos1 = pos2 """)
citeline_sponsor_exploded.createOrReplaceTempView('citeline_sponsor_exploded')

citeline_sponsor_case_wise_sorted = spark.sql("""
select distinct case when rlike(trim(lower(sponsor_type)), 'industry') then "case1"
                     when rlike(trim(lower(sponsor_type)), 'academic') then "case1"
                     when rlike(trim(lower(sponsor_name)), 'university') then "case1"
                     when rlike(trim(lower(sponsor_name)), 'universidad') then "case1"
                else "case2" end as case_type,
        regexp_replace(lower(trim(sponsor_name)), 'astellas pharma', 'astellas') as sponsor_name, sponsor_type
from citeline_sponsor_exploded
""")
citeline_sponsor_case_wise_sorted = citeline_sponsor_case_wise_sorted.dropDuplicates()
citeline_sponsor_case_wise_sorted.createOrReplaceTempView('citeline_sponsor_case_wise_sorted')

# Here we get the parent_child_sponsor_name, parent_sponsor and subsidiary_sponsor
citeline_sponsor_temp = citeline_sponsor_case_wise_sorted \
    .withColumn("raw_sponsor_name", when(citeline_sponsor_case_wise_sorted.case_type == "case2", "Other").otherwise(
    citeline_sponsor_case_wise_sorted.sponsor_name)) \
    .withColumn("parent_child_sponsor_name",
                when(citeline_sponsor_case_wise_sorted.case_type == "case2", "Other").otherwise(
                    get_parent_subsidiary_UDF("sponsor_name"))) \
    .withColumn("parent_sponsor", when(citeline_sponsor_case_wise_sorted.case_type == "case2", "Other").otherwise(
    splitUDF("parent_child_sponsor_name"))) \
    .withColumn("subsidiary_sponsor", when(citeline_sponsor_case_wise_sorted.case_type == "case2", "Other").otherwise(
    concatUDF("parent_child_sponsor_name"))) \
    .withColumn("sponsor_type", when(citeline_sponsor_case_wise_sorted.case_type == "case2", "Other").otherwise(
    citeline_sponsor_case_wise_sorted.sponsor_type))
citeline_sponsor_temp = citeline_sponsor_temp.dropDuplicates()
citeline_sponsor_temp.createOrReplaceTempView('citeline_sponsor_temp')

# Get top 20 pharma sponsor names here
top20_pharma = spark.sql(""" select distinct parent_sponsor from citeline_sponsor_temp 
where lower(trim(sponsor_type)) = 'industry, top 20 pharma'
""")
top20_pharma = top20_pharma.dropDuplicates()
top20_pharma.createOrReplaceTempView('top20_pharma')

top20_pharma.repartition(1).write.mode("overwrite").format("com.crealytics.spark.excel").option("header", "true") \
    .save("{bucket_path}/uploads/SPONSOR/Top20_Sponsors/Top20_Sponsors.xlsx".format(bucket_path=bucket_path))

# add the result in top_pharma[] list
top_pharma = top20_pharma.rdd.map(lambda x: x[0]).collect()

# Here we get the final subsidiary_sponsor name
citeline_sponsor_df = citeline_sponsor_temp \
    .withColumn("subsidiary_sponsor", when(citeline_sponsor_case_wise_sorted.case_type == "case2", "Other") \
                .otherwise(get_subsidiary_sponsor_UDF("subsidiary_sponsor")))
citeline_sponsor_df = citeline_sponsor_df.dropDuplicates()
citeline_sponsor_df.write.mode("overwrite").saveAsTable('citeline_sponsor_df')

citeline_sponsor = spark.sql("""
select 
  distinct 'citeline' as source, raw_sponsor_name, sponsor_type, parent_sponsor, 
  coalesce(NULLIF(subsidiary_sponsor, ''), parent_sponsor) as subsidiary_sponsor
from citeline_sponsor_df
""")
citeline_sponsor = citeline_sponsor.dropDuplicates()
citeline_sponsor.write.mode("overwrite").saveAsTable('citeline_sponsor')

citeline_std = spark.sql("""
select distinct 'citeline' as source, raw_sponsor_name, sn as exploded_sponsor, parent_sponsor 
from citeline_sponsor_df lateral view outer explode(split(parent_child_sponsor_name, '\\\^')) one AS sn 
group by 1,2,3,4""")
citeline_std = citeline_std.dropDuplicates()
citeline_std.write.mode("overwrite").saveAsTable('citeline_std')

current_Delta = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/SPONSOR/Sponsor_Delta/Sponsor_Delta.xlsx".format(bucket_path=bucket_path))
current_Delta = current_Delta.dropDuplicates()
current_Delta.createOrReplaceTempView('current_Delta')

titleUDF = udf(lambda x: x.title())
current_Delta = current_Delta.withColumn("exploded_sponsor", titleUDF("exploded_sponsor")) \
    .withColumn("parent_sponsor", titleUDF("parent_sponsor"))
current_Delta = current_Delta.dropDuplicates()
current_Delta.createOrReplaceTempView('current_Delta')

Mapping_File = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/SPONSOR/Sponsor_Mapping_File/Sponsor_Mapping.xlsx".format(bucket_path=bucket_path))
Mapping_File = Mapping_File.dropDuplicates()
Mapping_File.createOrReplaceTempView('Mapping_File')

final_mapping_file = spark.sql("""
select distinct raw_sponsor_name, exploded_sponsor, parent_sponsor from current_Delta
union
select distinct raw_sponsor_name, exploded_sponsor, parent_sponsor from Mapping_File
""")
final_mapping_file = final_mapping_file.dropDuplicates()
final_mapping_file.write.mode("overwrite").saveAsTable('final_mapping_file')

# To update sponsor_type, parent_sponsor, subsidiary_sponsor on the raw_sponsor_name
master_File = spark.read.format("com.crealytics.spark.excel").option("header", "true") \
    .load("{bucket_path}/uploads/SPONSOR/Master_File/Sponsor_Master_File.xlsx".format(bucket_path=bucket_path))
master_File.dropDuplicates()
master_File.createOrReplaceTempView('master_File')

temp1_d_sponsor = spark.sql(""" 
select distinct A.raw_sponsor_name, coalesce(B.parent_sponsor,A.parent_sponsor) as parent_sponsor,
coalesce(B.subsidiary_sponsor,A.subsidiary_sponsor) as subsidiary_sponsor
from
(select distinct B.raw_sponsor_name,
case when A.exploded_sponsor = 'Other'
then coalesce(B.exploded_sponsor, A.exploded_sponsor) else B.exploded_sponsor end as subsidiary_sponsor,
case when A.parent_sponsor = 'Other'
then coalesce(B.parent_sponsor, A.parent_sponsor) else B.parent_sponsor end as parent_sponsor
from
(select distinct raw_sponsor_name, exploded_sponsor, parent_sponsor from final_mapping_file) A
left join
(select distinct raw_sponsor_name, exploded_sponsor, parent_sponsor from citeline_std) B
on lower(trim(A.raw_sponsor_name)) = lower(trim(B.raw_sponsor_name)))A
left join master_File B on lower(trim(A.raw_sponsor_name)) = lower(trim(B.raw_sponsor_name))
""")
temp1_d_sponsor = temp1_d_sponsor.dropDuplicates()
temp1_d_sponsor.write.mode("overwrite").saveAsTable('temp1_d_sponsor')

multiple_parent_sponsors_email = spark.sql(""" 
select distinct raw_sponsor_name, subsidiary_sponsor, parent_sponsor
from temp1_d_sponsor where raw_sponsor_name in	
(select distinct raw_sponsor_name from temp1_d_sponsor group by 1 having count(distinct parent_sponsor)>1)	
order by 1
""")
multiple_parent_sponsors_email = multiple_parent_sponsors_email.dropDuplicates()
# multiple_parent_sponsors_email.createOrReplaceTempView('multiple_parent_sponsors_email')
multiple_parent_sponsors_email.write.mode('overwrite').saveAsTable('multiple_parent_sponsors_email')

temp_d_sponsor = spark.sql(""" 
select * from
((select distinct A.raw_sponsor_name, A.parent_sponsor as parent_sponsor, A.subsidiary_sponsor as subsidiary_sponsor
from temp1_d_sponsor A left join multiple_parent_sponsors_email B
on lower(trim(A.raw_sponsor_name)) = lower(trim(B.raw_sponsor_name))
where B.parent_sponsor is null and B.subsidiary_sponsor is null)
union
(select raw_sponsor_name, 'Other' as subsidiary_sponsor, 'Other' parent_sponsor from multiple_parent_sponsors_email))
""")
temp_d_sponsor = temp_d_sponsor.dropDuplicates()
# temp_d_sponsor.createOrReplaceTempView('temp_d_sponsor')
temp_d_sponsor.write.mode("overwrite").saveAsTable('temp_d_sponsor')

##Sent email with data of multiple parent sponsors
delta_env = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "env"])
server_host = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_server"])
server_port = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_port"])
email_recipient_list = configuration.get_configuration(
    [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_recipient_list"])
print("email_recipient_list", email_recipient_list)
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
    msg["Subject"] = "Enviroment : {env} | [URGENT] Update Unique Parent Sponsor for Same Subsidiary Sponsor".format(
        env=delta_env)
    body = """Hello Team ,\n\nBased on our sponsor run, attached is the list of raw sponsor name which are having multiple parent sponsors.\nPlease update this file on priority and acknowledge once done!\n\nRegards,\n{client_name} DE Team\nClinical Development Excellence""".format(client_name=client_name)
    msg.attach(MIMEText(body))
    attachment_name = "multiple_parent_sponsors_email.csv"
    if (attachment_name):
        part = MIMEApplication(
            multiple_parent_sponsors_email.toPandas().to_csv(escapechar="\\", doublequote=False, encoding='utf-8'),
            Name=attachment_name)
        content_dis = "multiple_parent_sponsors_email; filename=" + attachment_name
        part['Content-Disposition'] = content_dis
        msg.attach(part)
    server = smtplib.SMTP(host=server_host, port=server_port)
    server.connect(server_host, server_port)
    server.starttls()
    server.send_message(msg)
    server.quit()
except Exception as E:
    print("Email Failed with error : ", E)

## Mapping file to be updated with the Current Citeline runs' data
mapping_file_write = spark.sql("""select * from default.final_mapping_file  """)
mapping_file_write = mapping_file_write.dropDuplicates()
mapping_file_write.registerTempTable('mapping_file_write')

sponsor_mapping_code_path = CommonConstants.AIRFLOW_CODE_PATH + '/Sponsor_Mapping.xlsx'

if os.path.exists(sponsor_mapping_code_path):
    os.system('rm ' + sponsor_mapping_code_path)

# create new mapping file : union of new citeline data and delta from KM
mapping_file_write.toPandas().to_excel("Sponsor_Mapping.xlsx", index=False)

##Moving delta file in Archive folder with date
CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
s3_mapping_file_location = "s3://{}/clinical-data-lake/uploads/SPONSOR/Sponsor_Mapping_File/Sponsor_Mapping.xlsx".format(
    s3_bucket_name)

prev_mapping_file_location = "s3://{}/clinical-data-lake/uploads/SPONSOR/Sponsor_Mapping_File/Archive/".format(
    s3_bucket_name)

# creating backup of existing sponsor mapping file in archive folder
file_copy_command = "aws s3 cp {} {}".format(
    os.path.join(s3_mapping_file_location),
    os.path.join(prev_mapping_file_location, "Sponsor_Mapping_" + CURRENT_DATE + ".xlsx"))

print("Command to create backup of file on s3 - {}".format(file_copy_command))

file_copy_status = os.system(file_copy_command)
if file_copy_status != 0:
    raise Exception("Failed to create backup of sponsor mapping file on s3")

file_delete_command = "aws s3 rm {} ".format(os.path.join(s3_mapping_file_location))
file_delete_status = os.system(file_delete_command)
if file_delete_status != 0:
    raise Exception("Failed to delete sponsor mapping file on s3")

# COPY new mapping file on s3
os.system(
    "aws s3 cp Sponsor_Mapping.xlsx {bucket_path}/uploads/SPONSOR/Sponsor_Mapping_File/Sponsor_Mapping.xlsx".format(
        bucket_path=bucket_path))

##Moving delta file in Archive folder with date
CURRENT_DATE = datetime.strftime(datetime.now(), "%Y%m%d%H%M%S")
s3_delta_file_location = "s3://{}/clinical-data-lake/uploads/SPONSOR/Sponsor_Delta/Sponsor_Delta.xlsx".format(
    s3_bucket_name)

prev_delta_file_location = "s3://{}/clinical-data-lake/uploads/SPONSOR/Sponsor_Delta/Archive/".format(s3_bucket_name)

# creating backup of existing Sponsor delta file in archive folder
file_copy_command = "aws s3 cp {} {}".format(
    os.path.join(s3_delta_file_location),
    os.path.join(prev_delta_file_location, "Sponsor_Delta_" + CURRENT_DATE + ".xlsx"))

print("Command to create backup of file on s3 - {}".format(file_copy_command))

file_copy_status = os.system(file_copy_command)
if file_copy_status != 0:
    raise Exception("Failed to create backup of Sponsor delta file on s3")

# deleting the old delta file
file_delete_command = "aws s3 rm {} ".format(os.path.join(s3_delta_file_location))
file_delete_status = os.system(file_delete_command)
if file_delete_status != 0:
    raise Exception("Failed to delete Sponsor delta file on s3")

##Creating an empty delta file
schema = StructType([ \
    StructField("raw_sponsor_name", StringType(), True), \
    StructField("exploded_sponsor", StringType(), True), \
    StructField("parent_sponsor", StringType(), True), \
    ])
Sponsor_Delta = spark.createDataFrame(data=[], schema=schema)
Sponsor_Delta.registerTempTable('Sponsor_Delta')
# creating new delta file
Sponsor_Delta.toPandas().to_excel("Sponsor_Delta.xlsx", index=False)
os.system("aws s3 cp Sponsor_Delta.xlsx {bucket_path}/uploads/SPONSOR/Sponsor_Delta/Sponsor_Delta.xlsx".format(
    bucket_path=bucket_path))