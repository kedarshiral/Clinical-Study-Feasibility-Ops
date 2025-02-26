################################ Module Information #####################################################
#  Module Name         : DQS USL Calculation
#  Purpose             : To create the source data for the CTMS Refactored Site
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : IR Refactored Study Site Investigator
#  Last changed on     : 08-09-2022
#  Last changed by     : Rakshit 
#  Reason for change   : Initial Code $db_envelopment
##########################################################################################################


# Importing python modules
import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, first as first_
from pyspark.sql import functions as F
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

dqs_study = spark.sql(""" select distinct * from $$client_name_ctfo_datastore_staging_$$db_env.dqs_study """)
dqs_study = dqs_study.dropDuplicates()
dqs_study.registerTempTable("dqs_study")

dqs_person_total = spark.sql(
    """ select distinct * from $$client_name_ctfo_datastore_staging_$$db_env.dqs_person_total limit 0 """)
dqs_person_total = dqs_person_total.dropDuplicates()
dqs_person_total.registerTempTable("dqs_person_total")

dqs_study_total = spark.sql(
    """ select distinct * from $$client_name_ctfo_datastore_staging_$$db_env.dqs_study_total limit 0 """)
dqs_study_total = dqs_study_total.dropDuplicates()
dqs_study_total.registerTempTable("dqs_study_total")

dqs_facility_total = spark.sql(
    """ select distinct * from $$client_name_ctfo_datastore_staging_$$db_env.dqs_facility_total limit 0 """)
dqs_facility_total = dqs_facility_total.dropDuplicates()
dqs_facility_total.registerTempTable("dqs_facility_total")

temp_dqs_study = spark.sql(""" select distinct * from (select member_study_id,study_title,phase,primary_indication,mesh_heading,data_source,sponsor,row_number() over (partition by member_study_id order by pt_file_id desc) as rnk from dqs_study)b 
where b.rnk=1   """)
temp_dqs_study = temp_dqs_study.dropDuplicates()
temp_dqs_study.registerTempTable("temp_dqs_study")

temp_dqs_study_total = spark.sql(
    """ select distinct * from (select member_study_id,member_study_title,ct_gov_name,intervention,ie_criteria,description,study_details_link, row_number() over (partition by member_study_id order by pt_file_id desc) as rnk from dqs_study_total)b where b.rnk =1   """)
temp_dqs_study_total = temp_dqs_study_total.dropDuplicates()
temp_dqs_study_total.registerTempTable("temp_dqs_study_total")

temp_person_study_total = spark.sql(""" select distinct * from (select person_golden_id,person_city,person_state,person_country,specialty,opted_in_consented,person_vpds,gcp_source,person_details_link,debarred,profile_cv,person_phase_one_yn, row_number() over (partition by person_golden_id order by pt_file_id desc) as rnk
from dqs_person_total )b
where b.rnk =1
""")
temp_person_study_total = temp_person_study_total.dropDuplicates()
temp_person_study_total.registerTempTable('temp_person_study_total')

temp_person_dqs_study = spark.sql(""" select distinct * from (select person_golden_id,person_member_id,first_name,last_name,person_data_source,email,person_phone,row_number() over (partition by person_golden_id order by pt_file_id desc) as rnk
from dqs_study) b
where b.rnk=1 
 """)
temp_person_dqs_study = temp_person_dqs_study.dropDuplicates()
temp_person_dqs_study.registerTempTable('temp_person_dqs_study')

temp_facility_study_total = spark.sql(""" select distinct * from (select facility_golden_id,country_iso_code,facility_details_link,facility_vpds,facility_phase_one_yn,facility_profile, row_number() over (partition by facility_golden_id order by pt_file_id desc) as rnk
from dqs_facility_total )b
where b.rnk =1
""")
temp_facility_study_total = temp_facility_study_total.dropDuplicates()
temp_facility_study_total.registerTempTable('temp_facility_study_total')

temp_facility_dqs_study = spark.sql(""" select distinct * from (select facility_golden_id,facility_member_id,facility_name,facility_address,facility_city,facility_postal,facility_state,facility_country,
row_number() over (partition by facility_golden_id order by pt_file_id desc) as rnk
from dqs_study )b
where b.rnk =1 
 """)
temp_facility_dqs_study = temp_facility_dqs_study.dropDuplicates()
temp_facility_dqs_study.registerTempTable("temp_facility_dqs_study")

usl_ir = spark.sql("""select distinct
A.nct_number,
A.member_study_id,
B.study_title,
B.phase,
B.primary_indication,
B.mesh_heading,
B.data_source,
B.sponsor,
A.facility_golden_id,
C.facility_member_id,
C.facility_name,
C.facility_address,
C.facility_city,
C.facility_postal,
C.facility_state,
C.facility_country,
A.facility_phone,
A.site_status,
min(A.protocol_date) as protocol_date,
min(A.selection_date) as selection_date,
min(A.irb_submission_date) as irb_submission_date,
max(A.irb_approval_date) as irb_approval_date,
min(A.site_open_dt) as site_open_dt,
min(A.first_subject_consented_dt) as first_subject_consented_dt,
min(A.first_subject_enrolled_dt) as first_subject_enrolled_dt,
max(A.last_subject_consented_dt) as last_subject_consented_dt ,
max(A.last_subject_enrolled_dt) as last_subject_enrolled_dt,
max(A.last_subject_last_visit_dt) as last_subject_last_visit_dt,
max(A.consented) as consented,
max(A.enrolled) as enrolled,
max(A.completed) as completed,
max(A.en_months) as en_months,
max(A.patients_per_site_per_month) as patients_per_site_per_month,
percentile(case when trim(lower(A.pct_screen_fail)) like '%\%%' then regexp_replace(pct_screen_fail,"\%","") else pct_screen_fail end,0.5,10000) as pct_screen_fail,
percentile(case when trim(lower(A.pct_complete)) like '%\%%' then regexp_replace(pct_complete,"\%","") else pct_complete end,0.5,10000) as pct_complete,
percentile(A.irb_cycle_time,0.5,10000) as irb_cycle_time,
percentile(A.site_startup_time,0.5,10000) as site_startup_time,
max(A.total_enrolled) as total_enrolled,
max(A.total_sites) as total_sites,
max(A.median_enrolled_per_site) as median_enrolled_per_site,
max(A.median_enroll_months) as median_enroll_months,
max(A.median_patients_per_site_per_month) as median_patients_per_site_per_month,
max(A.median_pct_screen_fail) as median_pct_screen_fail,
max(A.median_pct_complete) as median_pct_complete,
max(A.median_irb_cycle_time) as median_irb_cycle_time,
max(A.median_startup_time) as median_startup_time,
A.person_golden_id,
D.person_member_id,
D.first_name,
D.last_name,
D.person_data_source,
A.role,
D.email,
D.person_phone as person_phone,
'ir' as source,
E.member_study_title,
E.ct_gov_name,
E.intervention,
E.ie_criteria,
E.description,
E.study_details_link,
max(F.study_close_date) as study_close_date,
percentile(F.study_avg_enrollment_per_site,0.5,10000) as study_avg_enrollment_per_site,
percentile(case when trim(lower(F.perc_zero_enroll_sites)) like '%\%%' then regexp_replace(perc_zero_enroll_sites,"\%","") else perc_zero_enroll_sites end,0.5,10000) as perc_zero_enroll_sites,
percentile(case when trim(lower(F.perc_one_enroll_sites)) like '%\%%' then regexp_replace(perc_one_enroll_sites,"\%","") else perc_one_enroll_sites end,0.5,10000) as perc_one_enroll_sites,
H.person_city,
H.person_state,
H.person_country,
H.specialty,
H.opted_in_consented,
H.person_vpds,
max(G.gcp_date) as gcp_date,
H.gcp_source,
H.person_details_link,
H.debarred,
H.profile_cv,
max(G.person_total_ct_gov_studies_since2008) as person_total_ct_gov_studies_since2008,
H.person_phase_one_yn,
I.country_iso_code,
I.facility_details_link,
I.facility_vpds,
max(J.persons_public_data) as persons_public_data,
max(J.persons_total) as persons_total,
max(J.persons_my_view) as persons_my_view,
max(J.persons_my_company) as persons_my_company,
max(J.facility_total_ct_gov_studies_since2008) as facility_total_ct_gov_studies_since2008,
I.facility_phase_one_yn,
percentile(J.avg_enrollment_per_site,0.5,10000) as facility_avg_enrollment_per_site,
I.facility_profile
from dqs_study A
left join temp_dqs_study B on lower(trim(A.member_study_id))= lower(trim(B.member_study_id)) 
left join temp_facility_dqs_study C on lower(trim(A.facility_golden_id))=lower(trim(C.facility_golden_id))
left join temp_person_dqs_study D on lower(trim(A.person_golden_id))=lower(trim(D.person_golden_id))
left join temp_dqs_study_total E on lower(trim(A.member_study_id))= lower(trim(E.member_study_id)) 
left join dqs_study_total F on lower(trim(A.member_study_id))= lower(trim(F.member_study_id)) 
left join dqs_person_total G on lower(trim(A.person_golden_id))=lower(trim(G.person_golden_id))
left join temp_person_study_total H on lower(trim(A.person_golden_id))=lower(trim(H.person_golden_id))
left join temp_facility_study_total I on lower(trim(A.facility_golden_id))=lower(trim(I.facility_golden_id))
left join dqs_facility_total J on lower(trim(A.facility_golden_id))=lower(trim(J.facility_golden_id))
group by 1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10,11,12,13,14,15,16,17,18,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,66,67,68,69,70,71,73,74,75,76,78,79,80,81,87,89
""")
usl_ir = usl_ir.dropDuplicates()
usl_ir.write.mode("overwrite").saveAsTable("usl_ir")

spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   usl_ir 
""")

if usl_ir.count() == 0:
    print("Skipping copy_hdfs_to_s3 for usl_ir as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.usl_ir")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")
