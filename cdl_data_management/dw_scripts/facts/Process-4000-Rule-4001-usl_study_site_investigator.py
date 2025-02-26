################################# Module Information #####################################################
#  Module Name         : Trial Universe Metric Calculation Data
#  Purpose             : To create the source data for the CTMS Refactored Site
#  Pre-requisites      : L1 Staging tables , dimension and relation tables
#  Execution Steps     : This code can be triggered through Airflow DAG as well as standalone on pyspark
#  Output              : CTMS Refactored Site
#  Last changed on     : 28-10-2021
#  Last changed by     : Sankarsh/Kashish
#  Reason for change   : Initial Code $db_envelopment
##########################################################################################################


# Importing python modules
import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col,concat
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from CommonUtils import *
from pyspark.sql.types import StructType, StructField, IntegerType,StringType
from pyspark.sql.functions import col, first as first_
from pyspark.sql import functions as F
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility

configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

path = bucket_path + "/applications/commons/temp/" \
                     "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

#########Site module queries#########

trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')


ctms_study_site_temp=spark.sql("""SELECT aa.*
FROM   (select study_code from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study where lower(trim(meta_is_current)) = 'y'
                 and origin <> 'FPOS') a
inner join (SELECT a.*
        FROM   $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site a where Lower(Trim(cancelled)) = 'no'
       AND Lower(Trim(confirmed)) = 'yes'
       AND Lower(Trim(meta_is_current)) = 'y' and lower(trim(study_code)) not like '%iit%' and (center_id is not null or trim(center_id)<>'')  ) aa on lower(trim(a.study_code))=lower(trim(aa.study_code))  """)
ctms_study_site_temp.write.mode('overwrite').saveAsTable('ctms_study_site_temp')




############site,study,country details##########################################
site_details_temp_1 = spark.sql(""" SELECT final.src_site_id,
	final.site_trial_status ,final.site_name, final.src_country_id, final.src_trial_id, final.study_site_id
FROM (
	SELECT inq.src_site_id,
		   inq.site_name,
		   inq.src_country_id,
		   inq.src_trial_id,
		   inq.study_site_id,
		   inq.site_trial_status,
		row_number() OVER (
			PARTITION BY inq.study_site_id,inq.src_trial_id,inq.src_site_id ORDER BY inq.precedence
			) AS rnk
	FROM (
		SELECT center_id AS src_site_id,
			   center_name      AS site_name,
			   study_country_id AS src_country_id,
			   study_code       AS src_trial_id,
			   study_site_id        AS study_site_id,
			   status           AS site_trial_status,
			CASE
				WHEN lower(trim(status)) in ('active', 'csr approved', 'initiated', 'last subject enrolled', 'site ready to enrol')
					THEN 1
				WHEN lower(trim(status)) in ('all sites closed', 'closed', 'database clean', 'database locked', 'last subject completed')
					THEN 2
				WHEN lower(trim(status)) in ('created', 'identified', 'pending','planned', 'selected')
					THEN 3
				WHEN lower(trim(status)) in ('cancelled', 'closed no subject', 'discontinued', 'evaluation', 'hold', 'not selected')
					THEN 4 else 5
				END AS precedence
		FROM ctms_study_site_temp
		WHERE  Lower(Trim(cancelled)) = 'no'
       AND Lower(Trim(confirmed)) = 'yes' and Lower(Trim(meta_is_current)) = 'y' and lower(trim(study_code)) not like '%iit%'
       GROUP  BY 1,2,3, 4,5,6
		) inq
	) final
WHERE final.rnk = 1 """)

site_details_temp_1 = site_details_temp_1.dropDuplicates()
site_details_temp_1.registerTempTable('site_details_temp_1')


'''
site_details_temp_2 = spark.sql("""SELECT a.center_id AS src_site_id,
Concat_ws(';', a.address_line_1, a.address_line_2, a.address_line_3, a.address_line_4) AS site_address,
a.town_city        AS site_city,
a.state_province_county  AS site_state,
b.country AS site_country,
a.postal_zip_code  AS site_zip,
Concat_ws(';', a.phone_number_1, a.phone_number_2, a.phone_number_3, a.phone_number_4, a.phone_number_5) AS site_phone
FROM   $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address a
       LEFT JOIN ctms_study_site_temp b
              ON lower(trim(a.center_id)) = lower(trim(b.center_id))
WHERE  Lower(Trim(a.primary_address)) = 'yes' AND Lower(Trim(b.cancelled)) = 'no'  AND Lower(Trim(b.confirmed)) = 'yes' and Lower(Trim(b.meta_is_current)) = 'y' group by 1,2,3,4,5,6,7 """)

site_details_temp_2 = site_details_temp_2.dropDuplicates()
site_details_temp_2.registerTempTable('site_details_temp_2')
'''

site_details_temp_2 = spark.sql("""SELECT a.center_id AS src_site_id,
Concat_ws(';', a.address_line_1, a.address_line_2, a.address_line_3, a.address_line_4) AS site_address,
a.town_city        AS site_city,
a.state_province_county  AS site_state,
a.country AS site_country,
a.postal_zip_code  AS site_zip,
Concat_ws(';', a.phone_number_1, a.phone_number_2, a.phone_number_3, a.phone_number_4, a.phone_number_5) AS site_phone
FROM   ctms_study_site_temp b
       LEFT JOIN $$client_name_ctfo_datastore_staging_$$db_env.ctms_centre_address a
              ON lower(trim(a.center_id)) = lower(trim(b.center_id))
WHERE  Lower(Trim(a.primary_address)) = 'yes' AND Lower(Trim(b.cancelled)) = 'no'  AND Lower(Trim(b.confirmed)) = 'yes' and Lower(Trim(b.meta_is_current)) = 'y' group by 1,2,3,4,5,6,7 """)

site_details_temp_2 = site_details_temp_2.dropDuplicates()
site_details_temp_2.registerTempTable('site_details_temp_2')

site_details_temp_3 = spark.sql("""SELECT study_code AS src_trial_id,
        center_id      AS src_site_id,
       Count(DISTINCT primary_investigator_id) AS count_investigators
FROM   ctms_study_site_temp
WHERE  Lower(Trim(confirmed)) = 'yes'
       AND Lower(Trim(cancelled)) = 'no' and Lower(Trim(meta_is_current)) = 'y'
GROUP  BY 1,2  """)
site_details_temp_3 = site_details_temp_3.dropDuplicates()
site_details_temp_3.registerTempTable('site_details_temp_3')


study_site_country_details_final = spark.sql("""SELECT a.src_site_id,
a.src_trial_id,
a.site_name,
a.src_country_id,
a.site_trial_status,
a.study_site_id,
b.site_address,
b.site_city,
b.site_state,
b.site_country,
b.site_zip,
b.site_phone,
c.count_investigators,
'' as nct_id,
'' as site_type,
'ctms' as source
FROM   site_details_temp_1 a
       left JOIN site_details_temp_2 b
               ON lower(trim(a.src_site_id)) = lower(trim(b.src_site_id))
       left JOIN site_details_temp_3 c
               ON lower(trim(a.src_site_id)) = lower(trim(c.src_site_id))
                  AND lower(trim(a.src_trial_id)) = lower(trim(c.src_trial_id)) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16""")

study_site_country_details_final = study_site_country_details_final.dropDuplicates()
#study_site_country_details_final.registerTempTable('study_site_country_details_final')
study_site_country_details_final.write.mode('overwrite').saveAsTable('study_site_country_details_final')

############site,study,country details##########################################

##########patient details###########################################################
patient_details_planned = spark.sql("""
select   ctms_study_site.study_code as study_code,
         ctms_study_site.study_site_id as study_site_id ,
         A.entered_study as patients_consented_planned,
         A.entered_study as patients_screened_planned,
         A.entered_treatment as patients_randomized_planned
from
(select study_code, study_site_id  from ctms_study_site_temp
where Lower(Trim(confirmed)) = 'yes' AND Lower(Trim(cancelled)) = 'no' and Lower(Trim(meta_is_current)) = 'y' group by 1,2) ctms_study_site
left join
(select  a.study_site_id,a.study_code,entered_study,entered_treatment  from
(select  study_code,study_site_id,entered_treatment,entered_study,picture_as_of,figure_type,picture_number,study_period_number
from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'planned' group by 1,2,3,4,5,6,7,8) a
inner join
(select inq.study_site_id,study_code,picture_number,picture_as_of,study_period_number from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,picture_number desc,study_period_number desc ) as rnk
from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'planned' )inq where inq.rnk=1
) d
on lower(trim(a.study_site_id)) = lower(trim(d.study_site_id))  and lower(trim(a.study_code)) = lower(trim(d.study_code)) and lower(trim(a.picture_number))=lower(trim(d.picture_number)) and (lower(trim(a.picture_as_of)) = lower(trim(d.picture_as_of)) or a.picture_as_of is null and d.picture_as_of is null ) and 
lower(trim(a.study_period_number))=lower(trim(d.study_period_number))
group by 1,2,3,4)A
on lower(trim(ctms_study_site.study_code)) = lower(trim(A.study_code)) and lower(trim(ctms_study_site.study_site_id))  = lower(trim(A.study_site_id))
group by 1,2,3,4,5
""")
patient_details_planned = patient_details_planned.dropDuplicates()
#patient_details_planned.registerTempTable('patient_details_planned')
patient_details_planned.write.mode('overwrite').saveAsTable('patient_details_planned')


patient_details_confirmed = spark.sql("""
select   ctms_study_site.study_code as study_code,
         ctms_study_site.study_site_id as study_site_id,
         A.entered_treatment as patients_randomized_actual,
         A.entered_study as patients_consented_actual,
         A.entered_study as patients_screened_actual,
         A.dropped_treatment as patients_dropped_actual,
         A.dropped_at_screening as dropped_at_screening_actual,
         A.completed_treatment as patients_completed_actual
from
(select study_code, study_site_id  from ctms_study_site_temp
where Lower(Trim(confirmed)) = 'yes' AND Lower(Trim(cancelled)) = 'no' and Lower(Trim(meta_is_current)) = 'y' group by 1,2) ctms_study_site
left join
(select  a.study_site_id,a.study_code,entered_study,entered_treatment,dropped_treatment,dropped_at_screening,completed_treatment  from
(select study_code,study_site_id,entered_study,entered_treatment,dropped_treatment,dropped_at_screening,completed_treatment,
picture_as_of,figure_type,picture_number,study_period_number
from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' and Lower(Trim(meta_is_current)) = 'y' group by 1,2,3,4,5,6,7,8,9,10,11) a
inner join
(select inq.study_site_id,study_code,picture_number,picture_as_of,study_period_number from (select *,row_number() over (partition by study_code,study_site_id order by picture_as_of desc ,picture_number desc,study_period_number desc ) as rnk
from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_subject_figures where lower(trim(figure_type)) = 'confirmed' and Lower(Trim(meta_is_current)) = 'y' )inq where inq.rnk=1
) d
on lower(trim(a.study_site_id)) = lower(trim(d.study_site_id))  and lower(trim(a.study_code)) = lower(trim(d.study_code)) and lower(trim(a.picture_number))=lower(trim(d.picture_number)) and (lower(trim(a.picture_as_of)) = lower(trim(d.picture_as_of)) or a.picture_as_of is null and d.picture_as_of is null ) and 
lower(trim(a.study_period_number))=lower(trim(d.study_period_number))
group by 1,2,3,4,5,6,7)A
on lower(trim(ctms_study_site.study_code)) = lower(trim(A.study_code)) and lower(trim(ctms_study_site.study_site_id))  = lower(trim(A.study_site_id))
group by 1,2,3,4,5,6,7,8
""")
patient_details_confirmed = patient_details_confirmed.dropDuplicates()
#patient_details_confirmed.registerTempTable('patient_details_confirmed')
patient_details_confirmed.write.mode('overwrite').saveAsTable('patient_details_confirmed')


###########patient details##############################################################


###########Milestone details###########################################################

df_milestone=spark.sql("""select a.study_code,a.study_site_id,b.milestone,b.actual_date ,b.current_plan_date,b.baseline_plan_date from (select * from ctms_study_site_temp where Lower(Trim(meta_is_current)) = 'y' and  Lower(Trim(confirmed)) = 'yes'
       AND Lower(Trim(cancelled)) = 'no') a left join (select study_code,milestone,study_site_id,actual_date,current_plan_date,baseline_plan_date from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_site_milestones where Lower(Trim(meta_is_current)) = 'y') b
       on lower(trim(a.study_code))=lower(trim(b.study_code)) and lower(trim(a.study_site_id))=lower(trim(b.study_site_id))
       group by 1,2,3,4,5,6""")

df_milestone.registerTempTable('df_milestone')


df_milestone_actual_1 = df_milestone.groupBy("study_code","study_site_id").pivot("milestone").agg(first_("actual_date"))
df_milestone_actual_1.registerTempTable('df_milestone_actual_1')

#Remove Space from ColumnName
df_milestone_actual_2 = df_milestone_actual_1.select([F.col(col).alias(col.replace(' ', '_')) for col in df_milestone_actual_1.columns])
df_milestone_actual_2.registerTempTable('df_milestone_actual_2')

#Remove / from column Name
df_milestone_actual_3 = df_milestone_actual_2.select([F.col(col).alias(col.replace('/', '_')) for col in df_milestone_actual_2.columns])
df_milestone_actual_3.registerTempTable('df_milestone_actual_3')

df_milestone_actual_final=spark.sql("""select study_code,study_site_id,Site_Contract_Signed as site_contract_signed_actual_dt,
Site_Qualification_visit as site_qualification_visit_actual_dt,
Local_IRB_IEC_Submission as irb_iec_submission_actual_dt,
Local_IRB_IEC_Approval as irb_iec_approval_actual_dt,
Site_Initiation_Visit as site_initiation_visit_actual_dt,
Site_Ready_to_Enrol as site_ready_to_enrol_actual_dt,
First_Subject_First_Visit as first_subject_first_visit_actual_dt,
First_Subject_First_Rando_Treat as first_subject_first_treatment_actual_dt,
Last_Subject_First_Visit as last_subject_first_visit_actual_dt,
Last_Subject_First_Rando_Treat as last_subject_first_treatment_actual_dt,
Last_Subject_Last_Treatment as last_subject_last_treatment_actual_dt,
Last_Subject_Last_Visit as last_subject_last_visit_actual_dt,
Site_Closure_Visit as site_closure_visit_actual_dt from df_milestone_actual_3 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15""")

#df_milestone_actual_final.registerTempTable('df_milestone_actual_final')
df_milestone_actual_final.write.mode('overwrite').saveAsTable('df_milestone_actual_final')



############################################## Baseline ##########################


df_milestone_baseline_1 = df_milestone.groupBy("study_code","study_site_id").pivot("milestone").agg(first_("baseline_plan_date"))
df_milestone_baseline_1.registerTempTable('df_milestone_baseline_1')

#Remove Space from ColumnName
df_milestone_baseline_2 = df_milestone_baseline_1.select([F.col(col).alias(col.replace(' ', '_')) for col in df_milestone_baseline_1.columns])
df_milestone_baseline_2.registerTempTable('df_milestone_baseline_2')

#Remove / from column Name
df_milestone_baseline_3 = df_milestone_baseline_2.select([F.col(col).alias(col.replace('/', '_')) for col in df_milestone_baseline_2.columns])
df_milestone_baseline_3.registerTempTable('df_milestone_baseline_3')

df_milestone_baseline_final=spark.sql("""select study_code,study_site_id,Site_Contract_Signed as site_contract_signed_bsln_dt,
Site_Qualification_visit as site_qualification_visit_bsln_dt,
Local_IRB_IEC_Submission as irb_iec_submission_bsln_dt,
Local_IRB_IEC_Approval as irb_iec_approval_bsln_dt,
Site_Initiation_Visit as site_initiation_visit_bsln_dt,
Site_Ready_to_Enrol as site_ready_to_enrol_bsln_dt,
First_Subject_First_Visit as first_subject_first_visit_bsln_dt,
First_Subject_First_Rando_Treat as first_subject_first_treatment_bsln_dt,
Last_Subject_First_Visit as last_subject_first_visit_bsln_dt,
Last_Subject_First_Rando_Treat as last_subject_first_treatment_bsln_dt,
Last_Subject_Last_Treatment as last_subject_last_treatment_bsln_dt,
Last_Subject_Last_Visit as last_subject_last_visit_bsln_dt,
Site_Closure_Visit as site_closure_visit_bsln_dt from df_milestone_baseline_3 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15""")

#df_milestone_baseline_final.registerTempTable('df_milestone_baseline_final')
df_milestone_baseline_final.write.mode('overwrite').saveAsTable('df_milestone_baseline_final')



################################################# Current Planned ##########################################


df_milestone_current_1 = df_milestone.groupBy("study_code","study_site_id").pivot("milestone").agg(first_("current_plan_date"))
df_milestone_current_1.registerTempTable('df_milestone_current_1')

#Remove Space from ColumnName
df_milestone_current_2 = df_milestone_current_1.select([F.col(col).alias(col.replace(' ', '_')) for col in df_milestone_current_1.columns])
df_milestone_current_2.registerTempTable('df_milestone_current_2')

#Remove / from column Name
df_milestone_current_3 = df_milestone_current_2.select([F.col(col).alias(col.replace('/', '_')) for col in df_milestone_current_2.columns])
df_milestone_current_3.registerTempTable('df_milestone_current_3')

df_milestone_current_final=spark.sql("""select study_code,study_site_id,Site_Contract_Signed as site_contract_signed_planned_dt,
Site_Qualification_visit as site_qualification_visit_planned_dt,
Local_IRB_IEC_Submission as irb_iec_submission_planned_dt,
Local_IRB_IEC_Approval as irb_iec_approval_planned_dt,
Site_Initiation_Visit as site_initiation_visit_planned_dt,
Site_Ready_to_Enrol as site_ready_to_enrol_planned_dt,
First_Subject_First_Visit as first_subject_first_visit_planned_dt,
First_Subject_First_Rando_Treat as first_subject_first_treatment_planned_dt,
Last_Subject_First_Visit as last_subject_first_visit_planned_dt,
Last_Subject_First_Rando_Treat as last_subject_first_treatment_planned_dt,
Last_Subject_Last_Treatment as last_subject_last_treatment_planned_dt,
Last_Subject_Last_Visit as last_subject_last_visit_planned_dt,
Site_Closure_Visit as site_closure_visit_planned_dt from df_milestone_current_3 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15""")

#df_milestone_current_final.registerTempTable('df_milestone_current_final')
df_milestone_current_final.write.mode('overwrite').saveAsTable('df_milestone_current_final')



#################milestone details################################################

###################investigator details##########################################

ctms_study_site=spark.sql("""select Cast(primary_investigator_id as BigInt) as investigator_id,study_code,study_site_id, confirmed,cancelled,meta_is_current  from ctms_study_site_temp""")
ctms_study_site.registerTempTable("ctms_study_site")

ctms_person_spec=spark.sql("""SELECT person_id FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_site_staff_specialty 
group by 1 having count(distinct(specialty))<=2""")
ctms_person_spec=ctms_person_spec.dropDuplicates()
ctms_person_spec.registerTempTable('ctms_person_spec')

ctms_person_spec_final=spark.sql("""select person_id,trim(concat_ws('\|',collect_set(specialty))) as investigator_specialty
  from (select a.person_id,b.specialty from ctms_person_spec a left join 
$$client_name_ctfo_datastore_staging_$$db_env.ctms_site_staff_specialty b on lower(trim(a.person_id))=lower(trim(b.person_id))) group by 1  """)
ctms_person_spec_final=ctms_person_spec_final.dropDuplicates()
ctms_person_spec_final.registerTempTable('ctms_person_spec_final')

investigator_details = spark.sql("""SELECT aa.study_code AS study_code,
       aa.study_site_id  AS study_site_id,
       aa.person_id AS  src_investigator_id,
       Concat_ws(' ', aa.first_name , aa.last_name) AS src_investigator_name,
       e.investigator_phone,
       aa.PRINCIPAL_EMAIL_ADDRESS as investigator_email,
       k.investigator_specialty,
       h.investigator_address,
       h.investigator_city,
       h.investigator_state,
       investigator_zip,
       investigator_country,
       aa.blacklisted   AS investigator_debarred_flag,
       aa.ACTIVE as investigator_active_flag,
       CASE WHEN aa.primary_investigator_id IS NULL THEN 'No' ELSE 'Yes' END AS primary_investigator_flag,
       '' as investigator_role
FROM  (select a.*,b.primary_investigator_id,b.study_code,b.study_site_id from (select Cast(investigator_id as string) as primary_investigator_id ,study_code,study_site_id, confirmed,cancelled,meta_is_current from ctms_study_site) b
    left join $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_staff a on lower(trim(a.person_id)) = lower(trim(b.primary_investigator_id))
    where Lower(Trim(b.confirmed)) = 'yes'  AND Lower(Trim(b.cancelled)) = 'no' and Lower(Trim(b.meta_is_current)) = 'y') aa
       LEFT JOIN (SELECT c.person_id,
                    Concat_ws(';', c.phone_number_1, c.phone_number_2, c.phone_number_3, c.phone_number_4, c.phone_number_5) AS investigator_phone
                     FROM  $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_staff d
                            left JOIN (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_staff_address WHERE  Lower(Trim(primary_address)) = 'yes') c
                                   ON lower(trim(c.person_id)) = lower(trim(d.person_id))
                  ) e
       ON lower(trim(aa.person_id)) = lower(trim(e.person_id))
LEFT JOIN (SELECT
            f.person_id,
            Concat_ws(';', f.address_line_1, f.address_line_2, f.address_line_3, f.address_line_4) AS investigator_address,
            f.town_city AS investigator_city,
            f.state_province_county AS investigator_state,
            f.postal_zip_code  AS investigator_zip,
            f.country    AS investigator_country
             FROM $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_staff g
             left JOIN (select * from $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_staff_address WHERE  Lower(Trim(primary_address)) = 'yes' ) f
                           ON lower(trim(f.person_id)) = lower(trim(g.person_id))) h
       ON lower(trim(aa.person_id)) = lower(trim(h.person_id))
LEFT JOIN (SELECT i.person_id,
                  i.investigator_specialty
           FROM   $$client_name_ctfo_datastore_staging_$$db_env.ctms_study_staff j 
                  LEFT JOIN ctms_person_spec_final i
                         ON lower(trim(i.person_id)) = lower(trim(j.person_id))) k
       ON lower(trim(aa.person_id)) = lower(trim(k.person_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15""")
investigator_details = investigator_details.dropDuplicates()
investigator_details.write.mode('overwrite').saveAsTable('investigator_details')


######################################################  Final DF   #########################################

ctms_refactoring_site_final_df = spark.sql("""Select trial_site.*,a.site_trial_status from  (SELECT 
a.src_site_id,             
a.site_name,               
a.site_address,            
a.site_city ,              
a.site_state,              
a.site_country,          
a.site_zip,            
a.site_phone,            
a.src_country_id,         
a.src_trial_id,            
a.nct_id,                           
a.count_investigators,     
a.site_type,
sum(b.patients_consented_planned) as patients_consented_planned,      
sum(c.patients_consented_actual) as patients_consented_actual,       
sum(b.patients_screened_planned) as patients_screened_planned,       
sum(c.patients_screened_actual) as patients_screened_actual,        
sum(c.dropped_at_screening_actual) as dropped_at_screening_actual ,    
sum(b.patients_randomized_planned) as patients_randomized_planned,     
sum(c.patients_randomized_actual) as patients_randomized_actual,      
sum(c.patients_completed_actual) as patients_completed_actual,       
sum(c.patients_dropped_actual) as patients_dropped_actual ,  
'' as site_contract_issues_bsln_dt,	
'' as site_contract_issues_planned_dt,	
'' as site_contract_issues_actual_dt,	
'' as site_contract_negotiation_bsln_dt,	
'' as site_contract_negotiation_planned_dt,	
'' as site_contract_negotiation_actual_dt,	
min(f.site_contract_signed_bsln_dt) as site_contract_signed_bsln_dt ,  
min(g.site_contract_signed_planned_dt ) as site_contract_signed_planned_dt  ,
min(e.site_contract_signed_actual_dt) as site_contract_signed_actual_dt,
min(f.site_qualification_visit_bsln_dt) as site_qualification_visit_bsln_dt ,   
min(g.site_qualification_visit_planned_dt) as site_qualification_visit_planned_dt ,
min(e.site_qualification_visit_actual_dt)  as site_qualification_visit_actual_dt,    
min(f.irb_iec_submission_bsln_dt) as irb_iec_submission_bsln_dt,      
min(g.irb_iec_submission_planned_dt) as  irb_iec_submission_planned_dt,   
min(e.irb_iec_submission_actual_dt) as irb_iec_submission_actual_dt  ,   
max(f.irb_iec_approval_bsln_dt) as irb_iec_approval_bsln_dt  ,      
max(g.irb_iec_approval_planned_dt) as irb_iec_approval_planned_dt,     
max(e.irb_iec_approval_actual_dt) as irb_iec_approval_actual_dt ,     
min(f.site_initiation_visit_bsln_dt) as site_initiation_visit_bsln_dt   ,
min(g.site_initiation_visit_planned_dt) as site_initiation_visit_planned_dt,
min(e.site_initiation_visit_actual_dt) as site_initiation_visit_actual_dt, 
min(f.site_ready_to_enrol_bsln_dt) as site_ready_to_enrol_bsln_dt ,    
min(g.site_ready_to_enrol_planned_dt) as site_ready_to_enrol_planned_dt,
min(e.site_ready_to_enrol_actual_dt) as site_ready_to_enrol_actual_dt ,  
min(f.first_subject_first_visit_bsln_dt) as first_subject_first_visit_bsln_dt  ,  
min(g.first_subject_first_visit_planned_dt) as first_subject_first_visit_planned_dt ,
min(e.first_subject_first_visit_actual_dt) as first_subject_first_visit_actual_dt,
min(f.first_subject_first_treatment_bsln_dt) as first_subject_first_treatment_bsln_dt,
min(g.first_subject_first_treatment_planned_dt) as first_subject_first_treatment_planned_dt,
min(e.first_subject_first_treatment_actual_dt) as first_subject_first_treatment_actual_dt,
max(f.last_subject_first_visit_bsln_dt) as last_subject_first_visit_bsln_dt,    
max(g.last_subject_first_visit_planned_dt) as  last_subject_first_visit_planned_dt, 
max(e.last_subject_first_visit_actual_dt) as  last_subject_first_visit_actual_dt , 
max(f.last_subject_first_treatment_bsln_dt) as last_subject_first_treatment_bsln_dt,
max(g.last_subject_first_treatment_planned_dt) as last_subject_first_treatment_planned_dt ,
max(e.last_subject_first_treatment_actual_dt) as last_subject_first_treatment_actual_dt ,
max(f.last_subject_last_treatment_bsln_dt) as last_subject_last_treatment_bsln_dt,
max(g.last_subject_last_treatment_planned_dt) as last_subject_last_treatment_planned_dt,
max(e.last_subject_last_treatment_actual_dt) as last_subject_last_treatment_actual_dt,
max(f.last_subject_last_visit_bsln_dt) as last_subject_last_visit_bsln_dt,
max(g.last_subject_last_visit_planned_dt)  as last_subject_last_visit_planned_dt, 
max(e.last_subject_last_visit_actual_dt) as last_subject_last_visit_actual_dt ,  
max(f.site_closure_visit_bsln_dt) as  site_closure_visit_bsln_dt ,    
max(g.site_closure_visit_planned_dt) as site_closure_visit_planned_dt  ,  
max(e.site_closure_visit_actual_dt) as site_closure_visit_actual_dt  ,   
a.source ,                 
'14' as fsiv_fsfr_delta,
'' as enrollment_rate,
'' as screen_failure_rate,
'' as lost_opp_time,
'' as enrollment_duration
from study_site_country_details_final a
left join patient_details_planned b on lower(trim(a.src_trial_id))=lower(trim(b.study_code)) and lower(trim(a.study_site_id))=lower(trim(b.study_site_id))
left join patient_details_confirmed c on lower(trim(a.src_trial_id))=lower(trim(c.study_code)) and lower(trim(a.study_site_id))=lower(trim(c.study_site_id))
left join df_milestone_actual_final e on lower(trim(a.src_trial_id))=lower(trim(e.study_code)) and lower(trim(a.study_site_id))=lower(trim(e.study_site_id))
left join df_milestone_baseline_final f on lower(trim(a.src_trial_id))=lower(trim(f.study_code)) and lower(trim(a.study_site_id))=lower(trim(f.study_site_id))
left join df_milestone_current_final g on lower(trim(a.src_trial_id))=lower(trim(g.study_code)) and lower(trim(a.study_site_id))=lower(trim(g.study_site_id))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,23,24,25,26,27,28,68,69,70,71,72,73 ) trial_site left join study_site_country_details_final a on lower(trim(a.src_trial_id))=lower(trim(trial_site.src_trial_id)) and lower(trim(a.src_site_id))=lower(trim(trial_site.src_site_id))
""")
ctms_refactoring_site_final_df = ctms_refactoring_site_final_df.dropDuplicates()
#ctms_refactoring_site_final_df.registerTempTable('ctms_refactoring_site_final_df')
ctms_refactoring_site_final_df.write.mode('overwrite').saveAsTable('ctms_refactoring_site_final_df')

ctms_refactoring_site_final_df_new=spark.sql("""select * from (select aa.*,row_number() over(partition by src_trial_id,src_site_id order by precedence asc) as rnk from
(select a.*,t.status as standard_status,case when lower(trim(t.status))='ongoing' then 1
     when lower(trim(t.status))='completed' then 2
	 when lower(trim(t.status))='planned' then 3
	 when lower(trim(t.status))='others' then 4 else 5 end as precedence from 
ctms_refactoring_site_final_df a
left join trial_status_mapping t on lower(trim(t.raw_status))=lower(trim(a.site_trial_status))) aa) where rnk=1 
""")
ctms_refactoring_site_final_df_new = ctms_refactoring_site_final_df_new.dropDuplicates()
ctms_refactoring_site_final_df_new.registerTempTable('ctms_refactoring_site_final_df_new')


ctms_refactoring_site_final_df_base=spark.sql(""" select a.*,
b.src_investigator_id,     
b.src_investigator_name,   
b.investigator_phone ,     
b.investigator_email ,     
b.investigator_specialty,  
b.investigator_address ,   
b.investigator_city   ,    
b.investigator_state   ,   
b.investigator_zip,        
b.investigator_country,    
b.investigator_debarred_flag,    
b.investigator_active_flag,      
b.investigator_role,       
b.primary_investigator_flag from ctms_refactoring_site_final_df_new a 
left join ( select aa.src_site_id,aa.src_trial_id,d.src_investigator_id,     
d.src_investigator_name,d.investigator_phone , 
d.investigator_email ,  d.investigator_specialty,  
d.investigator_address ,   
d.investigator_city   ,    
d.investigator_state   ,   
d.investigator_zip,        
d.investigator_country,    
d.investigator_debarred_flag,    
d.investigator_active_flag,      
d.investigator_role,       
d.primary_investigator_flag from  study_site_country_details_final aa 
left join  investigator_details d on lower(trim(aa.src_trial_id))=lower(trim(d.study_code)) and lower(trim(aa.study_site_id))=lower(trim(d.study_site_id))  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16 ) b 
on lower(trim(a.src_site_id))=lower(trim(b.src_site_id)) and lower(trim(a.src_trial_id))=lower(trim(b.src_trial_id))  """)
ctms_refactoring_site_final_df_base=ctms_refactoring_site_final_df_base.dropDuplicates()
ctms_refactoring_site_final_df_base.registerTempTable('ctms_refactoring_site_final_df_base')




usl_study_site_investigator = spark.sql("""SELECT 
src_site_id, 
site_name,               
site_address,            
site_city ,              
site_state,              
site_country,          
site_zip,            
site_phone,            
src_country_id,
src_trial_id,                     
nct_id,                           
count_investigators,     
site_type,               
site_trial_status,  
patients_consented_planned,      
patients_consented_actual,       
patients_screened_planned,       
patients_screened_actual,        
dropped_at_screening_actual ,    
patients_randomized_planned,     
patients_randomized_actual,      
patients_completed_actual,       
patients_dropped_actual ,
site_contract_issues_bsln_dt,	
site_contract_issues_planned_dt,	
site_contract_issues_actual_dt,	
site_contract_negotiation_bsln_dt,	
site_contract_negotiation_planned_dt,	
site_contract_negotiation_actual_dt,	
site_contract_signed_bsln_dt,  
site_contract_signed_planned_dt ,
site_contract_signed_actual_dt,
site_qualification_visit_bsln_dt ,   
site_qualification_visit_planned_dt ,
site_qualification_visit_actual_dt  ,
irb_iec_submission_bsln_dt,      
irb_iec_submission_planned_dt,   
irb_iec_submission_actual_dt ,   
irb_iec_approval_bsln_dt  ,      
irb_iec_approval_planned_dt,     
irb_iec_approval_actual_dt ,     
site_initiation_visit_bsln_dt   ,
site_initiation_visit_planned_dt,
site_initiation_visit_actual_dt, 
site_ready_to_enrol_bsln_dt ,    
site_ready_to_enrol_planned_dt,  
site_ready_to_enrol_actual_dt ,  
first_subject_first_visit_bsln_dt ,  
first_subject_first_visit_planned_dt,
first_subject_first_visit_actual_dt ,
first_subject_first_treatment_bsln_dt,
first_subject_first_treatment_planned_dt,
first_subject_first_treatment_actual_dt,
last_subject_first_visit_bsln_dt,    
last_subject_first_visit_planned_dt, 
last_subject_first_visit_actual_dt , 
last_subject_first_treatment_bsln_dt,
last_subject_first_treatment_planned_dt,
last_subject_first_treatment_actual_dt,
last_subject_last_treatment_bsln_dt ,
last_subject_last_treatment_planned_dt,
last_subject_last_treatment_actual_dt,
last_subject_last_visit_bsln_dt,
last_subject_last_visit_planned_dt , 
last_subject_last_visit_actual_dt ,  
site_closure_visit_bsln_dt  ,    
site_closure_visit_planned_dt ,  
site_closure_visit_actual_dt ,
src_investigator_id,       
src_investigator_name,   
investigator_phone ,     
investigator_email ,     
investigator_specialty,  
investigator_address ,   
investigator_city   ,    
investigator_state   ,   
investigator_zip,        
investigator_country,    
investigator_debarred_flag,    
investigator_active_flag,      
investigator_role,       
primary_investigator_flag,
source ,                 
CAST(fsiv_fsfr_delta AS INT) as fsiv_fsfr_delta ,
enrollment_rate,
screen_failure_rate,
lost_opp_time,
enrollment_duration
from ctms_refactoring_site_final_df_base
""")
usl_study_site_investigator = usl_study_site_investigator.dropDuplicates()
#usl_study_site_investigator.registerTempTable('usl_study_site_investigator')
usl_study_site_investigator.write.mode('overwrite').saveAsTable('usl_study_site_investigator')



spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.sql("""insert overwrite table
$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
   usl_study_site_investigator where src_site_id is not null
""")


if usl_study_site_investigator.count() == 0:
    print("Skipping copy_hdfs_to_s3 for usl_study_site_investigator as zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("$$client_name_ctfo_datastore_app_commons_$$db_env.usl_study_site_investigator")

try:
    print("Closing spark context")
    spark.stop()
except:
    print("Error while closing spark context")

