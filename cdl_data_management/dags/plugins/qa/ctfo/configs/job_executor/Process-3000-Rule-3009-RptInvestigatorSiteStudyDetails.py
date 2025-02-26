


################################# Module Information ######################################
#  Module Name         : Site Investigator reporting table
#  Purpose             : This will create below reporting layer table
#                           a. f_rpt_site_study_details_filters
#                           b. f_rpt_site_study_details
#                           c. f_rpt_src_site_details
#                           d. f_rpt_investigator_details
#                           e. f_rpt_site_study_details_non_performance
#  Pre-requisites      : Source table required:
#  Last changed on     : 03-02-2021
#  Last changed by     : Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

################################### High level Process #####################################
# 1. Create required table and stores data on HDFS
############################################################################################
import datetime

from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.functions import trim
from pyspark.sql.types import *
from CommonUtils import CommonUtils

data_dt = datetime.datetime.now().strftime('%Y%m%d')
cycle_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

path = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/" \
       "commons/temp/" \
       "kpi_output_dimension/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id"

path_csv = "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/" \
           "commons/temp/" \
           "kpi_output_dimension/table_name"

spark.conf.set("spark.sql.crossJoin.enabled", "true")
spark.sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
spark.conf.set("spark.sql.crossJoin.enabled", "True")

################## creating table with all dimensions information ################

sanofi_cluster_pta_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/sanofi_cluster_pta_mapping.csv")
sanofi_cluster_pta_mapping.createOrReplaceTempView("sanofi_cluster_pta_mapping")

standard_country_mapping = spark.read.format("csv").option("header", "true") \
    .load(
    "s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/"
    "uploads/standard_country_mapping.csv")
standard_country_mapping.createOrReplaceTempView("standard_country_mapping")

trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/'
          'uploads/trial_status.csv')
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')

r_trial_inv_site_roles = spark.sql("""
select
    d.ctfo_trial_id, e.ctfo_site_id, e.ctfo_investigator_id
from
    sanofi_ctfo_datastore_app_commons_$$db_env.r_site_investigator e
inner join
    sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_investigator d
on e.ctfo_investigator_id = d.ctfo_investigator_id
inner join
    sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_site f
on e.ctfo_site_id = f.ctfo_site_id and d.ctfo_trial_id = f.ctfo_trial_id
group by 1,2,3
""")
r_trial_inv_site_roles.createOrReplaceTempView("r_trial_inv_site_roles")
#r_trial_inv_site_roles.write.mode("overwrite").saveAsTable("r_trial_inv_site_roles")

golden_id_det_inv_1_rpt = spark.sql("""select B.ctfo_trial_id, B.ctfo_site_id,B.ctfo_investigator_id, C.data_src_nm, src_site_id
from
    r_trial_inv_site_roles B
inner join
    (select
        data_src_nm, ctfo_site_id,src_site_id
    from
        (select
            data_src_nm, ctfo_site_id, explode(split(src_site_id, ';')) as src_site_id
        from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site
    ) group by 1,2,3) C
on B.ctfo_site_id = C.ctfo_site_id
group by 1,2,3,4,5
""")
golden_id_det_inv_1_rpt.createOrReplaceTempView("golden_id_det_inv_1_rpt")


golden_id_det_inv_2_rpt = spark.sql("""select B.ctfo_trial_id, B.ctfo_site_id,B.ctfo_investigator_id,B.src_site_id,C.data_src_nm, src_investigator_id
from
    golden_id_det_inv_1_rpt B
inner join
    (select
        data_src_nm, ctfo_investigator_id,src_investigator_id
    from
        (select
            data_src_nm, ctfo_investigator_id, explode(split(src_investigator_id, ';')) as src_investigator_id
        from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_inv
    ) group by 1,2,3) C
on B.ctfo_investigator_id = C.ctfo_investigator_id AND Lower(Trim(C.data_src_nm)) = Lower(Trim(B.data_src_nm))
group by 1,2,3,4,5,6
""")
golden_id_det_inv_2_rpt.createOrReplaceTempView("golden_id_det_inv_2_rpt")



goldeniddet_its_rpt = spark.sql("""
select
    B.ctfo_trial_id, B.ctfo_site_id,B.ctfo_investigator_id, B.data_src_nm,B.src_site_id,
    B.src_investigator_id,E.src_trial_id
from
    golden_id_det_inv_2_rpt B
inner join
    (select data_src_nm, ctfo_trial_id, src_trial_id
    from sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial
    group by 1,2,3) E
on trim(B.ctfo_trial_id) = trim(E.ctfo_trial_id) and
lower(trim(B.data_src_nm)) = lower(trim(E.data_src_nm))
group by 1,2,3,4,5,6,7
""")
goldeniddet_its_rpt = goldeniddet_its_rpt.dropDuplicates()
goldeniddet_its_rpt.createOrReplaceTempView("goldeniddet_its_rpt")
#goldeniddet_its_rpt.write.mode("overwrite").saveAsTable("goldeniddet_its_rpt")

centre_loc_map = spark.sql("""
select study_code as trial_no, (country_code) as country_code, study_country_id,study_site_id,
       center_id as centre_loc_id,primary_investigator_id
from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site where lower(trim(Confirmed)) = 'yes' and lower(trim(cancelled)) = 'no'
group by 1,2,3,4,5,6
""")
centre_loc_map = centre_loc_map.dropDuplicates()
centre_loc_map.createOrReplaceTempView("centre_loc_map")


ctms_gsm_its_rpt = spark.sql("""
select * from (select A.ctfo_trial_id,
       A.ctfo_site_id,
       A.ctfo_investigator_id,
       B.trial_no as study_code,
       A.src_site_id,
       A.src_investigator_id,
       B.primary_investigator_id,
       (B.study_country_id) as study_country_id,
       (B.study_site_id) as study_site_id,
                  row_number() over (partition by A.ctfo_trial_id, A.ctfo_site_id, A.ctfo_investigator_id order by B.trial_no) as rnk
from (select ctfo_trial_id,ctfo_site_id, ctfo_investigator_id, data_src_nm,
    src_trial_id, CAST(src_investigator_id as BIGINT) as src_investigator_id,src_site_id
    from goldeniddet_its_rpt
    where lower(trim(data_src_nm)) = 'ctms' group by 1,2,3,4,5,6,7) A
inner join
    (select trial_no,
            centre_loc_id,CAST(primary_investigator_id as BIGINT) as primary_investigator_id,study_country_id,study_site_id from centre_loc_map group by 1,2,3,4,5) B
on lower(trim(A.src_trial_id)) = lower(trim(B.trial_no)) and lower(trim(A.src_site_id)) = lower(trim(B.centre_loc_id))
and lower(trim(A.src_investigator_id)) = lower(trim(B.primary_investigator_id))
group by 1,2,3,4,5,6,7,8,9)a where rnk=1
""")
ctms_gsm_its_rpt = ctms_gsm_its_rpt.dropDuplicates()
#ctms_gsm_its_rpt.createOrReplaceTempView("ctms_gsm_its_rpt")
ctms_gsm_its_rpt.write.mode("overwrite").saveAsTable("ctms_gsm_its_rpt")

ctms_its_status_rpt = spark.sql("""select a.ctfo_trial_id,a.ctfo_site_id,a.ctfo_investigator_id,a.status from (select cgsm.ctfo_trial_id,cgsm.ctfo_site_id,cgsm.ctfo_investigator_id, tsm.status from
(select ctfo_trial_id,ctfo_site_id, study_code, ctfo_investigator_id,primary_investigator_id,study_site_id from ctms_gsm_its_rpt group by 1,2,3,4,5,6) cgsm
left join
(select study_site.study_code,study_site.primary_investigator_id,study_site.study_site_id,study_site.trial_status from
(select CAST(person_id as BIGINT) as person_id  from sanofi_ctfo_datastore_staging_$$db_env.ctms_site_staff where lower(trim(investigator)) = 'yes' group by 1) site_staff
left join (select CAST(primary_investigator_id as BIGINT) as primary_investigator_id , study_code,status as trial_status,study_site_id from sanofi_ctfo_datastore_staging_$$db_env.ctms_study_site
  group by 1,2,3,4) study_site
on site_staff.person_id = study_site.primary_investigator_id
group by 1,2,3,4) c
on cgsm.study_code = c.study_code and cgsm.primary_investigator_id=c.primary_investigator_id and cgsm.study_site_id=c.study_site_id
left join (select distinct raw_status,status from trial_status_mapping) tsm  on lower(trim(c.trial_status))=lower(trim(tsm.raw_status))
group by 1,2,3,4)a""")
ctms_its_status_rpt = ctms_its_status_rpt.dropDuplicates()
ctms_its_status_rpt.write.mode("overwrite").saveAsTable("ctms_its_status_rpt")

dqs_gsm_its_rpt = spark.sql("""
select * from(select
    A.ctfo_trial_id,A.ctfo_site_id,A.ctfo_investigator_id, A.data_src_nm, B.nct_number, B.member_study_id,
  B.facility_golden_id,B.person_golden_id,
  row_number() over (partition by A.ctfo_trial_id, A.ctfo_site_id, A.ctfo_investigator_id order by B.facility_golden_id) as rnk
from
    (select ctfo_trial_id,ctfo_site_id, ctfo_investigator_id, data_src_nm,
    src_trial_id, src_investigator_id,src_site_id
    from goldeniddet_its_rpt
    where lower(trim(data_src_nm)) = 'ir' group by 1,2,3,4,5,6,7) A
inner join
    (select nct_number, member_study_id,person_golden_id,facility_golden_id
    from sanofi_ctfo_datastore_staging_$$db_env.dqs_study
    group by 1,2,3,4) B
on trim(A.src_trial_id) = trim(B.member_study_id) and trim(A.src_investigator_id) = trim(B.person_golden_id) and trim(A.src_site_id) = trim(B.facility_golden_id)
group by 1,2,3,4,5,6,7,8)a where rnk=1
""")
dqs_gsm_its_rpt = dqs_gsm_its_rpt.dropDuplicates()
#dqs_gsm_its_rpt.createOrReplaceTempView("dqs_gsm_its_rpt")
dqs_gsm_its_rpt.write.mode("overwrite").saveAsTable("dqs_gsm_its_rpt")

ir_status_its_rpt = spark.sql("""select A.ctfo_trial_id,A.ctfo_site_id,A.ctfo_investigator_id,tsm.status
from (select ctfo_trial_id,ctfo_site_id, ctfo_investigator_id,member_study_id,person_golden_id,facility_golden_id from dqs_gsm_its_rpt) A
left join  (select member_study_id,facility_golden_id,person_golden_id,site_status from sanofi_ctfo_datastore_staging_$$db_env.dqs_study   group by 1,2,3,4) B
on trim(A.member_study_id) = trim(B.member_study_id) and trim(A.person_golden_id) = trim(B.person_golden_id) and trim(A.facility_golden_id) = trim(B.facility_golden_id)
left join (select distinct raw_status,status from trial_status_mapping) tsm
on lower(trim(B.site_status))=lower(trim(tsm.raw_status))
        group by 1,2,3,4""")
ir_status_its_rpt.registerTempTable("ir_status_its_rpt")
ir_status_its_rpt.write.mode("overwrite").saveAsTable("ir_status_its_rpt")


aact_raw_data=spark.sql("""SELECT a.nct_id as src_trial_id,
                          b.id as src_investigator_id,a.status,a.id as src_site_id
                   FROM   sanofi_ctfo_datastore_staging_$$db_env.aact_facilities a
                    inner join sanofi_ctfo_datastore_staging_$$db_env.aact_facility_investigators b
                    on lower(trim(a.nct_id))=lower(trim(b.nct_id)) and lower(trim(a.id))=lower(trim(b.facility_id)) where lower(trim(b.role))='principal investigator' """)
aact_raw_data=aact_raw_data.dropDuplicates()
aact_raw_data.registerTempTable('aact_raw_data')


aact_gsm_its_status = spark.sql("""select iq.* from (SELECT B.ctfo_trial_id,
       C.ctfo_site_id,
       D.ctfo_investigator_id,
       B.data_src_nm,
       B.src_trial_id,
       C.src_site_id,
       D.src_investigator_id,
       tsm.status,
       row_number() over (partition by B.ctfo_trial_id,C.ctfo_site_id,D.ctfo_investigator_id order by  B.src_trial_id,C.src_site_id,D.src_investigator_id) as rnk
FROM  aact_raw_data aa inner join (SELECT data_src_nm,
                         ctfo_trial_id,
                         src_trial_id
                  FROM
sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_trial where  Lower(Trim(data_src_nm)) = 'aact'
                  GROUP  BY 1,
                            2,
                            3) B
              ON aa.src_trial_id = B.src_trial_id
       INNER JOIN (SELECT data_src_nm,
                         ctfo_site_id,
                         src_site_id
                  FROM   (SELECT data_src_nm,
                                 ctfo_site_id,
                                src_site_id
                          FROM
       sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_site where  Lower(Trim(data_src_nm)) = 'aact' )
                  GROUP  BY 1,
                            2,
                            3 ) C
              ON aa.src_site_id = C.src_site_id
                 INNER JOIN (SELECT data_src_nm, ctfo_investigator_id,src_investigator_id
                  FROM   (SELECT data_src_nm, ctfo_investigator_id,
                                 src_investigator_id
                          FROM
       sanofi_ctfo_datastore_app_commons_$$db_env.xref_src_inv where  Lower(Trim(data_src_nm)) = 'aact')
                  GROUP  BY 1,
                            2,
                            3
                            ) D
              ON aa.src_investigator_id = D.src_investigator_id
              inner join (select distinct raw_status,status from trial_status_mapping) tsm
              on lower(trim(aa.status))=lower(trim(tsm.raw_status))
GROUP  BY 1,
          2,
          3,
          4,
          5,
          6,7,8 ) iq where iq.rnk=1
""")
aact_gsm_its_status = aact_gsm_its_status.dropDuplicates()
#aact_gsm_its_status.createOrReplaceTempView('aact_gsm_its_status')
aact_gsm_its_status.write.mode("overwrite").saveAsTable("aact_gsm_its_status")

geo_mapping = spark.sql("""select coalesce(cs.standard_country,'Others') as standard_country,scpm.iso2_code,scpm.iso3_code,scpm.region,
scpm.region_code,scpm.sanofi_cluster,scpm.sanofi_csu,scpm.post_trial_flag,
scpm.post_trial_details
from (select distinct(standard_country) as standard_country
    from standard_country_mapping )  cs
left join
        sanofi_cluster_pta_mapping scpm
        on lower(trim(cs.standard_country))=lower(trim(scpm.standard_country))
""")
geo_mapping.createOrReplaceTempView("country_mapping")


f_rpt_investigator_details_temp_1 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,
     investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address
from
    (select
        i_ctfo_investigator_id as ctfo_investigator_id, i_ctfo_site_id as ctfo_site_id,
        i_ctfo_trial_id as ctfo_trial_id
    from temp_inv_info) A
inner join
    (select ctfo_investigator_id, investigator_name as investigator_full_nm, investigator_specialty,
    investigator_email, investigator_phone, investigator_address, investigator_city,
    investigator_state,
    investigator_country, geo_cd
    from sanofi_ctfo_datastore_app_commons_$$db_env.d_investigator) d_investigator
on A.ctfo_investigator_id = d_investigator.ctfo_investigator_id
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
f_rpt_investigator_details_temp_1 = f_rpt_investigator_details_temp_1.dropDuplicates()

f_rpt_investigator_details_temp_1.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_1")

f_rpt_investigator_details_temp_2 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,
     investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address,
        temp_site_info.site_nm, temp_site_info.site_address,
    temp_site_info.site_city, temp_site_info.site_state, temp_site_info.site_country
from
    f_rpt_investigator_details_temp_1 A
        inner join
    temp_site_info
on A.ctfo_site_id=temp_site_info.s_ctfo_site_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
""")
f_rpt_investigator_details_temp_2 = f_rpt_investigator_details_temp_2.dropDuplicates()

f_rpt_investigator_details_temp_2.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_2")

f_rpt_investigator_details_temp_3 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,
     investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address,
        site_nm,site_address,
    site_city, site_state, site_country,
        trial_status, trial_start_dt, trial_phase,completion_dt as trial_end_dt, trial_title as
    study_name,temp_nct_id_ctfo_trial_id_map.protocol_ids as protocol_ids,temp_nct_id_ctfo_trial_id_map.nct_id as nct_id
from
f_rpt_investigator_details_temp_2 A
inner join
    (select
        ctfo_trial_id, trial_phase, trial_start_dt,trial_status,trial_title,
        enrollment_close_date as completion_dt
        from sanofi_ctfo_datastore_app_commons_$$db_env.d_trial
        group by 1,2,3,4,5,6) d_trial
on A.ctfo_trial_id=d_trial.ctfo_trial_id
left join
    temp_nct_id_ctfo_trial_id_map
on A.ctfo_trial_id=temp_nct_id_ctfo_trial_id_map.n_ctfo_trial_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
""")
f_rpt_investigator_details_temp_3 = f_rpt_investigator_details_temp_3.dropDuplicates()
f_rpt_investigator_details_temp_3.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_3")

f_rpt_investigator_details_temp_4 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,
     investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address,
        site_nm,site_address,
    site_city, site_state, site_country,
        trial_status, trial_start_dt, trial_phase, trial_end_dt,
    study_name,protocol_ids,nct_id,
        concat_ws('|',collect_list(temp_sponsor_info.sponsor_nm))  as sponsor_nm,
        concat_ws('|',collect_list(temp_sponsor_info.sponsor_type))  as sponsor_type
from
f_rpt_investigator_details_temp_3 A
left join
    temp_sponsor_info
on A.ctfo_trial_id=temp_sponsor_info.s_ctfo_trial_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24

""")
f_rpt_investigator_details_temp_4 = f_rpt_investigator_details_temp_4.dropDuplicates()

f_rpt_investigator_details_temp_4.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_4")

f_rpt_investigator_details_temp_5 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id, A.ctfo_trial_id, A.ctfo_site_id,
    investigator_full_nm, investigator_specialty, investigator_email, investigator_phone,investigator_city,
    investigator_state, investigator_country,geo_cd, investigator_address,
    site_nm,site_address,site_city, site_state, site_country,
    trial_status, trial_start_dt,trial_end_dt, trial_phase,
    study_name,protocol_ids,nct_id,sponsor_nm,sponsor_type,
    total_recruitment_months_prec as total_recruitment_months,
    last_subject_in_dt_prec as last_subject_in_dt, ready_to_enroll_dt_prec as ready_to_enroll_dt,
    randomization_rate,screen_fail_rate_prec as screen_failure_rate
from
f_rpt_investigator_details_temp_4 A
left join f_investigator_trial_site_final
on A.ctfo_investigator_id = f_investigator_trial_site_final.ctfo_investigator_id
and A.ctfo_trial_id = f_investigator_trial_site_final.ctfo_trial_id
""")
f_rpt_investigator_details_temp_5 = f_rpt_investigator_details_temp_5.dropDuplicates()

f_rpt_investigator_details_temp_5.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_5")

f_rpt_investigator_details_temp_6 = spark.sql("""
select /* broadcast(f_investigator_trial_site_final) */
    A.ctfo_investigator_id,
        A.ctfo_trial_id,
        A.ctfo_site_id,
    investigator_full_nm,
        investigator_specialty,
        investigator_email,
        investigator_phone,
     investigator_city,
    investigator_state,
        investigator_country,
        investigator_address,
        site_nm,
        site_address,
    site_city,
        site_state,
        site_country,
        trial_status,
        trial_start_dt,
        trial_end_dt,
        trial_phase,
    study_name,
        protocol_ids,
        nct_id,
         sponsor_nm,
     sponsor_type,
     total_recruitment_months,
    last_subject_in_dt,
        ready_to_enroll_dt,
        screen_failure_rate,
        concat_ws('|',collect_set(therapeutic_area)) as therapeutic_area,
        concat_ws('|',collect_set(disease_nm)) as disease_nm,
        d_geo.country,
     d_geo.country_code,
    d_geo.region, d_geo.region_code,
    randomization_rate
from
f_rpt_investigator_details_temp_5 A
left join
    (select d_ctfo_trial_id as ctfo_trial_id, therapeutic_area, disease_nm
    from temp_disease_info group by 1,2,3) B
on A.ctfo_trial_id = B.ctfo_trial_id
left join
    (select geo_cd, standard_country as country, iso2_code as country_code, region, region_code
    from sanofi_ctfo_datastore_app_commons_$$db_env.d_geo group by 1,2,3,4,5) d_geo
on A.geo_cd = d_geo.geo_cd
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,32,33,34,35,36
""")
f_rpt_investigator_details_temp_6 = f_rpt_investigator_details_temp_6.dropDuplicates()

f_rpt_investigator_details_temp_6.write.mode("overwrite").saveAsTable(
    "f_rpt_investigator_details_temp_6")

'''
inv_workload = spark.sql("""
select
    ctfo_investigator_id,
    count(A.ctfo_trial_id) as total_trials_all_ta,
    count(case when trial_status in ('Active (Not Recruiting)', 'Active (Recruiting)',
     'Enrolling by Invitation',
    'Initiating', 'Open for Recruitment')
        then A.ctfo_trial_id
    end) as current_workload,
    trial_status
from
    (select ctfo_investigator_id, ctfo_trial_id
    from sanofi_ctfo_datastore_app_commons_$$db_env.r_trial_investigator group by 1,2) A
left join
    (select ctfo_trial_id, trial_status from sanofi_ctfo_datastore_app_commons_$$db_env.d_trial
     group by 1,2) B
on A.ctfo_trial_id = B.ctfo_trial_id
group by ctfo_investigator_id,trial_status
""")
inv_workload = inv_workload.dropDuplicates()
inv_workload.write.mode("overwrite").saveAsTable("inv_workload")
'''

d_trial_citeline_status=spark.sql("""select A.ctfo_investigator_id,A.ctfo_site_id,A.ctfo_trial_id,tsm.status
from goldeniddet_its_rpt A
left join  sanofi_ctfo_datastore_app_commons_$$db_env.d_trial dt
on  A.ctfo_trial_id=dt.ctfo_trial_id
left join (select distinct raw_status,status from trial_status_mapping) tsm
on lower(trim(tsm.raw_status))=lower(trim(dt.trial_status))
where lower(trim(A.data_src_nm))='citeline'
group by 1,2,3,4""")
d_trial_citeline_status.write.mode("overwrite").saveAsTable("d_trial_citeline_status")

f_rpt_investigator_site_study_details = spark.sql("""
select /* broadcast(inv_workload) */
    A.ctfo_investigator_id,
    A.ctfo_site_id,
    A.ctfo_trial_id,
    A.nct_id,
    trial_phase,
    coalesce(ctms_gsm.status,dqs_gsm.status,aact_gsm.status,B.status,'Others') as trial_status,
    therapeutic_area,
    disease_nm as disease,
    sponsor_nm as sponsor,
    sponsor_type,
    A.region,
    A.region_code,
    A.country,
    A.country_code,
    site_nm,
    site_address,
    site_city,
    site_state,
    site_country,
    investigator_full_nm,
    investigator_specialty,
    investigator_email,
    investigator_phone,
    investigator_city,
    investigator_state,
    investigator_country,
    country_mapping.sanofi_cluster as sanofi_cluster,
    trial_start_dt,
    last_subject_in_dt,
    ready_to_enroll_dt,
    trial_end_dt,
    screen_failure_rate,
    total_recruitment_months,
    A.randomization_rate,
    '' as total_trials_all_ta,
    '' as current_workload,
    investigator_address,
    concat(A.ctfo_trial_id, A.ctfo_site_id) as trial_site_id,
    concat(A.ctfo_trial_id, A.ctfo_site_id, A.ctfo_investigator_id) as trial_site_inv_id
 from f_rpt_investigator_details_temp_6 A
 left join d_trial_citeline_status B on A.ctfo_investigator_id = B.ctfo_investigator_id and A.ctfo_trial_id = B.ctfo_trial_id and A.ctfo_site_id = B.ctfo_site_id
 left join country_mapping on lower(trim(A.site_country)) = lower(trim(country_mapping.standard_country))
 left join aact_gsm_its_status aact_gsm on A.ctfo_investigator_id = aact_gsm.ctfo_investigator_id and A.ctfo_trial_id = aact_gsm.ctfo_trial_id and A.ctfo_site_id = aact_gsm.ctfo_site_id
 left join ir_status_its_rpt dqs_gsm on A.ctfo_investigator_id = dqs_gsm.ctfo_investigator_id and A.ctfo_trial_id = dqs_gsm.ctfo_trial_id and A.ctfo_site_id = dqs_gsm.ctfo_site_id
 left join ctms_its_status_rpt ctms_gsm on A.ctfo_investigator_id = ctms_gsm.ctfo_investigator_id and A.ctfo_trial_id = ctms_gsm.ctfo_trial_id and A.ctfo_site_id = ctms_gsm.ctfo_site_id
 """)
f_rpt_investigator_site_study_details = f_rpt_investigator_site_study_details.dropDuplicates()
f_rpt_investigator_site_study_details.write.mode("overwrite").saveAsTable("f_rpt_investigator_site_study_details")
#f_rpt_investigator_site_study_details.registerTempTable('f_rpt_investigator_site_study_details')
write_path = path.replace("table_name", "f_rpt_investigator_details")
write_path_csv = path_csv.replace("table_name", "f_rpt_investigator_site_study_details")
write_path_csv = write_path_csv + "_csv/"
f_rpt_investigator_site_study_details.write.format("parquet").mode("overwrite") \
    .save(path=write_path)

if "$$flag" == "Y":
    f_rpt_investigator_site_study_details.coalesce(1).write.option("header", "false") \
        .option("sep", "|").option('quote', '"') \
        .option('escape', '"').option("multiLine", "true").mode('overwrite').csv(write_path_csv)

spark.sql("""insert overwrite table
sanofi_ctfo_datastore_app_fa_$$env.f_rpt_investigator_site_study_details
partition(pt_data_dt, pt_cycle_id)
select *, "$$data_dt" as pt_data_dt,
   "$$cycle_id" as pt_cycle_id
from
    f_rpt_investigator_site_study_details
""")

if f_rpt_investigator_site_study_details.count() == 0:
    print("Skipping copy_hdfs_to_s3 for f_rpt_investigator_site_study_details as "
          "zero records are present.")
else:
    CommonUtils().copy_hdfs_to_s3("sanofi_ctfo_datastore_app_fa_$$env"
                                  ".f_rpt_investigator_site_study_details")

try:
    print("Closing spark context")
except:
    print("Error while closing spark context")







