import datetime
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import *
from PySparkUtility import PySparkUtility
import CommonConstants as CommonConstants
from MySQLConnectionManager import MySQLConnectionManager
from ConfigUtility import JsonConfigUtility
import MySQLdb
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from CommonUtils import CommonUtils
from pyspark.sql.functions import lower, col
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local").appName('sprk-job').enableHiveSupport().getOrCreate())
configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

########## Status Variablization ###################
trial_status_mapping = spark.read.format('csv').option('header', 'true').option('delimiter', ',') \
    .load('{bucket_path}/'
          'uploads/trial_status.csv'.format(bucket_path=bucket_path))
trial_status_mapping.createOrReplaceTempView('trial_status_mapping')

# Values in Variables

Ongoing = trial_status_mapping.filter(trial_status_mapping.br_status == "Ongoing").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Completed = trial_status_mapping.filter(trial_status_mapping.br_status == "Completed").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Planned = trial_status_mapping.filter(trial_status_mapping.br_status == "Planned").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()
Others = trial_status_mapping.filter(trial_status_mapping.br_status == "Others").select(
    lower(col('raw_status'))).rdd.flatMap(lambda x: x).distinct().collect()

# Open JSOn
import json

with open("Status_variablization.json", "r") as jsonFile:
    data = json.load(jsonFile)

# Save Values in Json
data["Ongoing"] = Ongoing
data["Completed"] = Completed
data["Planned"] = Planned
data["Others"] = Others

# JSON WRITE
with open("Status_variablization.json", "w") as jsonFile:
    json.dump(data, jsonFile)

Ongoing_variable = tuple(Ongoing)
Completed_variable = tuple(Completed)
Planned_variable = tuple(Planned)
Others_variable = tuple(Others)

Ongoing_Completed = tuple(Ongoing) + tuple(Completed)
Ongoing_Planned = tuple(Ongoing) + tuple(Planned)
Ongoing_Completed_Planned = tuple(Ongoing) + tuple(Completed) + tuple(Planned)


def Randomization_rate(grain):
    if grain == "Trial_site":
        print("Trial_site")
        trial_site_mce_ctms = spark.sql("""select * from trial_site_mce where lower(trim(source))='ctms'""")
        trial_site_mce_ctms = trial_site_mce_ctms.dropDuplicates()
        trial_site_mce_ctms.registerTempTable('trial_site_mce_ctms')
        trial_site_mce_ir = spark.sql("""select * from trial_site_mce where lower(trim(source))='ir'""")
        trial_site_mce_ir = trial_site_mce_ir.dropDuplicates()
        trial_site_mce_ir.registerTempTable('trial_site_mce_ir')

        ctms_entered_study = spark.sql("""
        select ctfo_site_id,ctfo_trial_id, 
        patients_consented_actual as entered_study,
        patients_randomized_actual as entered_treatment,
        patients_screened_actual as entered_screening,
        patients_dropped_actual as dropped_treatment
        from trial_site_mce_ctms
        """)
        ctms_entered_study = ctms_entered_study.dropDuplicates()
        ctms_entered_study.registerTempTable('ctms_entered_study')
        # ctms_entered_study.write.mode("overwrite").saveAsTable("ctms_entered_study")
        ctms_dates = spark.sql("""select ctfo_site_id,ctfo_trial_id,
        site_initiation_visit_actual_dt as siv_dt,
        first_subject_first_visit_actual_dt  as fsfv_dt,
        last_subject_first_visit_actual_dt as lsfv_dt,
        cntry_last_subject_first_treatment_actual_dt as cntry_lsfr_dt,
        last_subject_first_treatment_actual_dt as lsfr_dt,
        first_subject_first_treatment_actual_dt as fsfr_dt,
        last_subject_last_visit_actual_dt as lslv_dt
        from trial_site_mce_ctms """)
        ctms_dates = ctms_dates.dropDuplicates()
        ctms_dates.registerTempTable('ctms_dates')
        # ctms_dates.write.mode("overwrite").saveAsTable("ctms_dates")
        # changed
        ctms_ppm = spark.sql(
            """select ctfo_site_id, ctfo_trial_id, patients_randomized_planned as ppm from trial_site_mce_ctms """)
        ctms_ppm = ctms_ppm.dropDuplicates()
        ctms_ppm.registerTempTable('ctms_ppm')
        # ctms_ppm.write.mode("overwrite").saveAsTable("ctms_ppm")

        #### randomization_rate

        ctms_percentage_randomized = spark.sql(""" select a.ctfo_site_id,a.ctfo_trial_id,
        case when (a.entered_treatment is null  or  b.ppm is null)  then null 
             when b.ppm==0 then null 
             else (a.entered_treatment/b.ppm ) end as percentage_randomized 
        from
        ctms_entered_study a
        left join
        ctms_ppm b on lower(trim(a.ctfo_site_id)) = lower(trim(b.ctfo_site_id)) and lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id))
        group by 1,2,3
        """)
        ctms_percentage_randomized = ctms_percentage_randomized.dropDuplicates()
        ctms_percentage_randomized.registerTempTable('ctms_percentage_randomized')

        ctms_elapsed_duration = spark.sql("""select B.ctfo_trial_id, B.ctfo_site_id,fsiv_fsfr_delta,
        case when (B.num is null or B.den is null or B.den=0) then null when B.num==0 then 0 else (B.num/B.den) end as elapsed_duration 
        from
            (select A.ctfo_trial_id,
                A.ctfo_site_id,fsiv_fsfr_delta,

                           (datediff(COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,fsiv_fsfr_delta)),A.lsfr_curr_dt)) as den,
                           (datediff(COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,fsiv_fsfr_delta)),CURRENT_DATE())) as num
                from
                (select ctfo_trial_id, ctfo_site_id, study_last_subject_first_treatment_planned_dt as lsfr_curr_dt, 
                study_first_subject_first_treatment_planned_dt as fsfr_dt, 
                study_first_site_initiation_visit_planned_dt as fsiv_dt,fsiv_fsfr_delta from
                trial_site_mce_ctms) A 
                       group by 1,2,3,4,5) B 
        group by 1,2,3,4""")
        ctms_elapsed_duration = ctms_elapsed_duration.dropDuplicates()
        ctms_elapsed_duration.registerTempTable('ctms_elapsed_duration')
        # ctms_elapsed_duration.write.mode("overwrite").saveAsTable('ctms_elapsed_duration')
        ctms_randomization_duration = spark.sql("""select cgsm.ctfo_site_id, cgsm.ctfo_trial_id,
        case
        when lower(trim(cgsm.site_trial_status)) in {on_status}
        then
        case when d.siv_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),d.siv_dt)/30.4
        when d.siv_dt is null and d.fsfr_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),date_sub(d.fsfr_dt,fsiv_fsfr_delta))/30.4
        when COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is null and e.percentage_randomized>0.5 and (d.siv_dt is not null or d.fsfr_dt is not null) then datediff(current_date(),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,fsiv_fsfr_delta)))/30.4
        when COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is null and e.percentage_randomized<0.5 and (d.siv_dt is not null or d.fsfr_dt is not null) and ced.elapsed_duration > 0.5 then (datediff(current_date(),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,fsiv_fsfr_delta))))/30.4
        else null
        end
        when lower(trim(cgsm.site_trial_status)) in {com_status}
        then  case when d.siv_dt is null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,fsiv_fsfr_delta)))/30.4 
        when d.siv_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),d.siv_dt)/30.4
        when d.siv_dt is null and d.fsfr_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),date_sub(d.fsfr_dt,fsiv_fsfr_delta))/30.4
        when COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is null and (d.siv_dt is not null or d.fsfr_dt is not null) and d.cntry_lsfr_dt is not null then datediff(d.cntry_lsfr_dt,COALESCE(d.siv_dt,date_sub(d.fsfr_dt,fsiv_fsfr_delta)))/30.4
        end
        else null
        end as duration
        from
        (select ctfo_trial_id, ctfo_site_id,site_trial_status from trial_site_mce_ctms group by 1,2,3) cgsm
        left join ctms_dates d on lower(trim(cgsm.ctfo_site_id)) = lower(trim(d.ctfo_site_id)) and lower(trim(cgsm.ctfo_trial_id)) = lower(trim(d.ctfo_trial_id))
        left join ctms_elapsed_duration ced on lower(trim(cgsm.ctfo_site_id)) = lower(trim(ced.ctfo_site_id)) and lower(trim(cgsm.ctfo_trial_id)) = lower(trim(ced.ctfo_trial_id))
        left join ctms_percentage_randomized e on lower(trim(cgsm.ctfo_site_id)) = lower(trim(e.ctfo_site_id)) and lower(trim(cgsm.ctfo_trial_id)) = lower(trim(e.ctfo_trial_id))
        group by 1,2,3""".format(on_status=Ongoing_variable, com_status=Completed_variable))
        ctms_randomization_duration = ctms_randomization_duration.dropDuplicates()
        # ctms_randomization_duration.registerTempTable('ctms_randomization_duration')
        ctms_randomization_duration.write.mode("overwrite").saveAsTable("ctms_randomization_duration")

        ctms_randomization_rate_site = spark.sql("""
        select a.ctfo_site_id, a.ctfo_trial_id,
        case when (b.entered_treatment is null or a.duration is null or a.duration==0) then null 
             when b.entered_treatment==0 then 0 
             else (b.entered_treatment/a.duration) end as randomization_rate
        from
        ctms_randomization_duration a
        left join ctms_entered_study b on lower(trim(a.ctfo_site_id)) = lower(trim(b.ctfo_site_id)) and lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id))
        group by 1,2,3
        """)
        ctms_randomization_rate_site = ctms_randomization_rate_site.dropDuplicates()
        ctms_randomization_rate_site.registerTempTable('ctms_randomization_rate_site')
        ctms_randomization_rate_site.write.mode("overwrite").saveAsTable("ctms_randomization_rate_site")

        dqs_randomization_rate = spark.sql("""
        select C.ctfo_trial_id,C.ctfo_site_id,C.recruitment_duration, 
        Case when (C.patients_randomized is null or C.recruitment_duration is null or C.recruitment_duration==0) then null 
             when C.patients_randomized==0 then 0 
             else C.patients_randomized/C.recruitment_duration end as dqs_randomization_rate from
        (select A.ctfo_trial_id,A.ctfo_site_id, A.patients_randomized,
        case when lower(trim(A.trial_status)) in {com_status}
                    then
                            case when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
                            then datediff(coalesce(A.last_subject_randomized_dt,A.cntry_last_subject_randomized_dt),A.site_ready_to_enroll_dt)/30.4
                            when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
                            then datediff(coalesce(A.last_subject_randomized_dt,A.cntry_last_subject_randomized_dt),A.first_subject_randomized_dt)/30.4
                            when A.first_subject_randomized_dt is null
                            then datediff(coalesce(A.last_subject_randomized_dt,A.cntry_last_subject_randomized_dt),A.site_ready_to_enroll_dt)/30.4
                            when A.site_ready_to_enroll_dt is null
                            then datediff(coalesce(A.last_subject_randomized_dt,A.cntry_last_subject_randomized_dt),A.first_subject_randomized_dt)/30.4
                            end
             when lower(trim(A.trial_status)) in {on_status}
                    then
                            case when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
                            then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.site_ready_to_enroll_dt)/30.4
                            when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
                            then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.first_subject_randomized_dt)/30.4
                            when A.first_subject_randomized_dt is null
                            then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.site_ready_to_enroll_dt)/30.4
                            when A.site_ready_to_enroll_dt is null
                            then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.first_subject_randomized_dt)/30.4
                            end
        else null end as recruitment_duration
        from
        (select ctfo_trial_id,ctfo_site_id, patients_randomized_actual as patients_randomized,site_ready_to_enrol_actual_dt as site_ready_to_enroll_dt,
        first_subject_first_treatment_actual_dt as first_subject_randomized_dt,
        site_trial_status as trial_status,last_subject_first_treatment_actual_dt as last_subject_randomized_dt ,cntry_last_subject_first_treatment_actual_dt as cntry_last_subject_randomized_dt
        from
        trial_site_mce_ir ) A
        ) C""".format(on_status=Ongoing_variable, com_status=Completed_variable))
        dqs_randomization_rate = dqs_randomization_rate.dropDuplicates()
        # dqs_randomization_rate.createOrReplaceTempView("dqs_randomization_rate_site")
        dqs_randomization_rate.write.mode("overwrite").saveAsTable("dqs_randomization_rate_site")

    if grain == "Trial_Country":
        trial_country_mce_ctms = spark.sql("""select * from trial_country_mce where lower(trim(source))='ctms'""")
        trial_country_mce_ctms = trial_country_mce_ctms.dropDuplicates()
        trial_country_mce_ctms.registerTempTable('trial_country_mce_ctms')
        trial_country_mce_ir = spark.sql("""select * from trial_country_mce where lower(trim(source))='ir'""")
        trial_country_mce_ir = trial_country_mce_ir.dropDuplicates()
        trial_country_mce_ir.registerTempTable('trial_country_mce_ir')
        country_ctms_kpis_2 = spark.sql("""
        select  
        a.ctfo_trial_id,
        a.patients_randomized_actual as  entered_treatment,
        a.patients_randomized_planned as planned_patients,
        case when (a.patients_randomized_actual is null or a.patients_randomized_planned  is null 
        or a.patients_randomized_planned =0) then null else (a.patients_randomized_actual/a.patients_randomized_planned )  end as percentage_randomized,
        a.no_of_sites_actual,
        a.country_trial_status as trial_status,
        a.country
        from
        trial_country_mce_ctms a  
        group by 1,2,3,4,5,6,7
        """.format(on_com=Ongoing_Completed))
        country_ctms_kpis_2 = country_ctms_kpis_2.dropDuplicates()
        country_ctms_kpis_2.registerTempTable('country_ctms_kpis_2')
        # ctms_kpis_2.write.mode("overwrite").saveAsTable("ctms_kpis_2")
        country_ctms_dates = spark.sql("""select   
        a.ctfo_trial_id,
        a.country,
        a.fsiv_fsfr_delta,
        min(a.fsiv_dt) as fsiv_dt,
        min(a.fsfr_dt) as fsfr_dt,
        max(a.lsfv_dt) as lsfv_dt,
        max(a.lsfr_dt) as lsfr_dt,
        max(a.cntry_lsfr_dt) as cntry_lsfr_dt
        from
        (select ctfo_trial_id,
        first_site_initiation_visit_actual_dt as fsiv_dt,
        first_subject_first_treatment_actual_dt as fsfr_dt,
        last_subject_first_visit_actual_dt as lsfv_dt, 
        country,
        lsfr_dt,
        last_subject_first_treatment_actual_dt as cntry_lsfr_dt,
        fsiv_fsfr_delta from trial_country_mce_ctms group by 1,2,3,4,5,6,7,8) a 
        group by 1,2,3""")

        country_ctms_dates = country_ctms_dates.dropDuplicates()
        country_ctms_dates.registerTempTable('country_ctms_dates')
        # ctms_dates.write.mode("overwrite").saveAsTable("ctms_dates")
        country_ctms_elapsed_duration = spark.sql("""
        select 
        B.ctfo_trial_id,
        B.country,
        case when (B.num is null or B.den  is null or  B.den==0) then null when B.num==0 then 0 
           else (B.num/B.den) end as elapsed_duration  
        from
        (select A.ctfo_trial_id,A.country, (datediff(coalesce(A.fsiv_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta)),A.lsfr_curr_dt)) as den,(datediff(coalesce(A.fsiv_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta)),CURRENT_DATE())) as num
        from
        (select a.ctfo_trial_id,a.country,a.fsiv_fsfr_delta,
        max(a.lsfr_curr_dt) as lsfr_curr_dt,
        min(a.fsiv_dt) as fsiv_dt,
        min(a.fsfr_dt) as fsfr_dt
        from
        (select ctfo_trial_id,country,last_subject_first_treatment_planned_dt as lsfr_curr_dt,first_site_initiation_visit_planned_dt as fsiv_dt,
        first_subject_first_treatment_planned_dt as fsfr_dt,fsiv_fsfr_delta  from
        trial_country_mce_ctms group by 1,2,3,4,5,6) a
        group by 1,2,3) A group by 1,2,3,4) B group by 1,2,3""")

        country_ctms_elapsed_duration = country_ctms_elapsed_duration.dropDuplicates()
        country_ctms_elapsed_duration.registerTempTable('country_ctms_elapsed_duration')
        # ctms_elapsed_duration.write.mode("overwrite").saveAsTable("ctms_elapsed_duration")
        country_ctms_randomiztion_duration = spark.sql("""
        select cgsm.ctfo_trial_id,cgsm.country,
        case
        when lower(trim(A.trial_status)) in {on_status}
        then
        case when A.fsiv_dt is not null and A.lsfr_dt is not null then datediff(A.lsfr_dt,A.fsiv_dt)/30.4
        when A.fsiv_dt is null and A.fsfr_dt is not null and A.lsfr_dt is not null then datediff(A.lsfr_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta))/30.4
        when A.lsfr_dt is null and A.percentage_randomized>0.5 and (A.fsiv_dt is not null or A.fsfr_dt is not null) then datediff(current_date(),COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta)))/30.4
        when A.lsfr_dt is null and A.percentage_randomized<0.5 and (A.fsiv_dt is not null or A.fsfr_dt is not null) and A.elapsed_duration > 0.5 then (datediff(current_date(),COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta))))/30.4
        else null
        end
        when lower(trim(A.trial_status)) in {com_status}
        then  case when A.fsiv_dt is null and A.lsfr_dt is not null then datediff(A.lsfr_dt,COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta)))/30.4
        when A.fsiv_dt is not null and A.lsfr_dt is not null then datediff(A.lsfr_dt,A.fsiv_dt)/30.4
        when A.fsiv_dt is null and A.fsfr_dt is not null and A.lsfr_dt is not null then datediff(A.lsfr_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta))/30.4
        when A.lsfr_dt is null and (A.fsiv_dt is not null or A.fsfr_dt is not null) and A.cntry_lsfr_dt is not null then datediff(A.cntry_lsfr_dt,COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta)))/30.4
        end
        else null
        end as duration,
        A.trial_status,A.fsfr_dt,A.lsfr_dt,A.fsiv_dt
        from
        (select ctfo_trial_id,country from trial_country_mce_ctms group by 1,2) cgsm
        left join
        ( select b.ctfo_trial_id,b.country,b.trial_status,d.fsfr_dt ,d.fsiv_dt,d.cntry_lsfr_dt,d.lsfr_dt,ced.elapsed_duration,b.percentage_randomized,d.fsiv_fsfr_delta from
        country_ctms_kpis_2 b
        left join (select cntry_lsfr_dt,fsfr_dt ,fsiv_dt,lsfr_dt,ctfo_trial_id,country,fsiv_fsfr_delta from country_ctms_dates group by 1,2,3,4,5,6,7) d 
        on lower(trim(b.ctfo_trial_id))=lower(trim(d.ctfo_trial_id)) and lower(trim(b.country))=lower(trim(d.country))
        left join
        (select ctfo_trial_id, country,elapsed_duration from
        country_ctms_elapsed_duration group by 1,2,3) ced 
        on lower(trim(b.ctfo_trial_id))=lower(trim(ced.ctfo_trial_id)) and lower(trim(b.country))=lower(trim(ced.country)) ) A
        on lower(trim(A.ctfo_trial_id))=lower(trim(cgsm.ctfo_trial_id)) and lower(trim(A.country))=lower(trim(cgsm.country))
        group by 1,2,3,4,5,6,7
        """.format(on_status=Ongoing_variable, com_status=Completed_variable))
        country_ctms_randomiztion_duration = country_ctms_randomiztion_duration.dropDuplicates()
        # ctms_randomiztion_duration.registerTempTable('ctms_randomiztion_duration')
        country_ctms_randomiztion_duration.write.mode("overwrite").saveAsTable("country_ctms_randomiztion_duration")

        country_ctms_randomization_rate = spark.sql("""
        select cgsm.ctfo_trial_id,cgsm.country,
        case when (A.entered_treatment_per_site is null or A.duration is null or  A.duration==0) then null
        when A.entered_treatment_per_site == 0 then 0
        else A.entered_treatment_per_site/A.duration end as randomization_rate,
        A.duration as enrollment_duration
        from
        (select ctfo_trial_id,country from trial_country_mce_ctms group by 1,2) cgsm
        left join
        ( select a.ctfo_trial_id,a.country,a.duration,b.entered_treatment/b.no_of_sites_actual as entered_treatment_per_site from
        ( select country, ctfo_trial_id,duration from country_ctms_randomiztion_duration group by 1,2,3) a
        left join
        country_ctms_kpis_2 b on lower(trim(b.ctfo_trial_id))=lower(trim(a.ctfo_trial_id)) and lower(trim(b.country))=lower(trim(a.country)) group by 1,2,3,4) A
        on lower(trim(A.ctfo_trial_id))=lower(trim(cgsm.ctfo_trial_id))  and lower(trim(cgsm.country))=lower(trim(A.country))
        group by 1,2,3,4
        """)
        country_ctms_randomization_rate = country_ctms_randomization_rate.dropDuplicates()
        # ctms_randomization_rate.registerTempTable('ctms_randomization_rate_mce')
        country_ctms_randomization_rate.write.mode("overwrite").saveAsTable("country_ctms_randomization_rate")

        ################### CTMS Enrollment Duration##############################
        country_ctms_enrollment_duration = spark.sql("""
        select cgsm.ctfo_trial_id,cgsm.country,duration as enrollment_duration from 
        (select ctfo_trial_id,country from trial_country_mce_ctms) cgsm 
        left join 
        country_ctms_randomiztion_duration ccrd on lower(trim(ccrd.ctfo_trial_id))=lower(trim(cgsm.ctfo_trial_id)) and lower(trim(ccrd.country))=lower(trim(cgsm.country)) 
        group by 1,2,3        
        """)
        country_ctms_enrollment_duration = country_ctms_enrollment_duration.dropDuplicates()
        # country_ctms_enrollment_duration.registerTempTable('country_ctms_enrollment_duration')
        country_ctms_enrollment_duration.write.mode("overwrite").saveAsTable("country_ctms_enrollment_duration")

        ########## DQS RR#################
        dqs_site_details = spark.sql("""SELECT inner_q.*
            FROM (
                SELECT B.ctfo_trial_id, B.country
                    ,B.patients_randomized_actual,first_subject_first_visit_actual_dt as first_subject_enrolled_dt
                        ,first_subject_first_treatment_actual_dt as first_subject_randomized_dt ,last_subject_first_visit_actual_dt as last_subject_enrolled_dt
                        ,last_subject_last_visit_actual_dt as last_subject_last_visit_dt,site_ready_to_enrol_actual_dt as site_ready_to_enroll_dt,lsfr_dt as last_subject_randomized_dt
                    ,country_trial_status
                FROM  (
                    SELECT ctfo_trial_id, country
                        ,patients_randomized_actual,first_subject_first_visit_actual_dt
                        ,first_subject_first_treatment_actual_dt,last_subject_first_visit_actual_dt
                        ,last_subject_last_visit_actual_dt,lsfr_dt,country_trial_status,site_ready_to_enrol_actual_dt
                    FROM trial_country_mce_ir
                    GROUP BY 1,2,3,4,5,6,7,8,9,10) B 
                ) AS inner_q""")

        dqs_site_details = dqs_site_details.dropDuplicates()
        dqs_site_details.registerTempTable('dqs_site_details')

        country_dqs_randomization_rate = spark.sql("""
            select C.ctfo_trial_id,
             case when (C.patients_randomized is null or C.recruitment_duration  is null or  C.recruitment_duration==0) then null
             when C.patients_randomized==0 then 0
             else C.patients_randomized/C.recruitment_duration end as dqs_randomization_rate,C.recruitment_duration as enrollment_duration,
             C.country from
             (select A.ctfo_trial_id, 
             case when (d.patients_randomized_actual is null or d.no_of_gid  is null or  d.no_of_gid==0) then null
             else d.patients_randomized_actual/d.no_of_gid end as patients_randomized,
             A.country as country,
             case when lower(trim(A.trial_status)) in {com_status}
             then
             case when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
             then datediff(A.last_subject_randomized_dt,A.site_ready_to_enroll_dt)/30.4
             when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
             then datediff(A.last_subject_randomized_dt,A.first_subject_randomized_dt)/30.4
             when A.first_subject_randomized_dt is null
             then datediff(A.last_subject_randomized_dt,A.site_ready_to_enroll_dt)/30.4
             when A.site_ready_to_enroll_dt is null
             then datediff(A.last_subject_randomized_dt,A.first_subject_randomized_dt)/30.4
                                        end
             when lower(trim(A.trial_status)) in {on_status}
             then
                                        case when A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
                                        then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.site_ready_to_enroll_dt)/30.4
                                        when A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
                                        then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.first_subject_randomized_dt)/30.4
                                        when A.first_subject_randomized_dt is null
                                        then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.site_ready_to_enroll_dt)/30.4
                                        when A.site_ready_to_enroll_dt is null
                                        then datediff(coalesce(A.last_subject_randomized_dt,current_date()),A.first_subject_randomized_dt)/30.4
                                        end
             else null end as recruitment_duration
             from
             (select ctfo_trial_id,country,site_ready_to_enroll_dt,
             first_subject_randomized_dt,last_subject_randomized_dt, country_trial_status as trial_status from
             dqs_site_details group by 1,2,3,4,5,6) A
             left join (select patients_randomized_actual,no_of_sites_actual as no_of_gid,country,ctfo_trial_id from 
             trial_country_mce_ir  group by 1,2,3,4)d 
             on Lower(trim(A.country)) = Lower(trim(d.country)) and trim(A.ctfo_trial_id)=trim(d.ctfo_trial_id)
             group by 1,2,3,4) C
             group by 1,2,3,4""".format(on_status=Ongoing_variable, com_status=Completed_variable))
        country_dqs_randomization_rate = country_dqs_randomization_rate.dropDuplicates()
        # country_dqs_randomization_rate.registerTempTable('country_dqs_randomization_rate')
        country_dqs_randomization_rate.write.mode("overwrite").saveAsTable("country_dqs_randomization_rate")

        ##############################Enrollment duration#######################################
        country_dqs_enrollment_duration = spark.sql("""
        select cgsm.ctfo_trial_id,cgsm.country,ccrd.enrollment_duration from 
        (select ctfo_trial_id,country from trial_country_mce_ir) cgsm 
        left join 
        country_dqs_randomization_rate ccrd on lower(trim(ccrd.ctfo_trial_id))=lower(trim(cgsm.ctfo_trial_id)) and lower(trim(ccrd.country))=lower(trim(cgsm.country)) 
        group by 1,2,3        
        """)
        country_dqs_enrollment_duration = country_dqs_enrollment_duration.dropDuplicates()
        # country_dqs_enrollment_duration.registerTempTable('country_dqs_enrollment_duration')
        country_dqs_enrollment_duration.write.mode("overwrite").saveAsTable("country_dqs_enrollment_duration")

    if grain == "Trial_Inv":
        trial_inv_mce_ctms = spark.sql("""select * from trial_inv_mce where lower(trim(source))='ctms'""")
        trial_inv_mce_ctms = trial_inv_mce_ctms.dropDuplicates()
        trial_inv_mce_ctms.registerTempTable('trial_inv_mce_ctms')
        trial_inv_mce_ir = spark.sql("""select * from trial_inv_mce where lower(trim(source))='ir'""")
        trial_inv_mce_ir = trial_inv_mce_ir.dropDuplicates()
        trial_inv_mce_ir.registerTempTable('trial_inv_mce_ir')
        ctms_elapsed_duration_inv = spark.sql("""
        select 
        B.ctfo_trial_id,B.ctfo_investigator_id,ctfo_site_id,
        case when (B.num is null or B.den  is null or  B.den==0) then null when B.num==0 then 0 else (B.num/B.den) end as elapsed_duration 
        from
        (select A.ctfo_trial_id,A.ctfo_investigator_id,ctfo_site_id,(datediff(coalesce(A.fsiv_dt,date_sub(A.fsfr_dt,14)),A.lsfr_curr_dt)) as den,(datediff(coalesce(A.fsiv_dt,date_sub(A.fsfr_dt,14)),CURRENT_DATE())) as num
        from
        (select ctfo_trial_id,ctfo_investigator_id,ctfo_site_id,last_subject_first_treatment_planned_dt as lsfr_curr_dt,
        first_site_initiation_visit_planned_dt as fsiv_dt,
        first_subject_first_treatment_planned_dt as fsfr_dt from
        trial_inv_mce_ctms 
        group by 1,2,3,4,5,6) A group by 1,2,3,4,5) B group by 1,2,3,4""")
        ctms_elapsed_duration_inv = ctms_elapsed_duration_inv.dropDuplicates()
        ctms_elapsed_duration_inv.registerTempTable('ctms_elapsed_duration_inv')
        # ctms_elapsed_duration_inv.write.mode("overwrite").saveAsTable("ctms_elapsed_duration_inv")
        ctms_dates_inv = spark.sql("""select a.ctfo_trial_id,a.ctfo_investigator_id,a.ctfo_site_id,
        min(a.siv_dt) as siv_dt,
        min(a.fsfv_dt) as fsfv_dt,
        max(a.lsfv_dt) as lsfv_dt,
        max(a.cntry_lsfv_dt) as cntry_lsfv_dt,
        min(a.fsfr_dt) as fsfr_dt,
        max(a.lslv_dt) as lslv_dt,
        max(a.lsfr_dt) as lsfr_dt,
        max(a.lsfr_dt_final) as lsfr_dt_final,
        max(a.cntry_lsfr_dt) as cntry_lsfr_dt
        from

        (select ctfo_trial_id, ctfo_investigator_id,ctfo_site_id,
        siv_dt_f as siv_dt,
        first_subject_first_visit_actual_dt as fsfv_dt,last_subject_first_visit_actual_dt as lsfv_dt,
        first_subject_first_treatment_actual_dt as fsfr_dt,last_subject_last_visit_actual_dt as lslv_dt ,
        cntry_lsfv_dt,cntry_lsfr_dt,study_last_subject_first_treatment_actual_dt as lsfr_dt,lsfr_dt_final
        from trial_inv_mce_ctms group by 1,2,3,4,5,6,7,8,9,10,11,12) a
        group by 1,2,3""")
        ctms_dates_inv = ctms_dates_inv.dropDuplicates()
        ctms_dates_inv.registerTempTable('ctms_dates_inv')
        # ctms_dates_inv.write.mode("overwrite").saveAsTable("ctms_dates_inv")
        ctms_entered_study_inv = spark.sql("""select B.ctfo_trial_id,B.ctfo_investigator_id,B.ctfo_site_id,sum(B.patients_randomized_actual) as entered_treatment
        from
        (select ctfo_trial_id, ctfo_investigator_id,ctfo_site_id,
         patients_randomized_actual
        from trial_inv_mce_ctms group by 1,2,3,4) B
        group by 1,2,3""")
        ctms_entered_study_inv = ctms_entered_study_inv.dropDuplicates()
        ctms_entered_study_inv.registerTempTable('ctms_entered_study_inv')
        # ctms_entered_study_inv.write.mode("overwrite").saveAsTable("ctms_entered_study_inv")
        ctms_ppm_inv = spark.sql("""select B.ctfo_trial_id,B.ctfo_investigator_id,B.ctfo_site_id,sum(B.patients_randomized_planned) as ppm
        from
        (select ctfo_trial_id, ctfo_investigator_id,ctfo_site_id,
        patients_randomized_planned
        from trial_inv_mce_ctms group by 1,2,3,4) B
        group by 1,2,3""")
        ctms_ppm_inv = ctms_ppm_inv.dropDuplicates()
        ctms_ppm_inv.registerTempTable('ctms_ppm_inv')
        # ctms_ppm_inv.write.mode("overwrite").saveAsTable("ctms_ppm_inv")
        ctms_percentage_randomized_inv = spark.sql(""" select a.ctfo_trial_id,a.ctfo_investigator_id ,a.ctfo_site_id,case when a.entered_treatment is null or b.ppm is null 
        then null when  b.ppm==0 then null else  a.entered_treatment/b.ppm end as percentage_randomized
        from
        ctms_entered_study_inv a
        left join
        ctms_ppm_inv b on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id)) and lower(trim(a.ctfo_investigator_id))=lower(trim(b.ctfo_investigator_id)) and lower(trim(a.ctfo_site_id))=lower(trim(b.ctfo_site_id))
        group by 1,2,3,4""")
        ctms_percentage_randomized_inv = ctms_percentage_randomized_inv.dropDuplicates()
        ctms_percentage_randomized_inv.registerTempTable('ctms_percentage_randomized_inv')
        ctms_percentage_randomized_inv.write.mode("overwrite").saveAsTable("ctms_percentage_randomized_inv")

        ctms_randomiztion_duration_inv = spark.sql("""select cgsm.ctfo_trial_id,cgsm.ctfo_investigator_id ,cgsm.ctfo_site_id,
        case
        when lower(trim(b.site_trial_status)) in {on_status}
        then
        case when d.siv_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),d.siv_dt)/30.4
        when d.siv_dt is null and d.fsfr_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),date_sub(d.fsfr_dt,14))/30.4
        when COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is null and e.percentage_randomized>0.5 and (d.siv_dt is not null or d.fsfr_dt is not null) then datediff(current_date(),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14)))/30.4
        when COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is null and e.percentage_randomized<0.5 and (d.siv_dt is not null or d.fsfr_dt is not null) and ced.elapsed_duration> 0.5 then (datediff(current_date(),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14))))/30.4
        else null
        end
        when lower(trim(b.site_trial_status)) in {com_status}
        then  case when d.siv_dt is null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),COALESCE(d.siv_dt,date_sub(d.fsfr_dt,14)))/30.4
        when d.siv_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),d.siv_dt)/30.4
        when d.siv_dt is null and d.fsfr_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),date_sub(d.fsfr_dt,14))/30.4
        end
        else null
        end as duration
        from
        (select ctfo_trial_id, ctfo_investigator_id,ctfo_site_id from trial_inv_mce_ctms group by 1,2,3) cgsm
        left join ctms_dates_inv d on  lower(trim(cgsm.ctfo_trial_id)) = lower(trim(d.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(d.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(d.ctfo_site_id))
        left join ctms_elapsed_duration_inv ced on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(ced.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(ced.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(ced.ctfo_site_id))
        left join ctms_percentage_randomized_inv e on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(e.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(e.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(e.ctfo_site_id))
        left join trial_inv_mce_ctms b on lower(trim(cgsm.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id)) and lower(trim(cgsm.ctfo_investigator_id)) = lower(trim(b.ctfo_investigator_id)) and lower(trim(cgsm.ctfo_site_id)) = lower(trim(b.ctfo_site_id))
        group by 1,2,3,4""".format(on_status=Ongoing_variable, com_status=Completed_variable))
        ctms_randomiztion_duration_inv = ctms_randomiztion_duration_inv.dropDuplicates()
        # ctms_randomiztion_duration_inv.registerTempTable('ctms_randomiztion_duration_inv')
        ctms_randomiztion_duration_inv.write.mode("overwrite").saveAsTable("ctms_randomiztion_duration_inv")

        ctms_randomization_rate_inv = spark.sql("""
        select a.ctfo_trial_id,a.ctfo_investigator_id,a.ctfo_site_id, 
        case when (b.entered_treatment is null or a.duration is null or a.duration==0 ) then null 
        when b.entered_treatment==0 then 0
        else b.entered_treatment/a.duration end as ctms_randomization_rate
        from
        ctms_randomiztion_duration_inv a
        left join ctms_entered_study_inv b on lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id)) and lower(trim(a.ctfo_investigator_id)) = lower(trim(b.ctfo_investigator_id)) and lower(trim(a.ctfo_site_id)) = lower(trim(b.ctfo_site_id))
        group by 1,2,3,4
        """)

        ctms_randomization_rate_inv = ctms_randomization_rate_inv.dropDuplicates()
        # ctms_randomization_rate_inv.registerTempTable('ctms_randomization_rate_inv')
        ctms_randomization_rate_inv.write.mode("overwrite").saveAsTable("ctms_randomization_rate_inv")

        #########################################t Total_recruitemnt_months #############################################

        ctms_total_recruitment_months = spark.sql("""
        select a.ctfo_trial_id,a.ctfo_site_id,a.ctfo_investigator_id,
        CASE
         WHEN cast(a.enrollment_duration as double) < 0 THEN 0
         ELSE cast(a.enrollment_duration as double)
        END        AS total_recruitment_months
        from(
           select  cgsm.ctfo_trial_id, cgsm.ctfo_investigator_id,cgsm.ctfo_site_id,cast(crdi.duration as double) as enrollment_duration from
           (select ctfo_trial_id, ctfo_investigator_id,ctfo_site_id from trial_inv_mce_ctms group by 1,2,3) cgsm
           left join ctms_randomiztion_duration_inv crdi
           on lower(trim(crdi.ctfo_trial_id)) = lower(trim(cgsm.ctfo_trial_id)) and lower(trim(crdi.ctfo_investigator_id)) = lower(trim(cgsm.ctfo_investigator_id)) and lower(trim(crdi.ctfo_site_id)) = lower(trim(cgsm.ctfo_site_id))
           ) a
           group by 1,2,3,4
        """)
        ctms_total_recruitment_months = ctms_total_recruitment_months.dropDuplicates()
        # ctms_total_recruitment_months.registerTempTable('ctms_total_recruitment_months')
        ctms_total_recruitment_months.write.mode("overwrite").saveAsTable("ctms_total_recruitment_months")

        ###################### DQS RR #########################################
        dqs_randomization_rate_inv = spark.sql("""select C.ctfo_trial_id,C.ctfo_investigator_id,C.ctfo_site_id,C.recruitment_duration,
        (C.patients_randomized/C.recruitment_duration) as dqs_randomization_rate from 
        (select A.ctfo_trial_id,A.ctfo_investigator_id, A.ctfo_site_id,A.patients_randomized,
        case when lower(trim(A.trial_status)) in {com_status}
        then
           case when A.site_ready_to_enroll_dt is not null and A.first_subject_randomized_dt is not null and  A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
                  then datediff(coalesce(A.study_last_subject_randomized_dt), A.site_ready_to_enroll_dt)/30.4
             when  A.site_ready_to_enroll_dt is not null and A.first_subject_randomized_dt is not null and A.first_subject_randomized_dt<A.site_ready_to_enroll_dt
                  then datediff(coalesce(A.study_last_subject_randomized_dt), A.first_subject_randomized_dt)/30.4
             when A.first_subject_randomized_dt is null and A.site_ready_to_enroll_dt is not null
                  then datediff(coalesce(A.study_last_subject_randomized_dt), A.site_ready_to_enroll_dt)/30.4
             when A.site_ready_to_enroll_dt is null and A.first_subject_randomized_dt is not null
                then datediff(coalesce(A.study_last_subject_randomized_dt), A.first_subject_randomized_dt)/30.4
           end
        when lower(trim(A.trial_status)) in {on_status}
        then
           case
              when A.site_ready_to_enroll_dt is not null and A.first_subject_randomized_dt is not null and A.site_ready_to_enroll_dt<=A.first_subject_randomized_dt
              then datediff(coalesce(A.study_last_subject_randomized_dt,current_date()), A.site_ready_to_enroll_dt)/30.4
              when A.site_ready_to_enroll_dt is not null and A.first_subject_randomized_dt is not null and A.first_subject_randomized_dt<A.site_ready_to_enroll_dt 
                 then datediff(A.first_subject_randomized_dt,coalesce(A.study_last_subject_randomized_dt,current_date()))

              when A.first_subject_randomized_dt is null and A.site_ready_to_enroll_dt is not null 
              then datediff(coalesce(A.study_last_subject_randomized_dt,current_date()), A.site_ready_to_enroll_dt)/30.4
              when A.site_ready_to_enroll_dt is null and A.first_subject_randomized_dt is not null
              then datediff(coalesce(A.study_last_subject_randomized_dt,current_date()), A.first_subject_randomized_dt)/30.4
           end
        end
         as recruitment_duration
        from
        (select ctfo_trial_id,ctfo_investigator_id,ctfo_site_id, patients_randomized_actual as patients_randomized,
        site_ready_to_enroll_actual_dt as site_ready_to_enroll_dt,
        first_subject_first_treatment_actual_dt as first_subject_randomized_dt,site_trial_status as trial_status,cntry_lsfr_dt as study_last_subject_randomized_dt
          from trial_inv_mce_ir group by 1,2,3,4,5,6,7,8) A
         group by 1,2,3,4,5) C
        group by 1,2,3,4,5""".format(on_status=Ongoing_variable, com_status=Completed_variable))
        dqs_randomization_rate_inv = dqs_randomization_rate_inv.dropDuplicates()
        dqs_randomization_rate_inv.createOrReplaceTempView("dqs_randomization_rate_inv_test")
        dqs_randomization_rate_inv.write.mode("overwrite").saveAsTable("dqs_randomization_rate_inv")

        ############################## dqs total_recruitmnet_months ##############################

        dqs_total_recruitment_months = spark.sql("""
        select a.ctfo_trial_id,a.ctfo_site_id,a.ctfo_investigator_id,
        CASE
         WHEN cast(a.enrollment_duration as double) < 0 THEN 0
         ELSE cast(a.enrollment_duration as double)
        END        AS total_recruitment_months
        from(
           select  cgsm.ctfo_trial_id, cgsm.ctfo_investigator_id,cgsm.ctfo_site_id,cast(drri.recruitment_duration as double) as enrollment_duration from
           (select ctfo_trial_id, ctfo_investigator_id,ctfo_site_id from trial_inv_mce_ir group by 1,2,3) cgsm
           left join dqs_randomization_rate_inv drri
           on lower(trim(drri.ctfo_trial_id)) = lower(trim(cgsm.ctfo_trial_id)) and lower(trim(drri.ctfo_investigator_id)) = lower(trim(cgsm.ctfo_investigator_id)) and lower(trim(drri.ctfo_site_id)) = lower(trim(cgsm.ctfo_site_id))
           ) a
           group by 1,2,3,4
        """)

        dqs_total_recruitment_months = dqs_total_recruitment_months.dropDuplicates()
        # dqs_total_recruitment_months.registerTempTable('dqs_total_recruitment_months')
        dqs_total_recruitment_months.write.mode("overwrite").saveAsTable("dqs_total_recruitment_months")

    if grain == "Trial_Universe":
        trial_uni_mce_ctms = spark.sql("""select * from trial_uni_mce where lower(trim(source))='ctms'""")
        trial_uni_mce_ctms = trial_uni_mce_ctms.dropDuplicates()
        trial_uni_mce_ctms.registerTempTable('trial_uni_mce_ctms')
        trial_uni_mce_ir = spark.sql("""select * from trial_uni_mce where lower(trim(source))='ir'""")
        trial_uni_mce_ir = trial_uni_mce_ir.dropDuplicates()
        trial_uni_mce_ir.registerTempTable('trial_uni_mce_ir')

        ###########CTMS Randomization Rate ##########################################################

        ctms_dates_test = spark.sql("""select ctfo_trial_id,
        first_site_initiation_visit_actual_dt as fsiv_dt,
        cntry_lsfr_dt,
        last_subject_first_treatment_actual_dt as lsfr_dt,
        first_subject_first_treatment_actual_dt as fsfr_dt,
        fsiv_fsfr_delta
        from
        trial_uni_mce_ctms
        group by 1,2,3,4,5,6""")

        ctms_dates_test = ctms_dates_test.dropDuplicates()
        ctms_dates_test.registerTempTable('ctms_dates_test')
        # ctms_dates_test.write.mode("overwrite").saveAsTable("ctms_dates_test")

        ctms_percentage_randomized_test = spark.sql(""" select ctfo_trial_id ,
        case when (patients_randomized_actual is null or patients_randomized_planned  is null or patients_randomized_planned==0) then null 
        else (patients_randomized_actual/ patients_randomized_planned)  end as percentage_randomized
        from
        trial_uni_mce_ctms
        """)

        ctms_percentage_randomized_test = ctms_percentage_randomized_test.dropDuplicates()
        ctms_percentage_randomized_test.registerTempTable('ctms_percentage_randomized_test')
        # ctms_percentage_randomized_test.write.mode("overwrite").saveAsTable("ctms_percentage_randomized_test")

        # updated
        ctms_elapsed_duration_test = spark.sql("""select B.ctfo_trial_id,
        case when (B.num is null or B.den is null or B.den==0) then null when B.num==0 then 0
        else (B.num/B.den)  end as elapsed_duration 
        from
        (select   A.ctfo_trial_id,(datediff(COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta)),A.lsfr_curr_dt)) as den,
        (datediff(COALESCE(A.fsiv_dt,date_sub(A.fsfr_dt,A.fsiv_fsfr_delta)),CURRENT_DATE())) as num
        from
        (select ctfo_trial_id,
        last_subject_first_treatment_planned_dt as lsfr_curr_dt,
        first_subject_first_treatment_planned_dt as fsfr_dt,
        first_site_initiation_visit_planned_dt as fsiv_dt,
        fsiv_fsfr_delta
        from
        trial_uni_mce_ctms
        group by 1,2,3,4,5) A group by 1,2,3) B group by 1,2""")

        ctms_elapsed_duration_test = ctms_elapsed_duration_test.dropDuplicates()
        ctms_elapsed_duration_test.registerTempTable('ctms_elapsed_duration_test')
        # ctms_elapsed_duration_test.write.mode("overwrite").saveAsTable("ctms_elapsed_duration_test")

        ctms_randomization_duration_uni = spark.sql("""
        select xref.ctfo_trial_id,
        case
                       when lower(trim(xref.standard_status)) in {on_status}
                       then
                                      case when d.fsiv_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null 
                                                     then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),d.fsiv_dt)/30.4
                                      when d.fsiv_dt is null and d.fsfr_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null 
                                                     then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),date_sub(d.fsfr_dt,d.fsiv_fsfr_delta))/30.4
                                      when COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is null and e.percentage_randomized>0.5 and (d.fsiv_dt is not null or d.fsfr_dt is not null) 
                                                     then datediff(current_date(),COALESCE(d.fsiv_dt,date_sub(d.fsfr_dt,d.fsiv_fsfr_delta)))/30.4
                                      when COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is null and e.percentage_randomized<0.5 and (d.fsiv_dt is not null or d.fsfr_dt is not null) and ced.elapsed_duration > 0.5     then (datediff(current_date(),COALESCE(d.fsiv_dt,date_sub(d.fsfr_dt,d.fsiv_fsfr_delta))))/30.4
                                      else null
                                      end
                       when lower(trim(xref.standard_status)) in {com_status}
                                      then  case when d.fsiv_dt is null then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),COALESCE(d.fsiv_dt,date_sub(d.fsfr_dt,d.fsiv_fsfr_delta)))/30.4 
                       when d.fsiv_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null 
                                      then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),d.fsiv_dt)/30.4
                       when d.fsiv_dt is null and d.fsfr_dt is not null and COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is not null 
                                      then datediff(COALESCE(d.lsfr_dt,d.cntry_lsfr_dt),date_sub(d.fsfr_dt,d.fsiv_fsfr_delta))/30.4
                       when COALESCE(d.lsfr_dt,d.cntry_lsfr_dt) is null and (d.fsiv_dt is not null or d.fsfr_dt is not null) and d.cntry_lsfr_dt is not null 
                                      then datediff(d.cntry_lsfr_dt,COALESCE(d.fsiv_dt,date_sub(d.fsfr_dt,d.fsiv_fsfr_delta)))/30.4
                       end
        else null
        end as duration
        from
        (select ctfo_trial_id, standard_status from trial_uni_mce_ctms) xref
        left join ctms_dates_test d on lower(trim(xref.ctfo_trial_id)) = lower(trim(d.ctfo_trial_id))
        left join ctms_elapsed_duration_test ced on  lower(trim(xref.ctfo_trial_id)) = lower(trim(ced.ctfo_trial_id))
        left join ctms_percentage_randomized_test e on  lower(trim(xref.ctfo_trial_id)) = lower(trim(e.ctfo_trial_id))
        group by 1,2

        """.format(on_status=Ongoing_variable, com_status=Completed_variable))

        ctms_randomization_duration_uni = ctms_randomization_duration_uni.dropDuplicates()
        ctms_randomization_duration_uni.registerTempTable('ctms_randomization_duration_uni')
        # ctms_randomization_duration_uni.write.mode("overwrite").saveAsTable("ctms_randomization_duration_uni")

        # rate
        ctms_randomization_rate_uni = spark.sql("""
        select ctfo_trial_id, 
        case when (num is null or den is null or den==0) then null when num==0 then 0
        else (num/den)  end as randomization_rate
        from 
        (select a.ctfo_trial_id, case when (b.patients_randomized_actual is null or b.number_of_sites_actual is null or b.number_of_sites_actual==0) then null 
        else (b.patients_randomized_actual/b.number_of_sites_actual)  end as num,
        a.duration as den
        from
        ctms_randomization_duration_uni a
        left join 
        trial_uni_mce_ctms b on lower(trim(a.ctfo_trial_id)) = lower(trim(b.ctfo_trial_id))
        group by 1,2,3) group by 1,2
        """)
        ctms_randomization_rate_uni = ctms_randomization_rate_uni.dropDuplicates()
        ctms_randomization_rate_uni.registerTempTable('ctms_randomization_rate_uni')
        # ctms_randomization_rate_uni.write.mode("overwrite").saveAsTable("ctms_randomization_rate_uni")

        ########################################### DQS RR ########################################################

        dqs_randomization_duration_uni = spark.sql("""
        select ctfo_trial_id,
        case 
        when lower(trim(standard_status)) in {on_status}
        then
               case when first_subject_first_treatment_actual_dt is null and trial_start_dt_rd is  null then null
                    when first_subject_first_treatment_actual_dt<=trial_start_dt_rd and first_subject_first_treatment_actual_dt is not null and trial_start_dt_rd is not null then 
                    datediff(coalesce(last_subject_first_treatment_actual_dt,current_date()),first_subject_first_treatment_actual_dt)/30.4 
                    when first_subject_first_treatment_actual_dt>trial_start_dt_rd and trial_start_dt_rd is not null and first_subject_first_treatment_actual_dt is not null then
                    datediff(coalesce(last_subject_first_treatment_actual_dt,current_date()),trial_start_dt_rd)/30.4
                    when first_subject_first_treatment_actual_dt is null and trial_start_dt_rd is  not null then
                    datediff(coalesce(last_subject_first_treatment_actual_dt,current_date()),trial_start_dt_rd)/30.4 
                    when trial_start_dt_rd is null and first_subject_first_treatment_actual_dt is  not null then
                    datediff(coalesce(last_subject_first_treatment_actual_dt,current_date()),first_subject_first_treatment_actual_dt)/30.4
                    end

        when lower(trim(standard_status)) in {com_status}   
        then
                 case when first_subject_first_treatment_actual_dt is null and trial_start_dt_rd is  null then null
                      when last_subject_first_treatment_actual_dt is  null then null
                    when first_subject_first_treatment_actual_dt<=trial_start_dt_rd and first_subject_first_treatment_actual_dt is not null and trial_start_dt_rd is not null then 
                    datediff(last_subject_first_treatment_actual_dt,first_subject_first_treatment_actual_dt)/30.4
                    when first_subject_first_treatment_actual_dt>trial_start_dt_rd and trial_start_dt_rd is not null and first_subject_first_treatment_actual_dt is not null then
                    datediff(last_subject_first_treatment_actual_dt,trial_start_dt_rd)/30.4
                    when first_subject_first_treatment_actual_dt is null and trial_start_dt_rd is  not null then
                    datediff(last_subject_first_treatment_actual_dt,trial_start_dt_rd)/30.4 
                    when trial_start_dt_rd is null and first_subject_first_treatment_actual_dt is  not null then
                    datediff(last_subject_first_treatment_actual_dt,first_subject_first_treatment_actual_dt)/30.4
                    end  

        end as randomization_duration

        from trial_uni_mce_ir            

        """.format(on_status=Ongoing_variable, com_status=Completed_variable))

        dqs_randomization_duration_uni = dqs_randomization_duration_uni.dropDuplicates()
        dqs_randomization_duration_uni.registerTempTable('dqs_randomization_duration_uni')
        # dqs_randomization_duration_uni.write.mode('overwrite').saveAsTable('dqs_randomization_duration_uni')

        dqs_randomization_rate_uni = spark.sql("""
        select ctfo_trial_id,
        case when (num is null or den is null or den==0) then null when num==0 then 0
        else (num/den)  end as randomization_rate 
        from
        (select a.ctfo_trial_id,
         case when (a.patients_randomized_actual is null or a.number_of_sites_actual is null or a.number_of_sites_actual==0) then null 
         else (a.patients_randomized_actual/a.number_of_sites_actual)  end as num,
         b.randomization_duration as den
         from trial_uni_mce_ir a 
         left join dqs_randomization_duration_uni b 
         on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id)))
        """)
        dqs_randomization_rate_uni = dqs_randomization_rate_uni.dropDuplicates()
        dqs_randomization_rate_uni.registerTempTable('dqs_randomization_rate_uni')
        # dqs_randomization_rate_uni.write.mode('overwrite').saveAsTable('dqs_randomization_rate_uni')

