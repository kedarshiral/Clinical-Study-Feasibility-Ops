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

def Screen_failure_rate(grain):
    if grain == "Trial_site":
        trial_site_mce_ctms = spark.sql("""select * from trial_site_mce where lower(trim(source))='ctms'""")
        trial_site_mce_ctms = trial_site_mce_ctms.dropDuplicates()
        trial_site_mce_ctms.registerTempTable('trial_site_mce_ctms')
        ctms_screen_fail_rate = spark.sql("""select ctfo_trial_id, ctfo_site_id,
             case when lower(trim(trial_status)) in {on_sel_status}
                             and actual_date is not null
                then case when  (DROPPED_AT_SCREENING is null or entered_study is null )  then null
                                  when  (entered_study == 0)  then null
                                  else  ((DROPPED_AT_SCREENING)/(entered_study))*100
                         end
             when lower(trim(trial_status)) in {com_status}
                then case when  (DROPPED_AT_SCREENING is null or entered_study is null) then null
                                  when  (entered_study ==0)  then null
                                  else ((DROPPED_AT_SCREENING)/(entered_study))*100
                         end
         end as ctms_sfr from (select ctfo_trial_id,ctfo_site_id,site_trial_status as trial_status,patients_screened_actual as entered_study,dropped_at_screening_actual as DROPPED_AT_SCREENING ,last_subject_first_visit_actual_dt as actual_date from trial_site_mce_ctms
        group by 1,2,3,4,5,6)  group by 1,2,3 """.format(on_sel_status=Ongoing_variable, com_status=Completed_variable,status=Ongoing_Completed_Planned))

        ctms_screen_fail_rate = ctms_screen_fail_rate.dropDuplicates()
        #ctms_screen_fail_rate.registerTempTable('ctms_screen_fail_rate')
        ctms_screen_fail_rate.write.mode("overwrite").saveAsTable("ctms_screen_fail_rate_site")

    elif grain == "Trial_Country":
        trial_country_mce_ctms = spark.sql("""select * from trial_country_mce where lower(trim(source))='ctms'""")
        trial_country_mce_ctms = trial_country_mce_ctms.dropDuplicates()
        trial_country_mce_ctms.registerTempTable('trial_country_mce_ctms')

        trial_country_mce_dqs = spark.sql("""select * from trial_country_mce where lower(trim(source))='ir' """)
        trial_country_mce_dqs = trial_country_mce_dqs.dropDuplicates()
        trial_country_mce_dqs.registerTempTable('trial_country_mce_dqs')  
        
        ctms_screen_fail_rate_country = spark.sql("""select ctfo_trial_id,country,
        case when lower(trim(country_trial_status)) in {on_sel_status} 
                                and last_subject_first_visit_actual_dt is not null  
                        then case when  (dropped_at_screening_actual is null or patients_screened_actual is null or patients_screened_actual==0) then null  else ((dropped_at_screening_actual)/(patients_screened_actual))*100
                                end
                when lower(trim(country_trial_status)) in {com_status}
                        then case when  (dropped_at_screening_actual is null or patients_screened_actual is null or patients_screened_actual==0) then null  else ((dropped_at_screening_actual)/(patients_screened_actual))*100
                                end
        end as ctms_sfr 
        from trial_country_mce_ctms
        group by 1,2,3""".format(on_sel_status=Ongoing_variable, com_status=Completed_variable,status=Ongoing_Completed_Planned))
        
        ctms_screen_fail_rate_country = ctms_screen_fail_rate_country.dropDuplicates()
        #ctms_screen_fail_rate_country.registerTempTable("ctms_screen_fail_rate_country")
        ctms_screen_fail_rate_country.write.mode("overwrite").saveAsTable("ctms_screen_fail_rate_country")

        #validated the avaialability of columns used in ctms_screen_fail_rate df in trial_country_mce_ctms df 

        dqs_screen_failure_calc_temp = spark.sql("""select country,ctfo_trial_id,
        case when (sum(patients_consented_actual) is null or sum(patients_randomized_actual) is null) then null
        when sum(patients_consented_actual)==0 then null
        else ((sum(patients_consented_actual)-sum(patients_randomized_actual))/sum(patients_consented_actual))*100 end as dqs_sfr from 
         trial_country_mce_dqs group by 1,2""")
        dqs_screen_failure_calc_temp = dqs_screen_failure_calc_temp.dropDuplicates()
        dqs_screen_failure_calc_temp.registerTempTable("dqs_screen_failure_calc_temp")
        
        dqs_screen_failure_calc_country = spark.sql(""" select a.ctfo_trial_id, a.country, 
        case when lower(trim(country_trial_status)) in {on_status} and b.last_subject_first_visit_actual_dt is not null then a.dqs_sfr 
        when lower(trim(country_trial_status)) in {com_status} then a.dqs_sfr 
        else null 
        end as dqs_sfr from dqs_screen_failure_calc_temp a left join 
        trial_country_mce_dqs b 
        on lower(trim(a.ctfo_trial_id))=lower(trim(b.ctfo_trial_id))and lower(trim(a.country))=lower(trim(b.country))
        group by 1,2,3""".format(on_status=Ongoing_variable,com_status=Completed_variable))
        dqs_screen_failure_calc_country = dqs_screen_failure_calc_country.dropDuplicates()
        dqs_screen_failure_calc_country.write.mode("overwrite").saveAsTable("dqs_screen_failure_calc_country")

        #validated the avaialability of columns used in ctms_screen_fail_rate df in trial_country_mce_ctms df 

    elif grain == "Trial_Inv":
        trial_inv_mce_ctms = spark.sql("""select * from trial_inv_mce where lower(trim(source))='ctms'""")
        trial_inv_mce_ctms = trial_inv_mce_ctms.dropDuplicates()
        trial_inv_mce_ctms.registerTempTable('trial_inv_mce_ctms')
        ctms_sfr_inv = spark.sql("""select aa.ctfo_trial_id,aa.ctfo_investigator_id,aa.ctfo_site_id,
        case when lower(trim(aa.trial_status)) in {on_status} and aa.last_subject_first_visit_actual_dt is not null then  
            case when (aa.dropped_at_screening_actual is null or aa.patients_screened_actual is null or             aa.patients_screened_actual ==0) then null
                    else (aa.dropped_at_screening_actual/aa.patients_screened_actual )*100
                    end
            when lower(trim(aa.trial_status)) in {com_status}
                then
                case when (aa.dropped_at_screening_actual is null or aa.patients_screened_actual  is null or  aa.patients_screened_actual ==0) then null
                    else (aa.dropped_at_screening_actual/aa.patients_screened_actual )*100
                    end
        end as ctms_sfr from 
            (select ctfo_trial_id,ctfo_investigator_id,ctfo_site_id,site_trial_status as trial_status, 
            dropped_at_screening_actual,patients_screened_actual, last_subject_first_visit_actual_dt 
            from trial_inv_mce_ctms
            group by 1,2,3,4,5,6,7)aa group by 1,2,3,4""".format(on_status=Ongoing_variable, com_status=Completed_variable,status=Ongoing_Completed_Planned))
        ctms_sfr_inv = ctms_sfr_inv.dropDuplicates()
        #ctms_sfr_inv.registerTempTable('ctms_sfr_inv')
        ctms_sfr_inv.write.mode("overwrite").saveAsTable("ctms_sfr_inv")
        
        # validate the avaialability of dqs screen fail rate in investigator mce as it could be simply put in investigator MCE 
        # PERCENTILE(pct_screen_fail,0.5,10000) as pct_screen_fail as dqs_screen_fail_rate
        #COALESCE(impact.screen_failure_rate,dqs.screen_failure_rate) as screen_fail_rate
        
    elif grain == "Trial_Universe":
        trial_uni_mce_ctms = spark.sql("""select * from trial_uni_mce where lower(trim(source))='ctms'""")
        trial_uni_mce_ctms = trial_uni_mce_ctms.dropDuplicates()
        trial_uni_mce_ctms.registerTempTable('trial_uni_mce_ctms')
        
        # Read dqs tables here 

        ctms_screen_fail_rate_uni = spark.sql("""
        select ctfo_trial_id,standard_status,
        case when lower(trim(standard_status)) in  {on_status}
        and last_subject_first_visit_actual_dt is not null
        then case when (dropped_at_screening_actual is null or patients_screened_actual  is null or  patients_screened_actual ==0) then null
             else (dropped_at_screening_actual/patients_screened_actual )*100
             end
        when lower(trim(standard_status)) in {com_status}
        then case when   (dropped_at_screening_actual is null or patients_screened_actual  is null or  patients_screened_actual ==0) then null
            else (dropped_at_screening_actual/patients_screened_actual )*100
             end
        end as ctms_sfr
        from trial_uni_mce_ctms 
        group by 1,2,3 """.format(on_status=Ongoing_variable, com_status=Completed_variable, on_com_status=Ongoing_Completed))
        ctms_screen_fail_rate_uni = ctms_screen_fail_rate_uni.dropDuplicates()
        #ctms_screen_fail_rate_uni.write.mode("overwrite").saveAsTable("ctms_screen_fail_rate_uni")
        ctms_screen_fail_rate_uni.registerTempTable("ctms_screen_fail_rate_uni")
        
        
        
