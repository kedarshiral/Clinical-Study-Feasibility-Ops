################################# Module Information ######################################
#  Module Name         : Investigator records combine
#  Purpose             : This will execute queries for creating
# temp_all_inv_dedupe_results_final table
#  Pre-requisites      : Source table required: temp_all_inv_match_score
#  Last changed on     : 24-05-2021
#  Last changed by     : Sandeep Kumar/Kuldeep/Himanshi
#  Reason for change   : NA
#  Return Values       : NA
############################################################################################

import os
import json
import datetime
import multiprocessing as mp
import multiprocessing.pool
from pyhive import hive
import sys
import dedupe
from DedupeUtility import KMValidate
import CommonConstants as CommonConstants
from ConfigUtility import JsonConfigUtility
from pyspark.sql.functions import initcap

sys.path.insert(1, CommonConstants.EMR_CODE_PATH+'/configs/job_executor')


configuration = JsonConfigUtility(CommonConstants.AIRFLOW_CODE_PATH + '/' + CommonConstants.ENVIRONMENT_CONFIG_FILE)
bucket_path = configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])


timeStart = datetime.datetime.now()

path = bucket_path + '/applications/commons/temp/mastering/investigator/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'

# Reading the final dataframe from Dedupe io
temp_all_inv_match_score = spark.sql("""
select * from temp_all_inv_match_score
""")
temp_all_inv_match_score.registerTempTable('temp_all_inv_match_score')
#write_path = path.replace('table_name', 'temp_all_inv_match_score')
#temp_all_inv_match_score.repartition(100).write.mode('overwrite').parquet(write_path)

spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_match_score PARTITION (
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id'
    )
    SELECT *
    FROM temp_all_inv_match_score
    """)

CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_match_score')

temp_all_inv_dedupe_results = spark.sql("""
select
    match_score.cluster_id,
    match_score.score,
    data_prep.uid,
    data_prep.hash_uid,
    data_prep.data_source as data_src_nm,
    data_prep.ds_inv_id,
    data_prep.name,
    data_prep.email,
    data_prep.phone
from temp_all_inv_data_prep data_prep
left outer join temp_all_inv_match_score match_score
on trim(data_prep.uid) = trim(match_score.uid)
""")
temp_all_inv_dedupe_results = temp_all_inv_dedupe_results.dropDuplicates()
temp_all_inv_dedupe_results.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results')
temp_all_inv_dedupe_results.repartition(100).write.mode('overwrite').parquet(write_path)


#Declustering the records where following conditions hold true within a cluster :
#1) First Name!=First Name or Last Name!= Last Name
#2) First Name!=Last Name or First Name!= Last Name
A1=spark.sql("""
select *,
    regexp_replace(substring_index(trim(lower(name)), ' ', 1),
        '[^0-9A-Za-z_ ]','') as first_name,
    regexp_replace(substring_index(trim(lower(name)), ' ', -1),
        '[^0-9A-Za-z_ ]','') as last_name
    from temp_all_inv_dedupe_results where cluster_id is not null and name is not null and trim(name)!=''""")
A1.registerTempTable("A1")

temp_all_inv_dedupe_results_1=spark.sql(""" select distinct
case when dedupe_result_1.temp_cluster_id is not null then 'Rule_01'
when base_0.cluster_id is not null and dedupe_result_1.grouped_uid is null then 'Rule_01'
else null
end as rule_id,
case when dedupe_result_1.temp_cluster_id is not null then dedupe_result_1.temp_cluster_id
when base_0.cluster_id is not null and dedupe_result_1.grouped_uid is null then concat('unclustered',ds_inv_id)
else base_0.cluster_id
end as cluster_id,
case when dedupe_result_1.temp_cluster_id is not null then '0.80'
when base_0.cluster_id is not null and dedupe_result_1.grouped_uid is null then 0
else score
end as score,
base_0.uid,
base_0.hash_uid,
base_0.data_src_nm,
base_0.ds_inv_id,
base_0.name,
base_0.email,
base_0.phone
from temp_all_inv_dedupe_results base_0
left outer join
(select uid_1 as grouped_uid, concat_ws('_',collect_set(new_uid)) as temp_cluster_id from
(select uid_1, concat_ws('|',uid_1,uid_2) as temp_uid
from 
(select A1.uid as uid_1,
A1.cluster_id as temp_cluster_id, A1.first_name,A1.last_name,
A2.uid as uid_2
from
 A1 inner join
 A1 as A2
	on lower(trim(A1.cluster_id))=lower(trim(A2.cluster_id)) and lower(trim(A1.uid))!=lower(trim(A2.uid)) and ((lower(trim(A1.first_name))=lower(trim(A2.first_name)) and lower(trim(A1.last_name))=lower(trim(A2.last_name))) or (lower(trim(A1.first_name))=lower(trim(A2.last_name)) and lower(trim(A1.last_name))=lower(trim(A2.first_name))))))
	lateral view outer explode(split(temp_uid,'\\\|'))one as new_uid group by 1) dedupe_result_1
	on lower(trim(base_0.uid))=lower(trim(dedupe_result_1.grouped_uid))
""")
temp_all_inv_dedupe_results_1 = temp_all_inv_dedupe_results_1.dropDuplicates()
temp_all_inv_dedupe_results_1.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results_1')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_1')
temp_all_inv_dedupe_results_1.repartition(100).write.mode('overwrite').parquet(write_path)

#Clustering the records where different where the following conditions hold true :
#1) First Name=First Name and Last Name= Last Name and atleast one phone or email(without domain@) matches
#2) First Name=Last Name and First Name= Last Name and atleast one phone or email (without domain@) matches
B1=spark.sql("""
select *, case when cluster_id is null then concat_ws('',first_name, last_name, new_phone) else cluster_id end as temp_cluster_id from
(select *,
    regexp_replace(substring_index(trim(lower(name)), ' ', 1),
        '[^0-9A-Za-z_ ]','') as first_name,
    regexp_replace(substring_index(trim(lower(name)), ' ', -1),
        '[^0-9A-Za-z_ ]','') as last_name,
		regexp_replace(trim(temp_phone),"[^0-9|]",'') as new_phone
from temp_all_inv_dedupe_results_1
	lateral view outer explode(split(phone,'\\\|'))one as temp_phone
where name is not null and trim(name)!='' and temp_phone is not null and trim(temp_phone)!='')
	""")
B1.registerTempTable('B1')


B2=spark.sql("""
select *, case when cluster_id is null then concat_ws('',first_name, last_name, new_email) else cluster_id end as temp_cluster_id from
(select *,
    regexp_replace(substring_index(trim(lower(name)), ' ', 1),
        '[^0-9A-Za-z_ ]','') as first_name,
    regexp_replace(substring_index(trim(lower(name)), ' ', -1),
        '[^0-9A-Za-z_ ]','') as last_name,
		split(regexp_replace(lower(trim(temp_email)),'[^0-9A-Za-z_@| ]','' ),'@')[0] as new_email
from temp_all_inv_dedupe_results_1
	lateral view outer explode(split(email,'\\\|'))two as temp_email
where name is not null and trim(name)!='' and temp_email is not null and trim(temp_email)!='')
	""")
B2.registerTempTable('B2')

temp1=spark.sql("""
select CONCAT_WS('',coalesce(A.first_name,''),coalesce(A.last_name,''),A.new_phone) as key,
 A.uid as new_uid, A.temp_cluster_id as new_cluster_id
from
 B1 as A ,B1 as C
	where lower(trim(A.temp_cluster_id))!=lower(trim(C.temp_cluster_id)) and 
	lower(trim(A.uid))!=lower(trim(C.uid)) and lower(trim(A.first_name))=lower(trim(C.first_name)) and lower(trim(A.last_name))=lower(trim(C.last_name)) and lower(trim(A.new_phone))=lower(trim(C.new_phone))
UNION
select CONCAT_WS('',coalesce(A.first_name,''),coalesce(A.last_name,''),A.new_phone) as key,
 A.uid as new_uid, A.temp_cluster_id as new_cluster_id
from
 B1 as A ,B1 as C
	where lower(trim(A.temp_cluster_id))!=lower(trim(C.temp_cluster_id)) and 
	lower(trim(A.uid))!=lower(trim(C.uid)) and lower(trim(A.last_name))=lower(trim(C.first_name)) and lower(trim(A.first_name))=lower(trim(C.last_name)) and lower(trim(A.new_phone))=lower(trim(C.new_phone))
UNION
select CONCAT_WS('',coalesce(A.first_name,''),coalesce(A.last_name,''),A.new_email) as key,
 A.uid as new_uid, A.temp_cluster_id as new_cluster_id
from
 B2 as A ,B2 as C
	where lower(trim(A.temp_cluster_id))!=lower(trim(C.temp_cluster_id)) and 
	lower(trim(A.uid))!=lower(trim(C.uid)) and lower(trim(A.first_name))=lower(trim(C.first_name)) and lower(trim(A.last_name))=lower(trim(C.last_name)) and lower(trim(A.new_email))=lower(trim(C.new_email))
UNION
select CONCAT_WS('',coalesce(A.first_name,''),coalesce(A.last_name,''),A.new_email) as key,
 A.uid, A.temp_cluster_id
from
 B2 as A ,B2 as C
	where lower(trim(A.temp_cluster_id))!=lower(trim(C.temp_cluster_id)) and 
	lower(trim(A.uid))!=lower(trim(C.uid)) and lower(trim(A.last_name))=lower(trim(C.first_name)) and lower(trim(A.first_name))=lower(trim(C.last_name)) and lower(trim(A.new_email))=lower(trim(C.new_email))
""")
temp1.write.mode('overwrite').saveAsTable('temp1')

temp3=spark.sql("""select a.key, b.temp_uid, a.new_cluster_id from
temp1 a
left outer join 
(select key, concat_ws('|',collect_set(new_uid)) as temp_uid from temp1 group by 1) b
on lower(trim(a.key))=lower(trim(b.key))
""")
temp3.registerTempTable('temp3')


temp2=spark.sql("""select key, new_uid, new_cluster_id from
(select key, temp_uid, new_cluster_id from
(select distinct *, rank() over (partition by key order by new_cluster_id) as rnk from temp3)
where rnk=1)
lateral view outer explode(split(temp_uid,'\\\|'))two as new_uid
""")
temp2.registerTempTable('temp2')

B_Final_1=spark.sql("""
select distinct
case when dedupe_result_2.new_uid is not null then 'Rule_02'
else rule_id
end as rule_id,
case when dedupe_result_2.new_uid is not null then dedupe_result_2.new_cluster_id
else base_1.cluster_id
end as cluster_id,
case when dedupe_result_2.new_uid is not null then 0.80
else base_1.score
end as score,
base_1.uid,
base_1.hash_uid,
base_1.data_src_nm,
base_1.ds_inv_id,
base_1.name,
base_1.email,
base_1.phone 
from temp_all_inv_dedupe_results_1 base_1
left outer join
temp2 dedupe_result_2
on lower(trim(base_1.uid))=lower(trim(dedupe_result_2.new_uid))
	""")
B_Final_1.write.mode('overwrite').saveAsTable('B_Final_1')

temp_all_inv_dedupe_results_2=spark.sql("""select rule_id,
cluster_id,
score,
uid, hash_uid, data_src_nm, ds_inv_id, name, email, phone from 
(select *,
row_number() over (partition by uid order by cluster_id) as rnk from B_Final_1 where uid in
(select uid from B_Final_1 group by 1 having count(*)>1)) where rnk=1
union 
select * from B_Final_1 where uid not in (select uid from B_Final_1 group by 1 having count(*)>1)
""")
temp_all_inv_dedupe_results_2 = temp_all_inv_dedupe_results_2.dropDuplicates()
temp_all_inv_dedupe_results_2.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results_2')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_2')
temp_all_inv_dedupe_results_2.repartition(100).write.mode('overwrite').parquet(write_path)

# Update the records with existing clusters where first name & last name is matching but email & phone number is null
T1 = spark.sql("""
    select
                    uid,cluster_id,name,
                    regexp_replace(substring_index(trim(lower(name)), ' ', 1),'[^0-9A-Za-z_ ]','') as first_name,
           regexp_replace(substring_index(trim(lower(name)), ' ', -1),'[^0-9A-Za-z_ ]','') as last_name
    from temp_all_inv_dedupe_results_2 where (trim(phone) is null or trim(phone)='') and (trim(email) is null or trim(email)='') and name is not null """)
T1.registerTempTable('T1')

T2 = spark.sql("""
    select
                    A.uid,A.cluster_id,A.name,
                    regexp_replace(substring_index(trim(lower(A.name)), ' ', 1),'[^0-9A-Za-z_ ]','') as first_name,
           regexp_replace(substring_index(trim(lower(A.name)), ' ', -1),'[^0-9A-Za-z_ ]','') as last_name
    from temp_all_inv_dedupe_results_2 A left outer join T1 on lower(trim(A.uid))=lower(trim(T1.uid))
    where T1.uid is null""")
T2.registerTempTable('T2')

T3 = spark.sql("""
    select * from
    (select
                cluster_id,name,
         first_name,
        last_name,
        rank() over(partition by first_name ,last_name order by cluster_id ) rnk
    from T2 where cluster_id is not null ) where rnk = 1 """)
T3.registerTempTable('T3')

T4 = spark.sql("""
select T1.uid, T3.cluster_id, 
T1.name,
         T1.first_name,
        T1.last_name,
        'Rule_03' as rule_id,
        0.80 as score
from T1 left join T3 on lower(trim(T1.first_name))=lower(trim(T3.first_name)) and lower(trim(T1.last_name))=lower(trim(T3.last_name))
""")
T4.registerTempTable('T4')

temp_all_inv_dedupe_results_3 = spark.sql("""
    select distinct coalesce(T4.rule_id,temp1.rule_id) as rule_id,
           coalesce(T4.cluster_id, temp1.cluster_id) as cluster_id,
           coalesce(T4.score,temp1.score) as score,
           temp1.uid,
           temp1.hash_uid,
           temp1.data_src_nm,
           temp1.ds_inv_id,
           temp1.name,
           temp1.email,
           temp1.phone
    from temp_all_inv_dedupe_results_2 temp1 left join T4 on lower(trim(temp1.uid))=lower(trim(T4.uid))""")
temp_all_inv_dedupe_results_3 = temp_all_inv_dedupe_results_3.dropDuplicates()
temp_all_inv_dedupe_results_3.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results_3')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_3')
temp_all_inv_dedupe_results_3.repartition(100).write.mode('overwrite').parquet(write_path)


#The clustering within data source should be maintained only as per the records clustered in data prep and not on the basis of IO/Logic. Hence, the below rule ensures this case.
temp_inv=spark.sql("""
    select *, hash_uid as new_cluster_id from temp_all_inv_dedupe_results_3 where cluster_id is null
    """)
temp_inv.registerTempTable("temp_inv")

temp_inv_1=spark.sql("""
    select distinct 
     a.rule_id,
            coalesce(temp.new_cluster_id,a.cluster_id) as cluster_id,
            a.score,
            a.uid,
            a.hash_uid,
            a.data_src_nm,
            a.ds_inv_id,
            a.name,
            a.email,
            a.phone
     from temp_all_inv_dedupe_results_3 a left join temp_inv temp on lower(trim(a.uid))=lower(trim(temp.uid)) 
     where round(cast(coalesce(a.score,0.0) as double),2) < 0.80 or a.score is null
    """)
temp_inv_1.write.mode('overwrite').saveAsTable('temp_inv_1')

# Applied a rule to decluster records with Citeline, DQS & CTMS data source withing the single cluster. Only the self-clustering performed in Data Prep will still prevail.
temp_all_inv_dedupe_results_4_int=spark.sql("""select
        rule_id,
        cluster_id,
        score,
        uid,
        hash_uid,
        split(datasource_inv_id,"_")[0] as data_src_nm,
        datasource_inv_id as ds_inv_id,
        name,
        email,
        phone
        from temp_inv_1 
        lateral view outer explode (split(ds_inv_id,"\;")) as datasource_inv_id""")
temp_all_inv_dedupe_results_4_int.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results_4_int')


temp_all_inv_dedupe_results_4ex_int=spark.sql("""select 
A.rule_id,
A.cluster_id,
A.score,
A.uid,
A.hash_uid,
A.data_src_nm,
A.ds_inv_id,
A.name,
A.email,
A.phone,
B.ds_inv_id as prep_ds_inv_id, B.data_source as prep_data_src_nm from (select
rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_inv_id,
name,
email,
phone
from temp_all_inv_dedupe_results_4_int ) A
left join (select distinct uid, ds_inv_id, data_source from temp_all_inv_data_prep) B on trim(B.uid)= trim(A.uid) 
group by 1,2,3,4,5,6,7,8,9,10,11,12
""")
temp_all_inv_dedupe_results_4ex_int.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_inv_dedupe_results_4ex_int')

temp_all_inv_dedupe_results_4ex_int1 = spark.sql("""select rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_inv_id,
name,
email,
phone, prep_ds_inv_id, dense_rank() over(partition by  cluster_id order by uid) as rnk  from temp_all_inv_dedupe_results_4ex_int where prep_data_src_nm like '%;%'
group by 1,2,3,4,5,6,7,8,9,10,11""")
temp_all_inv_dedupe_results_4ex_int1.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_inv_dedupe_results_4ex_int1')

temp_all_inv_dedupe_results_4ex_int2=spark.sql("""select a.rule_id,
a.cluster_id,
a.score,
a.uid,
a.hash_uid,
a.data_src_nm,
a.ds_inv_id,
a.name,
a.email, 
a.phone, a.prep_ds_inv_id, dense_rank() over(partition by  a.cluster_id, a.data_src_nm order by a.uid) as rnk from temp_all_inv_dedupe_results_4ex_int a left join temp_all_inv_dedupe_results_4ex_int1 b on
lower(trim(a.cluster_id))=lower(trim(b.cluster_id))
where b.cluster_id is null""")
temp_all_inv_dedupe_results_4ex_int2.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results_4ex_int2')

temp_2_inv=spark.sql("""select ds_inv_id from temp_all_inv_dedupe_results_4ex_int 
minus
(select ds_inv_id from temp_all_inv_dedupe_results_4ex_int1
union
select ds_inv_id from temp_all_inv_dedupe_results_4ex_int2)
""")
temp_2_inv.registerTempTable("temp_2_inv")

temp_all_inv_dedupe_results_4ex_int3 = spark.sql(""" select * from temp_all_inv_dedupe_results_4ex_int1
union
select A.rule_id,
A.cluster_id,
A.score,
A.uid,
A.hash_uid,
A.data_src_nm,
A.ds_inv_id,
A.name,
A.email, 
A.phone, A.prep_ds_inv_id, null as rnk from temp_all_inv_dedupe_results_4ex_int A where ds_inv_id in (select ds_inv_id from temp_2_inv)
group by 1,2,3,4,5,6,7,8,9,10,11
""")
temp_all_inv_dedupe_results_4ex_int3.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_inv_dedupe_results_4ex_int3')

temp_all_inv_dedupe_results_4_ex = spark.sql("""
select rule_id,
case when rnk =1 or (data_src_nm = 'aact' and rnk is null)  then cluster_id
when rnk<>1  then concat_ws('|',cluster_id,rnk) 
when  (data_src_nm <> 'aact' and rnk is null) then concat_ws('|',cluster_id,hash_uid) end as cluster_id,
case when  rnk =1 or (data_src_nm = 'aact' and rnk is null)  then 0.95
when rnk<>1  then 0.95 
when (data_src_nm <> 'aact' and rnk is null) then null end as score,
uid,
hash_uid,
data_src_nm,
ds_inv_id,
name,
email,
phone from temp_all_inv_dedupe_results_4ex_int3
group by 1,2,3,4,5,6,7,8,9,10
union
select rule_id,
case when rnk =1 or trim(data_src_nm) = 'aact'  then cluster_id
when rnk<>1 and trim(data_src_nm) != 'aact'  then concat_ws('|',cluster_id,rnk)  end as cluster_id,
case when  rnk =1 or trim(data_src_nm) = 'aact'  then 0.95
when rnk<>1 and trim(data_src_nm) != 'aact'  then null end as score,
uid,
hash_uid,
data_src_nm,
ds_inv_id,
name,
email,
phone from temp_all_inv_dedupe_results_4ex_int2
group by 1,2,3,4,5,6,7,8,9,10
union
select rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_inv_id,
name,
email,
phone
from temp_all_inv_dedupe_results_3
where round(cast(coalesce(score,0.0) as double),2) >= 0.80
""")
temp_all_inv_dedupe_results_4_ex.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable("temp_all_inv_dedupe_results_4_ex")

temp_hash=spark.sql("""select hash_uid from temp_all_inv_dedupe_results_4_ex group by 1 having count(*)>1""")
temp_hash.registerTempTable("temp_hash")

temp_all_inv_dedupe_results_4=spark.sql("""select rule_id,
cluster_id,
0.90 as score,
uid,
hash_uid,
data_src_nm,
ds_inv_id,
name,
email,
phone
from temp_all_inv_dedupe_results_4_ex where hash_uid in (select hash_uid from temp_hash)
union
select rule_id,
cluster_id,
score,
uid,
hash_uid,
data_src_nm,
ds_inv_id,
name,
email,
phone
from temp_all_inv_dedupe_results_4_ex where hash_uid not in (select hash_uid from temp_hash)""")
temp_all_inv_dedupe_results_4.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('temp_all_inv_dedupe_results_4')
temp_all_inv_dedupe_results_4 = temp_all_inv_dedupe_results_4.dropDuplicates()
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_4')
temp_all_inv_dedupe_results_4.repartition(100).write.mode('overwrite').parquet(write_path)


# TO remove duplicate hash_uid
temp_all_inv_uni= spark.sql("""
select
        a.rule_id,
        a.cluster_id,
        a.score,
        a.uid,
        a.hash_uid,
        a.data_src_nm,
        a.ds_inv_id,
        a.name,
        a.email,
        a.phone
     from (select
        temp.rule_id,
        temp.cluster_id,
        temp.score,
        temp.uid,
        temp.hash_uid,
        temp.data_src_nm,
        temp.ds_inv_id,
        temp.name,
        temp.email,
        temp.phone,
        row_number() over(partition by temp.ds_inv_id order by score desc nulls last,cluster_id desc nulls last, uid desc nulls last, hash_uid desc nulls last) as rnk
    from temp_all_inv_dedupe_results_4 temp) a
    where a.rnk=1
""")
temp_all_inv_uni = temp_all_inv_uni.dropDuplicates()
temp_all_inv_uni.write.mode('overwrite').saveAsTable('temp_all_inv_uni')
write_path = path.replace('table_name', 'temp_all_inv_uni')
temp_all_inv_uni.repartition(100).write.mode('overwrite').parquet(write_path)

# Assigning intger id to cluster id
temp_inv_cluterid = spark.sql("""
select
    cluster_id,
    row_number() over(order by null) as int_final_cluster_id
from temp_all_inv_uni
where cluster_id is not null
group by cluster_id
""")
#temp_inv_cluterid.registerTempTable('temp_inv_cluterid')
temp_inv_cluterid.write.mode('overwrite').saveAsTable('temp_inv_cluterid')

# Replacing alphanumeric cluster id with integer cluster id
temp_all_inv_dedupe_results_final_5= spark.sql("""
select /*+ broadcast(b) */
        rule_id,
    cast(b.int_final_cluster_id as string) as cluster_id,
    case when int_final_cluster_id is not null
    then final_ta.score else null end as score,
    final_ta.uid,
    final_ta.hash_uid,
    final_ta.data_src_nm,
    final_ta.ds_inv_id,
    final_ta.name,
    final_ta.email,
    final_ta.phone
from temp_all_inv_uni final_ta
left outer join temp_inv_cluterid b
on trim(final_ta.cluster_id) = trim(b.cluster_id)
""")
temp_all_inv_dedupe_results_final_5 = temp_all_inv_dedupe_results_final_5.dropDuplicates()
temp_all_inv_dedupe_results_final_5.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results_final_5')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_5')
temp_all_inv_dedupe_results_final_5.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_inv_dedupe_results_final = spark.sql("""
select distinct
        rule_id,
    cluster_id,
    score,
    uid,
    hash_uid,
    data_src_nm,
    ds_inv_id,
    name,
    email,
    phone,
    cast(Null as string) as KM_comment
from
temp_all_inv_dedupe_results_final_5
""")
temp_all_inv_dedupe_results_final = temp_all_inv_dedupe_results_final.dropDuplicates()
temp_all_inv_dedupe_results_final.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results_final')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final')
#temp_all_inv_dedupe_results_final.repartition(100).write.mode('overwrite').parquet(write_path)
#write csv
csv_path = bucket_path + '/applications/commons/temp/km_validation/Dedupe_output/investigator/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'
csv_write_path = csv_path.replace('table_name', 'temp_all_inv_dedupe_results_final')
temp_all_inv_dedupe_results_final.repartition(1).write.mode('overwrite').format('csv').option(
    'header', 'true').option('delimiter', ',').option('quoteAll', 'true').save(csv_write_path)


spark.sql("""
    INSERT OVERWRITE TABLE
    $$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_dedupe_results_final PARTITION (
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id'
    )
    SELECT *
    FROM temp_all_inv_dedupe_results_final
    """)

CommonUtils().copy_hdfs_to_s3('$$client_name_ctfo_datastore_app_commons_$$db_env.temp_all_inv_dedupe_results_final')

# csv file for km validation
#temp_all_inv_dedupe_results_final.repartition(1).write.mode('overwrite').format('csv')\
#    .option('header', 'true').option('delimiter', ',').option('quoteAll', 'true')\
#    .save('{bucket_path}/applications'
#          '/commons/temp/km_validation/km_inputs/investigator/pt_data_dt=$$data_dt/'.format(bucket_path=bucket_path))



