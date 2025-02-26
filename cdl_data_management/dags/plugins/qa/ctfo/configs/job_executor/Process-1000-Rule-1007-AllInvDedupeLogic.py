





################################# Module Information ######################################
#  Module Name         : Investigator records combine
#  Purpose             : This will execute queries for creating
# temp_all_inv_dedupe_results_final table
#  Pre-requisites      : Source table required: temp_all_inv_match_score
#  Last changed on     : 24-05-2021
#  Last changed by     : Sandeep Kumar
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

sys.path.insert(1, '/app/clinical_design_center/data_management/sanofi_ctfo/configs/job_executor')
from DedupeUtility import KMValidate

timeStart = datetime.datetime.now()

path = 's3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications/' \
       'commons/temp/mastering/investigator/table_name/pt_data_dt=$$data_dt/pt_cycle_id=$$cycle_id'


temp_all_inv_match_score = spark.sql("""
select * from temp_all_inv_match_score
""")

write_path = path.replace('table_name', 'temp_all_inv_match_score')
temp_all_inv_match_score.repartition(100).write.mode('overwrite').parquet(write_path)

temp_all_inv_dedupe_results = spark.sql("""
select
    match_score.cluster_id,
    match_score.score,
    data_prep.uid,
    data_prep.hash_uid,
    data_prep.data_src_nm,
    data_prep.ds_inv_id,
    data_prep.name,
    data_prep.email,
    data_prep.phone
from temp_all_inv_data_prep data_prep
left outer join temp_all_inv_match_score match_score
on data_prep.uid = match_score.uid
""")

temp_all_inv_dedupe_results.write.mode('overwrite').saveAsTable('temp_all_inv_dedupe_results')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results')
temp_all_inv_dedupe_results.repartition(100).write.mode('overwrite').parquet(write_path)

#Verify the records where either first name or last name is not matching. There are cases where
#  first name and email are matched but last name is not matched due to typo so those cases
# have not been de-clustered as they are correctly clustered
# firstnme , last name and email are macthing, then cl
temp_all_inv_dedupe_results_final_1 = spark.sql("""
select /*+ broadcast(dedupe_results_ruled) */
    dedupe_results_ruled.rule_id,
    coalesce(dedupe_results_ruled.null_cluster_id, dedupe_results_main.cluster_id) as cluster_id,
    coalesce(dedupe_results_ruled.null_score, dedupe_results_main.score) as score,
    dedupe_results_main.uid,
    dedupe_results_main.hash_uid,
    dedupe_results_main.data_src_nm,
    dedupe_results_main.ds_inv_id,
    dedupe_results_main.name,
    dedupe_results_main.email,
    dedupe_results_main.phone
from temp_all_inv_dedupe_results dedupe_results_main
left outer join (
select
        'Rule_01' as rule_id,
    inv_dedupe_results_unclustered.*,
        'unclustered' as null_cluster_id,
    0 as null_score
from (
select
    cluster_id,
    uid,
    case
        when (first_name_0 <> first_name_1 and last_name_0 <> last_name_1 and email_0 <> email_1)
        then 'False'
        when (first_name_0 = first_name_1 and last_name_0 <> last_name_1 and email_0 <> email_1)
        then 'False'
        when (first_name_0 <> first_name_1 and last_name_0 <> last_name_1 and email_0 = email_1)
        then 'False'
        when (first_name_0 <> first_name_1 and last_name_0 = last_name_1 and email_0 <> email_1)
        then 'False'

    else 'True' end as result
from (
select
cluster_id,
    uid,
    split(split(email,'#')[0],'\\\|')[0] as email_0,
    coalesce(split(split(email,'#')[1],'\\\|')[0],split(split(email,'#')[0],'\\\|')[0]) as email_1,
    split(first_name,'\\\|')[0] as first_name_0,
    coalesce(split(first_name,'\\\|')[1],split(first_name,'\\\|')[0]) as first_name_1,
    split(last_name,'\\\|')[0] as last_name_0,
    coalesce(split(last_name,'\\\|')[1],split(last_name,'\\\|')[0]) as last_name_1
from (
select
    cluster_id,
    concat_ws('\\|',collect_set(uid)) as uid,
    concat_ws('\#',collect_set(email)) as email,
    concat_ws('\\|',collect_set(first_name)) as first_name,
    concat_ws('\\|',collect_set(last_name)) as last_name
from (
select
    inv_dedupe_results.cluster_id,
    uid,
    regexp_replace(lower(trim(email)),'[^0-9A-Za-z_@| ]','' ) as email,
    regexp_replace(substring_index(trim(lower(inv_dedupe_results.name)), ' ', 1),
    '[^0-9A-Za-z_ ]','') as first_name,
    regexp_replace(substring_index(trim(lower(inv_dedupe_results.name)), ' ', -1),
    '[^0-9A-Za-z_ ]','') as last_name
from temp_all_inv_dedupe_results inv_dedupe_results
where inv_dedupe_results.score is not null and score!=1.0
group by 1,2,3,4,5) inv_dedupe_results_collect
group by 1) inv_dedupe_results_split) inv_dedupe_results_case) inv_dedupe_results_unclustered
where result = 'False') dedupe_results_ruled
on
dedupe_results_main.cluster_id = dedupe_results_ruled.cluster_id
""")

temp_all_inv_dedupe_results_final_1.write.mode('overwrite').\
    saveAsTable('temp_all_inv_dedupe_results_final_1')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_1')
temp_all_inv_dedupe_results_final_1.repartition(100).write.mode('overwrite').parquet(write_path)


# Applied a rule to assign a cluster for exact match on investigator name and email without
#  domain name (example @gmail.com) for un-clustered records

temp_all_inv_dedupe_results_final_2 = spark.sql("""
select /*+ broadcast(dedupe_results_final_1_ruled) */
        coalesce(dedupe_results_final_1_ruled.rule_id,dedupe_results_final_1_main.rule_id)
        as rule_id,
    coalesce(dedupe_results_final_1_ruled.cluster_id, dedupe_results_final_1_main.cluster_id)
     as cluster_id,
    coalesce(dedupe_results_final_1_ruled.score, dedupe_results_final_1_main.score) as score,
    dedupe_results_final_1_main.uid,
    dedupe_results_final_1_main.hash_uid,
    dedupe_results_final_1_main.data_src_nm,
    dedupe_results_final_1_main.ds_inv_id,
    dedupe_results_final_1_main.name,
    dedupe_results_final_1_main.email,
    dedupe_results_final_1_main.phone
from temp_all_inv_dedupe_results_final_1 dedupe_results_final_1_main
left outer join (
select
        'Rule_02' as rule_id,
    new_uid as uid,
    cluster_id,
    score
from (
select
    concat(name,'_',new_email) as cluster_id,
        0.80 as score,
        concat_ws(';',collect_set(uid)) as uid,
        size(collect_set(uid)) cnt,
        name,
    new_email
from (
select
        regexp_replace(regexp_replace(regexp_replace(dedupe_results_final_1.name,' ','<>')
        ,'><',''),'<>',' ') as name,
        dedupe_results_final_1.uid,
        split(regexp_replace(lower(trim(email)),'[^0-9A-Za-z_@]','' ),'@')[0] as new_email
from temp_all_inv_dedupe_results_final_1 dedupe_results_final_1
where cluster_id is null and email is not null
 and (lower(trim(email))<>'')  ) dedupe_results_final_1_splited
 where  new_email is not null and  (lower(trim(new_email))<>'' )
group by 5,6
having cnt > 1) a lateral view explode (split(uid,'\;'))one as new_uid
) dedupe_results_final_1_ruled
on dedupe_results_final_1_main.uid = dedupe_results_final_1_ruled.uid
""")


temp_all_inv_dedupe_results_final_2.write.mode('overwrite')\
    .saveAsTable('temp_all_inv_dedupe_results_final_2')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_2')
temp_all_inv_dedupe_results_final_2.repartition(100).write.mode("overwrite").parquet(write_path)

#Applied a rule to assign a cluster for exact match on investigator name and first phone
# for un-clustered records


temp_all_inv_dedupe_results_final_3 = spark.sql("""
select /*+ broadcast(dedupe_results_final_2_ruled) */
        coalesce(dedupe_results_final_2_ruled.rule_id,dedupe_results_final_2_main.rule_id)
        as rule_id,
    coalesce(dedupe_results_final_2_ruled.cluster_id, dedupe_results_final_2_main.cluster_id)
    as cluster_id,
    coalesce(dedupe_results_final_2_ruled.score, dedupe_results_final_2_main.score) as score,
    dedupe_results_final_2_main.uid,
    dedupe_results_final_2_main.hash_uid,
    dedupe_results_final_2_main.data_src_nm,
    dedupe_results_final_2_main.ds_inv_id,
    dedupe_results_final_2_main.name,
    dedupe_results_final_2_main.email,
    dedupe_results_final_2_main.phone
from temp_all_inv_dedupe_results_final_2 dedupe_results_final_2_main
left outer join (
select
        'Rule_03' as rule_id,
    new_uid as uid,
    cluster_id,
    score
from (
select
    concat(name,'_',new_phone) as cluster_id,
        0.80 as score,
        concat_ws(';',collect_set(uid)) as uid,
        size(collect_set(uid)) cnt,
        name,
    new_phone
from (
select
    regexp_replace(regexp_replace(regexp_replace(dedupe_results_final_2.name,' ','<>'),'><',''),
    '<>',' ') as name,
        dedupe_results_final_2.uid,
        split(lower(trim(phone)),'\\\|')[0] as new_phone
from temp_all_inv_dedupe_results_final_2 dedupe_results_final_2
where cluster_id is null and  phone is not null and (lower(trim(phone))<>'') ) dedupe_results_final_2_splited
where new_phone is not null and (lower(trim(new_phone))<>'')
group by 5,6
having cnt > 1) a lateral view explode (split(uid,'\;'))one as new_uid
) dedupe_results_final_2_ruled
on dedupe_results_final_2_main.uid = dedupe_results_final_2_ruled.uid
""")


temp_all_inv_dedupe_results_final_3.write.mode('overwrite')\
    .saveAsTable('temp_all_inv_dedupe_results_final_3')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_3')
temp_all_inv_dedupe_results_final_3.repartition(100).write.mode('overwrite').parquet(write_path)


#Applied a rule to assign a cluster for exact match on phone, email and first name, last name
#  for un-clustered records
temp_all_inv_dedupe_results_final_4 = spark.sql("""
select /*+ broadcast(b) */
        coalesce(dedupe_results_final_3_ruled.rule_id,dedupe_results_final_3_main.rule_id)
        as rule_id,
        coalesce(dedupe_results_final_3_ruled.cluster_id, dedupe_results_final_3_main.cluster_id)
         as cluster_id,
    coalesce(dedupe_results_final_3_ruled.score, dedupe_results_final_3_main.score) as score,
    dedupe_results_final_3_main.uid,
    dedupe_results_final_3_main.hash_uid,
    dedupe_results_final_3_main.data_src_nm,
    dedupe_results_final_3_main.ds_inv_id,
    dedupe_results_final_3_main.name,
    dedupe_results_final_3_main.email,
    dedupe_results_final_3_main.phone
from temp_all_inv_dedupe_results_final_3 dedupe_results_final_3_main
left outer join (
select
        'Rule_04' as rule_id,
    new_uid as uid,
    cluster_id as cluster_id,
    zip_score as score
from (
select
    first_name,
    last_name,
    new_phone,
    new_email,
    concat_ws('\\|',collect_set(uid)) as uid ,
    size(collect_set(uid)) cnt,
    concat_ws('\\|',collect_set(uid)) as cluster_id,
    0.75 as zip_score
from (
select
    uid,
    lower(trim(split(new_name," ")[0])) as first_name,
    lower(trim(reverse(split(reverse(new_name)," ")[0]))) as last_name,
    new_phone,
    new_email
from (
select
    dedupe_results_final_3.*,
    regexp_replace(lower(trim(name)),'[^0-9A-Za-z_ ]','') as new_name,
    regexp_replace(lower(trim(phone)),'[^0-9A-Za-z_ ]','') as new_phone,
    regexp_replace(lower(trim(email)),'[^0-9A-Za-z_ ]','') as new_email
from temp_all_inv_dedupe_results_final_3 dedupe_results_final_3)
where (cluster_id is null or cluster_id = 'unclustered')  and name is not null )
 dedupe_results_final_3_splited
group by 1,2,3,4
having cnt>1) dedupe_results_final_3_group lateral view explode(split(uid,'\\\|'))one as new_uid)
dedupe_results_final_3_ruled
on dedupe_results_final_3_main.uid = dedupe_results_final_3_ruled.uid
""")


temp_all_inv_dedupe_results_final_4.write.mode('overwrite')\
    .saveAsTable('temp_all_inv_dedupe_results_final_4')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_4')
temp_all_inv_dedupe_results_final_4.repartition(100).write.mode('overwrite').parquet(write_path)



# Applied a rule to cluster record if one phone and first name/last name is matching

temp_all_inv_dedupe_results_final_5 = spark.sql("""
select /*+ broadcast(b) */
    coalesce(dedupe_results_final_4_ruled.rule_id,dedupe_results_final_4_main.rule_id) as rule_id,
    coalesce(dedupe_results_final_4_ruled.cluster_id, dedupe_results_final_4_main.cluster_id)
    as cluster_id,
    coalesce(dedupe_results_final_4_ruled.score, dedupe_results_final_4_main.score) as score,
    dedupe_results_final_4_main.uid,
    dedupe_results_final_4_main.hash_uid,
    dedupe_results_final_4_main.data_src_nm,
    dedupe_results_final_4_main.ds_inv_id,
    dedupe_results_final_4_main.name,
    dedupe_results_final_4_main.email,
    dedupe_results_final_4_main.phone
from temp_all_inv_dedupe_results_final_4 dedupe_results_final_4_main
left outer join (select
    'Rule_05' as rule_id,
    new_uid as uid,
    cluster_id as cluster_id,
    score
from (select
name,
concat_ws('\\|',collect_set(uid)) as uid ,
concat_ws('\\|',collect_set(uid)) as cluster_id,
0.75 as score
from (
select
        explode(split(uid,'\\\|'))  as uid,
        name ,
    0.75 as score
from (
        select concat_ws('\\|',collect_set(uid)) as uid ,
        size(collect_set(uid)) as cnt,
        new_phone,
        name,
        rank() over(partition by name order by size(collect_set(uid)) desc) as rnk
 from (
select
    uid,
        explode(split(lower(trim(phone)),'\\\|')) as new_phone,
    CONCAT_WS(' ',coalesce(first_name,''),coalesce(middle_name,last_value(middle_name ,true)
    OVER (PARTITION BY concat(coalesce(first_name,''),coalesce(last_name,''))
    ORDER BY middle_name desc rows between unbounded preceding and 1 preceding))
    ,coalesce(last_name,'')) as name
from (
select
    dedupe_results_final_4.*,
    lower(trim(split(name,' ')[0])) as first_name,
    case
        when(lower(trim(split(name,' ')[1])) = lower(trim(reverse(split(reverse(name),' ')[0]))))
         then NULL
        else lower(trim(split(name,' ')[1]))
    end as middle_name,
    lower(trim(reverse(split(reverse(name),' ')[0]))) as last_name
from temp_all_inv_dedupe_results_final_4 dedupe_results_final_4
)
where name is not null )
group by 3,4 having size(collect_set(uid))>1) where   rnk = 1 and  new_phone is not null and  (lower(trim(new_phone))<>'' )) dedupe_results_final_4_split
group by 1) dedupe_results_final_4_group lateral view explode(split(uid,'\\\|'))one as new_uid)
dedupe_results_final_4_ruled
on dedupe_results_final_4_main.uid = dedupe_results_final_4_ruled.uid
""")

temp_all_inv_dedupe_results_final_5.write.mode('overwrite')\
    .saveAsTable('temp_all_inv_dedupe_results_final_5')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_5')
temp_all_inv_dedupe_results_final_5.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule to cluster record if one email and first name/last name is matching

temp_all_inv_dedupe_results_final_6 = spark.sql("""
select /*+ broadcast(b) */
    coalesce(dedupe_results_final_5_ruled.rule_id,dedupe_results_final_5_main.rule_id) as rule_id,
    coalesce(dedupe_results_final_5_ruled.cluster_id, dedupe_results_final_5_main.cluster_id)
    as cluster_id,
    coalesce(dedupe_results_final_5_ruled.score, dedupe_results_final_5_main.score) as score,
    dedupe_results_final_5_main.uid,
    dedupe_results_final_5_main.hash_uid,
    dedupe_results_final_5_main.data_src_nm,
    dedupe_results_final_5_main.ds_inv_id,
    dedupe_results_final_5_main.name,
    dedupe_results_final_5_main.email,
    dedupe_results_final_5_main.phone
from temp_all_inv_dedupe_results_final_5 dedupe_results_final_5_main
left outer join (select
    'Rule_06' as rule_id,
    new_uid as uid,
    cluster_id as cluster_id,
    score,
        new_email,
        name

from (select
name,
concat_ws('\\|',collect_set(uid)) as uid ,
concat_ws('\\|',collect_set(uid)) as cluster_id,
0.75 as score,
new_email
from (
select
        explode(split(uid,'\\\|'))  as uid,
        name ,
                new_email,
    0.75 as score
from (
        select concat_ws('\\|',collect_set(uid)) as uid ,
        size(collect_set(uid)) as cnt,
        new_email,
        name,
        rank() over(partition by name order by size(collect_set(uid)) desc) as rnk
 from (
select
    uid,
        explode(split(lower(trim(email)),'\\\|')) as new_email,
    CONCAT_WS(' ',coalesce(first_name,''),coalesce(middle_name,last_value(middle_name ,true)
    OVER (PARTITION BY concat(coalesce(first_name,''),coalesce(last_name,''))
    ORDER BY middle_name desc rows between unbounded preceding and 1 preceding))
    ,coalesce(last_name,'')) as name
from (
select
    dedupe_results_final_5.*,
    lower(trim(split(name,' ')[0])) as first_name,
    case
        when(lower(trim(split(name,' ')[1])) = lower(trim(reverse(split(reverse(name),' ')[0]))))
         then NULL
        else lower(trim(split(name,' ')[1]))
    end as middle_name,
    lower(trim(reverse(split(reverse(name),' ')[0]))) as last_name
from temp_all_inv_dedupe_results_final_5 dedupe_results_final_5 where email is not null
 and (lower(trim(email))<>''))
where name is not null  )
group by 3,4 having size(collect_set(uid))>1) where   rnk = 1 and  new_email is not null and  (lower(trim(new_email))<>'' )) dedupe_results_final_5_split
group by 1,5) dedupe_results_final_5_group lateral view explode(split(uid,'\\\|'))one as new_uid)
dedupe_results_final_5_ruled
on dedupe_results_final_5_main.uid = dedupe_results_final_5_ruled.uid
""")
temp_all_inv_dedupe_results_final_6.dropDuplicates()
temp_all_inv_dedupe_results_final_6.write.mode('overwrite')\
    .saveAsTable('temp_all_inv_dedupe_results_final_6')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_6')
temp_all_inv_dedupe_results_final_6.repartition(100).write.mode('overwrite').parquet(write_path)

# Applied a rule to decluster the records with same data_src_nm
temp_all_inv_dedupe_results_final_7= spark.sql("""
select distinct
    coalesce(inv_dedupe_2.rule_id,inv_dedupe_1.rule_id) as rule_id,
    coalesce(inv_dedupe_2.cluster_id, inv_dedupe_1.cluster_id) as cluster_id,
    coalesce(inv_dedupe_2.score, inv_dedupe_1.score) as score,
    inv_dedupe_1.uid,
    inv_dedupe_1.hash_uid,
    inv_dedupe_1.data_src_nm,
    inv_dedupe_1.ds_inv_id,
    inv_dedupe_1.name,
    inv_dedupe_1.email,
    inv_dedupe_1.phone
from temp_all_inv_dedupe_results_final_6 inv_dedupe_1
left outer join (
select distinct
    'Rule_07' as rule_id,
    uid,
    cluster_id as cluster_id,
    case when rnk = 1 then score
             when rnk <>1 then 0.50
                 end as score
from (
select inv_dedupe.cluster_id,
inv_dedupe.name,
inv_dedupe.email,
inv_dedupe.uid,
inv_dedupe.score,
row_number() over (partition by inv_dedupe.cluster_id order by inv_dedupe.score desc ) as rnk
from
(
select
    inv_dedupe_results_1.cluster_id,
    inv_dedupe_results_1.name,
    inv_dedupe_results_1.email,
    inv_dedupe_results_1.uid,
    inv_dedupe_results_1.data_src_nm,
    inv_dedupe_results_1.score
from  temp_all_inv_dedupe_results_final_6  inv_dedupe_results_1
where inv_dedupe_results_1.data_src_nm in ('ctms','ir')) inv_dedupe ) where rnk = 1) inv_dedupe_2
on inv_dedupe_1.uid = inv_dedupe_2.uid
""")
temp_all_inv_dedupe_results_final_7.dropDuplicates()
temp_all_inv_dedupe_results_final_7.write.mode('overwrite')\
    .saveAsTable('temp_all_inv_dedupe_results_final_7')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_7')
temp_all_inv_dedupe_results_final_7.repartition(100).write.mode('overwrite').parquet(write_path)

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
    row_number() over(partition by temp.hash_uid order by temp.score desc ) as rnk
from default.temp_all_inv_dedupe_results_final_7 temp) a
where a.rnk=1
""")
temp_all_inv_uni.dropDuplicates()

temp_all_inv_uni.write.mode('overwrite')\
    .saveAsTable('temp_all_inv_uni')
temp_all_inv_uni.repartition(100).write.mode('overwrite').parquet(write_path)






# Assigning intger id to cluster id
temp_inv_cluterid = spark.sql("""
select
    cluster_id,
    row_number() over(order by null) as int_final_cluster_id
from temp_all_inv_uni
where cluster_id is not null
and lower(trim(cluster_id)) not like "unclustered%"
group by cluster_id having count(*)>1
""")
temp_inv_cluterid.registerTempTable('temp_inv_cluterid')
temp_inv_cluterid.write.mode('overwrite').saveAsTable('temp_inv_cluterid')



# Replacing alphanumeric cluster id with integer cluster id

temp_all_inv_dedupe_results_final_8= spark.sql("""
select /*+ broadcast(b) */
        rule_id,
    cast(b.int_final_cluster_id as string) as cluster_id,
    case when int_final_cluster_id is not null
    then dedupe_results_final_5.score else null end as score,
    dedupe_results_final_5.uid,
    dedupe_results_final_5.hash_uid,
    dedupe_results_final_5.data_src_nm,
    dedupe_results_final_5.ds_inv_id,
    dedupe_results_final_5.name,
    dedupe_results_final_5.email,
    dedupe_results_final_5.phone
from temp_all_inv_uni dedupe_results_final_5
left outer join temp_inv_cluterid b
on dedupe_results_final_5.cluster_id = b.cluster_id
""")
temp_all_inv_dedupe_results_final_8.dropDuplicates()
#temp_all_inv_dedupe_results_final_8.registerTempTable("temp_all_inv_dedupe_results_final_8")
temp_all_inv_dedupe_results_final_8.write.mode('overwrite')\
    .saveAsTable('temp_all_inv_dedupe_results_final_8')
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final_8')
temp_all_inv_dedupe_results_final_8.repartition(100).write.mode('overwrite').parquet(write_path)

final_table = KMValidate('temp_all_inv_dedupe_results_final_8', 'investigator', '$$data_dt','$$cycle_id','$$s3_env')

temp_all_inv_dedupe_results_final = spark.sql("""
select distinct
        rule_id,
    final_cluster_id as cluster_id,
    final_score as score,
    uid,
    hash_uid,
    data_src_nm,
    ds_inv_id,
    name,
    email,
    phone,
    final_KM_comment as KM_comment
from
"""+final_table+"""
""")

temp_all_inv_dedupe_results_final.registerTempTable('temp_all_inv_dedupe_results_final')
spark.sql("""
    INSERT OVERWRITE TABLE
    sanofi_ctfo_datastore_app_commons_$$db_env.temp_all_inv_dedupe_results_final PARTITION (
    pt_data_dt='$$data_dt',
    pt_cycle_id='$$cycle_id'
    )
    SELECT *
    FROM temp_all_inv_dedupe_results_final
    """)
write_path = path.replace('table_name', 'temp_all_inv_dedupe_results_final')
temp_all_inv_dedupe_results_final.repartition(100).write.mode('overwrite').parquet(write_path)

# CommonUtils().copy_hdfs_to_s3('ctfo_internal_datastore_app_commons_$$db_env.temp_all_inv_dedupe_
# results_final')


# csv file for km validation
temp_all_inv_dedupe_results_final.repartition(1).write.mode('overwrite').format('csv')\
    .option('header', 'true').option('delimiter', ',').option('quoteAll', 'true')\
    .save('s3://aws-a0199-use1-00-$$s3_env-s3b-snfi-ctf-data01/clinical-data-lake/applications'
          '/commons/temp/km_validation/km_inputs/investigator/$$data_dt/')





