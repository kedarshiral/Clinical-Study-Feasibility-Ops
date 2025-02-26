use audit_information;

/* View status of all processes with cluster details across the system */
drop view audit_information.vw_log_all_process_dtl;
create view audit_information.vw_log_all_process_dtl
as
select 
a.process_id,  
b.process_name, a.cluster_id, a.cluster_name, a.cluster_status, 
case when a.master_instance_type is null then b.master_instance_type else a.master_instance_type end as master_instance_type, 
a.core_market_type, 
case when a.core_instance_type is null then b.core_instance_type else a.core_instance_type end as core_instance_type, 
a.core_spot_bid_price, 
case when a.num_core_instances is null then b.num_core_instances else a.num_core_instances end as num_core_instances,
a.task_market_type,
case when a.task_instance_type is null then b.task_instance_type else a.task_instance_type end as task_instance_type, 
a.task_spot_bid_price, 
case when a.num_task_instances is null then b.num_task_instances else a.num_task_instances end as num_task_instances,
b.subnet_id, a.cluster_start_time, a.cluster_stop_time,
case 
when a.cluster_stop_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, a.cluster_stop_time))
when (upper(cluster_status) <> 'TERMINATED' and a.cluster_stop_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, now()))
else null end as cluster_run_time 
from
audit_information.log_cluster_dtl a
left outer join audit_information.ctl_cluster_config b on a.process_id = b.process_id;


/* View status of all ingestion runs at a dataset level */
drop view audit_information.vw_log_ingest_dataset_dtl;
create view audit_information.vw_log_ingest_dataset_dtl
as
select 
a.process_id,  
b.process_name, a.cluster_id, a.cluster_name, a.cluster_status, 
case when a.master_instance_type is null then b.master_instance_type else a.master_instance_type end as master_instance_type, 
a.core_market_type, 
case when a.core_instance_type is null then b.core_instance_type else a.core_instance_type end as core_instance_type, 
a.core_spot_bid_price, 
case when a.num_core_instances is null then b.num_core_instances else a.num_core_instances end as num_core_instances,
a.task_market_type,
case when a.task_instance_type is null then b.task_instance_type else a.task_instance_type end as task_instance_type, 
a.task_spot_bid_price, 
case when a.num_task_instances is null then b.num_task_instances else a.num_task_instances end as num_task_instances,
b.subnet_id, a.cluster_start_time, a.cluster_stop_time,
case 
when a.cluster_stop_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, a.cluster_stop_time))
when (upper(cluster_status) <> 'TERMINATED' and a.cluster_stop_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, now()))
else null end as cluster_run_time,
c.batch_id, c.batch_status, c.batch_start_time, c.batch_end_time,
case 
when c.batch_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.batch_start_time, c.batch_end_time))
when (upper(c.batch_status) not in ('SUCCEEDED', 'FAILED') and c.batch_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.batch_start_time, now()))
else null end as batch_run_time,
d.dataset_id, d.dataset_name, e.workflow_name
from
audit_information.log_cluster_dtl a
left outer join audit_information.ctl_cluster_config b on a.process_id = b.process_id
left outer join audit_information.log_batch_dtl c on a.cluster_id = c.cluster_id and a.process_id = c.process_id
left outer join audit_information.ctl_dataset_master d on c.dataset_id = d.dataset_id
left outer join audit_information.ctl_workflow_master e on c.process_id = e.process_id and c.dataset_id = e.dataset_id;  


/* View status of all ingestion runs at a file level */
drop view audit_information.vw_log_ingest_file_dtl;
create view audit_information.vw_log_ingest_file_dtl
as
select 
a.process_id,  
b.process_name, a.cluster_id, a.cluster_name, a.cluster_status, 
case when a.master_instance_type is null then b.master_instance_type else a.master_instance_type end as master_instance_type, 
a.core_market_type, 
case when a.core_instance_type is null then b.core_instance_type else a.core_instance_type end as core_instance_type, 
a.core_spot_bid_price, 
case when a.num_core_instances is null then b.num_core_instances else a.num_core_instances end as num_core_instances,
a.task_market_type,
case when a.task_instance_type is null then b.task_instance_type else a.task_instance_type end as task_instance_type, 
a.task_spot_bid_price, 
case when a.num_task_instances is null then b.num_task_instances else a.num_task_instances end as num_task_instances,
b.subnet_id, a.cluster_start_time, a.cluster_stop_time,
case 
when a.cluster_stop_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, a.cluster_stop_time))
when (upper(cluster_status) <> 'TERMINATED' and a.cluster_stop_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, now()))
else null end as cluster_run_time,
c.batch_id, c.batch_status, c.batch_start_time, c.batch_end_time,
case 
when c.batch_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.batch_start_time, c.batch_end_time))
when (upper(c.batch_status) not in ('SUCCEEDED', 'FAILED') and c.batch_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.batch_start_time, now()))
else null end as batch_run_time,
d.dataset_id, d.dataset_name, e.workflow_name,
f.file_id, f.file_name, f.file_status, g.file_process_name, g.file_process_status, g.file_process_start_time, g.file_process_end_time,
case 
when g.file_process_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, g.file_process_start_time, g.file_process_end_time))
when (upper(g.file_process_status) not in ('SUCCEEDED', 'FAILED', 'SKIPPED') and g.file_process_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, g.file_process_start_time, now()))
else null end as file_process_run_time,
g.record_count, g.process_message
from
audit_information.log_cluster_dtl a
left outer join audit_information.ctl_cluster_config b on a.process_id = b.process_id
left outer join audit_information.log_batch_dtl c on a.cluster_id = c.cluster_id and a.process_id = c.process_id
left outer join audit_information.ctl_dataset_master d on c.dataset_id = d.dataset_id
left outer join audit_information.ctl_workflow_master e on c.process_id = e.process_id and c.dataset_id = e.dataset_id
left outer join audit_information.log_file_smry f on c.batch_id = f.batch_id and c.process_id = f.process_id and c.dataset_id = f.dataset_id 
left outer join audit_information.log_file_dtl g on c.batch_id = g.batch_id and c.process_id = g.process_id and c.dataset_id = g.dataset_id  and f.file_id = f.file_id; 



/* View status of all pre-DQM data standardization steps during ingestion runs */
drop view audit_information.vw_log_ingest_pre_dqm_std_dtl;
create view audit_information.vw_log_ingest_pre_dqm_std_dtl
as
select 
a.process_id, 
b.process_name, a.cluster_id, a.cluster_name, a.cluster_status, 
case when a.master_instance_type is null then b.master_instance_type else a.master_instance_type end as master_instance_type, 
a.core_market_type, 
case when a.core_instance_type is null then b.core_instance_type else a.core_instance_type end as core_instance_type, 
a.core_spot_bid_price, 
case when a.num_core_instances is null then b.num_core_instances else a.num_core_instances end as num_core_instances,
a.task_market_type,
case when a.task_instance_type is null then b.task_instance_type else a.task_instance_type end as task_instance_type, 
a.task_spot_bid_price, 
case when a.num_task_instances is null then b.num_task_instances else a.num_task_instances end as num_task_instances,
b.subnet_id, a.cluster_start_time, a.cluster_stop_time,
case 
when a.cluster_stop_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, a.cluster_stop_time))
when (upper(cluster_status) <> 'TERMINATED' and a.cluster_stop_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, now()))
else null end as cluster_run_time,
c.batch_id, c.batch_status, c.batch_start_time, c.batch_end_time,
case 
when c.batch_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.batch_start_time, c.batch_end_time))
when (upper(c.batch_status) not in ('SUCCEEDED', 'FAILED') and c.batch_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.batch_start_time, now()))
else null end as batch_run_time,
d.dataset_name, e.workflow_name,
f.file_id, f.file_name, f.file_status, g.column_name, g.function_name, g.function_params, g.status
from
audit_information.log_cluster_dtl a
left outer join audit_information.ctl_cluster_config b on a.process_id = b.process_id
left outer join audit_information.log_batch_dtl c on a.cluster_id = c.cluster_id and a.process_id = c.process_id
left outer join audit_information.ctl_dataset_master d on c.dataset_id = d.dataset_id
left outer join audit_information.ctl_workflow_master e on c.process_id = e.process_id and c.dataset_id = e.dataset_id 
left outer join audit_information.log_file_smry f on c.batch_id = f.batch_id and c.process_id = f.process_id and c.dataset_id = f.dataset_id 
left outer join audit_information.log_pre_dqm_dtl g on c.batch_id = g.batch_id and c.process_id = g.process_id and c.dataset_id = g.dataset_id and f.file_id = f.file_id; 



/* View status of all DQM steps during ingestion runs */
drop view audit_information.vw_log_ingest_dqm_dtl;
create view audit_information.vw_log_ingest_dqm_dtl
as
select 
a.process_id,
b.process_name, a.cluster_id, a.cluster_name, a.cluster_status, 
case when a.master_instance_type is null then b.master_instance_type else a.master_instance_type end as master_instance_type, 
a.core_market_type, 
case when a.core_instance_type is null then b.core_instance_type else a.core_instance_type end as core_instance_type, 
a.core_spot_bid_price, 
case when a.num_core_instances is null then b.num_core_instances else a.num_core_instances end as num_core_instances,
a.task_market_type,
case when a.task_instance_type is null then b.task_instance_type else a.task_instance_type end as task_instance_type, 
a.task_spot_bid_price, 
case when a.num_task_instances is null then b.num_task_instances else a.num_task_instances end as num_task_instances,
b.subnet_id, a.cluster_start_time, a.cluster_stop_time,
case 
when a.cluster_stop_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, a.cluster_stop_time))
when (cluster_status <> 'TERMINATED' and a.cluster_stop_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, now()))
else null end as cluster_run_time,
c.batch_id, c.batch_status, c.batch_start_time, c.batch_end_time,
case 
when c.batch_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.batch_start_time, c.batch_end_time))
when (upper(c.batch_status) not in ('SUCCEEDED', 'FAILED') and c.batch_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.batch_start_time, now()))
else null end as batch_run_time,
d.dataset_id, d.dataset_name, e.workflow_name,
f.file_id, f.file_name, f.file_status, g.column_name, g.qc_type, g.qc_param, g.criticality, g.error_count, g.qc_message, g.qc_failure_percentage, g.qc_status, g.qc_start_time, g.qc_end_time,
case 
when g.qc_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, g.qc_start_time, g.qc_end_time))
when (upper(g.qc_status) 2not in ('SUCCEEDED', 'FAILED') and g.qc_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, g.qc_start_time, now()))
else null end as qc_run_time
from
audit_information.log_cluster_dtl a
left outer join audit_information.ctl_cluster_config b on a.process_id = b.process_id
left outer join audit_information.log_batch_dtl c on a.cluster_id = c.cluster_id and a.process_id = c.process_id
left outer join audit_information.ctl_dataset_master d on c.dataset_id = d.dataset_id
left outer join audit_information.ctl_workflow_master e on c.process_id = e.process_id and c.dataset_id = e.dataset_id 
left outer join audit_information.log_file_smry f on c.batch_id = f.batch_id and c.process_id = f.process_id and c.dataset_id = f.dataset_id 
left outer join audit_information.log_dqm_smry g on c.batch_id = g.batch_id and c.dataset_id = g.dataset_id and f.file_id = f.file_id;



/* View status of all data transformation runs at a step level */
drop view audit_information.vw_log_transform_step_dtl;
create view audit_information.vw_log_transform_step_dtl
as
select 
a.process_id,
b.process_name, a.cluster_id, a.cluster_name, a.cluster_status, 
case when a.master_instance_type is null then b.master_instance_type else a.master_instance_type end as master_instance_type, 
a.core_market_type, 
case when a.core_instance_type is null then b.core_instance_type else a.core_instance_type end as core_instance_type, 
a.core_spot_bid_price, 
case when a.num_core_instances is null then b.num_core_instances else a.num_core_instances end as num_core_instances,
a.task_market_type,
case when a.task_instance_type is null then b.task_instance_type else a.task_instance_type end as task_instance_type, 
a.task_spot_bid_price, 
case when a.num_task_instances is null then b.num_task_instances else a.num_task_instances end as num_task_instances,
b.subnet_id, a.cluster_start_time, a.cluster_stop_time,
case 
when a.cluster_stop_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, a.cluster_stop_time))
when (upper(cluster_status) <> 'TERMINATED' and a.cluster_stop_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, now()))
else null end as cluster_run_time,
c.cycle_id, c.data_date, c.cycle_status, c.cycle_start_time, c.cycle_end_time,
case 
when c.cycle_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.cycle_start_time, c.cycle_end_time))
when (upper(c.cycle_status) not in ('SUCCEEDED', 'FAILED') and c.cycle_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.cycle_start_time, now()))
else null end as cycle_run_time,
d.step_name, d.step_status, d.step_start_time, d.step_end_time,
case 
when d.step_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, d.step_start_time, d.step_end_time))
when (upper(d.step_status) not in ('SUCCEEDED', 'FAILED') and d.step_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, d.step_start_time, now()))
else null end as step_run_time,
e.workflow_name
from
audit_information.log_cluster_dtl a
left outer join audit_information.ctl_cluster_config b on a.process_id = b.process_id
left outer join audit_information.log_cycle_dtl c on a.cluster_id = c.cluster_id and a.process_id = c.process_id
left outer join audit_information.log_step_dtl d on c.cycle_id = d.cycle_id and c.process_id = d.process_id and c.data_date = d.data_date and c.frequency = d.frequency
left outer join audit_information.ctl_step_workflow_master e on c.process_id = e.process_id and d.step_name = e.step_name;


/* View status of all data transformation runs at a rule level */
drop view audit_information.vw_log_transform_rule_dtl;
create view audit_information.vw_log_transform_rule_dtl
as
select 
a.process_id,
b.process_name, a.cluster_id, a.cluster_name, a.cluster_status, 
case when a.master_instance_type is null then b.master_instance_type else a.master_instance_type end as master_instance_type, 
a.core_market_type, 
case when a.core_instance_type is null then b.core_instance_type else a.core_instance_type end as core_instance_type, 
a.core_spot_bid_price, 
case when a.num_core_instances is null then b.num_core_instances else a.num_core_instances end as num_core_instances,
a.task_market_type,
case when a.task_instance_type is null then b.task_instance_type else a.task_instance_type end as task_instance_type, 
a.task_spot_bid_price, 
case when a.num_task_instances is null then b.num_task_instances else a.num_task_instances end as num_task_instances,
b.subnet_id, a.cluster_start_time, a.cluster_stop_time,
case 
when a.cluster_stop_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, a.cluster_stop_time))
when (upper(cluster_status) <> 'TERMINATED' and a.cluster_stop_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, a.cluster_start_time, now()))
else null end as cluster_run_time,
c.cycle_id, c.data_date, c.cycle_status, c.cycle_start_time, c.cycle_end_time,
case 
when c.cycle_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.cycle_start_time, c.cycle_end_time))
when (upper(c.cycle_status) not in ('SUCCEEDED', 'FAILED') and c.cycle_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, c.cycle_start_time, now()))
else null end as cycle_run_time,
d.step_name, d.step_status, d.step_start_time, d.step_end_time,
case 
when d.step_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, d.step_start_time, d.step_end_time))
when (upper(d.step_status) not in ('SUCCEEDED', 'FAILED') and d.step_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, d.step_start_time, now()))
else null end as step_run_time,
e.workflow_name,
f.rule_id, f.rule_type, f.rule_status, f.rule_start_time, f.rule_end_time,
case 
when f.rule_end_time is not null then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, f.rule_start_time, f.rule_end_time))
when (upper(f.rule_status) not in ('SUCCEEDED', 'FAILED') and f.rule_end_time is null) then SEC_TO_TIME(TIMESTAMPDIFF(SECOND, f.rule_start_time, now()))
else null end as rule_run_time,
g.validation_status, g.validation_message, g.error_record_count, g.error_details_path
from
audit_information.log_cluster_dtl a
left outer join audit_information.ctl_cluster_config b on a.process_id = b.process_id
left outer join audit_information.log_cycle_dtl c on a.cluster_id = c.cluster_id and a.process_id = c.process_id
left outer join audit_information.log_step_dtl d on c.cycle_id = d.cycle_id and c.process_id = d.process_id and c.data_date = d.data_date and c.frequency = d.frequency 
left outer join audit_information.ctl_step_workflow_master e on c.process_id = e.process_id and d.step_name = e.step_name
left outer join audit_information.log_rule_dtl f on c.cycle_id = f.cycle_id and c.process_id = f.process_id and c.data_date = f.data_date and d.step_name = f.step_name and c.frequency = f.frequency
left outer join audit_information.log_rule_validation_dtl g on c.cycle_id = g.cycle_id and c.process_id = g.process_id and c.data_date = g.data_date and d.step_name = g.step_name and f.rule_id = g.rule_id and c.frequency = g.frequency;



use audit_information;
/* View L1 DQM summary */
drop view audit_information.vw_log_dqm_dtl;
create view audit_information.vw_log_dqm_dtl
as
select application_id, a.process_id, process_name, a.dataset_id, a.batch_id, dataset_name, dataset_description, load_type, qc_id, column_name, qc_type, qc_param, criticality, 
batch_rnk, case when batch_rnk = 1 then 'Y' else 'N' end as latest_batch_ind,
sum(error_count) as error_count, sum(src_record_count) as src_record_count, sum(pre_dqm_record_count) as pre_dqm_record_count, sum(stg_record_count) as stg_record_count
from
(select a.*, b.process_name 
from
(select a.*, process_id, src_record_count, pre_dqm_record_count, stg_record_count
from
(select a.*, b.dataset_name, b.dataset_description, b.load_type
from
(select * from audit_information.log_dqm_smry
) a
inner join
(select distinct dataset_id, dataset_name, dataset_description, load_type from audit_information.ctl_dataset_master where dataset_type = 'raw') b
on a.dataset_id = b.dataset_id) a
inner join
(select process_id, dataset_id, batch_id, file_id, 
sum(case when file_process_name = 'Download Files from source location' then record_count else 0 end) as src_record_count,
sum(case when file_process_name = 'Pre DQM Standardization' then record_count else 0 end) as pre_dqm_record_count,
sum(case when file_process_name = 'Staging' then record_count else 0 end) as stg_record_count
from audit_information.log_file_dtl group by 1, 2, 3, 4) b
on a.dataset_id = b.dataset_id and a.batch_id = b.batch_id and a.file_id = b.file_id) a
inner join
(select distinct process_id, process_name from audit_information.ctl_cluster_config) b
on a.process_id = b.process_id) a
left outer join
(
select process_id, dataset_id, max(batch_id) as batch_id, 1 as batch_rnk from audit_information.log_batch_dtl where batch_status = 'SUCCEEDED' group by 1, 2

union 

select a.process_id, a.dataset_id, max(a.batch_id) as batch_id, 2 as batch_rnk from
(select process_id, dataset_id, batch_id from audit_information.log_batch_dtl where batch_status = 'SUCCEEDED' group by 1, 2, 3) a
left outer join
(select process_id, dataset_id, max(batch_id) as batch_id from audit_information.log_batch_dtl where batch_status = 'SUCCEEDED' group by 1, 2) b
on a.process_id = b.process_id and a.dataset_id = b.dataset_id and a.batch_id = b.batch_id
where b.batch_id is null
group by 1, 2
) b
on a.process_id = b.process_id and a.dataset_id = b.dataset_id and a.batch_id = b.batch_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;


/* View L2 DQ validation summary */
drop view audit_information.vw_log_dqm_validation_dtl;
create view audit_information.vw_log_dqm_validation_dtl
as
select 
a.process_id, process_name, frequency, step_name, a.rule_id, rule_name, data_date, a.cycle_id, 
cycle_rnk, case when cycle_rnk = 1 then 'Y' else 'N' end as latest_cycle_ind,
validation_status, validation_message, error_record_count, rule_name as error_table_name, error_details_path
from
(select a.*, b.process_name from
(select a.*, b.rule_name from
(select * from audit_information.log_rule_validation_dtl) a
inner join
(select distinct process_id, rule_id, rule_name from audit_information.ctl_rule_config) b
on a.rule_id = b.rule_id and a.process_id = b.process_id) a
inner join
(select distinct process_id, process_name from audit_information.ctl_cluster_config) b
on a.process_id = b.process_id) a
left outer join
(
select process_id, rule_id, max(cycle_id) as cycle_id, 1 as cycle_rnk from audit_information.log_rule_dtl where rule_status = 'SUCCEEDED' group by 1, 2

union

select a.process_id, a.rule_id, max(a.cycle_id) as cycle_id, 2 as cycle_rnk from
(select process_id, rule_id, cycle_id from audit_information.log_rule_dtl where rule_status = 'SUCCEEDED' group by 1, 2, 3) a
left outer join
(select process_id, rule_id, max(cycle_id) as cycle_id from audit_information.log_rule_dtl where rule_status = 'SUCCEEDED' group by 1, 2) b
on a.process_id = b.process_id and a.rule_id = b.rule_id and a.cycle_id = b.cycle_id
where b.cycle_id is null
group by 1, 2
) b
on a.process_id = b.process_id and a.rule_id = b.rule_id and a.cycle_id = b.cycle_id;