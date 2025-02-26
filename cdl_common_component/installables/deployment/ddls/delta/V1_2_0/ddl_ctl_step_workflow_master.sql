create table ctl_step_workflow_master
(
process_id int(11),
step_name varchar(200),
workflow_name varchar(200),
active_flag	char(1),
insert_by varchar(100),
insert_date	timestamp,
update_by varchar(100),
update_date	timestamp,
property_json json
);