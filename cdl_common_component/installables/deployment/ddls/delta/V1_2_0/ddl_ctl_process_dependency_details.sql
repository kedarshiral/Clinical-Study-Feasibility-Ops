create table ctl_process_dependency_details
(
process_id int(11),
process_name varchar(200),
frequency varchar(200),
dataset_id varchar(200),
dependency_pattern 	varchar(200),
dependency_value 	varchar(200),
override_dependency_value	varchar(200),
source_path	varchar(500),
data_dt	date,
write_dependency_value varchar(200)
);