ALTER TABLE ctl_dataset_master DROP COLUMN attr_1_name;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_1_value;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_2_name;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_2_value;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_3_name;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_3_value;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_4_name;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_4_value;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_5_name;
ALTER TABLE ctl_dataset_master DROP COLUMN attr_5_value;
ALTER TABLE ctl_dataset_master DROP COLUMN profiling_flag;
ALTER TABLE ctl_dataset_master DROP COLUMN post_dqm_location;

ALTER TABLE ctl_dataset_master ADD COLUMN dataset_type varchar(200) AFTER dataset_id;
ALTER TABLE ctl_dataset_master MODIFY static_file_pattern varchar(200) AFTER file_name_pattern;
ALTER TABLE ctl_dataset_master MODIFY multipart_ind	char(1) AFTER static_file_pattern;
ALTER TABLE ctl_dataset_master MODIFY hdfs_post_dqm_location varchar(500) AFTER s3_pre_dqm_location;
ALTER TABLE ctl_dataset_master MODIFY s3_post_dqm_location varchar(500) AFTER hdfs_post_dqm_location;

ALTER TABLE ctl_dataset_master ADD COLUMN table_name varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN table_partition_by varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN table_type varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN table_storage_format varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN table_delimiter varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN table_s3_path	varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN table_hdfs_path varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN table_analyze_flag char(1);
ALTER TABLE ctl_dataset_master ADD COLUMN dataset_publish_flag char(1);
ALTER TABLE ctl_dataset_master ADD COLUMN publish_table_name varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN publish_table_partition_by varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN publish_table_type varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN publish_table_storage_format	varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN publish_table_delimiter varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN publish_s3_path varchar(500);
ALTER TABLE ctl_dataset_master ADD COLUMN publish_hdfs_path	varchar(500);

ALTER TABLE ctl_dataset_master ADD COLUMN row_id_exclusion_flag char(1) AFTER header_available_flag;