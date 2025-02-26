ALTER TABLE ctl_dataset_master ADD COLUMN stitch_flag char(1) AFTER staging_table;
ALTER TABLE ctl_dataset_master ADD COLUMN stitch_type varchar(15) AFTER stitch_flag;
ALTER TABLE ctl_dataset_master ADD COLUMN stitch_key_column varchar(15) AFTER stitch_type;