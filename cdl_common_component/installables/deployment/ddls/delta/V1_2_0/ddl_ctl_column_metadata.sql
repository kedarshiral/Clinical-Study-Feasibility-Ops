ALTER TABLE ctl_column_metadata DROP COLUMN table_id;
ALTER TABLE ctl_column_metadata ADD COLUMN table_name varchar(200) AFTER column_id;
ALTER TABLE ctl_column_metadata ADD COLUMN column_tag varchar(1000) AFTER column_sequence_number;
ALTER TABLE ctl_column_metadata ADD COLUMN column_date_format varchar(50) AFTER column_data_type;