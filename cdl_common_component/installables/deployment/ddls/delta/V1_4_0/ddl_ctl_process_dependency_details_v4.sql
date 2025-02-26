ALTER TABLE ctl_process_dependency_details ADD COLUMN table_type varchar(200) AFTER frequency;
ALTER TABLE ctl_process_dependency_details CHANGE `data_dt` `data_date` date;