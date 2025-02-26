ALTER TABLE ctl_process_date_mapping ADD COLUMN process_override_flag char(1) AFTER data_date;
ALTER TABLE ctl_process_date_mapping CHANGE `process_override_flag` `process_interruption_flag` char(1);