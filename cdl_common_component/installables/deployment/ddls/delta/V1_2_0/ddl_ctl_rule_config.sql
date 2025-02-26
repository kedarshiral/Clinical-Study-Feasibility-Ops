CREATE TABLE `ctl_rule_config` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `rule_id` int(11) NOT NULL,
  `rule_type` varchar(200) DEFAULT NULL,
  `rule_name` varchar(200) DEFAULT NULL,
  `rule_description` varchar(500) DEFAULT NULL,
  `script_name` varchar(200) DEFAULT NULL,
  `script_display_name` varchar(200) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  `property_json` json DEFAULT NULL,
  PRIMARY KEY (`rule_id`)
);

-- If table is already created
ALTER TABLE ctl_rule_config DROP COLUMN  script_path;
ALTER TABLE ctl_rule_config MODIFY COLUMN rule_id int(11) AFTER step_name;
ALTER TABLE ctl_rule_config MODIFY COLUMN rule_type varchar(200) AFTER rule_id;
