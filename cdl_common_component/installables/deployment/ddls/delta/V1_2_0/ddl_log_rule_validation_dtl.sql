CREATE TABLE `log_rule_validation_dtl` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` int(11) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `rule_id` int(11) DEFAULT NULL,
  `validation_status` varchar(200) DEFAULT NULL,
  `validation_message` varchar(200) DEFAULT NULL,
  `error_record_count` int(11) DEFAULT NULL,
  `error_details_path` varchar(200) DEFAULT NULL
);

ALTER TABLE log_rule_validation_dtl MODIFY COLUMN cycle_id bigint(40);