CREATE TABLE `log_step_dtl` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` int(11) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `step_status` varchar(200) DEFAULT NULL,
  `step_start_time` timestamp NULL DEFAULT NULL,
  `step_end_time` timestamp NULL DEFAULT NULL
);

ALTER TABLE log_step_dtl MODIFY COLUMN cycle_id bigint(40)