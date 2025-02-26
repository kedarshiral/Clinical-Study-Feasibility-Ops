CREATE TABLE `log_cycle_dtl` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` int(11) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `cluster_id` varchar(200) DEFAULT NULL,
  `cycle_status` varchar(200) DEFAULT NULL,
  `cycle_start_time` timestamp NULL DEFAULT NULL,
  `cycle_end_time` timestamp NULL DEFAULT NULL
);

ALTER TABLE log_cycle_dtl MODIFY COLUMN cycle_id bigint(40)