CREATE TABLE `ctl_process_dependency_master` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `table_type` varchar(200) DEFAULT NULL,
  `dataset_id` varchar(200) DEFAULT NULL,
  `lag_week_num` int(4) DEFAULT NULL,
  `dependency_pattern` varchar(200) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL
);