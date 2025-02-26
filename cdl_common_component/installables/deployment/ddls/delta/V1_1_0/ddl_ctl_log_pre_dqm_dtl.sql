CREATE TABLE `log_pre_dqm_dtl` (
  `process_id` int(11) DEFAULT NULL,
  `process_name` varchar(100) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `workflow_name` varchar(200) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `column_name` varchar(20) DEFAULT NULL,
  `function_name` varchar(100) DEFAULT NULL,
  `function_params` varchar(100) DEFAULT NULL,
  `status` varchar(100) DEFAULT NULL,
  `updated_timestamp` timestamp NULL DEFAULT NULL,
  `file_id` int(11) DEFAULT NULL,
  `batch_id` bigint(40) DEFAULT NULL
);