DROP TABLE IF EXISTS `ctl_cluster_config`;
CREATE TABLE `ctl_cluster_config` (
  `process_id` int(11) NOT NULL AUTO_INCREMENT,
  `process_name` varchar(100) DEFAULT NULL,
  `frequency` varchar(50) DEFAULT NULL,
  `vpc_id` varchar(50) DEFAULT NULL,
  `subnet_id` varchar(50) DEFAULT NULL,
  `cluster_name` varchar(100) DEFAULT NULL,
  `region_name` varchar(50) DEFAULT NULL,
  `release_version` varchar(50) DEFAULT NULL,
  `master_instance_type` varchar(40) DEFAULT NULL,
  `master_instance_storage` int(11) DEFAULT NULL,
  `core_instance_type` varchar(40) DEFAULT NULL,
  `core_instance_storage` int(11) DEFAULT NULL,
  `core_market_type` varchar(40) DEFAULT NULL,
  `task_instance_type` varchar(40) DEFAULT NULL,
  `task_instance_storage` int(11) DEFAULT NULL,
  `task_market_type` varchar(40) DEFAULT NULL,
  `num_core_instances` int(11) DEFAULT NULL,
  `num_task_instances` int(11) DEFAULT NULL,
  `role_name` varchar(100) DEFAULT NULL,
  `instance_profile_name` varchar(100) DEFAULT NULL,
  `key_pair_name` varchar(100) DEFAULT NULL,
  `glue_catalog_flag` char(1) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `property_json` json DEFAULT NULL,
  `core_spot_bid_price` decimal(8,4) DEFAULT NULL,
  `task_spot_bid_price` decimal(8,4) DEFAULT NULL,
  PRIMARY KEY (`process_id`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


DROP TABLE IF EXISTS `ctl_column_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_column_metadata` (
  `column_id` int(11) NOT NULL AUTO_INCREMENT,
  `table_name` varchar(200) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `column_name` varchar(255) DEFAULT NULL,
  `column_data_type` varchar(50) DEFAULT NULL,
  `column_date_format` varchar(50) DEFAULT NULL,
  `column_description` varchar(255) DEFAULT NULL,
  `source_column_name` varchar(255) DEFAULT NULL,
  `column_sequence_number` smallint(6) DEFAULT NULL,
  `column_tag` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`column_id`)
) ENGINE=InnoDB AUTO_INCREMENT=201 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `ctl_data_standardization_functions`
--

DROP TABLE IF EXISTS `ctl_data_standardization_functions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_data_standardization_functions` (
  `function_id` int(11) NOT NULL AUTO_INCREMENT,
  `function_name` varchar(50) DEFAULT NULL,
  `function_description` varchar(200) DEFAULT NULL,
  `function_param_template` varchar(50) DEFAULT NULL,
  `display_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`function_id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `ctl_data_standardization_master`
--

DROP TABLE IF EXISTS `ctl_data_standardization_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_data_standardization_master` (
  `dataset_id` int(11) DEFAULT NULL,
  `column_name` varchar(50) DEFAULT NULL,
  `function_id` int(11) NOT NULL,
  `function_name` varchar(50) NOT NULL,
  `function_params` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `ctl_dataset_master`
--

DROP TABLE IF EXISTS `ctl_dataset_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_dataset_master` (
  `dataset_id` int(11) NOT NULL AUTO_INCREMENT,
  `dataset_type` varchar(200) DEFAULT NULL,
  `dataset_name` varchar(100) DEFAULT NULL,
  `dataset_description` varchar(200) DEFAULT NULL,
  `provider` varchar(100) DEFAULT NULL,
  `subject_area` varchar(200) DEFAULT NULL,
  `expiry_date` datetime DEFAULT NULL,
  `load_type` varchar(30) DEFAULT NULL,
  `arrival_frequency` varchar(20) DEFAULT NULL,
  `arrival_time` datetime DEFAULT NULL,
  `arrival_day_of_month` int(11) DEFAULT NULL,
  `arrival_day_of_week` int(11) DEFAULT NULL,
  `file_name_pattern` varchar(200) DEFAULT NULL,
  `static_file_pattern` varchar(200) DEFAULT NULL,
  `multipart_ind` char(1) DEFAULT NULL,
  `file_format` varchar(100) DEFAULT NULL,
  `file_field_delimiter` varchar(4) DEFAULT NULL,
  `header_available_flag` char(1) DEFAULT NULL,
  `row_id_exclusion_flag` varchar(40) DEFAULT NULL,
  `connection_id` int(11) DEFAULT NULL,
  `file_source_type` varchar(100) DEFAULT NULL,
  `file_source_location` varchar(512) DEFAULT NULL,
  `file_compression_codec` varchar(10) DEFAULT NULL,
  `file_num_of_fields` int(11) DEFAULT NULL,
  `pre_landing_location` varchar(500) DEFAULT NULL,
  `hdfs_landing_location` varchar(500) DEFAULT NULL,
  `s3_landing_location` varchar(500) DEFAULT NULL,
  `hdfs_pre_dqm_location` varchar(500) DEFAULT NULL,
  `s3_pre_dqm_location` varchar(500) DEFAULT NULL,
  `hdfs_post_dqm_location` varchar(500) DEFAULT NULL,
  `s3_post_dqm_location` varchar(500) DEFAULT NULL,
  `std_location` varchar(500) DEFAULT NULL,
  `staging_location` varchar(500) DEFAULT NULL,
  `archive_location` varchar(500) DEFAULT NULL,
  `pre_landing_table` varchar(200) DEFAULT NULL,
  `landing_table` varchar(200) DEFAULT NULL,
  `pre_dqm_table` varchar(200) DEFAULT NULL,
  `post_dqm_table` varchar(200) DEFAULT NULL,
  `std_table` varchar(200) DEFAULT NULL,
  `staging_table` varchar(200) DEFAULT NULL,
  `stitch_flag` char(1) DEFAULT NULL,
  `stitch_type` varchar(15) DEFAULT NULL,
  `stitch_key_column` varchar(15) DEFAULT NULL,
  `document_link` varchar(200) DEFAULT NULL,
  `tags` varchar(200) DEFAULT NULL,
  `table_name` varchar(500) DEFAULT NULL,
  `table_partition_by` varchar(500) DEFAULT NULL,
  `table_type` varchar(500) DEFAULT NULL,
  `table_storage_format` varchar(500) DEFAULT NULL,
  `table_delimiter` varchar(500) DEFAULT NULL,
  `table_s3_path` varchar(500) DEFAULT NULL,
  `table_hdfs_path` varchar(500) DEFAULT NULL,
  `table_analyze_flag` char(1) DEFAULT NULL,
  `table_analyze_optional_column_list` varchar(500) DEFAULT NULL,
  `dataset_publish_flag` char(1) DEFAULT NULL,
  `publish_type` varchar(50) DEFAULT NULL,
  `publish_table_name` varchar(500) DEFAULT NULL,
  `publish_table_partition_by` varchar(500) DEFAULT NULL,
  `publish_table_type` varchar(500) DEFAULT NULL,
  `publish_table_storage_format` varchar(500) DEFAULT NULL,
  `publish_table_delimiter` varchar(500) DEFAULT NULL,
  `publish_s3_path` varchar(500) DEFAULT NULL,
  `publish_hdfs_path` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`dataset_id`)
) ENGINE=InnoDB AUTO_INCREMENT=233 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ctl_dqm_master`
--

DROP TABLE IF EXISTS `ctl_dqm_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_dqm_master` (
  `qc_id` int(11) NOT NULL AUTO_INCREMENT,
  `dataset_id` int(11) DEFAULT NULL,
  `qc_type` varchar(50) DEFAULT NULL,
  `column_name` varchar(500) DEFAULT NULL,
  `qc_param` varchar(1000) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `qc_filter` varchar(1000) DEFAULT NULL,
  `criticality` varchar(20) DEFAULT NULL,
  `criticality_threshold_pct` int(11) DEFAULT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  `application_id` varchar(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`qc_id`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `ctl_process_date_mapping`
--

DROP TABLE IF EXISTS `ctl_process_date_mapping`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_process_date_mapping` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(50) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `process_interruption_flag` char(1) DEFAULT NULL,
  `insert_by` varchar(50) DEFAULT NULL,
  `insert_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `unique_process` (`process_id`,`frequency`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `ctl_process_dependency_details`
--

DROP TABLE IF EXISTS `ctl_process_dependency_details`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_process_dependency_details` (
  `process_id` int(11) DEFAULT NULL,
  `process_name` varchar(200) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `table_type` varchar(200) DEFAULT NULL,
  `dataset_id` varchar(200) DEFAULT NULL,
  `dependency_pattern` varchar(200) DEFAULT NULL,
  `dependency_value` bigint(40) DEFAULT NULL,
  `override_dependency_value` bigint(40) DEFAULT NULL,
  `source_path` varchar(500) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `write_dependency_value` bigint(40) DEFAULT NULL,
  `copy_data_s3_to_hdfs_status` char(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `ctl_process_dependency_master`
--

DROP TABLE IF EXISTS `ctl_process_dependency_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ctl_rule_config`
--

DROP TABLE IF EXISTS `ctl_rule_config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
  `spark_config` varchar(1000) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  `property_json` json DEFAULT NULL,
  PRIMARY KEY (`rule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `ctl_step_workflow_master`
--

DROP TABLE IF EXISTS `ctl_step_workflow_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_step_workflow_master` (
  `process_id` int(11) DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `workflow_name` varchar(200) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  `property_json` json DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `ctl_table_metadata`
--

DROP TABLE IF EXISTS `ctl_table_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_table_metadata` (
  `table_id` int(11) NOT NULL AUTO_INCREMENT,
  `dataset_id` int(11) DEFAULT NULL,
  `table_name` varchar(255) DEFAULT NULL,
  `table_description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ctl_workflow_master`
--

DROP TABLE IF EXISTS `ctl_workflow_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_workflow_master` (
  `workflow_id` int(11) NOT NULL AUTO_INCREMENT,
  `workflow_name` varchar(200) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `process_id` int(11) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  `property_json` varchar(500) DEFAULT NULL,
  `spark_config` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`workflow_id`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_batch_dtl`
--

DROP TABLE IF EXISTS `log_batch_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_batch_dtl` (
  `batch_id` bigint(40) NOT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `process_id` int(11) DEFAULT NULL,
  `cluster_id` varchar(300) DEFAULT NULL,
  `batch_status` varchar(200) DEFAULT NULL,
  `batch_start_time` timestamp NULL DEFAULT NULL,
  `batch_end_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`batch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `log_cluster_dtl`
--

DROP TABLE IF EXISTS `log_cluster_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_cluster_dtl` (
  `cluster_id` varchar(100) DEFAULT NULL,
  `cluster_name` varchar(100) DEFAULT NULL,
  `process_id` int(11) DEFAULT NULL,
  `cluster_status` varchar(40) DEFAULT NULL,
  `master_node_dns` varchar(50) DEFAULT NULL,
  `master_instance_type` varchar(100) DEFAULT NULL,
  `core_market_type` varchar(100) DEFAULT NULL,
  `core_instance_type` varchar(100) DEFAULT NULL,
  `num_core_instances` varchar(100) DEFAULT NULL,
  `core_spot_bid_price` varchar(100) DEFAULT NULL,
  `task_market_type` varchar(100) DEFAULT NULL,
  `task_instance_type` varchar(100) DEFAULT NULL,
  `num_task_instances` varchar(100) DEFAULT NULL,
  `task_spot_bid_price` varchar(100) DEFAULT NULL,
  `cluster_launch_user` varchar(50) DEFAULT NULL,
  `cluster_create_request_time` timestamp NULL DEFAULT NULL,
  `cluster_start_time` timestamp NULL DEFAULT NULL,
  `cluster_terminate_user` varchar(50) DEFAULT NULL,
  `cluster_stop_time` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_cluster_host_dtl`
--

DROP TABLE IF EXISTS `log_cluster_host_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_cluster_host_dtl` (
  `cluster_id` varchar(100) DEFAULT NULL,
  `host_ip_address` varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_cycle_dtl`
--

DROP TABLE IF EXISTS `log_cycle_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_cycle_dtl` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` bigint(40) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `cluster_id` varchar(200) DEFAULT NULL,
  `cycle_status` varchar(200) DEFAULT NULL,
  `cycle_start_time` timestamp NULL DEFAULT NULL,
  `cycle_end_time` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_data_copy_dtl`
--

DROP TABLE IF EXISTS `log_data_copy_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_data_copy_dtl` (
  `log_id` int(20) NOT NULL AUTO_INCREMENT,
  `process_name` varchar(50) DEFAULT NULL,
  `source_location` varchar(500) DEFAULT NULL,
  `target_location` varchar(500) DEFAULT NULL,
  `input_signature` varchar(5000) DEFAULT NULL,
  `copy_status` varchar(50) DEFAULT NULL,
  `copy_start_time` timestamp NULL DEFAULT NULL,
  `copy_end_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`log_id`)
) ENGINE=InnoDB AUTO_INCREMENT=876 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_dqm_smry`
--

DROP TABLE IF EXISTS `log_dqm_smry`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_dqm_smry` (
  `application_id` varchar(20) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `batch_id` bigint(20) DEFAULT NULL,
  `file_id` int(11) DEFAULT NULL,
  `qc_id` int(11) DEFAULT NULL,
  `column_name` varchar(500) DEFAULT NULL,
  `qc_type` varchar(50) DEFAULT NULL,
  `qc_param` varchar(1000) DEFAULT NULL,
  `criticality` varchar(20) DEFAULT NULL,
  `error_count` int(11) DEFAULT NULL,
  `qc_message` varchar(100) DEFAULT NULL,
  `qc_failure_percentage` varchar(20) DEFAULT NULL,
  `qc_status` varchar(20) DEFAULT NULL,
  `qc_start_time` timestamp NULL DEFAULT NULL,
  `qc_end_time` timestamp NULL DEFAULT NULL,
  `qc_create_by` varchar(20) DEFAULT NULL,
  `qc_create_date` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_file_dtl`
--

DROP TABLE IF EXISTS `log_file_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_file_dtl` (
  `file_id` int(11) DEFAULT NULL,
  `file_name` varchar(200) DEFAULT NULL,
  `file_process_name` varchar(200) DEFAULT NULL,
  `file_process_status` varchar(200) DEFAULT NULL,
  `file_process_start_time` timestamp NULL DEFAULT NULL,
  `file_process_end_time` timestamp NULL DEFAULT NULL,
  `batch_id` bigint(40) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `process_id` int(11) DEFAULT NULL,
  `cluster_id` varchar(50) DEFAULT NULL,
  `record_count` int(20) DEFAULT NULL,
  `process_message` varchar(150) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `log_file_smry`
--

DROP TABLE IF EXISTS `log_file_smry`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_file_smry` (
  `file_id` int(11) NOT NULL AUTO_INCREMENT,
  `file_name` varchar(200) DEFAULT NULL,
  `file_status` varchar(200) DEFAULT NULL,
  `batch_id` bigint(40) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `process_id` int(11) DEFAULT NULL,
  `cluster_id` varchar(50) DEFAULT NULL,
  `file_process_start_time` timestamp NULL DEFAULT NULL,
  `file_process_end_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`file_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1079 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_pre_dqm_dtl`
--

DROP TABLE IF EXISTS `log_pre_dqm_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `log_rule_dtl`
--

DROP TABLE IF EXISTS `log_rule_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_rule_dtl` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` bigint(40) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `rule_id` int(11) DEFAULT NULL,
  `rule_status` varchar(200) DEFAULT NULL,
  `rule_start_time` timestamp NULL DEFAULT NULL,
  `rule_end_time` timestamp NULL DEFAULT NULL,
  `rule_type` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_rule_validation_dtl`
--

DROP TABLE IF EXISTS `log_rule_validation_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_rule_validation_dtl` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` bigint(40) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `rule_id` int(11) DEFAULT NULL,
  `validation_status` varchar(200) DEFAULT NULL,
  `validation_message` varchar(200) DEFAULT NULL,
  `error_record_count` int(11) DEFAULT NULL,
  `error_details_path` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_step_dtl`
--

DROP TABLE IF EXISTS `log_step_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_step_dtl` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` bigint(40) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `step_status` varchar(200) DEFAULT NULL,
  `step_start_time` timestamp NULL DEFAULT NULL,
  `step_end_time` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ctl_audit_table_update_permission`
--

DROP TABLE IF EXISTS `ctl_audit_table_update_permission`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_audit_table_update_permission` (
  `table_name` varchar(100) DEFAULT NULL,
  `column_name` varchar(100) DEFAULT NULL,
  `group_id` varchar(50) DEFAULT NULL,
  `insert_flag` tinyint(1) DEFAULT NULL,
  `update_flag` tinyint(1) DEFAULT NULL,
  `delete_flag` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `log_audit_table_update`
--

DROP TABLE IF EXISTS `log_audit_table_update`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_audit_table_update` (
  `update_table` varchar(50) DEFAULT NULL,
  `update_columns` varchar(50) DEFAULT NULL,
  `effective_user` varchar(20) DEFAULT NULL,
  `update_time` timestamp NULL DEFAULT NULL,
  `query_type` varchar(20) DEFAULT NULL,
  `update_query` varchar(10240) DEFAULT NULL,
  `rows_updated` int(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


