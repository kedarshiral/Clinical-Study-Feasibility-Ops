DROP TABLE IF EXISTS `ctl_dataset_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_dataset_master` (
  `dataset_id` int(11) NOT NULL UNIQUE AUTO_INCREMENT,
  `dataset_type` varchar(200) DEFAULT NULL,
  `domain` varchar(200) DEFAULT NULL,
  `dataset_sub_type` varchar(200) DEFAULT NULL,
  `dataset_name` varchar(100) NOT NULL UNIQUE,
  `dataset_description` varchar(200) DEFAULT NULL,
  `provider` varchar(100) DEFAULT NULL,
  `subject_area` varchar(200) DEFAULT NULL,
  `expiry_date` datetime DEFAULT NULL,
  `load_type` varchar(30) DEFAULT NULL,
  `arrival_frequency` varchar(20) DEFAULT NULL,
  `arrival_time` datetime DEFAULT NULL,
  `arrival_day_of_month` int(11) DEFAULT NULL,
  `arrival_day_of_week` int(11) DEFAULT NULL,
  `file_name_pattern` varchar(200) NOT NULL,
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
  `staging_location` varchar(500) DEFAULT NULL,
  `archive_location` varchar(500) DEFAULT NULL,
  `pre_landing_table` varchar(200) DEFAULT NULL,
  `landing_table` varchar(200) DEFAULT NULL,
  `pre_dqm_table` varchar(200) DEFAULT NULL,
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
  `publish_s3_path` varchar(500) DEFAULT NULL,
  `geography_id` int(11) NOT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`dataset_id`, `dataset_name`, `geography_id`),
  KEY (dataset_id,geography_id)
) ENGINE=InnoDB AUTO_INCREMENT=233 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `ctl_workflow_master`
--

DROP TABLE IF EXISTS `ctl_workflow_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_workflow_master` (
  `workflow_id` int(11) NOT NULL UNIQUE AUTO_INCREMENT,
  `workflow_name` varchar(200) NOT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `process_id` varchar(100) NOT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  `property_json` varchar(500) DEFAULT NULL,
  `spark_config` varchar(1000) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL, 
  PRIMARY KEY (`workflow_id`, `workflow_name`),
  Foreign Key (dataset_id, geography_id) REFERENCES ctl_dataset_master(dataset_id, geography_id),
  Foreign Key (process_id, geography_id) REFERENCES ctl_cluster_config(process_id, geography_id)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `ctl_dqm_functions`
--

DROP TABLE IF EXISTS `ctl_dqm_functions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_dqm_functions` (
  `qc_function_id` int(11) NOT NULL AUTO_INCREMENT,
  `qc_function_name` varchar(50) DEFAULT NULL,
  `qc_function_description` varchar(200) DEFAULT NULL,
  `qc_param_template` varchar(50) DEFAULT NULL,
  `qc_display_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`qc_function_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `ctl_dqm_master`
--

DROP TABLE IF EXISTS `ctl_dqm_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_dqm_master` (
  `process_id` varchar(100) DEFAULT NULL,
  `qc_id` int(11) NOT NULL UNIQUE AUTO_INCREMENT,
  `dataset_id` int(11) DEFAULT NULL,
  `qc_type` varchar(50) DEFAULT NULL,
  `qc_query` varchar(1000) DEFAULT NULL,
  `column_name` varchar(500) DEFAULT NULL,
  `qc_param` varchar(1000) DEFAULT NULL,
  `current_version_table` varchar(100) DEFAULT NULL,
  `previous_version_table` varchar(100) DEFAULT NULL,
  `previous_version_number` varchar(100) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `qc_filter` varchar(1000) DEFAULT NULL,
  `criticality` varchar(20) DEFAULT NULL,
  `threshold_type` varchar(20) DEFAULT NULL,
  `threshold_value` int(11) DEFAULT NULL,
  `error_table_name` varchar(100) DEFAULT NULL,
  `status_message` varchar(100) DEFAULT NULL,
  `criticality_threshold_pct` int(11) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL, 
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  `application_id` varchar(20) DEFAULT '0',
  PRIMARY KEY (`qc_id`),
  Foreign Key (dataset_id, geography_id) REFERENCES ctl_dataset_master(dataset_id, geography_id)
  ON UPDATE CASCADE
  ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


DROP TABLE IF EXISTS `ctl_column_metadata`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_column_metadata` (
  `column_id` int(11) NOT NULL AUTO_INCREMENT,
  `table_name` varchar(200) DEFAULT NULL,
  `dataset_id` int(11) NOT NULL ,
  `column_name` varchar(255) DEFAULT NULL,
  `column_data_type` varchar(50) DEFAULT NULL,
  `column_date_format` varchar(50) DEFAULT NULL,
  `column_description` varchar(255) DEFAULT NULL,
  `source_column_name` varchar(255) DEFAULT NULL,
  `column_sequence_number` smallint(6) DEFAULT NULL,
  `column_tag` varchar(1000) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL,
    `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`column_id`),
  Foreign Key (dataset_id, geography_id) REFERENCES ctl_dataset_master(dataset_id, geography_id)
  ON UPDATE CASCADE
  ON DELETE CASCADE
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
  `dataset_id` int(11) NOT NULL,
  `column_name` varchar(50) NOT NULL,
  `function_id` int(11) NOT NULL,
  `function_name` varchar(50) NOT NULL,
  `function_params` varchar(50) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL,
    `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
   Foreign Key (dataset_id, geography_id) REFERENCES ctl_dataset_master(dataset_id, geography_id)
   ON UPDATE CASCADE
   ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


DROP TABLE IF EXISTS `ctl_cluster_config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_cluster_config` (
  `process_id` varchar(100) NOT NULL,
  `process_name` varchar(100) NOT NULL UNIQUE,
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
  `geography_id` int(11) NOT NULL,
    `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`process_id`,`process_name`,`geography_id`),
    KEY (process_id,geography_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `ctl_rule_config`
--

DROP TABLE IF EXISTS `ctl_rule_config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_rule_config` (
  `process_id` varchar(100) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `upstream_dependency` varchar (200),
  `rule_id` int(11) NOT NULL AUTO_INCREMENT,
  `rule_type` varchar(200) DEFAULT NULL,
  `rule_name` varchar(200) DEFAULT NULL,
  `rule_description` varchar(500) DEFAULT NULL,
  `metric_id` int (11) DEFAULT NULL,
  `script_name` varchar(200) DEFAULT NULL,
  `script_display_name` varchar(200) DEFAULT NULL,
  `spark_config` varchar(1000) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL,
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
-- Table structure for table `ctl_process_dependency_master`
--

DROP TABLE IF EXISTS `ctl_process_dependency_master`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_process_dependency_master` (
  `process_id` varchar(100) DEFAULT NULL,
  `application_id` varchar(200) DEFAULT NULL,
  `process_group` varchar(200) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `table_type` varchar(200) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `lag_week_num` int(4) DEFAULT NULL,
  `dependency_pattern` varchar(200) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
   Foreign Key (dataset_id, geography_id) REFERENCES ctl_dataset_master(dataset_id, geography_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


DROP TABLE IF EXISTS `ctl_data_access_request_details`;
CREATE TABLE `ctl_data_access_request_details` (
  `dataset_id` varchar(100) DEFAULT NULL,
  `dataset_name` varchar(100) DEFAULT NULL,
  `request_by` varchar(100) DEFAULT NULL,
  `requester_role_id` varchar(100) DEFAULT NULL,
  `request_date` timestamp NULL DEFAULT NULL,
  `request_status` varchar(100) DEFAULT NULL,
  `approved_by_1` varchar(100) DEFAULT NULL,
  `approved_by_2` varchar(100) DEFAULT NULL,
  `approved_by_3` varchar(100) DEFAULT NULL,
  `approver1_role_id` varchar(100) DEFAULT NULL,
  `approver2_role_id` varchar(100) DEFAULT NULL,
  `approver3_role_id` varchar(100) DEFAULT NULL,
  `approve_date` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `ctl_rule_request_details`;
CREATE TABLE `ctl_rule_request_details` (
  `rule_category` varchar(100) DEFAULT NULL,
  `application_id` varchar(200) DEFAULT NULL,
  `rule_id` varchar(100) DEFAULT NULL,
  `rule_name` varchar(100) DEFAULT NULL,
  `request_by` varchar(100) DEFAULT NULL,
  `requester_role_id` varchar(100) DEFAULT NULL,
  `request_date` timestamp NULL DEFAULT NULL,
  `request_status` varchar(100) DEFAULT NULL,
  `approved_by_1` varchar(100) DEFAULT NULL,
  `approved_by_2` varchar(100) DEFAULT NULL,
  `approved_by_3` varchar(100) DEFAULT NULL,
  `approver1_role_id` varchar(100) DEFAULT NULL,
  `approver2_role_id` varchar(100) DEFAULT NULL,
  `approver3_role_id` varchar(100) DEFAULT NULL,
  `request_completed_by` varchar(100) DEFAULT NULL,
  `approve_date` timestamp NULL DEFAULT NULL,
  `request_completed_date` timestamp NULL DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `ctl_byod_master`;
CREATE TABLE `ctl_byod_master` (
  `file_name` varchar(100) NOT NULL UNIQUE,
  `delimiter` varchar(10) DEFAULT NULL,
  `has_header` varchar(10) DEFAULT NULL,
  `source_location` varchar(500) DEFAULT NULL,
  `destination_location` varchar(500) DEFAULT NULL,
  `athena_database_name` varchar(100) DEFAULT NULL,
  `athena_table_name` varchar(100) DEFAULT NULL,
  `upload_status` varchar(100) DEFAULT NULL,
  `table_creation_status` varchar(100) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `uploaded_by` varchar(100) NOT NULL ,
  `uploader_role_id` varchar(100) NOT NULL ,
  `upload_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY(`file_name`, `uploaded_by`, `uploader_role_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `log_user_audit_dtl`;
CREATE TABLE `log_user_audit_dtl` (
  `event_id` int(11) AUTO_INCREMENT,
  `user_id` varchar(50) DEFAULT NULL,
  `role_id` varchar(50) DEFAULT NULL,
  `page_id` int(11) DEFAULT NULL,
  `action` varchar(100) DEFAULT NULL,
  `activity` varchar(100) DEFAULT NULL,
  `event_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `log_stats_dtl`;
CREATE TABLE `log_stats_dtl` (
  `scenario_name` varchar(200) DEFAULT NULL,
  `batch_id` bigint(40) NOT NULL,
  `task_id` varchar(200) DEFAULT NULL,
  `file_id` int(10) NOT NULL,
  `column_name` varchar(200) DEFAULT NULL,
  `data_type` varchar(200) DEFAULT NULL,
  `meanvalue` bigint(40) NOT NULL, 
  `sum` bigint(40) NOT NULL,
  `variance` bigint(40) NOT NULL,
  `skewness` bigint(40) NOT NULL,
  `kurtosis` bigint(40) NOT NULL,
  `standarddeviation` bigint(40) NOT NULL,
  `minvalue` bigint(40) NOT NULL,
  `maxvalue` bigint(40) NOT NULL,
  `meanlength` bigint(40) NOT NULL,
  `standarddeviationlength` bigint(40) NOT NULL,
  `nullcount` bigint(40) NOT NULL,
  `minlength` bigint(40) NOT NULL,
  `maxlength` bigint(40) NOT NULL,
  `distinctcount` bigint(40) NOT NULL,
  `totalcount` bigint(40) NOT NULL,
  `uniqueness_check` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



DROP TABLE IF EXISTS `log_profiler_smry`;
CREATE TABLE `log_profiler_smry` (
  `file_name` varchar(200) DEFAULT NULL,
  `file_location` varchar(200) DEFAULT NULL,
  `delimiter` varchar(200) DEFAULT NULL,
  `file_type` varchar(200) DEFAULT NULL,
  `accuracy_level` varchar(200) DEFAULT NULL,
  `has_header` varchar(200) DEFAULT NULL,
  `row_count` bigint(40) NOT NULL,
  `column_count` int(10) NOT NULL,
  `schema` varchar(200) DEFAULT NULL,
  `dataset_name` varchar(200) DEFAULT NULL,
  `tags` varchar(200) DEFAULT NULL,
  `relationship` varchar(200) DEFAULT NULL,
  `scenario_name` varchar(200) DEFAULT NULL,
  `batch_id` bigint(40) NOT NULL,
  `task_id` varchar(200) DEFAULT NULL,
  `file_id` int(10) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `ctl_projects_master`;
CREATE TABLE `ctl_projects_master` (
  `project_id` int(11) NOT NULL UNIQUE AUTO_INCREMENT,
  `project_name` varchar(50) NOT NULL UNIQUE,
  `description` varchar(100) DEFAULT NULL,
  `project_type` varchar(20) DEFAULT NULL,
  `shared_user_list` varchar(20) DEFAULT NULL,
  `git_location` varchar(20) DEFAULT NULL,
  `pivate_s3_path` varchar(50) DEFAULT NULL,
  `created_by` varchar(50) NOT NULL,
  `creator_role_id` varchar(50) NOT NULL,
  `creation_date` timestamp DEFAULT NULL,
  `last_updated_by` varchar(50) DEFAULT NULL,
  `last_updated_date` timestamp DEFAULT NULL, 
  PRIMARY KEY(`project_id`,`project_name`,`created_by`,`creator_role_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



DROP TABLE IF EXISTS `ctl_projects_notebook_details`;
CREATE TABLE `ctl_projects_notebook_details` (
  `project_id` varchar(50) NOT NULL,
  `notebook_id` int(11) NOT NULL UNIQUE AUTO_INCREMENT,
  `notebook_name` varchar(100) NOT NULL,
  `workbench_type` varchar(50) DEFAULT NULL,
  `notebook_configuration` varchar(100) DEFAULT NULL,
  `created_by` varchar(50) DEFAULT NULL,
  `created_date` timestamp DEFAULT NULL,
  `user_role_id` varchar(50) DEFAULT NULL,
  `last_updated_by` varchar(50) DEFAULT NULL,
  `last_updated_date` timestamp DEFAULT NULL,
  PRIMARY KEY(`notebook_id`,`notebook_name`,`project_id`)
) ENGINE=InnoDB AUTO_INCREMENT=233 DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `ctl_notebook_data_details`;
CREATE TABLE `ctl_notebook_data_details` (
  `project_id` varchar(50) DEFAULT NULL,
  `notebook_id` varchar(50) DEFAULT NULL,
  `database_name` varchar(100) DEFAULT NULL,
  `table_name` varchar(100) DEFAULT NULL,
  `s3_location` varchar(100) DEFAULT NULL,
  `notebook_command` varchar(200) DEFAULT NULL,
  `user_id` varchar(50) DEFAULT NULL,
  `role_id` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


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
  `process_id` varchar(100) DEFAULT NULL,
  `cluster_id` varchar(300) DEFAULT NULL,
  `batch_status` varchar(200) DEFAULT NULL,
  `code_version_tag` varchar(200) DEFAULT NULL,
  `batch_start_time` timestamp NULL DEFAULT NULL,
  `batch_end_time` timestamp NULL DEFAULT NULL,
  `geography_id` int(11) NOT NULL,
  PRIMARY KEY (`batch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


DROP TABLE IF EXISTS `log_cluster_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_cluster_dtl` (
  `cluster_id` varchar(100) DEFAULT NULL,
  `cluster_name` varchar(100) DEFAULT NULL,
  `process_id` varchar(100) DEFAULT NULL,
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
  `cluster_stop_time` timestamp NULL DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL
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
  `process_id` varchar(100) DEFAULT NULL,
  `cluster_id` varchar(50) DEFAULT NULL,
  `file_process_start_time` timestamp NULL DEFAULT NULL,
  `file_process_end_time` timestamp NULL DEFAULT NULL,
  `geography_id` int(11) NOT NULL,
  PRIMARY KEY (`file_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1079 DEFAULT CHARSET=utf8mb4;
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
  `process_id` varchar(100) DEFAULT NULL,
  `cluster_id` varchar(50) DEFAULT NULL,
  `record_count` int(20) DEFAULT NULL,
  `process_message` varchar(150) DEFAULT NULL,
  `geography_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `log_pre_dqm_dtl`
--

DROP TABLE IF EXISTS `log_pre_dqm_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_pre_dqm_dtl` (
  `process_id` varchar(100) DEFAULT NULL,
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
  `batch_id` bigint(40) DEFAULT NULL,
  `geography_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `log_dqm_smry`
--

DROP TABLE IF EXISTS `log_dqm_smry`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_dqm_smry` (
  `application_id` varchar(20) DEFAULT NULL, 
  `process_id` varchar(100) DEFAULT NULL,
  `dataset_id` int(11) DEFAULT NULL,
  `batch_id` bigint(20) DEFAULT NULL,
  `file_id` int(11) DEFAULT NULL,
  `qc_id` int(11) DEFAULT NULL,
  `column_name` varchar(500) DEFAULT NULL,
  `qc_type` varchar(50) DEFAULT NULL,
  `qc_query` varchar(1000) DEFAULT NULL,
  `qc_param` varchar(1000) DEFAULT NULL,
  `criticality` varchar(20) DEFAULT NULL,
  `threshold_type` varchar(20) DEFAULT NULL,
  `threshold_value` int(11) DEFAULT NULL,
  `error_table_name` varchar(100) DEFAULT NULL,
  `error_count` int(11) DEFAULT NULL,
  `cycle_id` bigint(20) DEFAULT NULL,
  `qc_status` varchar(20) DEFAULT NULL,
  `qc_message` varchar(100) DEFAULT NULL,
  `qc_failure_percentage` varchar(20) DEFAULT NULL,
  `qc_start_time` timestamp NULL DEFAULT NULL,
  `qc_end_time` timestamp NULL DEFAULT NULL,
  `qc_create_by` varchar(20) DEFAULT NULL,
  `qc_create_date` timestamp NULL DEFAULT NULL,
  `geography_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `log_cluster_host_dtl`
--

DROP TABLE IF EXISTS `log_cluster_host_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_cluster_host_dtl` (
  `cluster_id` varchar(100) DEFAULT NULL,
  `host_ip_address` varchar(100) DEFAULT NULL,
  `geography_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_cycle_dtl`
--

DROP TABLE IF EXISTS `log_cycle_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_cycle_dtl` (
  `process_id` varchar(100) DEFAULT NULL,
  `application_id` varchar(200) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` bigint(40) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `cluster_id` varchar(200) DEFAULT NULL,
  `cycle_status` varchar(200) DEFAULT NULL,
  `code_version_tag` varchar(200) DEFAULT NULL,
  `cycle_start_time` timestamp NULL DEFAULT NULL,
  `cycle_end_time` timestamp NULL DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `log_step_dtl`
--

DROP TABLE IF EXISTS `log_step_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_step_dtl` (
  `process_id` varchar(100) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` bigint(40) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `step_status` varchar(200) DEFAULT NULL,
  `step_start_time` timestamp NULL DEFAULT NULL,
  `step_end_time` timestamp NULL DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;




--
-- Table structure for table `log_rule_dtl`
--

DROP TABLE IF EXISTS `log_rule_dtl`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `log_rule_dtl` (
  `process_id` varchar(100) DEFAULT NULL,
  `frequency` varchar(200) DEFAULT NULL,
  `workflow_id` varchar(50) DEFAULT NULL,
  `cycle_id` bigint(40) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `step_name` varchar(200) DEFAULT NULL,
  `rule_id` int(11) DEFAULT NULL,
  `rule_status` varchar(200) DEFAULT NULL,
  `rule_start_time` timestamp NULL DEFAULT NULL,
  `rule_end_time` timestamp NULL DEFAULT NULL,
  `rule_type` varchar(200) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL
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
-- Table structure for table `ctl_process_dependency_details`
--

DROP TABLE IF EXISTS `ctl_process_dependency_details`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_process_dependency_details` (
  `process_id` varchar(100) DEFAULT NULL,
  `application_id` varchar(200) DEFAULT NULL,
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
  `copy_data_s3_to_hdfs_status` char(1) DEFAULT NULL,
  `record_count` int(20) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


---
--- Admin Data Model
---

---
--- ctl_user_master
---

DROP TABLE IF EXISTS `ctl_user_master`;
CREATE TABLE `ctl_user_master` (
  `user_id` varchar(50) NOT NULL UNIQUE,
  `first_name` varchar(100) DEFAULT NULL,
  `last_name` varchar(100) DEFAULT NULL,
  `is_approver` char(1) DEFAULT NULL,
  `email_id` varchar(100) DEFAULT NULL,
  `country_code` varchar(10) DEFAULT NULL,
  `phone_number` varchar(200) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL,
  `primary_language` varchar(200) DEFAULT NULL,
  `insert_by` varchar(200) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(200) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY(`user_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 
 
---
--- ctl_role_type_master
---

DROP TABLE IF EXISTS `ctl_role_type_master`;
CREATE TABLE `ctl_role_type_master` (
  `role_type_id` varchar(50) DEFAULT NULL,
  `role_type_name` varchar(100) DEFAULT NULL,
  PRIMARY KEY(`role_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


---
--- ctl_domain_master
---

DROP TABLE IF EXISTS `ctl_domain_master`;
CREATE TABLE `ctl_domain_master` (
  `domain_id` varchar(50) NOT NULL,
  `domain_name` varchar(100) NOT NULL UNIQUE,
  PRIMARY KEY(`domain_id`,`domain_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



---
--- ctl_role_config
---

DROP TABLE IF EXISTS `ctl_role_config`;
CREATE TABLE `ctl_role_config` (
  `role_id` varchar(50) NOT NULL UNIQUE,
  `role_name` varchar(100) NOT NULL UNIQUE,
  `role_desc` varchar(200) DEFAULT NULL,
  `domain_id` varchar(50) DEFAULT NULL,
  `role_type_id` varchar(50) DEFAULT NULL,
  `geography_id` int(11) DEFAULT NULL,
  PRIMARY KEY(`role_id`,`role_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




---
--- ctl_user_role_config
---

DROP TABLE IF EXISTS `ctl_user_role_config`;
CREATE TABLE `ctl_user_role_config` (
  `user_id` varchar(50) DEFAULT NULL,
  `role_id` varchar(50) DEFAULT NULL,
  `primary_flag` varchar(50) DEFAULT NULL,
  PRIMARY KEY(`user_id`,`role_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



---
--- ctl_app_page_master



---

DROP TABLE IF EXISTS `ctl_app_page_master`;
CREATE TABLE `ctl_app_page_master` (
  `page_id` int(11) not null unique AUTO_INCREMENT,
  `page_name` varchar(200) not null,
  `page_desc` varchar(200) DEFAULT NULL,
  `role_type_id` varchar(50) DEFAULT NULL,
  PRIMARY KEY(`page_id`,`page_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




---
--- ctl_role_page_config
---

DROP TABLE IF EXISTS `ctl_role_page_config`;
CREATE TABLE `ctl_role_page_config` (
  `role_id` varchar(50) DEFAULT NULL,
  `page_id` int(11) DEFAULT NULL,
  `access_type` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




---
--- ctl_application_master
---

DROP TABLE IF EXISTS `ctl_application_master`;
CREATE TABLE `ctl_application_master` (
  `application_id` varchar(50) NOT NULL UNIQUE,
  `application_name` varchar(100) DEFAULT NULL,
  `application_owner_user_id` varchar(50) DEFAULT NULL,
  `application_owner_role_id` varchar(50) DEFAULT NULL,
  `connection_id` varchar(50) DEFAULT NULL,
  PRIMARY KEY(`application_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




---
--- ctl_db_connection_master
---

DROP TABLE IF EXISTS `ctl_db_connection_master`;
CREATE TABLE `ctl_db_connection_master` (
  `connection_id` varchar(50) NOT NULL Unique,
  `connection_name` varchar(200) NOT NULL Unique,
  `connection_url` varchar(200) DEFAULT NULL,
  `database_name` varchar(200) DEFAULT NULL,
  `property_json` varchar(200) DEFAULT NULL,
  `active_flag` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`connection_id`,`connection_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




---
--- ctl_role_application_config
---

DROP TABLE IF EXISTS `ctl_role_application_config`;
CREATE TABLE `ctl_role_application_config` (
  `user_id` varchar(50) DEFAULT NULL,
  `role_id` varchar(50) DEFAULT NULL,
  `application_id` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




---
--- ctl_role_dataset_config_vw
---

DROP TABLE IF EXISTS `ctl_role_dataset_config_vw`;
CREATE TABLE `ctl_role_dataset_config_vw` (
  `user_id` varchar(50) DEFAULT NULL,
  `role_id` varchar(50) DEFAULT NULL,
  `dataset_id` varchar(50) DEFAULT NULL,
  `domain` varchar(50) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




---
--- ctl_user_dataset_exlusion
---

DROP TABLE IF EXISTS `ctl_user_dataset_exlusion`;
CREATE TABLE `ctl_user_dataset_exlusion` (
  `user_id` varchar(200) DEFAULT NULL,
  `role_id` varchar(200) DEFAULT NULL,
  `dataset_id` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



---
--- log_infra_dtl
---

DROP TABLE IF EXISTS `log_infra_dtl`;
CREATE TABLE `log_infra_dtl` (
  `application_id` varchar(50) DEFAULT NULL,
  `application_name` varchar(100) DEFAULT NULL,
  `environment_name` varchar(50) DEFAULT NULL,
  `aws_resource_name` varchar(50) DEFAULT NULL,
  `cost_value` varchar(50) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  `geography_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


---
--- log_infra_budget_dtl
---

DROP TABLE IF EXISTS `log_infra_budget_dtl`;
CREATE TABLE `log_infra_budget_dtl` (
  `application_id` varchar(50) DEFAULT NULL,
  `application_name` varchar(100) DEFAULT NULL,
  `environment_name` varchar(50) DEFAULT NULL,
  `aws_resource_name` varchar(50) DEFAULT NULL,
  `yearly_budget_cost` varchar(50)  DEFAULT NULL,
  `geography_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


---
--- ctl_data_acquisition_dtl
---

DROP TABLE IF EXISTS `ctl_data_acquisition_dtl`;
CREATE TABLE `ctl_data_acquisition_dtl` ` (`source_platform` varchar(200) DEFAULT NULL,
`src_system` varchar(200) DEFAULT NULL,
`database_name` varchar(200) DEFAULT NULL,
`object_name` varchar(200) DEFAULT NULL, 
`domain` varchar(200) DEFAULT NULL, 
`load_type` varchar(200) DEFAULT NULL,
`incremental_load_column_name` varchar(200) DEFAULT NULL, 
`override_last_refresh_date` json DEFAULT NULL,
`query` varchar(400) DEFAULT NULL,
`query_file_name` varchar(200) DEFAULT NULL,
`query_description` varchar(200) DEFAULT NULL,
`source_location` varchar(200) DEFAULT NULL,
`source_file_pattern` varchar(200) DEFAULT NULL,
`target_location` varchar(200) DEFAULT NULL,
`output_file_name` varchar(200) DEFAULT NULL,
`output_file_delimiter` varchar(2) DEFAULT NULL,
`output_file_suffix` varchar(200) DEFAULT NULL,
`output_file_format` varchar(200) DEFAULT NULL,
`output_file_escape_character` varchar(200) DEFAULT NULL,
`output_file_quote_character` varchar(200) DEFAULT NULL,
`output_file_header_flag` varchar(200) DEFAULT NULL,
`geography_id` int(11) DEFAULT 0 ,
`active_flag` varchar(200) DEFAULT NULL,
`insert_by` varchar(100) DEFAULT NULL,
`insert_date` timestamp DEFAULT NULL,
`update_by` varchar(100) DEFAULT NULL,
`update_date` timestamp DEFAULT NULL
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


---
--- ctl_geography_master
---

DROP TABLE IF EXISTS `ctl_geography_master`;
CREATE TABLE `ctl_geography_master` (
  `geography_id` int(11) NOT NULL UNIQUE,
  `geography_type` varchar(50) DEFAULT NULL,
  `geography_name` varchar(50) DEFAULT NULL,
  `geography_language` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`geography_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




---
--- ctl_rule_category_dtl
---

DROP TABLE IF EXISTS `ctl_rule_category_dtl`;
CREATE TABLE `ctl_rule_category_dtl` (
  `rule_category_id` int(11) NOT NULL UNIQUE AUTO_INCREMENT,
  `rule_category_name` varchar(50) NOT NULL UNIQUE,
  `rule_description` varchar(100) DEFAULT NULL,
  `table_name` varchar(50) DEFAULT NULL,
  `schema_name` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`rule_category_id`,`rule_category_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



---
--- alignment_exclustion
---

DROP TABLE IF EXISTS `alignment_exclustion`;
CREATE TABLE `alignment_exclustion` (
  `rule_name` varchar(100) DEFAULT NULL,
  `MKT_NM` varchar(100) DEFAULT NULL,
  `SLS_FRC_NM` varchar(100) DEFAULT NULL,
  `ALIGT_TYPE` varchar(100) DEFAULT NULL,
  `ALIGT_QTR` varchar(100) DEFAULT NULL,
  `PTY_TYPE` varchar(100) DEFAULT NULL,
  `IS_FROZEN_FLG` varchar(50) DEFAULT NULL,
  `ATTR_TYPE` varchar(100) DEFAULT NULL,
  `ATTR_VAL` varchar(100) DEFAULT NULL,
  `EXCL_FLG` varchar(50) DEFAULT NULL,
  `EXCL_RSN` varchar(100) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


---
--- alignment_config
---

DROP TABLE IF EXISTS `alignment_config`;
CREATE TABLE `alignment_config` (
  `rule_name` varchar(100) DEFAULT NULL,
  `MKT_NM` varchar(100) DEFAULT NULL,
  `SLS_FRC_NM` varchar(100) DEFAULT NULL,
  `ALIGT_PRTY` varchar(100) DEFAULT NULL,
  `ALIGT_TYPE` varchar(100) DEFAULT NULL,
  `ALIGT_QTR` varchar(100) DEFAULT NULL,
  `PTY_TYPE` varchar(100) DEFAULT NULL,
  `IS_FROZEN_FLG` varchar(50) DEFAULT NULL,
  `DATA_DT` date DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


---
--- alignment_override
---

DROP TABLE IF EXISTS `alignment_override`;
CREATE TABLE `alignment_override` (
  `rule_name` varchar(100) DEFAULT NULL,
  `MKT_NM` varchar(100) DEFAULT NULL,
  `SLS_FRC_NM` varchar(100) DEFAULT NULL,
  `ALIGT_TYPE` varchar(100) DEFAULT NULL,
  `ALIGT_QTR` varchar(100) DEFAULT NULL,
  `PTY_TYPE` varchar(100) DEFAULT NULL,
  `IS_FROZEN_FLG` varchar(50) DEFAULT NULL,
  `CUST_EID` int(100) DEFAULT NULL,
  `TERR_ID` varchar(100) DEFAULT NULL,
  `INCL_EXCL_FLG` varchar(100) DEFAULT NULL,
  `EXCL_RSN` varchar(100) DEFAULT NULL,
  `ALIGT_SPLT_PCT` varchar(100) DEFAULT NULL,
  `active_flag` char(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

---
--- ctl_process_date_mapping
---

DROP TABLE IF EXISTS `ctl_process_date_mapping`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_process_date_mapping`(
process_id varchar(100) NOT NULL,
frequency varchar(50) DEFAULT NULL,
data_date date NOT NULL,
process_interruption_flag char(1) DEFAULT NULL,
insert_by varchar(50) DEFAULT NULL,
insert_date timestamp DEFAULT NULL,
`geography_id` int(11) DEFAULT NULL,
PRIMARY KEY(process_id),
Foreign Key (process_id, geography_id) REFERENCES ctl_cluster_config(process_id, geography_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;


---
--- ctl_dqm_request_details
---


DROP TABLE IF EXISTS `ctl_dqm_request_details`;
CREATE TABLE `ctl_dqm_request_details` (
  `dataset_id` varchar(50) DEFAULT NULL,
  `dataset_name` varchar(50) DEFAULT NULL,
  `dataset_sub_type` varchar(50) DEFAULT NULL,
  `qc_id` varchar(100) DEFAULT NULL,
  `qc_type` varchar(100) DEFAULT NULL,
  `criticality` varchar(100) DEFAULT NULL,
  `column_name` varchar(100) DEFAULT NULL,
  `creator_user_id` varchar(100) DEFAULT NULL,
  `creator_role_id` varchar(100) DEFAULT NULL,
  `created_date` varchar(100) DEFAULT NULL,
  `dqm_request_status` varchar(100) DEFAULT NULL,
  `approved_by_1` varchar(100) DEFAULT NULL,
  `approved_by_2` varchar(100) DEFAULT NULL,
  `approved_by_3` varchar(100) DEFAULT NULL,
  `approver1_role_id` varchar(100) DEFAULT NULL,
  `approver2_role_id` varchar(100) DEFAULT NULL,
  `approver3_role_id` varchar(100) DEFAULT NULL,
  `approved_date` timestamp DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


---
--- log_data_acquisition_dtl
---


DROP TABLE IF EXISTS `log_data_acquisition_dtl`;
CREATE TABLE `log_data_acquisition_dtl` (
`geography_id` int(11) DEFAULT 0 ,
`cycle_id` bigint(20) NOT NULL,
`source_platform` varchar(200) DEFAULT NULL,
`src_system` varchar(200) DEFAULT NULL,
`database_name` varchar(200) DEFAULT NULL,
`object_name` varchar(200) DEFAULT NULL, 
`load_type` varchar(200) DEFAULT NULL, 
`incremental_load_column_name` varchar(200) DEFAULT NULL, 
`query` varchar(2000) DEFAULT NULL,
`query_file_name` varchar(200) DEFAULT NULL,
`query_description` varchar(200) DEFAULT NULL, 
`source_file_location` varchar(200) DEFAULT NULL,
`target_location` varchar(200) DEFAULT NULL,
`output_file_name` varchar(200) DEFAULT NULL,
`output_file_format` varchar(100) DEFAULT NULL,
`record_count` int(11) DEFAULT NULL,
`prev_max_refresh_timestamp` json DEFAULT NULL,
`curr_max_refresh_timestamp` json DEFAULT NULL,
`status` varchar(200) DEFAULT NULL,
`comments` varchar(200) DEFAULT NULL,
`load_start_time` timestamp DEFAULT NULL,
`load_end_time` timestamp DEFAULT NULL,
`insert_by` varchar(200) DEFAULT NULL,
`insert_date` timestamp DEFAULT NULL,
`update_by` varchar(200) DEFAULT NULL,
`update_date` timestamp DEFAULT NULL,
`override_flag` varchar(20) DEFAULT NULL);


DROP TABLE IF EXISTS `ctl_data_acquisition_connection_dtl`;
create table `ctl_data_acquisition_connection_dtl` 
(`source_platform` varchar(200) NOT NULL,
`src_system` varchar(200) NOT NULL, 
`connection_json` json NOT NULL,
`active_flag` char(1) DEFAULT NULL,
`insert_by` varchar(100) DEFAULT NULL,
`insert_date` timestamp NULL DEFAULT NULL,
`update_by` varchar(100) DEFAULT NULL,
`update_date` timestamp NULL DEFAULT NULL,
`geography_id` int(11) DEFAULT 0
);


--
-- Table structure for table `ctl_global_config`
--

DROP TABLE IF EXISTS `ctl_global_config`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ctl_global_config` (
  `global_id` int(11) NOT NULL UNIQUE AUTO_INCREMENT,
  `domain_id` varchar(50) NOT NULL,
  `application_id` varchar(200) DEFAULT NULL,
  `param_name` varchar(100) DEFAULT NULL,
  `param_value` json DEFAULT NULL,
  `geography_id` int(11) NULL,
  `insert_by` varchar(100) DEFAULT NULL,
  `insert_date` timestamp NULL DEFAULT NULL,
  `update_by` varchar(100) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY(global_id)
  ) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;
