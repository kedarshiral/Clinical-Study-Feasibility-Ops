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