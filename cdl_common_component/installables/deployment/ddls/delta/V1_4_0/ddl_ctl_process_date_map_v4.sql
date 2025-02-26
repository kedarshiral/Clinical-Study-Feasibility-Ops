CREATE TABLE `ctl_process_date_mapping` (
  `process_id` int(11) DEFAULT NULL,
  `frequency` varchar(50) DEFAULT NULL,
  `data_date` date DEFAULT NULL,
  `insert_by` varchar(50) DEFAULT NULL,
  `insert_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY `unique_process` (`process_id`,`frequency`)
);