ALTER TABLE log_cluster_dtl ADD COLUMN cluster_terminate_user varchar(250) AFTER cluster_start_time;
ALTER TABLE log_cluster_dtl ADD COLUMN cluster_launch_user varchar(250) AFTER master_node_dns;
