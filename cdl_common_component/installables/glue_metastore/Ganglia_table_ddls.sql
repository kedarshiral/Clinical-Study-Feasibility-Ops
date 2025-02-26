/* --------------------------------------------------------------------*/
/* Cluster Metrics -  Database Creation */
/* --------------------------------------------------------------------*/

/*
1. console - "aws glue create-database --database-input Name="ganglia_metrics"" name - database create

*/

/* --------------------------------------------------------------------*/
/* Level 0 Metric DDLs - Cluster Level */
/* --------------------------------------------------------------------*/

/*
Table Name: cluster_cpu_report
Type: Table
Graph: CPU report
Level: Cluster
Description: Provides the ganglia cluster level stats of cpu_report graph.
*/
CREATE EXTERNAL TABLE "ganglia_metrics".`cluster_cpu_report`(
  `timestamp` string COMMENT 'from deserializer', 
  `user\g` string COMMENT 'from deserializer', 
  `nice\g` string COMMENT 'from deserializer', 
  `system\g` string COMMENT 'from deserializer', 
  `wait\g` string COMMENT 'from deserializer', 
  `steal\g` string COMMENT 'from deserializer', 
  `idle\g` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `pt_process_name` string, 
  `pt_cluster_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/ganglia_reports/cluster_level/cpu_report'
TBLPROPERTIES (
  'classification'='csv', 
  'skip.header.line.count'='1')
  

/*
Table Name: cluster_load_report
Type: Table
Graph: Load report
Level: Cluster
Description: Provides the ganglia cluster level stats of load_report graph.
*/
CREATE EXTERNAL TABLE "ganglia_metrics".`cluster_load_report`(
  `timestamp` string COMMENT 'from deserializer', 
  `1-min` string COMMENT 'from deserializer', 
  `nodes` string COMMENT 'from deserializer', 
  `cpus` string COMMENT 'from deserializer', 
  `procs` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `pt_process_name` string, 
  `pt_cluster_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/ganglia_reports/cluster_level/load_report'
TBLPROPERTIES (
  'classification'='csv', 
  'skip.header.line.count'='1')
  

/*
Table Name: cluster_mem_report
Type: Table
Graph: Memory report
Level: Cluster
Description: Provides the ganglia cluster level stats of mem_report graph.
*/
CREATE EXTERNAL TABLE "ganglia_metrics".`cluster_mem_report`(
  `timestamp` string COMMENT 'from deserializer', 
  `use\g` string COMMENT 'from deserializer', 
  `share\g` string COMMENT 'from deserializer', 
  `cache\g` string COMMENT 'from deserializer', 
  `buffer\g` string COMMENT 'from deserializer', 
  `free\g` string COMMENT 'from deserializer', 
  `swap\g` string COMMENT 'from deserializer', 
  `total\g` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `pt_process_name` string, 
  `pt_cluster_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/ganglia_reports/cluster_level/mem_report'
TBLPROPERTIES (
  'classification'='csv', 
  'skip.header.line.count'='1')
  

/*
Table Name: cluster_network_report
Type: Table
Graph: Network report
Level: Cluster
Description: Provides the ganglia cluster level stats of network_report graph.
*/
CREATE EXTERNAL TABLE "ganglia_metrics".`cluster_network_report`(
  `timestamp` string COMMENT 'from deserializer', 
  `in` string COMMENT 'from deserializer', 
  `out` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `pt_process_name` string, 
  `pt_cluster_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/ganglia_reports/cluster_level/network_report'
TBLPROPERTIES (
  'classification'='csv', 
  'skip.header.line.count'='1')

/* --------------------------------------------------------------------*/
/* Level 0 Metric DDLs - Node Level */
/* --------------------------------------------------------------------*/

/*
Table Name: node_cpu_report
Type: Table
Graph: CPU report
Level: Node
Description: Provides the ganglia node level stats of cpu_report graph.
*/
CREATE EXTERNAL TABLE "ganglia_metrics".`node_cpu_report`(
  `timestamp` string COMMENT 'from deserializer', 
  `user\g` string COMMENT 'from deserializer', 
  `nice\g` string COMMENT 'from deserializer', 
  `system\g` string COMMENT 'from deserializer', 
  `wait\g` string COMMENT 'from deserializer', 
  `steal\g` string COMMENT 'from deserializer', 
  `idle\g` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `pt_process_name` string, 
  `pt_cluster_id` string, 
  `pt_node_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/ganglia_reports/node_level/cpu_report'
TBLPROPERTIES (
  'classification'='csv', 
  'skip.header.line.count'='1')
  
  
/*
Table Name: node_load_report
Type: Table
Graph: Load report
Level: Node
Description: Provides the ganglia node level stats of load_report graph.
*/
CREATE EXTERNAL TABLE "ganglia_metrics".`node_load_report`(
  `timestamp` string COMMENT 'from deserializer', 
  `1-min` string COMMENT 'from deserializer', 
  `nodes` string COMMENT 'from deserializer', 
  `cpus` string COMMENT 'from deserializer', 
  `procs` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `pt_process_name` string, 
  `pt_cluster_id` string, 
  `pt_node_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/ganglia_reports/node_level/load_report'
TBLPROPERTIES (
  'classification'='csv', 
  'skip.header.line.count'='1')
  
  
/*
Table Name: node_mem_report
Type: Table
Graph: Memory report
Level: Node
Description: Provides the ganglia node level stats of mem_report graph.
*/
CREATE EXTERNAL TABLE "ganglia_metrics".`node_mem_report`(
  `timestamp` string COMMENT 'from deserializer', 
  `use\g` string COMMENT 'from deserializer', 
  `share\g` string COMMENT 'from deserializer', 
  `cache\g` string COMMENT 'from deserializer', 
  `buffer\g` string COMMENT 'from deserializer', 
  `free\g` string COMMENT 'from deserializer', 
  `swap\g` string COMMENT 'from deserializer', 
  `total\g` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `pt_process_name` string, 
  `pt_cluster_id` string, 
  `pt_node_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/ganglia_reports/node_level/mem_report'
TBLPROPERTIES (
  'classification'='csv', 
  'skip.header.line.count'='1')
  
  
/*
Table Name: node_network_report`
Type: Table
Graph: Network report
Level: Node
Description: Provides the ganglia node level stats of network_report graph.
*/
CREATE EXTERNAL TABLE "ganglia_metrics".`node_network_report`(
  `timestamp` string COMMENT 'from deserializer', 
  `in` string COMMENT 'from deserializer', 
  `out` string COMMENT 'from deserializer')
PARTITIONED BY ( 
  `pt_process_name` string, 
  `pt_cluster_id` string, 
  `pt_node_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/ganglia_reports/node_level/network_report'
TBLPROPERTIES (
  'classification'='csv', 
  'skip.header.line.count'='1')

/* --------------------------------------------------------------------*/
/* Level 1 Metric DDLs - Cluster Level */
/* --------------------------------------------------------------------*/

/*
Name: L1_cluster_mem_report_view
Type: Level 1 View
Parent: cluster_mem_report
Level: Cluster
Description: Refined the data from the table. Removed NaNs etc.
*/
CREATE view "ganglia_metrics"."L1_cluster_mem_report_view" AS
SELECT timestamp,
         round((cast("use\g" AS double)/cast("total\g" AS double))*100,
         2) AS use_percent,
         round((cast("share\g" AS double)/cast("total\g" AS double))*100,
         2) AS share_percent,
         round((cast("cache\g" AS double)/cast("total\g" AS double))*100,
         2) AS cache_percent,
         round((cast("buffer\g" AS double)/cast("total\g" AS double))*100,
         2) AS buffer_percent,
         round((cast("free\g" AS double)/cast("total\g" AS double))*100,
         2) AS free_percent,
         round((cast("swap\g" AS double)/cast("total\g" AS double))*100,
         2) AS swap_percent,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."cluster_mem_report"
WHERE NOT ("use\g" = 'NaN'
        AND "total\g" = 'NaN'
        AND "share\g" = 'NaN'
        AND "cache\g" = 'NaN'
        AND "buffer\g" = 'NaN'
        AND "free\g" = 'NaN'
        AND "swap\g" = 'NaN'); 
		
		
/*
Name: L1_cluster_load_report_view
Type: Level 1 View
Parent: cluster_load_report
Level: Cluster
Description: Refined the data from the table. Removed NaNs etc.
*/
CREATE view "ganglia_metrics"."L1_cluster_load_report_view" AS
SELECT timestamp,
         round(cast("1-min" AS Double),
         2) AS "1-min",
         round(cast(nodes AS double),
         2) AS nodes,
         round(cast(cpus AS Double),
         2) AS cpus,
         round(cast(procs AS Double),
         2) AS procs,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."cluster_load_report"
WHERE NOT (nodes = 'NaN'
        AND cpus = 'NaN'
        AND procs = 'NaN'
        AND "1-min" = 'NaN'); 
		
		
/*
Name: L1_cluster_cpu_report_view
Type: Level 1 View
Parent: cluster_cpu_report
Level: Cluster
Description: Refined the data from the table. Removed NaNs etc.
*/
CREATE view L1_cluster_cpu_report_view AS
SELECT timestamp,
         round(cast("user\g" AS double),
         2) AS "cpu_cycles/user",
         round(cast("nice\g" AS double),
         2) AS "nice_process",
         round(cast("system\g" AS double),
         2) AS "cpu_cycles/system",
         round(cast("wait\g" AS double),
         2) AS "cpu_cycles/wait",
         round(cast("steal\g" AS double),
         2) AS "steal_time",
         round(cast("idle\g" AS double),
         2) AS "idle_time",
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."cluster_cpu_report"
WHERE NOT ("user\g" = 'NaN'
        AND "nice\g" = 'NaN'
        AND "system\g" = 'NaN'
        AND "wait\g" = 'NaN'
        AND "steal\g" = 'NaN'
        AND "idle\g" = 'NaN' )
		
		
/*
Name: L1_cluster_network_report_view
Type: Level 1 View
Parent: cluster_network_report
Level: Cluster
Description: Refined the data from the table. Removed NaNs etc.
*/
CREATE view "ganglia_metrics"."L1_cluster_network_report_view" AS
SELECT timestamp,
         round(cast("in" AS double),
         2) AS "bytes_in",
         round(cast("out" AS double),
         2) AS "bytes_out",
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."cluster_network_report"
WHERE NOT ("in" = 'NaN'
        AND "out" = 'NaN') 

/* --------------------------------------------------------------------*/
/* Level 1 Metric DDLs - Node Level */
/* --------------------------------------------------------------------*/

/*
Name: l1_node_mem_report_view
Type: Level 1 View
Parent: node_mem_report
Level: Node
Description: Refined the data from the table. Removed NaNs etc.
*/
CREATE VIEW "ganglia_metrics"."l1_node_mem_report_view" AS
SELECT "timestamp" ,
         "round"(((CAST("use\g" AS double) / CAST("total\g" AS double)) * 100),
         2) "use_percent" ,
         "round"(((CAST("share\g" AS double) / CAST("total\g" AS double)) * 100),
         2) "share_percent" ,
         "round"(((CAST("cache\g" AS double) / CAST("total\g" AS double)) * 100),
         2) "cache_percent" ,
         "round"(((CAST("buffer\g" AS double) / CAST("total\g" AS double)) * 100),
         2) "buffer_percent" ,
         "round"(((CAST("free\g" AS double) / CAST("total\g" AS double)) * 100),
         2) "free_percent" ,
         "round"(((CAST("swap\g" AS double) / CAST("total\g" AS double)) * 100),
         2) "swap_percent" ,
         "pt_node_id" ,
         "pt_cluster_id" ,
         "pt_process_name"
FROM "ganglia_metrics"."node_mem_report"
WHERE (NOT ((((((("use\g" = 'NaN')
        AND ("total\g" = 'NaN'))
        AND ("share\g" = 'NaN'))
        AND ("cache\g" = 'NaN'))
        AND ("buffer\g" = 'NaN'))
        AND ("free\g" = 'NaN'))
        AND ("swap\g" = 'NaN'))) 
		
		
/*
Name: l1_node_cpu_report_view
Type: Level 1 View
Parent: node_cpu_report
Level: Node
Description: Refined the data from the table. Removed NaNs etc.
*/
CREATE VIEW "ganglia_metrics"."l1_node_cpu_report_view" AS
SELECT "timestamp" ,
         "round"(CAST("user\g" AS double),
         2) "cpu_cycles/user" ,
         "round"(CAST("nice\g" AS double),
         2) "nice_process" ,
         "round"(CAST("system\g" AS double),
         2) "cpu_cycle/system" ,
         "round"(CAST("wait\g" AS double),
         2) "cpu_cycle/wait" ,
         "round"(CAST("steal\g" AS double),
         2) "steal_time" ,
         "round"(CAST("idle\g" AS double),
         2) "idle_time" ,
         "pt_node_id" ,
         "pt_cluster_id" ,
         "pt_process_name"
FROM "ganglia_metrics"."node_cpu_report"
WHERE (NOT (((((("user\g" = 'NaN')
        AND ("nice\g" = 'NaN'))
        AND ("system\g" = 'NaN'))
        AND ("wait\g" = 'NaN'))
        AND ("steal\g" = 'NaN'))
        AND ("idle\g" = 'NaN'))) 
		
		
/*
Name: l1_node_network_report_view
Type: Level 1 View
Parent: node_network_report
Level: Node
Description: Refined the data from the table. Removed NaNs etc.
*/
CREATE VIEW "ganglia_metrics"."l1_node_network_report_view" AS
SELECT "timestamp" ,
         "round"(CAST("in" AS double),
         2) "bytes_in" ,
         "round"(CAST("out" AS double),
         2) "bytes_out" ,
         "pt_node_id" ,
         "pt_cluster_id" ,
         "pt_process_name"
FROM "ganglia_metrics"."node_network_report"
WHERE (NOT (("in" = 'NaN')
        AND ("out" = 'NaN'))) 
		
		
/*
Name: l1_node_load_report_view
Type: Level 1 View
Parent: node_load_report
Level: Node
Description: Refined the data from the table. Removed NaNs etc.
*/
CREATE VIEW "ganglia_metrics"."l1_node_load_report_view" AS
SELECT "timestamp" ,
         IF(("1-min" = 'NaN'), 0.0, "round"(CAST("1-min" AS double), 2)) "1-min" , 
         IF(("nodes" = 'NaN'), 0.0, "round"(CAST("nodes" AS double), 2)) "nodes" , 
         IF(("cpus" = 'NaN'), 0.0, "round"(CAST("cpus" AS double), 2)) "cpus" , 
         IF(("procs" = 'NaN'), 0.0, "round"(CAST("procs" AS double), 2)) "procs" , 
         "pt_node_id" , 
         "pt_cluster_id" , 
         "pt_process_name"
FROM "ganglia_metrics"."node_load_report"
WHERE (NOT (((("nodes" = 'NaN')
        AND ("cpus" = 'NaN'))
        AND ("1-min" = 'NaN'))
        AND ("procs" = 'NaN'))) 

/* --------------------------------------------------------------------*/
/* Level 2 Metric DDLs - Cluster Level */
/* --------------------------------------------------------------------*/

/*
Name: l2_cluster_cpu_report_view
Type: Level 2 View
Parent: l1_cluster_cpu_report_view
Level: Cluster
Description: Performed aggregation operation on cluster ID.
*/
CREATE view "ganglia_metrics"."l2_cluster_cpu_report_view" AS
SELECT round(avg("cpu_cycles/user"),
         2) AS avg_cpu_cycles/user,
         round(avg("nice_process"),
         2) AS avg_nice_process,
         round(avg("cpu_cycles/system"),
         2) AS avg_cpu_cycles/system,
         round(avg("cpu_cycles/wait"),
         2) AS avg_cpu_cycles/wait,
         round(avg("steal_time"),
         2) AS avg_steal_time,
         round(avg("idle_time"),
         2) AS avg_idle_time,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."l1_cluster_cpu_report_view"
GROUP BY  pt_process_name, pt_cluster_id 


/*
Name: l2_cluster_mem_report_view
Type: Level 2 View
Parent: l1_cluster_mem_report_view
Level: Cluster
Description: Performed aggregation operation on cluster ID.
*/
CREATE view "ganglia_metrics"."l2_cluster_mem_report_view" AS
SELECT round(avg(use_percent),
         2) AS avg_use_percent,
         round(avg(share_percent),
         2) AS avg_share_percent,
         round(avg(cache_percent),
         2) AS avg_cache_percent,
         round(avg(buffer_percent),
         2) AS avg_buffer_percent,
         round(avg(free_percent),
         2) AS avg_free_percent,
         round(avg(swap_percent),
         2) AS avg_swap_percent,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."l1_cluster_mem_report_view"
GROUP BY  pt_process_name, pt_cluster_id 


/*
Name: l2_cluster_network_report_view
Type: Level 2 View
Parent: l1_cluster_network_report_view
Level: Cluster
Description: Performed aggregation operation on cluster ID.
*/
CREATE view "ganglia_metrics"."l2_cluster_network_report_view" AS
SELECT round(avg("bytes_in"),
         2) AS avg_bytes_in,
         round(avg("bytes_out"),
         2) AS avg_bytes_out,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."l1_cluster_network_report_view"
GROUP BY  pt_process_name, pt_cluster_id 


/*
Name: l2_cluster_load_report_view
Type: Level 2 View
Parent: l1_cluster_load_report_view
Level: Cluster
Description: Performed aggregation operation on cluster ID.
*/
CREATE view "ganglia_metrics"."l2_cluster_load_report_view" AS
SELECT round(avg("1-min"),
         2) AS load_avg,
         round(avg(nodes),
         2) AS avg_nodes_used,
         round(avg(cpus),
         2) AS avg_cpus_used,
         round(avg(procs),
         2) AS avg_procs_used,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."l1_cluster_load_report_view"
GROUP BY  pt_process_name, pt_cluster_id


/* --------------------------------------------------------------------*/
/* Level 2 Metric DDLs - Node Level */
/* --------------------------------------------------------------------*/

/*
Name: l2_node_cpu_report_view
Type: Level 2 View
Parent: l1_node_cpu_report_view
Level: Node
Description: Performed aggregation operation on node ID.
*/
CREATE VIEW "ganglia_metrics"."l2_node_cpu_report_view" AS
SELECT "round"("avg"("cpu_cycles/user"),
         2) "avg_cpu_cycles/user" ,
         "round"("avg"("nice_process"),
         2) "avg_nice_process" ,
         "round"("avg"("cpu_cycles/system"),
         2) "avg_cpu_cycles/system" ,
         "round"("avg"("cpu_cycles/wait"),
         2) "avg_cpu_cycles/wait" ,
         "round"("avg"("steal_time"),
         2) "avg_steal_time" ,
         "round"("avg"("idle_time"),
         2) "avg_idle_time" ,
         "pt_node_id" ,
         "pt_cluster_id" ,
         "pt_process_name"
FROM "ganglia_metrics"."l1_node_cpu_report_view"
GROUP BY  "pt_node_id", "pt_process_name", "pt_cluster_id"
ORDER BY  "pt_process_name" ASC 


/*
Name: L2_node_load_report_view
Type: Level 2 View
Parent: L1_node_load_report_view
Level: Node
Description: Performed aggregation operation on node ID.
*/
CREATE view "ganglia_metrics"."L2_node_load_report_view" AS
SELECT round(avg("1-min"),
         2) AS avg_load,
         round(avg(nodes),
         2) AS avg_node,
         round(avg(cpus),
         2) AS avg_cpu,
         round(avg(procs),
         2) AS avg_proc,
         pt_node_id,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."L1_node_load_report_view"
GROUP BY  pt_node_id, pt_process_name, pt_cluster_id
ORDER BY  pt_process_name


/*
Name: l2_node_mem_report_view
Type: Level 2 View
Parent: l1_node_mem_report_view
Level: Node
Description: Performed aggregation operation on node ID.
*/
CREATE view "ganglia_metrics"."l2_node_mem_report_view" AS
SELECT round(avg(use_percent),
         2) AS use_avg,
         round(avg(share_percent),
         2) AS avg_share_percent,
         round(avg(cache_percent),
         2) AS avg_cache_percent,
         round(avg(buffer_percent),
         2) AS avg_buffer_percent,
         round(avg(free_percent),
         2) AS avg_free_percent,
         round(avg(swap_percent),
         2) AS avg_swap_percent,
         pt_node_id,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."l1_node_mem_report_view"
GROUP BY  pt_node_id, pt_process_name, pt_cluster_id
ORDER BY  pt_process_name 


/*
Name: l2_node_network_report_view
Type: Level 2 View
Parent: l1_node_network_report_view
Level: Node
Description: Performed aggregation operation on node ID.
*/
CREATE view "ganglia_metrics"."l2_node_network_report_view" AS
SELECT round(avg(bytes_in),
         2) AS avg_bytes_in,
         round(avg(bytes_out),
         2) AS avg_bytes_out,
         pt_node_id,
         pt_cluster_id,
         pt_process_name
FROM "ganglia_metrics"."l1_node_network_report_view"
GROUP BY  pt_node_id, pt_process_name, pt_cluster_id
ORDER BY  pt_process_name 

/* --------------------------------------------------------------------*/