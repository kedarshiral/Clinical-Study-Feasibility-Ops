To use this frame work please follow the below instructions - 

1. Updates in the application config needed  -
	a. Update s3_base_bath to you destination s3 folder ex : s3://<bucket-name>/<folder-one> 
	b. Update format type either CSV or PARQUET.
	c. Update truncate load flag to True for trucate s3 location and load else update truncate load flag to false to create new batch folder for new load
	d. for query base unload please use "''" for conditions
		i. for ex: "select * from table_name where column_name = ''xyz'' " 
	e. Update DB connection details
	 	i. password should be base64encoded
	f. table name should be in following pattern - "schema_name"."table_name"

2. Run the framework using python3 CopyRedshiftDatatoS3.py inside /CopyRedshiftDataToS3/code
3. Log folder would be generated at /CopyRedshiftDatatoS3/logs/

