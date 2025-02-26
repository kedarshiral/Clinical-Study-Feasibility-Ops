from datetime import datetime

run_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
landing_table_name= "src_publication_pubmed_active"
active_select_col_list = spark.sql("select * from {0} where 1=2".format(landing_table_name)).columns
active_select_col_str = ",".join(active_select_col_list)

query = "select {0} from {1} ".format(active_select_col_str, landing_table_name)


pubmed_active_df = spark.sql(query)


current_ts = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
query = "select * from (select src_ctgov_all_active.disease_area_name as disease_area_name, nct_id as active_nct_id, explode(split(nct_alias, '\\\|')) as " \
                        "obsolete_nct_id from {0}.src_ctgov_all_active)" \
                        "where obsolete_nct_id != ''".format(
                    "default")
alias_df = spark.sql(query)
alias_df.registerTempTable("lnd_trials_ctgov_alias_mapping")


query = """
        SELECT pmid,
               explode(split(nct_id, '\\\|')) AS std_nct_id
        FROM {0}
    """.format("src_publication_pubmed_active")

status_message = "Exploding pubmed data based on nct_id, query=" + query
print(str(status_message))
exploded_df = spark.sql(query)


query = "select * from lnd_trials_ctgov_alias_mapping"
alias_mapping_df = spark.sql(query)

active_df = exploded_df.join(alias_mapping_df,exploded_df.std_nct_id == alias_mapping_df.obsolete_nct_id,
                                                 how='left').filter('active_nct_id is null')
current_ts = datetime.strftime(datetime.utcnow(), "%Y%m%d")
temp_table = "temp_table_" + current_ts
active_df.createOrReplaceTempView(temp_table)

query = "select pmid as obs_pmid, concat_ws('|', collect_list(std_nct_id)) as std_nct_id from {0} group by pmid".format(
                        temp_table)
active_df_grouped = spark.sql(query)

standard_table_df = pubmed_active_df.join(active_df_grouped,active_df_grouped.obs_pmid == pubmed_active_df.PMID,
                                                              how='left').drop('obs_pmid')

query = "select * from {0}".format("src_publication_pubmed_active")
std_df_col = spark.sql(query)
std_column_list = []
for column_mapping in std_df_col.dtypes:
    if column_mapping[0] not in ['pt_data_dt', 'pt_cycle_id','pt_batch_id','pt_file_id']:
        std_column_list.append(column_mapping[0])

standard_table_df = standard_table_df.select(std_column_list)


standard_table_df.registerTempTable("pubmed_temp_table")

spark.sql("insert into default.target_publication_pubmed_active partition(pt_data_date='$$data_dt',pt_cycle_id='$$cycle_id') select * from pubmed_temp_table")

CommonUtils().copy_hdfs_to_s3("default.target_publication_pubmed_active")



