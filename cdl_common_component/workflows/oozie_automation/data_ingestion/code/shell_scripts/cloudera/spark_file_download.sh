file_master_id=$1

cluster_id=$2
workflow_id=$3
write_batch_id_to_file='N'

keytab_ext='.keytab'
keytab_file_name=$USER$keytab_ext
domain_name='@ZS.COM'
principal_name=$USER$domain_name

spark-submit --jars spl.jar --packages com.databricks:spark-csv_2.10:1.4.0 --keytab $keytab_file_name --principal $principal_name FileDownloadHandler.py $file_master_id  $cluster_id $workflow_id $write_batch_id_to_file