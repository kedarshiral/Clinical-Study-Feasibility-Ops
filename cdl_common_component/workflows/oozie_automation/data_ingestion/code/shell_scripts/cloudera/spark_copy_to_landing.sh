file_master_id=$1
batch_id=$2
cluster_id=$3
workflow_id=$4


keytab_ext='.keytab'
keytab_file_name=$USER$keytab_ext
domain_name='@ZS.COM'
principal_name=$USER$domain_name

echo "batchid=$batch_id"

spark-submit --jars spl.jar --packages com.databricks:spark-csv_2.10:1.4.0 --keytab $keytab_file_name --principal $principal_name LandingCopyHandler.py $file_master_id $batch_id $cluster_id $workflow_id