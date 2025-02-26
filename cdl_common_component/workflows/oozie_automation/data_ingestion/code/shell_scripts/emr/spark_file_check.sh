
file_master_id=$1
batch_id=$2

cluster_id=$3
workflow_id=$4


spark-submit --jars spl.jar --packages com.databricks:spark-csv_2.10:1.4.0 FileCheckHandler.py $file_master_id $batch_id  $cluster_id $workflow_id