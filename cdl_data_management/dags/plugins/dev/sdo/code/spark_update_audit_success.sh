file_master_id=$1
batch_id=$5
conf_value=$6
cluster_id=$2
workflow_id=$3

process_id=$4
batch_id_list=$7


python3 AuditHandler.py $file_master_id $cluster_id $workflow_id $process_id $batch_id $batch_id_list