# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

file_master_id=$1
batch_id=$2

cluster_id=$3
workflow_id=$4


echo "batchid=$batch_id"

spark-submit --jars spl.jar --packages com.databricks:spark-csv_2.10:1.4.0 AuditHandler.py $file_master_id $batch_id $cluster_id $workflow_id