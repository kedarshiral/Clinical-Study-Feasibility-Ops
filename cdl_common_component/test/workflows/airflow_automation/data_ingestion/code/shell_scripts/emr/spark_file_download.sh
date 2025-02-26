# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

file_master_id=$1

cluster_id=$2
workflow_id=$3
write_batchid_to_file='N'

spark-submit --jars spl.jar --packages com.databricks:spark-csv_2.10:1.4.0 FileDownloadHandler.py $file_master_id  $cluster_id $workflow_id $write_batchid_to_file