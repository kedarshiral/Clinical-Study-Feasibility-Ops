file_master_id=$1
cluster_id=$2
conf_value="--executor-cores 3 --executor-memory 10g --driver-memory 16g --conf spark.yarn.executor.memoryOverhead=2G --packages com.crealytics:spark-excel_2.11:0.13.5 --name MappingUtility_"${file_master_id}

/usr/lib/spark/bin/spark-submit $conf_value MappingUtility.py $file_master_id $cluster_id