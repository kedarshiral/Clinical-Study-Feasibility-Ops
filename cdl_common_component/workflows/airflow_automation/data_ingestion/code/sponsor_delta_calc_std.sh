file_master_id=$1
cluster_id=$2
dag_id=$3
process_id=$4
conf_value=$5" --name SponsorDeltaCalcStd_"${file_master_id}

/usr/lib/spark/bin/spark-submit $conf_value SponsorDeltaCalcStd.py $file_master_id $cluster_id $process_id

