#!/bin/bash
#Script Name : Run the adapter codes

adapter_name=$1
adapter_name_lower=${adapter_name,,}

cd /usr/local/airflow/dags/plugins/qa/ctfo/code/adaptor/$adapter_name_lower/

# # # python $1Adapter.py -p payload_$adapter_name_lower.json >/usr/local/airflow/dags/plugins/qa/ctfo/code/adaptor/common_utilities/logs/$1.log

python3 $1Adapter.py -p payload_$adapter_name_lower.json
