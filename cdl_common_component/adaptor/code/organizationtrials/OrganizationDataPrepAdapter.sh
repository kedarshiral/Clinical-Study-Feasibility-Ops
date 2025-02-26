conf_value=$1" --name OrganizationDataPrepAdapter"

/usr/lib/spark/bin/spark-submit $conf_value OrganizationDataPrepAdapter.py
