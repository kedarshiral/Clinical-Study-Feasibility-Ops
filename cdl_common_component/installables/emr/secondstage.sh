# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
#!/bin/bash

#cluster_id=$(jq ".jobFlowId" /emr/instance-controller/lib/info/job-flow.json | tr -d '"')
#master_ip=$(jq ".Cluster.MasterPublicDnsName" <(echo $(aws emr describe-cluster --cluster-id $cluster_id))| tr -d '"')

set -e

if [ -z "$1" ]
  then
    echo "Bucket Name not supplied"
	exit 1
fi

bucket_name=$1
master_ip=$2
while true;
do
NODEPROVISIONSTATE=`sed -n '/localInstance [{]/,/[}]/{
/nodeProvisionCheckinRecord [{]/,/[}]/ {
/status: / { p }
/[}]/a
}
/[}]/a
}' /emr/instance-controller/lib/info/job-flow-state.txt | awk ' { print $2 }'`

if [ "$NODEPROVISIONSTATE" == "SUCCESSFUL" ]; then
sleep 10;
echo "Running my post provision bootstrap"
if [ -d /usr/lib/spark/bin ] ;
then
echo "Spark client directory already exists"
else
echo "Starting to install spark client"
cd /usr/lib/
sudo aws s3 cp s3://$bucket_name/python3/spark.zip .
sudo rm -rf /usr/lib/spark
sudo unzip spark.zip
cd /usr/lib/spark/conf
sudo sed -i 's/\$\$master_host\$\$/'${master_ip}'/g' *
cd /usr/lib/spark/bin
sudo chmod 755 *
sudo mkdir -p /etc/spark
sudo rm -rf /etc/spark/conf
sudo mkdir -p /etc/spark/conf
sudo cp -r /usr/lib/spark/conf/ /etc/spark/
fi
exit;
fi
sleep 10;
done
