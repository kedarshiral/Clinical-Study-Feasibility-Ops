#!/bin/bash -x
#Script Name : emr_bootstrap_modified.sh as per Python 3
#Script Arguments : 1) S3 Bucket Name env-param.json 2) Cluster code path  commonconstant - emrcodepath
#Script Description : This script will install all the required python packages and applications
if [ -z "$1" ]
  then
    echo "Bucket name not supplied"
        exit 1
fi

if [ -z "$2" ]
  then
    echo "Cluster code path not supplied"
        exit 1
fi

bucket_name=$1
cluster_code_path=$2
ENVIRONEMNT=$3

# Create Password
sudo echo ec2-user@20 | sudo passwd --stdin hadoop
sudo sed -i "/^[^#]*PasswordAuthentication[[:space:]]no/c\PasswordAuthentication yes" /etc/ssh/sshd_config
sudo service sshd restart

# Adapter and CC Ingestion uses python3.6 and DW uses python3.4,so to maintain installation for different processes we are getting the version of pip.
#pip_path="$(readlink -f $(which pip))"

path="$(readlink -f $(ls /usr/bin/pip-*))"
flag=`echo $path|awk '{print match($0,"pip-3.6")}'`
if [ $flag -gt 0 ];then
	pip_path="/usr/bin/pip-3.6"
	echo $pip_path;
else
	pip_path="/usr/bin/pip-3.4"
	echo $pip_path;
fi
export PIP_PATH=$pip_path

#export PIP_PATH=/usr/bin/pip-3.6


$PIP_PATH

#install mysql-connector
sudo $PIP_PATH install mysql-connector
sudo $PIP_PATH install awscli==1.16.140
sudo $PIP_PATH install fuzzywuzzy
sudo $PIP_PATH install python-Levenshtein

#Install python logstash
sudo $PIP_PATH install python3-logstash
sudo yum install git -y
echo "done git install ---"
sudo mkdir /mnt/var/lib/hadoop/tmp/s3a
sudo chmod +777 /mnt/var/lib/hadoop/tmp/s3a


javapath="$(readlink -f $(which java))"

javapath=${javapath%/*}
javapath=${javapath%/*}
javapath=${javapath%/*}
export JAVA_HOME=$javapath
echo ' export PATH=$PATH:$JAVA_HOME/jre/bin '  >> ~/.bashrc

#Changes Added For Pyhive package used by Hive Table To S3 Utility
sudo $PIP_PATH install pyhive==0.5.2
sudo $PIP_PATH install numpy
sudo $PIP_PATH install thrift==0.11.0
sudo yum install cyrus-sasl-devel-2.1.23-13.16.amzn1 -y
sudo $PIP_PATH install sasl==0.2.1
sudo $PIP_PATH install thrift_sasl==0.3.0
sudo $PIP_PATH install pandas==0.25.0
sudo $PIP_PATH install pytz
sudo $PIP_PATH install xmltodict
sudo $PIP_PATH install urllib3==1.24.1
sudo $PIP_PATH install boto3==1.9.91
sudo $PIP_PATH install botocore==1.12.91

# install pyjarowinkler for UDF
sudo $PIP_PATH install pyjarowinkler

# install packages for Mastering process
sudo $PIP_PATH install --upgrade setuptools==43.0.0
sudo yum install libffi-devel -y
sudo $PIP_PATH install --upgrade cffi
sudo $PIP_PATH install --upgrade cython
sudo $PIP_PATH install wheel
sudo $PIP_PATH install pandas
sudo $PIP_PATH install pandas-dedupe
sudo $PIP_PATH install BTrees==4.4.1
sudo $PIP_PATH install dedupe==1.10.0





#Download the python script (install_packages.py) from S3
#Installs spark onto the slave nodes.
#sudo aws s3 cp s3://$bucket_name/code/bootstrap/install_packages.py $cluster_code_path
#sudo /usr/bin/python3.4 $cluster_code_path/install_packages.py $bucket_name $cluster_code_path > $cluster_code_path/install_packages.log

#cluster_id=$(cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId")
#echo $(aws emr describe-cluster --cluster-id $cluster_id) > /tmp/test.json

#cluster_detail=$(cat /tmp/test.json | jq '.Cluster.Status.State')

#if [ "$cluster_detail" == "WAITING" ]

#!/bin/bash -x

#cluster_id=$(jq ".jobFlowId" /emr/instance-controller/lib/info/job-flow.json | tr -d '"')
#master_ip=$(jq ".Cluster.MasterPublicDnsName" <(echo $(aws emr describe-cluster --cluster-id $cluster_id))| tr -d '"')
#sudo aws s3 cp s3://$bucket_name/python3/secondstage.sh /home/hadoop/secondstage.sh
#sudo bash /home/hadoop/secondstage.sh $bucket_name $master_ip & > output.log

#Install numpy and scipy and pyspark

sudo $PIP_PATH install scipy==1.2.1
sudo $PIP_PATH install pyspark
sudo $PIP_PATH install regex
sudo $PIP_PATH install sparse_dot_topn
sudo $PIP_PATH install ftfy
sudo $PIP_PATH install sklearn
sudo $PIP_PATH install Gensim==0.13.4
sudo $PIP_PATH install unidecode
sudo $PIP_PATH install numpy==1.16.0
sudo $PIP_PATH install icd9cms
sudo $PIP_PATH install icd10-cm
sudo $PIP_PATH install dedupe

#sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh

sudo $PIP_PATH install selenium
sudo $PIP_PATH install fpdf
sudo $PIP_PATH install PyPDF2



bucket_name=$1
export APPLICATION_DIR=$2

export CC_GIT_BRANCH=$4
export DM_GIT_BRANCH=$5


export DM_DEPLOY_RESPONSE="Y"
export ENVIRONEMNT=`echo $ENVIRONEMNT | tr '[A-Z]' '[a-z]'`

#Below code changes set -x to set +x, which turns off the debug mode and does not print username and password on shell
if [[ $- =~ x ]]; then debug=1; set +x; fi

export GIT_USER=$6
export GIT_PASSWORD=$7

#Below code changes to enable debug mode by setting set +x to set -x
[[ $debug == 1 ]] && set -x

export TAR_RESPONSE="N"
export EMR_FLAG="TRUE"
export WORKING_DIR="/tmp/working_dir"

sudo mkdir -p $APPLICATION_DIR/code
sudo chmod 777 -R $APPLICATION_DIR/code/
sudo chown $USER:$USER $APPLICATION_DIR
# Downloading the deployment_script
cd /tmp
if [ -d "/tmp/cdl_common_component" ]
        then rm -rf /tmp/cdl_common_component
fi

sudo yum -y install git
HOME="/home/hadoop"

#cmd='git clone -b '"$CC_GIT_BRANCH"' https://'"$GIT_USER"':"$GIT_PASSWORD"@sourcecode.jnj.com/scm/asx-nckv/jrd_fido_cc.git'

cmd='git clone -b '"$CC_GIT_BRANCH"' https://'"$GIT_USER"':"$GIT_PASSWORD"@bitbucket.org/zs_businesstech/cdl_common_component.git'
echo $cmd

if [[ $- =~ x ]]; then debug=1; set +x; fi
git clone -b "$CC_GIT_BRANCH" https://"$GIT_USER":"$GIT_PASSWORD"@bitbucket.org/zs_businesstech/cdl_common_component.git 2> /dev/null
[[ $debug == 1 ]] && set -x

echo "Returned to Git clone file"
cmd_1='sudo cp /tmp/cdl_common_component/installables/deployment/scripts/deployment_script.sh $HOME'
echo $cmd_1
sudo cp /tmp/cdl_common_component/installables/deployment/scripts/deployment_script.sh $HOME

echo "Executing deployment script"
sh $HOME/deployment_script.sh
echo "Returned to bootstrap file"
echo "Done...."



cd $cluster_code_path/code/


files_1="$(ls -1 -d $PWD/*.sh $PWD/*.conf $PWD/*.template $PWD/*.html $PWD/*.json  -p | tr '\n' ',')"

files=${files_1::-1}
echo "${files}"

touch /home/hadoop/temp_file
destdir_file=/home/hadoop/temp_file
echo "$files" >> "$destdir_file"
echo $(cat /home/hadoop/temp_file)


py_files_1="$(ls -1 -d $PWD/*.py  -p | tr '\n' ',')"

py_files=${py_files_1::-1}
echo "${py_files}"

touch /home/hadoop/temp_py
destdir_py=/home/hadoop/temp_py
echo "$py_files" >> "$destdir_py"
echo $(cat /home/hadoop/temp_py)
	