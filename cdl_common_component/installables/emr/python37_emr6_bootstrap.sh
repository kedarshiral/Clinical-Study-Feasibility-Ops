#!/bin/bash -x
#Script Name : emr_bootstrap.sh as per Python 3-submit on Master
#Script Arguments : 1) S3 Bucket Name env-param.json 2) Cluster code path  commonconstant - emrcodepath
#Script Description : This script will install all the required python packages and applications

set -e

if [ -z "$1" ]
  then
    echo "Bucket Name not supplied"
        exit 1
fi

if [ -z "$2" ]
  then
    echo "Cluster code path not supplied"
        exit 1
fi

bucket_name=$1
cluster_code_path=$2

sudo mkdir -p $cluster_code_path/code
sudo chmod 777 -R $cluster_code_path/code/
sudo aws s3 cp s3://$bucket_name/FTPIngestion/code/code_master.tar.gz $cluster_code_path
sudo gunzip $cluster_code_path/code_master.tar.gz
sudo tar -xf $cluster_code_path/code_master.tar -C $cluster_code_path
sudo chmod 777 -R $cluster_code_path

#install python3-devel
sudo yum install python3-devel.x86_64 -y

#install mysql-connector
sudo /usr/bin/pip-3.7 install setuptools==36.2.7
sudo /usr/bin/pip-3.7 install wheel==0.36.2
sudo /usr/bin/pip-3.7 install mysql-connector==2.2.9
sudo /usr/bin/pip-3.7 install awscli==1.18.167


#Install python logstash
sudo /usr/bin/pip-3.7 install python3-logstash==0.4.80
sudo yum install git-2.18.4-2.72.amzn1 -y
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
sudo /usr/bin/pip-3.7 install pyhive==0.6.3
sudo /usr/bin/pip-3.7 install thrift==0.13.0
sudo yum install cyrus-sasl-devel-2.1.26-23.amzn2 -y
sudo /usr/bin/pip-3.7 install sasl==0.2.1
sudo /usr/bin/pip-3.7 install thrift_sasl==0.4.2
sudo /usr/bin/pip-3.7 install pysftp==0.2.9

#Install numpy and scipy
sudo /usr/bin/pip-3.7 install numpy==1.16.5
sudo /usr/bin/pip-3.7 install scipy==1.5.3
sudo /usr/bin/pip-3.7 install boto3==1.16.7

#Install packages for Ganglia
sudo /usr/bin/pip-3.7 install pytz==2020.1
sudo /usr/bin/pip-3.7 install selenium==3.141.0
sudo /usr/bin/pip-3.7 install fpdf==1.7.2
sudo /usr/bin/pip-3.7 install PyPDF2==1.26.0

#Spark Cluster Mode Setup

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

#sudo pip install --upgrade pip
sudo /usr/bin/pip-3.7 install pyspark==3.0.1

#changes made for installing s3-dist-cp for filetransfer utility
sudo aws s3 cp s3://$bucket_name/code/bootstrap/s3-dist-cp.tar.gz $cluster_code_path
sudo gunzip $cluster_code_path/s3-dist-cp.tar.gz
sudo tar -xf $cluster_code_path/s3-dist-cp.tar -C /usr/share/aws/emr
