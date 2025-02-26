#!/bin/bash

set -e

#this command logs the installation console output a log file placed in the home directory of the user running this script
exec > >(tee ~/airflow.log) 2>&1

# installing linux packages
sudo yum -y install bzip2-1.0.6
sudo yum -y install wget-1.19.5
sudo yum -y install gcc-8.3.1
sudo yum -y install git-2.27.0
sudo yum -y install python-devel-2.7.5 || sudo yum -y install python36-devel-3.6.8

# installing anaconda
sudo wget -P /opt https://repo.continuum.io/archive/Anaconda3-2019.10-Linux-x86_64.sh
sudo bash /opt/Anaconda3-2019.10-Linux-x86_64.sh -p/opt/anaconda3 -b 

# exporting the path to bashrc
echo 'export PATH="/opt/anaconda3/bin:$PATH"' | sudo tee -a /etc/bashrc	

# giving full access to anaconda installation folder
sudo chmod 777 /opt/anaconda3/

#listing the python version 
/opt/anaconda3/bin/python --version

# removing the downloaded installer
sudo rm -rf /opt/Anaconda3-2019.10-Linux-x86_64.sh

# restarting the profile
source ~/.bashrc

sudo /opt/anaconda3/bin/conda install pip -y

echo 'anaconda installed'

# Install MySQL
sudo rpm -Uvh http://repo.mysql.com/mysql-community-release-el7-5.noarch.rpm
sudo yum -y install mysql-server 
sudo yum -y install mysql-devel
echo "############################ Installed MySQL ############################"

# Start and setup MySQL
sudo service mysqld start
sudo chkconfig mysqld on
sudo mysqladmin -u root password test1
sudo mysql -uroot -ptest1 -e"create database airflow" 


status=$(mysql -uroot -ptest1 -se"show global variables like 'explicit_defaults_for_timestamp'"|cut -f2)
if [ "$status" = "OFF" ] 
then
    if [ -e /etc/my.cnf ]
    then
        sed "/\[mysqld\]/a explicit_defaults_for_timestamp=true" /etc/my.cnf | sudo tee /etc/my.cnf
    else
        echo "File /etc/my.cnf doesn't exist, please provide the correct path of my.cnf file"
        exit 1
    fi
else
    echo "Status of explicit_defaults_for_timestamp is already 'ON'"
fi

sudo service mysqld restart

echo "############################ Installing Required packages ############################"
sudo /opt/anaconda3/bin/pip install docutils==0.15.2
sudo /opt/anaconda3/bin/pip install python-daemon==2.1.2
sudo /opt/anaconda3/bin/pip install numpy==1.17.2
sudo /opt/anaconda3/bin/pip install pandas==0.25.1
sudo /opt/anaconda3/bin/pip install html5lib==1.0.1
sudo /opt/anaconda3/bin/pip install Fernet==1.0.1
sudo /opt/anaconda3/bin/pip install cryptography==2.7
sudo /opt/anaconda3/bin/pip install celery==5.0.5
sudo /opt/anaconda3/bin/pip install --upgrade setuptools==51.0.0
sudo /opt/anaconda3/bin/pip install ConfigParser==3.5.3
sudo /opt/anaconda3/bin/pip install mysql==0.0.2
sudo /opt/anaconda3/bin/pip install PyMySQL==0.10.1
sudo /opt/anaconda3/bin/pip install mysql-connector-python-rf==2.2.2
sudo /opt/anaconda3/bin/pip install selenium==3.141.0
sudo /opt/anaconda3/bin/pip install fpdf==1.7.2
sudo /opt/anaconda3/bin/pip install pypdf2==1.26.0
sudo /opt/anaconda3/bin/pip install flask_bcrypt==0.7.1
sudo /opt/anaconda3/bin/pip install PyHive==0.6.3
sudo /opt/anaconda3/bin/pip install mysqlclient==2.0.2
sudo /opt/anaconda3/bin/pip install python3-logstash==0.4.80
sudo /opt/anaconda3/bin/pip install --upgrade requests==2.25.1
sudo /opt/anaconda3/bin/pip install boto3==1.16.38
sudo /opt/anaconda3/bin/pip install awscli==1.18.198
sudo /opt/anaconda3/bin/pip install paramiko==2.7.2
sudo /opt/anaconda3/bin/pip install Flask-WTF==0.14.3
sudo /opt/anaconda3/bin/pip install --upgrade MarkupSafe==1.1.1
sudo /opt/anaconda3/bin/pip install marshmallow-sqlalchemy==0.22.0
echo "############################ Installed Python packages ############################"

# Created temorary directory
sudo mkdir -p /tmp/scripts
sudo chmod 777 /tmp/scripts
echo "############################ Created temporary directory ############################"

# Install Airflow
sudo /opt/anaconda3/bin/pip install apache-airflow==1.10.7 
echo "############################ Installed airflow ############################"

# Setup airflow
sudo mkdir -p /usr/lib/airflow
echo 'export AIRFLOW_HOME=/usr/lib/airflow' | sudo tee -a /etc/bashrc

read -p "Enter the new linux user name for the Airflow setup: " user_name
sudo useradd $user_name
sudo chown $user_name:$user_name /usr/lib/airflow

echo "############################ Setup airflow ############################"
# Configure airflow
sudo -i -u $user_name airflow initdb 
sudo -i -u $user_name mkdir -p /usr/lib/airflow/dags
sudo -i -u $user_name cp /usr/lib/airflow/airflow.cfg /usr/lib/airflow/airflow.cfg.template
#/opt/anaconda3/bin/airflow initdb
#sudo mkdir -p /usr/lib/airflow/dags/
#sudo cp /usr/lib/airflow/airflow.cfg /usr/lib/airflow/airflow.cfg.template
NUM_PROCESS=`nproc`
PARALLELISM=`expr $NUM_PROCESS \* 4`
DAG_CONCURRENCY=`expr $NUM_PROCESS \* 2`
MAX_ACTIVE_RUNS_PER_DAG=$NUM_PROCESS
WORKERS=$NUM_PROCESS
MAX_THREADS=$NUM_PROCESS

cat > /tmp/scripts/tmp_conf <<EOF
[core]
airflow_home = /usr/lib/airflow
dags_folder = /usr/lib/airflow/dags
base_log_folder = /usr/lib/airflow/logs
remote_log_conn_id =
encrypt_s3_logs = False
logging_level = INFO
logging_config_class =
log_format = [%%(asctime)s] {%%(filename)s:%%(lineno)d} %%(levelname)s - %%(message)s
simple_log_format = %%(asctime)s %%(levelname)s - %%(message)s
executor = LocalExecutor
sql_alchemy_conn = mysql://root:test1@localhost:3306/airflow
sql_alchemy_pool_size = 5
sql_alchemy_pool_recycle = 3600
parallelism = $PARALLELISM
dag_concurrency = $DAG_CONCURRENCY
dags_are_paused_at_creation = False
non_pooled_task_slot_count = 128
max_active_runs_per_dag = $MAX_ACTIVE_RUNS_PER_DAG
load_examples = False
dags/plugins_folder = /usr/lib/airflow/dags/plugins
fernet_key = MlJKRk1Ox1qz7H26-PqMlv0fcuKD_fimtVd_gtyQxkI=
donot_pickle = False
dagbag_import_timeout = 30
task_runner = BashTaskRunner
default_impersonation =
security =
unit_test_mode = False
task_log_reader = file.task
enable_xcom_pickling = True
killed_task_cleanup_time = 60

[cli]
api_client = airflow.api.client.local_client
endpoint_url = http://localhost:9237

[api]
auth_backend = airflow.api.auth.backend.default

[operators]
default_owner = Airflow
default_cpus = 1
default_ram = 512
default_disk = 512
default_gpus = 0

[webserver]
authenticate = True
auth_backend = airflow.contrib.auth.backends.password_auth
base_url = http://localhost:9237
web_server_host = 0.0.0.0
web_server_port = 9237
web_server_ssl_cert =
web_server_ssl_key =
web_server_worker_timeout = 120
worker_refresh_batch_size = 1
worker_refresh_interval = 30
secret_key = temporary_key
workers = $WORKERS
worker_class = sync
access_logfile = -
error_logfile = -
expose_config = False
filter_by_owner = False
owner_mode = user
dag_default_view = graph
dag_orientation = LR
demo_mode = False
log_fetch_timeout_sec = 5
hide_paused_dags_by_default = False
page_size = 100
rbac = True

[email]
email_backend = airflow.utils.email.send_email_smtp


[smtp]
smtp_host = localhost
smtp_starttls = True
smtp_ssl = False
smtp_port = 25
smtp_mail_from = airflow@example.com


[celery]
celery_app_name = airflow.executors.celery_executor
celeryd_concurrency = 16
worker_log_server_port = 8793
broker_url = sqla+mysql://root:test1@localhost:3306/airflow
celery_result_backend = db+mysql://root:test1@localhost:3306/airflow
flower_host = 0.0.0.0
flower_port = 5555
default_queue = default
celery_config_options = airflow.config_templates.default_celery.DEFAULT_CELERY_CONFIG

[dask]
cluster_address = 127.0.0.1:8786


[scheduler]
job_heartbeat_sec = 5
scheduler_heartbeat_sec = 5
run_duration = -1
min_file_process_interval = 0
dag_dir_list_interval = 300
print_stats_interval = 30
child_process_log_directory = /usr/lib/airflow/logs/scheduler
scheduler_zombie_task_threshold = 300
catchup_by_default = True
max_tis_per_query = 0
statsd_on = False
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow
max_threads = $MAX_THREADS
authenticate = False

[ldap]
uri =
user_filter = objectClass=*
user_name_attr = uid
group_member_attr = memberOf
superuser_filter =
data_profiler_filter =
bind_user = cn=Manager,dc=example,dc=com
bind_password = insecure
basedn = dc=example,dc=com
cacert = /etc/ca/ldap_ca.crt
search_scope = LEVEL

[mesos]
master = localhost:5050
framework_name = Airflow
task_cpu = 1
task_memory = 256
checkpoint = False
authenticate = False

[kerberos]
ccache = /tmp/airflow_krb5_ccache
principal = airflow
reinit_frequency = 3600
kinit_path = kinit
keytab = airflow.keytab


[github_enterprise]
api_rev = v3


[admin]
hide_sensitive_variable_fields = True
EOF
cat /tmp/scripts/tmp_conf | sudo tee /usr/lib/airflow/airflow.cfg
echo "############################ Configured airflow ############################"

# Setup airflow 
echo "############################ Setup airflow ############################"
# Create sample dag
cat > /tmp/scripts/tmp_code <<EOF
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today().strftime('%Y-%m-%d'),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2021, 1, 1),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=7))

# t1, t2 and t3 are examples of tasks
# created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5m',
    retries=3,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)

EOF

cat /tmp/scripts/tmp_code | sudo tee /usr/lib/airflow/dags/test_dag.py
echo "############################ Test Dag created ############################"

# Create first Admin user
echo "############################ Creating the Airflow Admin user ############################"
sudo -i -u $user_name airflow initdb 
read -p "Enter the user name for Airflow admin user: " airflow_name
read -p "Enter the password for Airflow admin user: " airflow_pwd
read -p "Enter the first name for Airflow admin user: " first_name
read -p "Enter the last name for Airflow admin user: " last_name
read -p "Enter the email address of Airflow admin user: " email_address
sudo -i -u $user_name airflow create_user -r Admin -u $airflow_name -f $first_name -l $last_name -p $airflow_pwd -e $email_address 

echo "############################ Installing Java ############################"
sudo yum -y install java-1.8.0-openjdk 

echo "############################ Installing phantomjs ############################"
cd ~
export PHANTOM_JS="phantomjs-2.1.1-linux-x86_64"
sudo wget https://bitbucket.org/ariya/phantomjs/downloads/$PHANTOM_JS.tar.bz2
sudo tar xvjf $PHANTOM_JS.tar.bz2
sudo mv $PHANTOM_JS /usr/local/share
sudo ln -sf /usr/local/share/$PHANTOM_JS/bin/phantomjs /usr/local/bin
sudo /usr/local/bin/phantomjs --version 

#echo "############################ Running the airflow ############################"
#sudo su $user_name 
#cd ~
#nohup airflow webserver $* >> /usr/lib/airflow/logs/webserver.logs 2>&1 < /dev/null &
#nohup airflow scheduler $* >> /usr/lib/airflow/logs/scheduler.logs 2>&1 < /dev/null &

