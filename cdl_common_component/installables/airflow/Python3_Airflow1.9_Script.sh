# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
#!/bin/bash

#this command logs the installation console output a log file placed in the home directory of the user running this script
exec > >(tee ~/airflow.log) 2>&1


# installing anaconda
sudo yum install bzip2 -y
sudo yum install wget -y
wget -P /opt https://repo.continuum.io/archive/Anaconda3-4.4.0-Linux-x86_64.sh
bash /opt/Anaconda3-4.4.0-Linux-x86_64.sh -p/opt/anaconda3 -b yes

# exporting the path to bashrc
echo 'export PATH="/opt/anaconda3/bin:$PATH"' | sudo tee -a /etc/bashrc

# giving full access to anaconda installation folder
chmod 777 /opt/anaconda3/

#listing the python version sinstalled
which python /usr/bin/python

# removing the downloaded installer
rm /opt/Anaconda3-4.4.0-Linux-x86_64.sh

# restarting the profile
source ~/.bashrc

/opt/anaconda3/bin/conda install pip -y

echo 'anaconda installed'

sudo yum -y install wget
sudo yum -y install gcc
sudo yum -y install git
sudo yum -y install python-devel
echo "############################ Installed Linux packages ############################"
/opt/anaconda3/bin/pip install docutils
/opt/anaconda3/bin/pip install python-daemon
/opt/anaconda3/bin/pip install numpy
/opt/anaconda3/bin/pip install pandas
/opt/anaconda3/bin/pip install html5lib
/opt/anaconda3/bin/pip install Fernet
/opt/anaconda3/bin/pip install cryptography
/opt/anaconda3/bin/sudo pip uninstall -y markupsafe
/opt/anaconda3/bin/sudo pip install markupsafe==0.23
/opt/anaconda3/bin/pip install celery
/opt/anaconda3/bin/pip install --upgrade setuptools
/opt/anaconda3/bin/pip install mysql
/opt/anaconda3/bin/pip install MySQL-python
/opt/anaconda3/bin/pip install mysql-connector-python-rf
/opt/anaconda3/bin/pip install selenium
/opt/anaconda3/bin/pip install fpdf
/opt/anaconda3/bin/pip install pypdf2
/opt/anaconda3/bin/pip install flask_bcrypt
/opt/anaconda3/bin/pip install PyHive
/opt/anaconda3/bin/pip install mysqlclient
/opt/anaconda3/bin/pip install python3-logstash
/opt/anaconda3/bin/pip install boto3
/opt/anaconda3/bin/pip install paramiko
# Required for RedHat 7.4
/opt/anaconda3/bin/pip install Flask-WTF==0.14
/opt/anaconda3/bin/pip install --upgrade MarkupSafe
echo "############################ Installed Python packages ############################"

# Created temorary directory
sudo mkdir -p /tmp/scripts
sudo chmod 777 /tmp/scripts
echo "############################ Created temporary directory ############################"

# Install MySQL
sudo rpm -Uvh http://repo.mysql.com/mysql-community-release-el7-5.noarch.rpm
/opt/anaconda3/bin/pip install mysqlclient
sudo yum -y install mysql-server
sudo yum -y install mysql-devel
sudo /opt/anaconda3/bin/pip install mysqlclient
echo "############################ Installed MySQL ############################"

# Start and setup MySQL
sudo service mysqld start
sudo chkconfig mysqld on
sudo mysqladmin -u root password test1
sudo mysql -uroot -ptest1 -e"create database airflow"
echo "############################ Set up MySQL for airflow ############################"

# Install Airflow
sudo wget -P /opt https://pypi.python.org/packages/9e/12/6c70f9ef852b3061a3a6c9af03bd9dcdcaecb7d75c8898f82e3a54ad5f87/apache-airflow-1.9.0.tar.gz
sudo tar -xvzf /opt/apache-airflow-1.9.0.tar.gz -C /opt/
cd /opt/apache-airflow-1.9.0
/opt/anaconda3/bin/python setup.py install
echo "############################ Installed airflow ############################"

# Setup airflow
sudo mkdir -p /usr/lib/airflow
echo 'export AIRFLOW_HOME=/usr/lib/airflow' | sudo tee -a /etc/bashrc
sudo useradd svc_etl
sudo chown svc_etl:svc_etl /usr/lib/airflow

echo "############################ Setup airflow ############################"

# Configure airflow
sudo -i -u svc_etl airflow initdb
sudo -i -u svc_etl mkdir -p /usr/lib/airflow/dags
sudo -i -u svc_etl cp /usr/lib/airflow/airflow.cfg /usr/lib/airflow/airflow.cfg.template
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
#/opt/anaconda3/bin/airflow initdb
sudo -i -u svc_etl airflow initdb
echo "############################ Setup airflow ############################"

cat > /tmp/airflow_authenticate.py <<EOF
import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
user = PasswordUser(models.User())
user.username = 'airflow'
user.email = ''
user._set_password = 'Admin@12345'
session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()
EOF

python /tmp/airflow_authenticate.py


# Create sample dag
cat > /tmp/scripts/tmp_code <<EOF

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
}

dag = DAG(
    'test',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
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
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)

EOF

cat /tmp/scripts/tmp_code | sudo tee /usr/lib/airflow/dags/test_dag.py
echo "############################ Test Dag created ############################"

# Removed temporary directory
sudo rm -rf /tmp/installables
sudo rm -rf /opt/apache-airflow-1.9.0
rm /opt/apache-airflow-1.9.0.tar.gz
#source deactivate airflow
echo "############################ Removed temorary directory ############################"

echo "############################ Installing the start script ############################"
sudo mkdir -p /opt/run_scripts
sudo chown svc_etl:svc_etl /opt/run_scripts
sudo yum install screen -y
cat > /opt/run_scripts/airflow.sh <<EOF
echo 'starting airflow webserver'
screen -S airflow -d -m  airflow webserver
screen -S airflow_scheduler -d -m  airflow scheduler
echo 'started airflow webserver'
EOF

echo "############################ Running the start script ############################"
sudo /opt/anaconda3/bin/pip install mysqlclient
sudo -i -u svc_etl airflow initdb
sudo -i -u svc_etl bash /opt/run_scripts/airflow.sh

yum -y install java-1.8.0-openjdk

cd ~
export PHANTOM_JS="phantomjs-1.9.8-linux-x86_64"
wget https://bitbucket.org/ariya/phantomjs/downloads/$PHANTOM_JS.tar.bz2
sudo tar xvjf $PHANTOM_JS.tar.bz2
sudo mv $PHANTOM_JS /usr/local/share
sudo ln -sf /usr/local/share/$PHANTOM_JS/bin/phantomjs /usr/local/bin

phantomjs --version

