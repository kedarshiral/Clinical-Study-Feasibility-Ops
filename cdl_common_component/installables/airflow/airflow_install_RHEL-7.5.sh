# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
#!/bin/bash

# Install Linux packages
sudo yum -y install wget
sudo yum -y install gcc
sudo yum -y install python-devel
echo "############################ Installed Linux packages ############################"

# Created temorary directory
sudo mkdir -p /tmp/installables
sudo chmod 777 /tmp/installables
echo "############################ Created temporary directory ############################"

# Install MySQL
cd /tmp/installables
sudo rpm -Uvh http://repo.mysql.com/mysql-community-release-el7-5.noarch.rpm
sudo yum -y install mysql-server
sudo yum -y install mysql-devel
echo "############################ Installed MySQL ############################"

# Install Python packages
sudo easy_install pip
sudo pip install docutils
sudo pip install python-daemon
sudo pip install numpy
sudo pip install pandas
sudo pip install html5lib
sudo pip install Fernet
sudo pip install cryptography
sudo pip uninstall -y markupsafe
sudo pip install markupsafe==0.23
sudo pip install celery
sudo pip install --upgrade setuptools
sudo pip install mysql
# Required for RedHat 7.4
sudo pip install Flask-WTF==0.14
sudo pip install --upgrade MarkupSafe
echo "############################ Installed Python packages ############################"

# Start and setup MySQL
sudo service mysqld start
sudo chkconfig mysqld on
sudo mysqladmin -u root password Admin@12345
sudo mysql -uroot -pAdmin@12345 -e"create database airflow"
echo "############################ Set up MySQL for Airflow ############################"

# Install Airflow
cd /tmp/installables
wget https://pypi.python.org/packages/9e/12/6c70f9ef852b3061a3a6c9af03bd9dcdcaecb7d75c8898f82e3a54ad5f87/apache-airflow-1.9.0.tar.gz
tar -xvzf apache-airflow-1.9.0.tar.gz
cd apache-airflow-1.9.0
sudo python setup.py install
echo "############################ Installed Airflow ############################"

# Setup Airflow
sudo mkdir -p /usr/lib/airflow
echo 'export AIRFLOW_HOME=/usr/lib/airflow' | sudo tee -a /etc/bashrc
sudo useradd airflow
sudo chown airflow:airflow /usr/lib/airflow
echo "############################ Setup Airflow ############################"

# Configure Airflow
sudo -i -u airflow airflow initdb
sudo -i -u airflow mkdir -p /usr/lib/airflow/dags
sudo -i -u airflow cp /usr/lib/airflow/airflow.cfg /usr/lib/airflow/airflow.cfg.template
cd /tmp/installables
NUM_PROCESS=`nproc`
PARALLELISM=`expr $NUM_PROCESS \* 4`
DAG_CONCURRENCY=`expr $NUM_PROCESS \* 2`
MAX_ACTIVE_RUNS_PER_DAG=$NUM_PROCESS
WORKERS=$NUM_PROCESS
MAX_THREADS=$NUM_PROCESS

cat > tmp_conf <<EOF

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
sql_alchemy_conn = mysql://root:Admin@12345@localhost:3306/airflow
sql_alchemy_pool_size = 5
sql_alchemy_pool_recycle = 3600
parallelism = $PARALLELISM
dag_concurrency = $DAG_CONCURRENCY
dags_are_paused_at_creation = False
non_pooled_task_slot_count = 128
max_active_runs_per_dag = $MAX_ACTIVE_RUNS_PER_DAG
load_examples = True
dags/plugins_folder = /usr/lib/airflow/dags/plugins
fernet_key = rad9IJOuMQc_FnG03Pqen-KWAK3UT4PUYgvgXPEOrdA=
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
endpoint_url = http://localhost:8080

[api]
auth_backend = airflow.api.auth.backend.default

[operators]
default_owner = Airflow
default_cpus = 1
default_ram = 512
default_disk = 512
default_gpus = 0

[webserver]
base_url = http://localhost:8080
web_server_host = 0.0.0.0
web_server_port = 8080
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
expose_config = True
authenticate = False
filter_by_owner = False
owner_mode = user
dag_default_view = graph
dag_orientation = LR
demo_mode = False
log_fetch_timeout_sec = 5
hide_paused_dags_by_default = False
page_size = 100

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
sql_alchemy_conn = mysql://root:Admin@12345@localhost:3306/airflow
sql_alchemy_pool_size = 5
sql_alchemy_pool_recycle = 3600
parallelism = $PARALLELISM
dag_concurrency = $DAG_CONCURRENCY
dags_are_paused_at_creation = False
non_pooled_task_slot_count = 128
max_active_runs_per_dag = $MAX_ACTIVE_RUNS_PER_DAG
load_examples = True
dags/plugins_folder = /usr/lib/airflow/dags/plugins
fernet_key = rad9IJOuMQc_FnG03Pqen-KWAK3UT4PUYgvgXPEOrdA=
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
endpoint_url = http://localhost:8080

[api]
auth_backend = airflow.api.auth.backend.default

[operators]
default_owner = Airflow
default_cpus = 1
default_ram = 512
default_disk = 512
default_gpus = 0

[webserver]
base_url = http://localhost:8080
web_server_host = 0.0.0.0
web_server_port = 8080
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
expose_config = True
authenticate = False
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
broker_url = sqla+mysql://airflow:airflow@localhost:3306/airflow
celery_result_backend = db+mysql://airflow:airflow@localhost:3306/airflow
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

cat tmp_conf | sudo -u airflow tee /usr/lib/airflow/airflow.cfg
echo "############################ Configured Airflow ############################"

# Setup Airflow
sudo -i -u airflow airflow initdb
echo "############################ Setup Airflow ############################"

# Start Airflow
cd /tmp/installables

cat > tmp_run_script.sh <<EOF
nohup airflow webserver $* >> /usr/lib/airflow/logs/webserver.logs 2>&1 < /dev/null &
nohup airflow scheduler $* >> /usr/lib/airflow/logs/scheduler.logs 2>&1 < /dev/null & 
EOF

chmod 777 tmp_run_script.sh
sudo -i -u airflow sh /tmp/installables/tmp_run_script.sh
echo "############################ Airflow started ############################"

# Create sample dag
cat > tmp_code <<EOF

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

cat tmp_code | sudo -u airflow tee /usr/lib/airflow/dags/test.py
echo "############################ Test Dag created ############################"

# Removed temporary directory
sudo rm -rf /tmp/installables
echo "############################ Removed temorary directory ############################"
echo ""
echo ""
echo "Airflow installation complete. Do the following to verify:"
echo "  -  Open Airlow Web Server at http://localhost:8080 to see if it is running fine"
echo "  -  Pause all the dags except dag 'test'"
echo "  -  Sudo to the airflow user and trigger the dag test. Command to trigger: 'airflow trigger_dag test'"
echo "  -  Open the Airflow Web Server to see the status of dag 'test'"

