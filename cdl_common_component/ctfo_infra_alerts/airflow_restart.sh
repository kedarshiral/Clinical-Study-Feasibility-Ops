

#!/bin/bash

service=airflow-webserver
if (( $(ps -ef | grep -v grep | grep $service | wc -l) > 0 ))
then
  echo "$service is running!!!"
else
  aws ses send-email  --from "jrd_fido_de_svc@its.jnj.com" --destination "ToAddresses=SFaris1@ITS.JNJ.com,APendkar@ITS.JNJ.com" --message "Subject={Data=Notification: Prod Airflow Webserver Down and Restarted. ,Charset=utf8},Body={Text={Data=ses says hi,Charset=utf8},Html={Data=<p>Hi Team <br><br>The Airflow webserver was stopped and it has been restarted successfully. Please let us know if you face any issues<br><br>Thanks<br>FIDO Infra Support</p>,Charset=utf8}}" --region us-east-1
  for p_id in `pgrep -f "airflow webserver"`; do kill -9  $p_id; done
  for p_id in `pgrep -f "airflow-webserver"`; do kill -9  $p_id; done
  nohup airflow webserver $* >> /apps/airflow/logs/webserver.log </dev/null &

fi

SERVICE=scheduler
if (( $(ps -ef | grep -v grep | grep $SERVICE | wc -l) > 0 ))
then
    echo "$SERVICE is running!!!"
else
    echo "Service $SERVICE is not and try to restart the service"
     for p_id in `pgrep -f "airflow scheduler"`; do kill -9  $p_id; done
         nohup airflow scheduler $* >> /apps/airflow/logs/scheduler.log </dev/null &
     aws ses send-email  --from "jrd_fido_de_svc@its.jnj.com" --destination "ToAddresses=SFaris1@ITS.JNJ.com,APendkar@ITS.JNJ.com" --message "Subject={Data=Notification: Prod  Airflow Scheduler Down and Restarted.,Charset=utf8},Body={Text={Data=ses says hi,Charset=utf8},Html={Data=<p>Hi Team <br><br>The Airflow scheduler was stopped and it has been restarted successfully. Please let us know if you face any issues.<br><br>Thanks<br>FIDO Infra Support</p>,Charset=utf8}}" --region us-east-1

fi
