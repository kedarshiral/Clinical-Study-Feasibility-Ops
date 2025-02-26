from NotificationUtility import NotificationUtility
import os, json
from LogSetup import logger
import psutil
import socket
import traceback

import pandas as pd

config_file_path = "/clinical_design_center/data_management/ctfo_infra_alerts/code/environment.json"


def send_cpu_usage_notification_email():
    """

    :param file_path:
    :return:
    """
    try:
        logger.info("Reading from Configuration file has started")
        with open(config_file_path) as f:
            json_data = json.load(f)
            data = json_data['cpu_utilization']
            ses_region = data["ses_region"]
            subject = data["subject"]
            sender = data["sender"]
            recipient_list = data["recipient_list"]
            cpu_usage_percentage = data["threshold_value"]
        cpu_usage_percentage = float(cpu_usage_percentage)
        subject = subject + " " + str(int(float(cpu_usage_percentage))) + "%"
        logger.info("Reading from Configuration file is Completed")

        email_template = "send_cpu_utilization_notification_template.html"
        new_obj = NotificationUtility()
        cpu_utilization = psutil.cpu_percent()
        if cpu_utilization > cpu_usage_percentage:
            statement = "CPU utilization is more than " + str(
                int(float(cpu_usage_percentage))) + "%: "
            details = dict()
            details["instance"] = socket.gethostname()
            details["Ip"] = socket.gethostbyname(socket.gethostname())
            details["cpuUtilization"] = cpu_utilization
            # details.append(socket.gethostbyname(socket.gethostname()))
            # details.append(cpu_utilization)
            # instance_details = list()
            # instance_details.append("InstanceName")
            # instance_details.append("InstanceIp")
            # instance_details.append("CPUUtilization")
            # instance_details_df = pd.DataFrame(instance_details)
            # instance_details_html = instance_details_df.to_html(index=False, header=False)
            cpu_utilization_df = pd.DataFrame([details])
            html = cpu_utilization_df.to_html(index=False, header=True)
            replace_variables = {
                "statement": statement, "html": html
            }
            replace_param_dict = replace_variables
            response = new_obj.send_notification(ses_region=ses_region, subject=subject, sender=sender,
                                                 recipient_list=recipient_list, email_template=email_template,
                                                 replace_param_dict=replace_param_dict)
            print(response)
            if response:
                logger.info(response)
            else:
                logger.error("Error Massage " + response['error'])
        else:
            pass

    except Exception as e:
        traceback.print_exc()
        logger.error("Error Message : " + str(e))


send_cpu_usage_notification_email()
