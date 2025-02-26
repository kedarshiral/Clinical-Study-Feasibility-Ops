from NotificationUtility import NotificationUtility
import os, json
from LogSetup import logger
import pandas as pd

config_file_path = "/clinical_design_center/data_management/ctfo_infra_alerts/code/environment.json"


def send_disk_usage_notification_email():
    """

    :param file_path:
    :return:
    """
    try:
        logger.info("Reading from Configuration file has started")
        with open(config_file_path) as f:
            json_data = json.load(f)
            data = json_data['hard_disk']
            ses_region = data["ses_region"]
            subject = data["subject"]
            sender = data["sender"]
            recipient_list = data["recipient_list"]
            disk_usage_percentage = data["threshold_value"]
        disk_usage_percentage=float(disk_usage_percentage)
        subject=subject+" "+str(int(float(disk_usage_percentage)))+"%"
        logger.info("Reading from Configuration file is Completed")

        email_template = "send_disk_space_notification_template.html"
        new_obj = NotificationUtility()

        disk_name_list = []
        count = 0
        disk_space = os.popen('df -h')
        disk_space_info = disk_space.read()
        pandas_dataframe_list = []
        # disk_space_df = pd.DataFrame([x.split(' ') for x in disk_space_info.split('\n')])
        for row, line in enumerate(disk_space_info.split('\n')):
            if row == 0:
                disk_detail_info = line.split(' ')
                disk_detail_info = [i for i in disk_detail_info if i]
                pandas_dataframe_list.append(disk_detail_info)
                pass
            else:
                disk_detail_info = line.split(' ')
                if disk_detail_info == [""]:
                    break
                else:
                    disk_detail_info = [i for i in disk_detail_info if i]
                    pandas_dataframe_list.append(disk_detail_info)
                    used_space_percentage = float(str(disk_detail_info[4]).replace("%", ""))
                    #print(used_space_percentage)
                    if used_space_percentage > disk_usage_percentage:
                        disk_name_list.append(disk_detail_info[0])
                        count = count + 1
                    else:
                        pass
        if count > 0:
            statement = ""
            if count == 1:
                statement = "Below Disk has consumed more than "+str(int(float(disk_usage_percentage)))+"% disk space:"
            if count > 1:
                statement = "Below Disks has consumed more than "+str(int(float(disk_usage_percentage)))+"% disk space:"
            disk_space_df = pd.DataFrame(pandas_dataframe_list)
            disk_space_df = disk_space_df[[0,1,2,4,5]]
            html = disk_space_df.to_html(index=False, header=False)
            disk_name_df = pd.DataFrame(disk_name_list)
            disk_name_html = disk_name_df.to_html(index=False, header=False)
            replace_variables = {"disk_name_list": disk_name_html,
                                 "statement": statement, "html": html}
            replace_param_dict = replace_variables
            response = new_obj.send_notification(ses_region=ses_region, subject=subject, sender=sender,
                                                 recipient_list=recipient_list, email_template=email_template,
                                                 replace_param_dict=replace_param_dict)
            if response:
                logger.info(response)
            else:
                logger.error("Error Massage "+response['error'])
        else:
            pass

    except Exception as e:
        logger.error("Error Message : " + str(e))


send_disk_usage_notification_email()
