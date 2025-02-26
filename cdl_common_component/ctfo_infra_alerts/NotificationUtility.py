#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "ZS Associates"
# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package and available on bit bucket code repository ( https://sourcecode.jnj.com/projects/ASX-NCKV/repos/jrd_fido_de/browse ) & all static servers.

"""
Doc_Type            : Tech Products
Tech Description    : This utility is used for sending Notification emails using AWS SES service.
                      The utility will be used by other utilities like File ingestion, orchestration
                      , cluster manager, etc. to send out notifications
Pre_requisites      : This utility needs NotificationUtilityConstants.py which contains the required
                      constants
Inputs              :
Outputs             : Notification utility execution status dictionary along with the result/error
                      in the format -
                      {"status":"SUCCESS/FAILED",
                      "result":"<Result if SUCCESS>",
                      "error":"<Error if FAILED>"}
Example             : Input Dictionary -
                      {
                          "ses_region": "<region>",
                          "subject": "$$<parameter>$$",
                          "sender": "<email_address@dummy.com>",
                          "recipient_list": ["<email_address1@dummy.com>",
                          "<email_address2@dummy.com>"],
                          "email_body": "Hi $$user_name$$\n\n\nRegards,\n$$user_name$$",
                          "email_template": "<dummy_template>",
                          "param_replace_dict": {"user_name":"<username_value>"}
                      }
                      Command to execute -
                      python NotificationUtility.py --json_file_path <file_name>.json
Config_file         : NA
"""

import os
import argparse
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import boto.ses
import boto3
import NotificationUtilityConstants
import traceback
#from LogSetup import logger
#from CTLogSetup import get_logger

# Get the custom logger instance for this utility
#logging = get_logger()

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# All module level constants are declared here
FAILED_STATUS = {NotificationUtilityConstants.STATUS_KEY:
                 NotificationUtilityConstants.STATUS_FAILED,
                 NotificationUtilityConstants.ERROR_KEY: "",
                 NotificationUtilityConstants.RESULT_KEY: ""}
SUCCESS_STATUS = {NotificationUtilityConstants.STATUS_KEY:
                  NotificationUtilityConstants.STATUS_SUCCESS,
                  NotificationUtilityConstants.ERROR_KEY: "",
                  NotificationUtilityConstants.RESULT_KEY: ""}


class NotificationUtility(object):
    """
    Class for Notification Utility
    """

    def _validate_inputs(self, ses_region, subject, sender, recipient_list, email_body,
                         email_template, replace_param_dict):
        """
        Purpose   :   This function will validate all the inputs for the notification utility
        Input     :   SES access key, SES secret key, SES region, email subject, email sender,
                      recipient list, email body, email template, replacement parameter dictionary
        Output    :   Returns True if all the inputs are validated else raises Exception
        """
        if ses_region is None or ses_region == "":
            status_message = "Please provide ses access key, secret key and region for sending " \
                             "notifications"
            raise Exception(status_message)
        if subject is None or subject == "":
            status_message = "Subject cannot be empty for sending notifications"
            raise Exception(status_message)
        if sender is None or sender == "":
            status_message = "Sender cannot be empty for sending notifications"
            raise Exception(status_message)
        if not recipient_list or type(recipient_list) != list:
            status_message = "Please provide recipient list in list form for sending notifications"
            raise Exception(status_message)
        if ((email_body is None or email_body == "") and (email_template is None or
                                                          email_template == "")) or \
                ((email_body is not None and email_body != "") and
                 (email_template is not None and email_template != "")):
            status_message = "Either email body or email template can be given"
            raise Exception(status_message)
        if email_template and not (os.path.exists(os.path.join(CURRENT_DIR, email_template)) or\
                os.path.isdir(os.path.join(CURRENT_DIR, email_template))):
            status_message = "Email template provided does not exists or is a directory"
            raise Exception(status_message)
        if replace_param_dict and type(replace_param_dict) != dict:
            status_message = "Replacement parameter list must be a valid dictionary"
            raise Exception(status_message)
        return True

    def _replace_params(self, param_string, param_file_path, replace_param_dict):
        """
        Purpose     :   This function will replace the parameters in the given parameterized string
        Input       :   Parameterized string, parameter file path, replacement dictionary
        Output      :   Modified string, after replacing the parameters
        """
        if param_file_path:
            replaced_string = open(os.path.join(CURRENT_DIR, param_file_path), "r").read()
        else:
            replaced_string = param_string
        if "$$" in replaced_string:
            if not replace_param_dict:
                status_message = "No replacement dictionary found for parameters in - " + \
                replaced_string
                raise Exception(status_message)
            for param in list(replace_param_dict.keys()):
                replaced_string = replaced_string.replace("$$" + param + "$$"
                                                          , replace_param_dict[param])
            #if "$$" in replaced_string:
            #    status_message = "All parameters are not replaced from the mail body - " +\
            #                     str(re.findall(r'\$\$.*?\$\$', replaced_string))
            #    raise Exception(status_message)
        return replaced_string

    def send_notification(self, ses_region, subject, sender, recipient_list, email_body=None,
                          email_template=None, replace_param_dict=None):
        """Function to send notification"""
        try:
            if self._validate_inputs(ses_region, subject, sender, recipient_list, email_body,
                                     email_template, replace_param_dict):
                email_body_string = self._replace_params(email_body, email_template,
                                                         replace_param_dict)
                email_subject = self._replace_params(subject, None, replace_param_dict)
                #ses_connection = boto.ses.connect_to_region(ses_region)
                email_subject = email_subject.replace("\n", " ").replace("\t", " ").\
                                replace("\r", " ")
                client = boto3.client('ses', region_name=ses_region)
                response = client.send_email(
                    Destination={'ToAddresses': recipient_list},
                    Message={
                        'Body': {
                            'Html': {
                                'Charset': 'UTF-8',
                                'Data': email_body_string
                            }
                        },
                        'Subject': {
                            'Charset': 'UTF-8',
                            'Data': email_subject,
                        }
                    },
                    Source=sender
                )

                # ses_connection.send_email(source=sender, subject=email_subject,
                #                          body=email_body_string, to_addresses=recipient_list,
                #                          format=NotificationUtilityConstants.MIME_TYPE)
                status_message = "Email sent successfully"
                SUCCESS_STATUS[NotificationUtilityConstants.RESULT_KEY] = status_message
                return SUCCESS_STATUS
        except Exception as ex:
            FAILED_STATUS[NotificationUtilityConstants.ERROR_KEY] = str(ex)
            return FAILED_STATUS

    def send_notif_with_pdf_attachment(self, ses_region, subject, sender, recipient_list,
                                       pdf_file_name,
                                       email_body=None, email_template=None,
                                       replace_param_dict=None):
        """Function to send notification with PDF attachment"""
        try:
            if self._validate_inputs(ses_region, subject, sender, recipient_list, email_body,
                                     email_template, replace_param_dict):
                #ses_connection = boto.ses.connect_to_region(ses_region)
                client = boto3.client('ses', region_name=ses_region)
                email_subject = self._replace_params(subject, None, replace_param_dict)
                email_subject = email_subject.replace("\n", " ").replace("\t", " ").\
                                replace("\r", " ")

                email_body_string = self._replace_params(email_body, email_template,
                                                         replace_param_dict)

                msg = MIMEMultipart()
                msg['Subject'] = email_subject

                # what a recipient sees if they don't use an email reader
                msg.preamble = 'Ganglia PDF report \n'

                # the message body
                part = MIMEText(email_body_string)
                part.add_header('Content-Type', 'text/html')
                msg.attach(part)

                # the attachment
                part = MIMEApplication(open(pdf_file_name, 'rb').read())
                part.add_header('Content-Disposition', 'attachment', filename=pdf_file_name)
                msg.attach(part)

                # and send the message
                #result = ses_connection.send_raw_email(msg.as_string()
                #                                       , source=sender
                #                                       , destinations=recipient_list)
                result = client.send_raw_email(
                    RawMessage={'Data': msg.as_string()},
                    Destinations= recipient_list,
                    Source=sender,
                )
                return NotificationUtilityConstants.STATUS_SUCCESS
        except Exception as ex:
            FAILED_STATUS[NotificationUtilityConstants.ERROR_KEY] = str(ex)
            return FAILED_STATUS


if __name__ == "__main__":
    # parse json and initialize input variables
    parser = argparse.ArgumentParser(description="Python utility to send email notifications")
    parser.add_argument("--json_file_path", help="json file path containing file arguments",
                        required=True)
    args = vars(parser.parse_args())
    file_path = args[NotificationUtilityConstants.JSON_FILE_PATH_KEY]
    with open(file_path) as f:
        data = json.load(f)
        data = data['NotificationUtility']
        input_ses_region = data[NotificationUtilityConstants.SES_REGION_KEY]
        input_subject = data[NotificationUtilityConstants.SUBJECT_KEY]
        input_sender = data[NotificationUtilityConstants.SENDER_KEY]
        input_recipient_list = data[NotificationUtilityConstants.RECIPIENT_LIST_KEY]
        input_email_body = data[NotificationUtilityConstants.EMAIL_BODY_KEY]
        input_email_template = data[NotificationUtilityConstants.EMAIL_TEMPLATE_KEY]
        input_replace_param_dict = data[NotificationUtilityConstants.PARAM_REPLACE_KEY]

    notification_utility = NotificationUtility()
    # main function to execute Notification Utility
    print(notification_utility.send_notification(input_ses_region,
                                                 input_subject, input_sender, input_recipient_list,
                                                 input_email_body, input_email_template,
                                                 input_replace_param_dict))
