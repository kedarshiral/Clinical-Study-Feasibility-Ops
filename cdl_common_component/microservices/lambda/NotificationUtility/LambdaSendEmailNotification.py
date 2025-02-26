# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.
import boto3
import logging
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

local_temp_file = "/tmp/template.html"
local_pdf_file = "/tmp/temp_pdf.pdf"


def success_response_formatter(result):
    """
    Success response formatting module
    :param result:
    :return:
    """
    try:
        success_response = {
            "status": "SUCCESS",
            "result": result,
            "error": ""
        }
        return success_response
    except Exception as exc:
        raise Exception(str(exc))


def error_response_formatter(error):
    """
    Error response formatting module
    :param error:
    :return:
    """
    error_response = {
        "status": "FAILED",
        "result": dict(),
        "error": str(error)
    }
    return error_response


def validate_input(input_payload):
    """

    :param input_payload:
    :return:
    """
    try:
        ses_region = input_payload.get("ses_region", "")
        subject = input_payload.get("subject", "")
        sender = input_payload.get("sender", "")
        recipient_list = input_payload.get("recipient_list", [])
        email_body = input_payload.get("email_body", "")
        email_template = input_payload.get("email_template", {})
        replace_param_dict = input_payload.get("replace_param_dict", {})
        attachment_path = input_payload.get("attachment_path", {})

        if not ses_region:
            status_message = "Please provide ses access key, secret key and region for sending notifications"
            raise Exception(status_message)
        if not subject:
            status_message = "Subject cannot be empty for sending notifications"
            raise Exception(status_message)
        if not sender:
            status_message = "Sender cannot be empty for sending notifications"
            raise Exception(status_message)
        if not recipient_list or type(recipient_list) != list:
            status_message = "Please provide recipient list in list form for sending notifications"
            raise Exception(status_message)
        if (not email_body and not email_template) or (bool(email_body) and bool(email_template)):
            status_message = "Either email body or email template can be given"
            raise Exception(status_message)
        if email_template:
            s3_file_path = email_template.get("template_path", "")
            if not s3_file_path:
                status_message = "S3 path for email template file not specified."
                raise Exception(status_message)
            else:
                bucket_name = email_template.get("s3_bucket", "")
                if not bucket_name:
                    status_message = "S3 bucket for email template file is not specified."
                    raise Exception(status_message)
                else:
                    s3 = boto3.resource('s3')
                    try:
                        s3.Bucket(bucket_name).download_file(s3_file_path, local_temp_file)
                    except Exception as ex:
                        logging.error(str(ex))
                        status_message = "Email template provided does not exists or is a directory"
                        raise Exception(status_message)
        if attachment_path:
            s3_file_path = attachment_path.get("pdf_file_path", "")
            if not s3_file_path:
                status_message = "S3 path for PDF file not specified."
                raise Exception(status_message)
            else:
                bucket_name = attachment_path.get("s3_bucket", "")
                if not bucket_name:
                    status_message = "S3 bucket for attachment PDF file is not specified."
                    raise Exception(status_message)
                else:
                    s3 = boto3.resource('s3')
                    try:
                        s3.Bucket(bucket_name).download_file(s3_file_path, local_pdf_file)
                    except Exception as ex:
                        logging.error(str(ex))
                        status_message = "PDF file path provided does not exists or is a directory"
                        raise Exception(status_message)
        if replace_param_dict and type(replace_param_dict) != dict:
            status_message = "Replacement parameter list must be a valid dictionary"
            raise Exception(status_message)
        return True
    except Exception as ex:
        logging.error(str(ex))
        raise Exception(str(ex))


def replace_params(param_string, param_file_path, replace_param_dict):
    """

    :param param_string:
    :param param_file_path:
    :param replace_param_dict:
    :return:
    """
    try:
        if param_file_path:
            replaced_string = open(local_temp_file, "r").read()
        else:
            replaced_string = param_string
        if "$$" in replaced_string:
            if not replace_param_dict:
                status_message = "No replacement dictionary found for parameters in - " + replaced_string
                raise Exception(status_message)
            for param in replace_param_dict.keys():
                replaced_string = replaced_string.replace("$$" + param + "$$", replace_param_dict[param])
        return replaced_string
    except Exception as ex:
        logging.error(str(ex))
        raise Exception(str(ex))


class EmailNotificationUtility(object):
    """
    Email Notification module
    """

    def __init__(self, payload):
        self.ses_region = payload["ses_region"]
        self.subject = payload["subject"]
        self.sender = payload["sender"]
        self.recipient_list = payload["recipient_list"]
        self.email_body = payload.get("email_body", "")
        self.email_template = payload.get("email_template", {})
        self.replace_param_dict = payload.get("replace_param_dict", {})
        self.attachment_path = payload.get("attachment_path", {})

    def send_notification(self):
        """
        Function to send notification
        :return:
        """
        try:

            email_body_string = replace_params(self.email_body, self.email_template,
                                               self.replace_param_dict)
            email_subject = replace_params(self.subject, None, self.replace_param_dict)
            ses_connection = boto3.client('ses', region_name=self.ses_region)
            email_subject = email_subject.replace("\n", " ").replace("\t", " ").replace("\r", " ")
            if not self.attachment_path:
                ses_connection.send_email(
                    Source=self.sender,
                    Destination={
                        "ToAddresses": self.recipient_list,
                    },
                    Message={
                        "Subject": {
                            "Data": email_subject,
                            "Charset": 'UTF-8'
                        },
                        "Body": {
                            "Html": {
                                "Data": email_body_string,
                                "Charset": 'UTF-8'
                            }
                        }
                    })
                status_message = "Email sent successfully"
            else:
                print("Send with attachment")
                msg = MIMEMultipart()
                msg['Subject'] = email_subject

                # what a recipient sees if they don't use an email reader
                msg.preamble = 'Ganglia PDF report \n'

                # the message body
                part = MIMEText(email_body_string, 'html')
                part.add_header('Content-Type', 'text/html')
                msg.attach(part)

                # the attachment
                part = MIMEApplication(open(local_pdf_file, 'rb').read())
                part.add_header('Content-Disposition', 'attachment', filename=local_pdf_file)
                msg.attach(part)

                # and send the message
                ses_connection = boto3.client('ses', region_name=self.ses_region)
                ses_connection.send_raw_email(
                    Source=self.sender,
                    Destinations=self.recipient_list,
                    RawMessage={
                        'Data': msg.as_string()
                    })
                status_message = "Email with PDF attachment sent successfully"
            return status_message
        except Exception as ex:
            logging.error(str(ex))
            raise Exception(str(ex))


def lambda_handler(event, context):
    """
    Lambda handler
    :param event:
    :param context:
    :return:
    """
    try:
        validate_input(event)
        email_obj = EmailNotificationUtility(event)
        # os.remove(local_temp_file)
        return success_response_formatter(email_obj.send_notification())
    except Exception as ex:
        logging.error(str(ex))
        return error_response_formatter("Error occurred while sending email notification: " + str(ex))
