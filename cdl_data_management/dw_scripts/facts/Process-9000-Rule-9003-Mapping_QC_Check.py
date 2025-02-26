# !/usr/bin/python
# -*- coding: utf-8 -*-
__AUTHOR__ = 'ZS Associates'

import traceback
import sys
import os
from LogSetup import logger

sys.path.insert(0, os.getcwd())
from pyspark.sql import *
from ConfigUtility import JsonConfigUtility
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import CommonConstants as CommonConstants
from PySparkUtility import PySparkUtility
from ExecutionContext import ExecutionContext
from CommonUtils import *
from CommonUtils import *

MODULE_NAME = "mapping_utility"


class MappingUtility:
    """
       Purpose             :   This module performs below operation:
                                 a. Run queries on processed mapping file.
                                 b. Prepare a mail with results
       Input               :   a. file_master_id
       Output              :   Sending a mail with results
    """

    def __init__(self):
        self.execution_context = ExecutionContext()
        self.execution_context.set_context({"module_name": MODULE_NAME})
        self.configuration = JsonConfigUtility(os.path.join(CommonConstants.AIRFLOW_CODE_PATH,
                                                            CommonConstants.ENVIRONMENT_CONFIG_FILE))
        self.spark = PySparkUtility(self.execution_context).get_spark_context(
            ["MappingUtility", CommonConstants.HADOOP_CONF_PROPERTY_DICT])
        self.email_recipient_list = self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_recipient_list"])
        bucket_path = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])
        self.qc_query_path = "{bucket_path}/uploads/qc_queries_other_than_disease_ta/Mapping_QCs_Queries.xlsx".format(
            bucket_path=bucket_path)
        self.sender_email = str(self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "delta_email_type_configurations", "ses_sender"]))
        self.client_name = str(self.configuration.get_configuration(
            [CommonConstants.ENVIRONMENT_PARAMS_KEY, "client_name"]))
        self.recipient_list = ", ".join(self.email_recipient_list)
        self.smtp_host = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_server"])
        self.smtp_port = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "smtp_port"])
        self.bucket_path = self.configuration.get_configuration([CommonConstants.ENVIRONMENT_PARAMS_KEY, "bucket_path"])

    def send_email(self, email_body_string, email_subject):
        try:
            msg = MIMEMultipart('alternative')
            msg['From'] = self.sender_email
            msg['To'] = self.recipient_list
            msg['Subject'] = email_subject
            e_body = MIMEText(email_body_string, 'html')
            msg.attach(e_body)
            server = smtplib.SMTP(host=self.smtp_host, port=self.smtp_port)
            server.connect(self.smtp_host, self.smtp_port)
            server.starttls()
            server.send_message(msg)
            server.quit()
            return True
        except:
            raise Exception(
                "Issue Encountered while sending the email to recipients --> " + str(traceback.format_exc()))

    def call_mapping_utility(self, **kwargs):
        try:
            excel_df = self.spark.read.format('com.crealytics.spark.excel').option('header', 'true').load(
                self.qc_query_path)

            mapping_name = excel_df.select('Mapping_name').rdd.map(
                lambda x: x[0]).collect()
            description = excel_df.select('Description').rdd.map(
                lambda x: x[0]).collect()
            mapping_path = excel_df.select('Mapping_path').rdd.map(
                lambda x: x[0]).collect()

            for each_row in excel_df.collect():

                if ('xlsx' in str(each_row.Mapping_path)):
                    map = self.spark.read.format('com.crealytics.spark.excel').option('header', 'true').load(
                        each_row.Mapping_path.format(bucket_path=self.bucket_path))
                else:
                    map = self.spark.read.format('csv').option('header', 'true').option('delimeter', 'true').load(
                        each_row.Mapping_path.format(bucket_path=self.bucket_path))
                map.registerTempTable('map')
                exec(each_row.Mapping_name + "= self.spark.sql(each_row.Query)")
                exec(each_row.Mapping_name + ".show()")

            msg1 = """\
                    <html>
                      <head></head>
                      <body>
                        <p>Hi Team,<br>
                        <br><br>
                        Please find below Validation Details For Mapping \n"""
            msg2 = """
                         <br><br>

                         Regards,
                         <br>
                         {client_name} DE Team
                         <br>
                         Clinical Development Excellence
                         <br>
                         <br>
                         <br>
                         <br>
                         <br>
                         </p>
                         <p style="text-align:center;">
                         This email is auto-generated. Do NOT Reply to this message</p>
                      </body>
                    </html>""".format(client_name=self.client_name)
            description_count = 1
            for each_df, each_desc in zip(mapping_name, description):
                adding_rows = "            <br><br><b>" + str(
                    description_count) + "." + each_desc + ":</b><br>\n            {" + each_df + "}\n"
                msg1 += adding_rows
                description_count = description_count + 1

            final_msg = msg1 + msg2
            cmd = "html_mesage = final_msg.format("
            cmd_count = 1
            for each_df in mapping_name:
                exec(each_df + " = " + each_df + ".toPandas()")
                if cmd_count == 1:
                    cmd += each_df + "=" + each_df + ".to_html(index=False)"
                elif cmd_count == len(mapping_name):
                    cmd += "," + each_df + "=" + each_df + ".to_html(index=False))"
                    exec(cmd)
                    whole_data = locals()
                    message = str(whole_data["html_mesage"])
                    email_subject = "Mapping validation [Sponsor, Drug, Status, Phase, Country, Age,Gender]"
                    status = self.send_email(str(message), email_subject)
                else:
                    cmd += "," + each_df + "=" + each_df + ".to_html(index=False)"
                cmd_count = cmd_count + 1
        except Exception as e:
            self.execution_context.set_context({"traceback": e})
            logger.error(e, extra=self.execution_context.get_context())


if __name__ == "__main__":
    try:
        new_obj = MappingUtility()
        new_obj.call_mapping_utility()
    except Exception as e:
        logger.error(str(e))
        raise e
