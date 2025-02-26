#! /usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "ZS Associates"

import os
import traceback
import datetime
import argparse
import shutil
import json
import requests
import boto3
from CommonUtils import CommonUtils
from ExecutionContext import ExecutionContext
from LogSetup import logger

"""
Module Name         : Cluster metric utilization utility
Purpose             : The module is used to fetch the performance stats data of the emr to a csv file and store it in
                      a S3 bucket.
Pre_requisites      :
Input               : IP address of emr, cluster-id, metric list to be fetched, graph list, time interval and S3 path.
Execution steps     : python <>
Last Changed on     : 22-11-2018
Last Changed by     : Abhishek Kurup
Reason for change   : Added exception handling and comments
"""

DEFAULT_GRAPHS = ['cpu_report', 'mem_report', 'load_report', 'network_report']
DEFAULT_METRICS = ['mem_report', 'disk_free', 'yarn.QueueMetrics.AppsSubmitted', 'yarn.QueueMetric.AvailableMB']
DEFAULT_INTERVAL = "custom"
DEFAULT_IP = "localhost"
DEFAULT_API = "&csv=1"
DEFAULT_CLUSTER_NAME = "pt_cluster_id="
DEFAULT_NODE_NAME = "pt_node_id="
DEFAULT_PROCESS_NAME = "pt_process_name="
LOCAL_FILE_STORE = "/tmp"
LOCAL_FOLDER_NAME = "ganglia_reports"
JSON_FILE_PATH_KEY = "json_file_path"
CLUSTER_ID_KEY = "cluster_id"
S3_PATH_KEY = "s3_path"
CLUSTER_IP_KEY = "cluster_ip"
METRIC_LIST_KEY = "metric_list"
GRAPH_LIST_KEY = "graph_list"
INTERVAL_KEY = "interval"
MODULE_NAME = "GangliaMetricFetchUtility"


class ClusterMetricUtilizationUtility:
    """
    Purpose: This class forms url, saves the CSVs in folder and stores it in S3
    """
    def __init__(self, execution_context=None):
        if execution_context is None:
            self.execution_context = ExecutionContext()
        else:
            self.execution_context = execution_context
        self.execution_context.set_context({"module_name": MODULE_NAME})

    def performance_data(self, cluster_id, s3_path, node_level, cluster_ip=None,
                         metric_list=None, graph_list=None):
        """
        Purpose: Fetches the provided data metric and stores it in S3 as a csv file.
        :param cluster_ip: IP address of EMR
        :param cluster_id: Cluster ID
        :param metric_list: List of metrics to be fetched
        :param graph_list: List of graph data needed for a metric
        :param node_level: Flag to fetch node level metrics
        :param s3_path: S3 path where the csv files will be stored
        :return: status
        """
        try:
            logger.info("cluster_id: "+str(cluster_id), extra=self.execution_context.get_context())
            logger.info("s3_path: "+str(s3_path), extra=self.execution_context.get_context())
            logger.info("node_level: "+str(node_level), extra=self.execution_context.get_context())
            level_flag = ""
            client = boto3.client('emr', )
            start_time = datetime.datetime.now()
            logger.info("Inside Metric Data Utility.", extra=self.execution_context.get_context())
            logger.info("Start time: " + str(start_time), extra=self.execution_context.get_context())
            if isinstance(cluster_ip, str):
                if (cluster_ip == "") or (cluster_ip is None):
                    logger.debug("The ip address is empty. Taking " + DEFAULT_IP + " as ip.",
                                 extra=self.execution_context.get_context())
                    cluster_ip = DEFAULT_IP
            else:
                raise Exception("The cluster IP should be of string type.")

            if isinstance(cluster_id, str):
                if (cluster_id is "") or (cluster_id is None):
                    logger.warning("The cluster id field is empty. This is a mandatory input.",
                                   extra=self.execution_context.get_context())
                    raise Exception("The cluster_id field cannot be None or empty.")
            else:
                raise Exception("The cluster ID should be of string type.")

            if isinstance(graph_list, list) or graph_list is None:
                if (graph_list == ['']) or (graph_list is None):
                    logger.debug("The graph list is empty. Taking the default values.",
                                 extra=self.execution_context.get_context())
                    graph_list = DEFAULT_GRAPHS
            else:
                raise Exception("The graph list should be of list type.")

            if isinstance(node_level, str) or node_level is None:
                if (node_level == '') or (node_level is None):
                    logger.debug("The node level flag is empty. Fetching cluster level results by default.",
                                 extra=self.execution_context.get_context())
                    level_flag = "N"
                else:
                    level_flag = node_level
            else:
                raise Exception("The node level should be of string type.")

            if isinstance(metric_list, list) or metric_list is None:
                if (metric_list == ['']) or (metric_list is None):
                    logger.debug("The metric list is empty. Taking the default values.",
                                 extra=self.execution_context.get_context())
                    metric_list = DEFAULT_METRICS
            else:
                raise Exception("The metric list should be of list type.")

            if isinstance(s3_path, str):
                if (s3_path is None) or (s3_path == ""):
                    logger.warning("The S3_path field is empty. This is a mandatory input.",
                                   extra=self.execution_context.get_context())
                    raise Exception("The cluster_id field cannot be None or empty.")
            else:
                raise Exception("The S3 path should be of string type.")

            logger.info("Fetching the cluster start time.", extra=self.execution_context.get_context())
            url = "http://{cluster_ip}:8088/ws/v1/cluster/info".format(cluster_ip=cluster_ip)
            cluster_info_response = requests.request("GET", url)
            start_timestamp = int(cluster_info_response.json()["clusterInfo"]["startedOn"]) / 1000
            logger.info("Cluster start timestamp: "+str(start_timestamp), extra=self.execution_context.get_context())
            now = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
            process_response = client.describe_cluster(ClusterId=cluster_id)
            process_name = process_response['Cluster']['Name']
            logger.info("level_flag: " + str(level_flag), extra=self.execution_context.get_context())
            if level_flag == "N":
                for graph in graph_list:
                    if graph is "":
                        logger.debug("The graph field is empty. Skipping it.",
                                     extra=self.execution_context.get_context())
                        continue
                    url = self.get_form_url(ip_address=cluster_ip, cluster_id=cluster_id,
                                            graph=graph, start_time=start_timestamp)
                    logger.info("Hitting the API...", extra=self.execution_context.get_context())
                    response = requests.request("GET", url)
                    if response.status_code == 200:
                        self.make_folder(csv_data=response.text, cluster_id=cluster_id,
                                         graph=graph, now=now, process_name=process_name)
                    else:
                        logger.error("Error occurred during API hit. " + str(response.text),
                                     extra=self.execution_context.get_context())
                        raise Exception("Error occurred in ganglia API")
            elif level_flag == "Y":
                node_list = []
                node_response = client.list_instances(ClusterId=cluster_id)
                for items in node_response['Instances']:
                    node_list.append(items['PrivateDnsName'])
                for nodes in node_list:
                    for graph in graph_list:
                        if graph is "":
                            logger.debug("The graph field is empty. Skipping it.",
                                         extra=self.execution_context.get_context())
                            continue
                        url = self.get_form_url(ip_address=cluster_ip, cluster_id=cluster_id,
                                                graph=graph, start_time=start_timestamp, node=nodes)
                        logger.info("Hitting the API...", extra=self.execution_context.get_context())
                        response = requests.request("GET", url)
                        if response.status_code == 200:
                            self.make_folder(csv_data=response.text, cluster_id=cluster_id, now=now,
                                             graph=graph, node=nodes, process_name=process_name)
                        else:
                            logger.error("Error occurred during API hit. " + str(response.text),
                                         extra=self.execution_context.get_context())
                            raise Exception("Error occurred in ganglia API")
            self.move_to_s3(s3_path)
            logger.info("Copied files to S3 path: " + s3_path,
                        extra=self.execution_context.get_context())
            logger.info("Now deleting the local files.",
                        extra=self.execution_context.get_context())
            shutil.rmtree(LOCAL_FILE_STORE + "/" + LOCAL_FOLDER_NAME)
            end_time = datetime.datetime.now()
            logger.info("End time: " + str(end_time), extra=self.execution_context.get_context())
            logger.info("Time taken: " + str(end_time.second - start_time.second) + "s",
                        extra=self.execution_context.get_context())
            return json.dumps({"status": "SUCCESS",
                               "result": "All the ganglia reports are generated and moved to the provided S3 path",
                               "error": ""})
        except Exception as error:
            traceback.print_exc()
            logger.error("An Exception occurred: "+str(error), extra=self.execution_context.get_context())
            return json.dumps({"status": "FAILED",
                               "result": "",
                               "error": str(error)})

    def move_to_s3(self, path):
        """
        Purpose: Sends the folder structure located at LOCAL_FILE_STORE/LOCAL_FOLDER_NAME to an S3 path.
        :param path: The S3 path where the folder needs to be stored.
        :return: The status of the operation. 1 = success and 0 = failure.
        """
        base_path = LOCAL_FILE_STORE+"/"+LOCAL_FOLDER_NAME
        for dir_name in os.listdir(base_path):
            command = \
                "aws s3 cp "+os.path.abspath(base_path)+"/"+dir_name+"/"+" "+path+"/"+dir_name+" --sse --recursive"
            CommonUtils().execute_shell_command(command)

    def make_folder(self, csv_data, cluster_id, graph, process_name, now, node=None):
        """
        Purpose: Creates the required folder structure to store the CSV files.
                 Folder structure: /cluster_id/metric_name/graph_name/file.csv
        :param node: List of node IDs present in the cluster to fetch node level data.
        :param now: Present time in timestamp format
        :param process_name: The name of the process
        :param csv_data: The csv file as a response format.
        :param cluster_id: cluster ID
        :param graph: graph name
        :return: The folder structure along with the csv file placed  in its corresponding place.
        """

        logger.debug("Inside make_folder function", extra=self.execution_context.get_context())
        if not os.path.exists(os.path.join(LOCAL_FILE_STORE, LOCAL_FOLDER_NAME)):
            logger.debug(
                "Path: " + LOCAL_FILE_STORE + "/" + LOCAL_FOLDER_NAME + " doesn't exist. Creating..",
                extra=self.execution_context.get_context())
            os.mkdir(os.path.join(LOCAL_FILE_STORE, LOCAL_FOLDER_NAME))
        base_path = LOCAL_FILE_STORE+"/"+LOCAL_FOLDER_NAME

        if node is None:
            if not os.path.exists(os.path.join(base_path, "cluster_level")):
                logger.debug(
                    "Path: " + base_path + "/" + "cluster_level" + " doesn't exist. Creating..",
                    extra=self.execution_context.get_context())
                os.mkdir(os.path.join(base_path, "cluster_level"))
            base_path = base_path + "/" + 'cluster_level'
        else:
            if not os.path.exists(os.path.join(base_path, "node_level")):
                logger.debug(
                    "Path: " + base_path + "/" + "node_level" + " doesn't exist. Creating..",
                    extra=self.execution_context.get_context())
                os.mkdir(os.path.join(base_path, "node_level"))
            base_path = base_path + "/" + 'node_level'

        if not os.path.exists(os.path.join(base_path, graph)):
            logger.debug(
                "Path: " + base_path + "/" + graph + " doesn't exist. Creating..",
                extra=self.execution_context.get_context())
            os.mkdir(os.path.join(base_path, graph))
        base_path = base_path + "/" + graph

        if not os.path.exists(os.path.join(base_path, DEFAULT_PROCESS_NAME + process_name+"_"+now)):
            logger.debug(
                "Path: " + base_path + "/" + DEFAULT_PROCESS_NAME + process_name+"_"+now +
                " doesn't exist. Creating..",
                extra=self.execution_context.get_context())
            os.mkdir(os.path.join(base_path, DEFAULT_PROCESS_NAME + process_name+"_"+now))
        base_path = base_path + "/" + DEFAULT_PROCESS_NAME + process_name+"_"+now

        if not os.path.exists(os.path.join(base_path, DEFAULT_CLUSTER_NAME + cluster_id)):
            logger.debug("Path: " + base_path + "/" + DEFAULT_CLUSTER_NAME + cluster_id +
                         " doesn't exist. Creating..", extra=self.execution_context.get_context())
            os.mkdir(os.path.join(base_path, DEFAULT_CLUSTER_NAME + cluster_id))
        base_path = base_path + "/" + DEFAULT_CLUSTER_NAME + cluster_id

        if node is not None:
            if not os.path.exists(os.path.join(base_path + "/" + DEFAULT_NODE_NAME + node)):
                logger.debug("Path: " + base_path + "/" + DEFAULT_NODE_NAME + node +
                             " doesn't exist. Creating..", extra=self.execution_context.get_context())
                os.mkdir(os.path.join(base_path + "/" + DEFAULT_NODE_NAME + node))

        if node is None:
            file_path = base_path + "/" + cluster_id + "_" + graph + ".csv"
        elif node is not None:
            file_path = base_path + "/" + DEFAULT_NODE_NAME + node + "/" + node + "_" + graph + ".csv"
        write_file = open(file_path, "w")
        write_file.write(csv_data)
        write_file.close()

    def get_form_url(self, ip_address, cluster_id, graph, start_time, node=None):
        """
        Purpose: Forms the appropriate api url to fetch the metric data.
        :param node: The ID of the node.
        :param start_time: The start time of the cluster(in timestamp).
        :param ip_address: IP address of the EMR.
        :param cluster_id: cluster ID
        :param graph: graph name
        :return: The API url to be used to fetch the data.
        """
        logger.debug("Inside form_url function", extra=self.execution_context.get_context())
        base_url = "http://"+ip_address+"/ganglia/graph.php?"
        url = base_url
        url = url + "r=" + DEFAULT_INTERVAL
        if node is not None:
            url = url + "&h=" + node
        url = url + "&cs=" + str(start_time)
        url = url + "&c=" + cluster_id
        url = url + "&g=" + graph
        url = url + DEFAULT_API
        logger.info("URL: "+url, extra=self.execution_context.get_context())
        return url


def main():
    """
    Purpose: Takes input from terminal
    :return: Prints the response of the utility.
    """
    try:
        parser = argparse.ArgumentParser(description="Python utility to move/copy file from source to destination")
        parser.add_argument("--cluster_id", help="Cluster ID")
        parser.add_argument("--node_level", choices=['Y', 'N'], help="List of node ids in the cluster")
        parser.add_argument("--s3_path", help="Path in S3 bucket to store the csv")
        parser.add_argument("--cluster_ip", help="Cluster IP")
        parser.add_argument("--metric_list", nargs='*', help="List of metrics needed.")
        parser.add_argument("--graph_list", nargs='*', help="List of graphs needed.")
        args = parser.parse_args()

        response = ClusterMetricUtilizationUtility().performance_data(args.cluster_id, args.s3_path,
                                                                      args.node_level, args.cluster_ip,
                                                                      args.metric_list, args.graph_list)
        print(response)
    except Exception as error:
        traceback.print_exc()
        logger.error("An error occurred. " + str(error),
                     extra=ClusterMetricUtilizationUtility().execution_context.get_context())


if __name__ == "__main__":
    main()
