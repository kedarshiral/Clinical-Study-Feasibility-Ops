"""Module for getting EMR Host"""
import logging
import time
import boto3

class Get_EMR_Host_IP:
    """Class to get EMR Host IP"""
    def get_ips(self, cluster_id, region):
        """Method to get IPs"""
        ip_list = []
        try:
            client = boto3.client(
                "emr",
                region_name=region
            )
            response = client.list_instances(
                ClusterId=cluster_id,
            )
            length = len(response['Instances'])
            for i in range(length):
                ip_list.append(response['Instances'][i]['PrivateIpAddress'])
            return ip_list

        except Exception as error:
            logging.error(str(traceback.format_exc()))
            raise error

    def get_running_instance_ips(self, cluster_id, region, instance_group_types_list):
        """Method to get running instance IPs"""
        ip_list = []
        retries = 1
        retry = True
        while retry is True and retries < 4:
            try:
                logging.info("Preparing boto client")
                client = boto3.client(
                    "emr",
                    region_name=region
                )
                logging.info("Triggering API call to list EMR instances")
                response = client.list_instances(
                    ClusterId=cluster_id,
                    InstanceGroupTypes=instance_group_types_list,
                    InstanceStates=['RUNNING'],
                )
                retry = False
                length = len(response['Instances'])
                logging.info("Response recieved: %s ", str(response))
                for i in range(length):
                    ip_list.append(response['Instances'][i]['PrivateIpAddress'])
                logging.info("Final IP list returned: %s ", str(ip_list))
                return ip_list

            except Exception as error:
                logging.info("Call to AWS API to list EMR instances failed due to : %s ",
                             str(error))
                if retries >= 4:
                    logging.info("Number of attempts: %s has reached the configured limit",
                                 str(retries))

                    raise error
                else:
                    retry = True
                    logging.info("Retrying API call. No of attempts so far: %s", str(retries))
                    time.sleep(2 ^ retries/100)
                    retries += 1
