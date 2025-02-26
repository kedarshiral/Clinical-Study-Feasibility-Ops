import json
import logging as logger
import traceback
import argparse
MODULE_NAME = "Modify config file"
CONFIG_FILE_PATH = '/home/svc_etl/sample/config.json'


class ModifyConfig:
    def modify_cluster_id(self, path, operation, cluster_ids):
        """
        Purpose: To add/remove the cluster IDs to/from the config json.
        :param path: Path to config json file.
        :param operation: To add or remove.
        :param cluster_ids: The list of cluster IDs to be added/removed.
        :return: Json response providing status of the process.
        """
        try:
            logger.info("Inside modify_cluster_id function")
            result = ""
            if (path is not None) or (path != "") or (isinstance(path, str)):
                if operation == "add" or operation == "remove":
                    try:
                        logger.info("Reading from the config file from " + path + " path")
                        json_file = open(path, 'r')
                        config_file = json.loads(str(json_file.read()))
                    except Exception as e:
                        logger.error("An error occurred: " + str(e))
                        traceback.print_exc()
                        raise Exception("Config file must be in a valid JSON format!")
                    finally:
                        json_file.close()
                    if type(config_file) == dict:
                        if "ClusterId" in config_file['Exclude']:
                            cluster_list = config_file['Exclude']['ClusterId']
                            if (cluster_ids is not None) and (len(cluster_ids) > 0):
                                for items in cluster_ids:
                                    if items in cluster_list:
                                        if operation == "remove":
                                            cluster_list.remove(items)
                                            logger.info("The cluster "+items+" is removed from the list.")
                                        else:
                                            logger.debug("The "+items+" cluster is already present in the list.")
                                            continue
                                    elif operation == "add":
                                        cluster_list.append(items)
                                        logger.info("The cluster " + items + " is added to the list.")
                                    elif operation == "remove":
                                        raise Exception("The " + items + " cluster is not present in the list.")
                            else:
                                raise Exception("The cluster_ids key cannot be None or empty.")
                            config_file['Exclude']['ClusterId'] = cluster_list
                        else:
                            raise Exception("clusterId key missing from config json file.")
                    else:
                        raise Exception("Could not parse the config as json.")
                    try:
                        json_file = open(path, 'w')
                        json_file.write(json.dumps(config_file))
                    except Exception as e:
                        logger.error("An error occurred: " + str(e))
                        traceback.print_exc()
                        raise Exception("Config file must be in a valid JSON format!")
                    finally:
                        json_file.close()
                else:
                    raise Exception("The operation key must have 'add' or 'remove' as a value and must not be None.")
            else:
                raise Exception("The path argument is not valid.")
            if operation == "add":
                result = "All the given cluster IDs are added."
            elif operation == "remove":
                result = "All the given cluster IDs are removed."
            return json.dumps({"status": "SUCCESS",
                               "result": result,
                               "error": ""})
        except Exception as e:
            traceback.print_exc()
            logger.error("An error occurred. "+str(e))
            return json.dumps({"status": "FAILED",
                               "result": "",
                               "error": str(e)})
        finally:
            json_file.close()

    def modify_tags(self, path, operation, tag_type, tags):
        """
        Purpose: TO add/remove tag definitions in include/exclude portion of config json.
        :param path: Path to config json file.
        :param operation: To add or remove.
        :param tag_type: Include or exclude.
        :param tags: List of tag definition to be added/removed
        :return: Json response providing the status of the process.
        """
        try:
            logger.info("Inside modify_tags function")
            result = ""
            if (path is not None) or (path != "") or (isinstance(path, str)):
                if operation == "add" or operation == "remove":
                    if tag_type == "include" or tag_type == "exclude":
                        try:
                            json_file = open(path, 'r')
                            config_file = json.loads(str(json_file.read()))
                        except Exception as e:
                            logger.error("An error occurred: " + str(e))
                            traceback.print_exc()
                            raise Exception("Config file must be in a valid JSON format!")
                        finally:
                            json_file.close()
                        if tag_type == "include":
                            if "Include" in config_file:
                                result = "The include part of the config json is modified with the provided tags."
                                if (tags is not None) and (isinstance(tags, list)) and (len(tags) > 0):
                                    for item in tags:
                                        check_flag = False
                                        for element in config_file['Include']:
                                            if element == item:
                                                check_flag = True
                                        if check_flag and operation == "add":
                                            logger.info("The " + str(item) + " definition is already present.")
                                        elif check_flag and operation == "remove":
                                            config_file['Include'].remove(item)
                                            logger.info("The " + str(item) + " definition is removed.")
                                        elif not check_flag and operation == "add":
                                            config_file['Include'].append(item)
                                            logger.info("The " + str(item) + " definition is added.")
                                        elif not check_flag and operation == "remove":
                                            logger.debug("The " + str(item) + " definition is not present in the file.")
                                else:
                                    raise Exception("tags key must be a dictionary with values(not empty).")
                            else:
                                raise Exception("There is no include key in config file.")
                        elif tag_type == "exclude":
                            result = "The exclude part of the config json is modified with the provided tags."
                            if "Tags" in config_file['Exclude']:
                                if (tags is not None) and (isinstance(tags, list)) and (len(tags) > 0):
                                    for item in tags:
                                        check_flag = False
                                        for element in config_file['Exclude']['Tags']:
                                            if element == item:
                                                check_flag = True
                                        if check_flag and operation == "remove":
                                            config_file['Exclude']['Tags'].remove(item)
                                            logger.info("The "+str(item)+" is removed from the configs.")
                                        elif check_flag and operation == "add":
                                            logger.debug("The " + str(item) + " definition is already present in the file.")
                                        elif not check_flag and operation == "remove":
                                            logger.debug("The " + str(item) + " definition is not present in the file.")
                                        elif not check_flag and operation == "add":
                                            config_file['Exclude']['Tags'].append(item)
                                            logger.info("The " + str(item) + " definition is added.")
                                else:
                                    raise Exception("tags key must be a dictionary with values(not empty).")
                            else:
                                raise Exception("There is some issue in Exclude part of the config file.")
                        try:
                            json_file = open(path, 'w')
                            json_file.write(json.dumps(config_file))
                            json_file.close()
                        except Exception as e:
                            logger.error("An error occurred: " + str(e))
                            traceback.print_exc()
                            raise Exception("Config file must be in a valid JSON format!")
                    else:
                        raise Exception("The tag_type key must have 'include' or 'exclude' as a value and must not be None.")
                else:
                    raise Exception("The operation key must have 'add' or 'remove' as a value and must not be None.")
            else:
                raise Exception("The path argument is not valid.")
            return json.dumps({"status": "SUCCESS",
                               "result": result,
                               "error": ""})
        except Exception as e:
            traceback.print_exc()
            logger.error("An error occurred. "+str(e))
            return json.dumps({"status": "FAILED",
                               "result": "",
                               "error": str(e)})
        finally:
            json_file.close()

    def fetch_tags(self, path, tag_type, tag_pair):
        """
        Purpose: To search a tag definition in the existing definitions.
        :param path: Path to the config json
        :param tag_type: Include or exclude
        :param tag_pair: The tag definition to be searched
        :return: The tag definition which contains the provided tags.
        """
        try:
            logger.info("Inside fetch_tags function")
            if (path is not None) or (path != "") or (isinstance(path, str)):
                if tag_type == "include" or tag_type == "exclude":
                    try:
                        json_file = open(path, 'r')
                        config_file = json.loads(str(json_file.read()))
                    except Exception as e:
                        logger.error("An error occurred: " + str(e))
                        traceback.print_exc()
                        raise Exception("Config file must be in a valid JSON format!")
                    tag_list = []
                    if tag_type == "include":
                        if "Include" in config_file:
                            tag_list = config_file['Include']
                        else:
                            raise Exception("The config file has no Include key in it.")
                    elif tag_type == "exclude":
                        if "Tags" in config_file['Exclude']:
                            tag_list = config_file['Exclude']['Tags']
                        else:
                            raise Exception("There is some issue in Exclude part of config json")
                    print("The search result is as follows:")
                    if (tag_pair is not None) and (isinstance(tag_pair, dict)) and bool(tag_pair):
                        for items in tag_list:
                            if list(tag_pair.items()) <= list(items.items()):
                                print((json.dumps(items)))
                    else:
                        raise Exception("tag_pair key must be a dictionary with values(not empty).")
                else:
                    raise Exception("The operation_type key must have 'include' or 'exclude' as a value and must not be None.")
            else:
                raise Exception("The path argument is not valid.")
        except Exception as e:
            traceback.print_exc()
            logger.error("An error occurred. "+str(e))
            return json.dumps({"status": "FAILED",
                               "result": "",
                               "error": str(e)})
        finally:
            json_file.close()


def main():
    try:
        parser = argparse.ArgumentParser(description="Python utility to modify the json config file of EMR termination utility.")
        parser.add_argument("--type", choices=['include', 'exclude'],
                            help="Mention the portion(include/exclude) of config the values need to be added.",
                            required=True)
        parser.add_argument("--operation", choices=['add', 'remove', 'fetch'], required=True,
                            help="Mention the operation(add/remove/fetch) that needs to be done.")
        parser.add_argument("--input_type", choices=['tags', 'ids'], required=True,
                            help="Mention the type of input(Tags/IDs) in small case.")
        parser.add_argument("--values", required=True,
                            help="Mention the values to update the config file.")
        parser.add_argument("--path", required=True,
                            help="The path to config file.")
        args = parser.parse_args()

        list = []
        values = args.values.split(';')
        if args.operation == "fetch":
            x = ModifyConfig().fetch_tags(path=args.path, tag_type=args.type,
                                          tag_pair=json.loads(args.values))
        else:
            if args.input_type == "tags":
                for item in values:
                    list.append(json.loads(item))
                x = ModifyConfig().modify_tags(path=args.path,
                                               operation=args.operation,
                                               tag_type=args.type,
                                               tags=list)
            elif args.input_type == "ids":
                x = ModifyConfig().modify_cluster_id(path=args.path,
                                                     operation=args.operation,
                                                     cluster_ids=values)
        print(x)
    except Exception as e:
        traceback.print_exc()
        print(e)


if __name__ == "__main__":
    main()

# python modify.py --path /appdata/dev/common_components/nntestbranch/sample/config.json --operation add --type include --input_type tags --values "{"ZS_Project_Code":"9902ZS3747","abc":"xyz"}"
