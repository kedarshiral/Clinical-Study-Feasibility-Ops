# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

from ConfigUtility import JsonConfigUtility
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
from XMLBeautify import prettify
import os
import sys
import CommonConstants as CommonConstants
import hadoopy

input_workflow_json = sys.argv[1]

if os.path.exists(input_workflow_json):

    workflow_steps = JsonConfigUtility(conf_file=input_workflow_json)
    workflow_steps_conf = workflow_steps.get_conf()
    workflow_process_steps = workflow_steps_conf.get("process_steps")
    number_of_process_steps = len(workflow_process_steps)
else:
    # logger.error("Input json file does not exists.. Exiting")
    print("Input Workflow Json(%s) does not exists.. Exiting" % (input_workflow_json))
    sys.exit()

# workflow_process = ({0:"convert to parquet and copy to landing", 1:"copy to prelanding", 2:"validate files on prelanding", 3:"post DQM standardization", 4:"Pre DQM satndardization", 5:"Unification", 6:"DQM"})
workflow_process = dict()
for current_process in workflow_process_steps:
    workflow_process[current_process.get('execution_sequence')] = current_process

ingestion_steps = []
for ordered_sequence in sorted(workflow_process.keys()):
    ingestion_steps.append(workflow_process.get(ordered_sequence))

first_step = ingestion_steps[0]

first_step_name = first_step.get('function_name')


wf = Element("workflow-app")
wf.set("name", "wf_" + str(workflow_steps_conf.get("dataset_id")))
wf.set("xmlns", "uri:oozie:workflow:0.5")

# Counter for ingestion steps

############################################################################################
# SETTING UP <start>,  <kill> tags of workflow.xml                                         #
############################################################################################
start = SubElement(wf, "start")
start.set("to", ingestion_steps[0].get("function_name"))

kill = SubElement(wf, "kill")
kill.set("name", "Kill")
kill_message = SubElement(kill, "message")
kill_message.text = "Action Failed. error message [${wf:errorMessage(wf.lastErrorNode())}]"

utility_file_list = ['FileUnzipUtility.py', 'HiveUtility.py', 'ExecutionContext.py', 'EdlConstants.py',
                     'ConfigUtility.py', 'MoveCopyUtility.py', 'data_standardisation.py']

file_list = hadoopy.ls(CommonConstants.SERVICE_CODE_HDFS_PATH)

############################################################################################
# CREATING <action> tag for each workflow_ingestion_step                                   #
############################################################################################
for count in range(0, len(ingestion_steps)):
    # previous_process = None
    current_process = ingestion_steps[count]
    if count < len(ingestion_steps) - 1:
        next_process = ingestion_steps[count + 1]

    if count > 0:
        previous_process = ingestion_steps[count - 1]
        # print previous_process

    action = SubElement(wf, "action")
    action.set("name", current_process.get("function_name"))

    ####################################################################################
    # CREATING SHELL Action as all utilities are python utilities                      #
    ####################################################################################
    shell_action = SubElement(action, "shell")
    shell_action.set("xmlns", "uri:oozie:shell-action:0.1")  ##WHY 0.1???
    jobtracker = SubElement(shell_action, "job-tracker")
    jobtracker.text = "${jobTracker}"
    namenode = SubElement(shell_action, "name-node")
    namenode.text = "${nameNode}"
    ex = SubElement(shell_action, "exec")
    ex.text = str(CommonConstants.SERVICE_CODE_HDFS_PATH).__add__(
        current_process.get('python_utility'))
    arg = SubElement(shell_action, "argument")
    arg.text = str(workflow_steps_conf.get("dataset_id"))
    if count > 0:
        # function_name = previous_process.get('function_name')
        # print '*****************"'+ function_name
        arg = SubElement(shell_action, "argument")
        arg.text = "${wf:actionData('" + previous_process.get('function_name') + "')['batch_id']}"

    ####################################################################################
    # ADDING python utility in <file> tag to make it available for Oozie execution.    #
    ####################################################################################
    for utility_file in file_list:
        file = SubElement(shell_action, 'file')
        file.text = utility_file

    ####################################################################################
    # CAPTURING SHELL action output                                                    #
    ####################################################################################
    capture_output = SubElement(shell_action, "capture-output")

    ####################################################################################
    # DETERMINING next action                                                           #
    ####################################################################################
    ok = SubElement(action, "ok")
    if count == number_of_process_steps - 1:
        ok.set("to", "End")
    else:
        ok.set("to", next_process.get("function_name"))

    ####################################################################################
    # TRIGERRING kill in case of action failure                                        #
    ####################################################################################
    error = SubElement(action, "error")
    error.set("to", "Kill")


############################################################################################
# CREATING <end> tag for the workflow                                                      #
############################################################################################
end = SubElement(wf, "end")
end.set("name", "End")

############################################################################################
# CONVERTING into XML form                                                                 #
############################################################################################
xml_string = prettify(wf)
wf_xml = xml_string[xml_string.find("?>") + 2:]
# logger.info("Generated Oozie xml is as below")
# logger.info(wf_xml)
print(wf_xml)

