from flask import Flask,session,redirect,render_template,url_for,request,send_from_directory,current_app
import os
from werkzeug import secure_filename
from elasticsearch import Elasticsearch
import json
from datetime import datetime
import MySQLdb
from impala.dbapi import connect
import ast
from ExecutionContext import ExecutionContext
from ConfigUtility import ConfigUtility
from schema import infer_schema
from Logsetup import logger
import traceback

app = Flask(__name__)
MODULE_NAME = "CommonComponentUI"
GLOBAL_PARAM_SECTION = "GlobalParameters"
ENVIRONMENT_CONFIG_FILE = "commoncomponent.conf"
IMPALA_CONNECTION = "ImpalaConnection"
FLASK_CONNECTION = "FlaskConnection"
MySQL_CONNECTION = "MySQLConnection"
MySQL_AUDIT_CONNECTION = "MySQL_Audit"
MYSQL_DQM_CONNECTION = "MySQL_DQM"
ES_CONNECTION = "EsConnection"
FILE_PATH = "FilePath"
process_name = "UI Display"
configuration = ConfigUtility(ENVIRONMENT_CONFIG_FILE)
execution_context = ExecutionContext()
impala_host = configuration.get_configuration(IMPALA_CONNECTION, "impala_host")
impala_port = configuration.get_configuration(IMPALA_CONNECTION, "impala_port")
service_name = configuration.get_configuration(IMPALA_CONNECTION, "service_name")
mechanism = configuration.get_configuration(IMPALA_CONNECTION, "mechanism")
flask_host = configuration.get_configuration(FLASK_CONNECTION, "flask_host")
flask_port = configuration.get_configuration(FLASK_CONNECTION, "flask_port")
mysql_host = configuration.get_configuration(MySQL_CONNECTION, "mysql_host")
es_host = configuration.get_configuration(ES_CONNECTION, "es_host")
es_port = configuration.get_configuration(ES_CONNECTION, "es_port")
es_index_name = configuration.get_configuration(ES_CONNECTION, "index_name")
es_document_type = configuration.get_configuration(ES_CONNECTION, "document_type")
file_path = configuration.get_configuration(FILE_PATH, "upload_path")
mysql_audit_username = configuration.get_configuration(MySQL_AUDIT_CONNECTION, "audit_username")
mysql_audit_database_name = configuration.get_configuration(MySQL_AUDIT_CONNECTION, "audit_database_name")
mysql_audit_password = configuration.get_configuration(MySQL_AUDIT_CONNECTION, "audit_password")
mysql_dqm_username = configuration.get_configuration(MYSQL_DQM_CONNECTION, "dqm_username")
mysql_dqm_database_name = configuration.get_configuration(MYSQL_DQM_CONNECTION, "dqm_database_name")
mysql_dqm_password = configuration.get_configuration(MYSQL_DQM_CONNECTION, "dqm_password")

@app.route('/')
def main_path():
    return redirect(url_for('init_app'))

@app.route('/app')
def validate_session(res):
    if not session.get('logged_in'):
        return render_template('login.html',message=res)
    else:
        return render_template("datacatalog.html",userName=res)

@app.route('/init/app')
def init_app():
    return render_template('login.html')

@app.route('/landing',methods=['POST'])
def login():
    if request.form['password'] == 'password' and request.form['username'] == 'admin':
        session['logged_in'] = True
    else:
        return validate_session("Wrong Credentials!!")
    return validate_session("admin")

@app.route("/logout")
def logout():
    session['logged_in'] = False
    return validate_session("logout Successfull!!")

@app.route("/registerData")
def registerData():
    return render_template("dataregistration.html")

@app.route("/dataCatalog")
def dataCatalog():
    return render_template("datacatalog.html")

@app.route('/getfile', methods=['GET','POST'])
def getfile():
    status_message = "starting function to getfile"
    execution_context.set_context_param({"function_status": 'STARTED'})
    logger.info(status_message, extra=execution_context.get_context())
    if request.method == 'POST':
    # for secure filenames. Read the documentation.
        file = request.files['file']
        filename = secure_filename(file.filename)
        # os.path.join is used so that paths work in every operating system
        file.save(os.path.join(file_path,filename))
        column_name, column_datatype = infer_schema(file_path+filename)
        column_info = dict(zip(column_name, column_datatype))
        schema={}
        for key, value in column_info.iteritems():
            schema[str(key)]=str(value)

    return json.dumps(schema)

@app.route("/submitFormData",methods=['POST'])
def saveDataForm():

    status_message = "starting function to saveDataForm"
    execution_context.set_context_param({"function_status": 'STARTED'})
    logger.info(status_message, extra=execution_context.get_context())

    dataset_provider = request.form['data'].split("&",-1)
    print (dataset_provider)
    dataset_provider_updated = [str(i).encode('utf8') for i in dataset_provider]
    keyList=[]
    valueList=[]
    for i  in dataset_provider_updated:
        key = i.split("=")[0]
        value = i.split("=")[1]
        keyList.append(key)
        valueList.append(value)

    finalJson=dict(zip(keyList, valueList))

    #making dict for mysql table insertion to be dynamic
    dataSourceDict=dict(zip(keyList, valueList))
    dataSourceDict["dataset_name"] = dataSourceDict["dataset_provider"]
    dataSourceDict.pop('dataset_tags')
    dataSourceDict.pop('dataset_hdfs_landing_table_desc')
    dataSourceDict.pop('dataset_hdfs_landing_table_tags')
    dataSourceDict.pop('dataset_hdfs_staging_table_desc')
    dataSourceDict.pop('dataset_hdfs_staging_table_tags')
    dataSourceDict.pop('dataset_hdfs_pre_landing_table_desc')
    dataSourceDict.pop('dataset_hdfs_pre_landing_table_tags')
    dataSourceDict.pop('dataset_subject_area_desc')


    #Get File Name

    print(request.form['file_name'])
    finalJson["sample_file_name"]=request.form['file_name']
    print (finalJson["sample_file_name"])

    #make datasetname same as sataset provider-need to change if needed
    finalJson["dataset_name"]=finalJson["dataset_provider"]

    #DataTag Has-a relationship
    dataset_tags = finalJson['dataset_tags']
    dataset_tags_array = dataset_tags.split(",",-1)
    finalJson['dataset_tags'] = dataset_tags_array


    #DataSet Schema as Dict for Has-A Relationship

    dataset_schema = {}
    finalJson['dataset_schema'] = dataset_schema


    finalJson['dataset_creation_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S').strip()

    #Get Schduleting Time
    dataset_dataprovider_sla = str(request.form['dataset_dataprovider_sla']).strip()
    if(finalJson['dataset_arrivals_frequency'] == 'Daily'):
        finalJson["dataset_arrival_time"]=dataset_dataprovider_sla
        finalJson["dataset_arrival_day_of_month"] = ''
        finalJson["dataset_arrival_day_of_week"] = ''

        dataSourceDict["dataset_arrival_time"] = dataset_dataprovider_sla
        dataSourceDict["dataset_arrival_day_of_month"] = ''
        dataSourceDict["dataset_arrival_day_of_week"] = ''

    elif(finalJson['dataset_arrivals_frequency'] =='Weekly'):
        spliSla=dataset_dataprovider_sla.split("|",-1)
        finalJson["dataset_arrival_time"] = spliSla[1]
        finalJson["dataset_arrival_day_of_month"] = ''
        finalJson["dataset_arrival_day_of_week"] = spliSla[0]

        dataSourceDict["dataset_arrival_time"] = spliSla[1]
        dataSourceDict["dataset_arrival_day_of_month"] = ''
        dataSourceDict["dataset_arrival_day_of_week"] = spliSla[0]
    elif(finalJson['dataset_arrivals_frequency'] =='Monthly'):
        spliSla = dataset_dataprovider_sla.split("|", -1)
        finalJson["dataset_arrival_time"] = spliSla[2]
        finalJson["dataset_arrival_day_of_month"] = spliSla[1]
        finalJson["dataset_arrival_day_of_week"] = spliSla[0]

        dataSourceDict["dataset_arrival_time"] = spliSla[2]
        dataSourceDict["dataset_arrival_day_of_month"] = spliSla[1]
        dataSourceDict["dataset_arrival_day_of_week"] = spliSla[0]
    else:
        finalJson["dataset_arrival_time"] = dataset_dataprovider_sla
        finalJson["dataset_arrival_day_of_month"] = ''
        finalJson["dataset_arrival_day_of_week"] = ''

        dataSourceDict["dataset_arrival_time"] = dataset_dataprovider_sla
        dataSourceDict["dataset_arrival_day_of_month"] = ''
        dataSourceDict["dataset_arrival_day_of_week"] = ''




    #Get Schema Information
    columns = []
    schema_info =request.form.getlist('schema_info')

    schema_info_str = [str(x) for x in schema_info]
  #  print(type(schema_info_str[0].split("],")[2].replace("[[","").replace("[","")))

    column_name = schema_info_str[0].split("],")[0].replace("[[","").replace("[","").replace("]]","").split(",",-1)
    column_datatype = schema_info_str[0].split("],")[1].replace("[[","").replace("[","").replace("]]","").split(",",-1)
    column_comment = schema_info_str[0].split("],")[2].replace("[[","").replace("[","").replace("]]","").split(",",-1)
    column_sequence = schema_info_str[0].split("],")[3].replace("[[","").replace("[","").replace("]]","").split(",",-1)
    tags_column = schema_info_str[0].split("],")[4].replace("[[", "").replace("[", "").replace("]]", "").split(",", -1)

    column_name_list=[]
    column_datatype_list=[]
    column_comment_list=[]
    column_sequence_list=[]
    tags_column_list = []

    for all in column_name:
        column_name_list.append(all.replace("\"",""))

    for all in column_datatype:
        column_datatype_list.append(all.replace("\"",""))

    for all in column_comment:
        column_comment_list.append(all.replace("\"",""))

    for all in column_sequence:
        column_sequence_list.append(all.replace("\"",""))

    for all in tags_column:
        tags_column_list.append(all.replace("\"",""))

    column_sequence_list = map(int, column_sequence_list)

    i=0
    for index in range(len(column_name)):
        column = {}
        column['column_name'] = column_name_list[index]
        column['column_datatype'] = column_datatype_list[index]
        column['column_comment'] = column_comment_list[index]
        column['column_sequence'] = str(column_sequence_list[index])
        column_tag_array = tags_column_list[index].split("|",-1)
        column['tags_column'] = column_tag_array
        columns.append(column)
        i=i+1

    dataset_schema['columns'] = columns
    finalJson['dataset_schema'] = dataset_schema

    #HDF DATA Configurable - need to put in config file

    finalJson["dataset_hdfs_pre_landing_location"]="/data/ddd/pre_landing"
    finalJson["dataset_hdfs_landing_location"]="/data/ddd/landing"
    finalJson["dataset_hdfs_pre_dqm_location"]="/data/ddd/pre_dqm"
    finalJson["dataset_hdfs_post_dqm_location"]="/data/ddd/post_dqm"
    finalJson["dataset_hdfs_std_location"]="/data/ddd/std"
    finalJson["dataset_hdfs_staging_location"]="/data/ddd/staging"
    finalJson["dataset_archive_location"]="/data/ddd/archive"
    #finalJson["dataset_hdfs_pre_landing_table"]="ddd_pre_landing"
    #finalJson["dataset_hdfs_landing_table"]="ddd_landing"
    finalJson["dataset_hdfs_pre_dqm_table"]="ddd_pre_dqm"
    finalJson["dataset_hdfs_post_dqm_table"]="ddd_post_dqm"
    finalJson["dataset_hdfs_std_table"]="ddd_std"
    #finalJson["dataset_hdfs_staging_table"]="ddd_staging"
    finalJson["dataset_file_num_of_fields"]=i

    # HDF DATA Configurable - need to put in config file(this is for mysql dic)
    dataSourceDict["dataset_hdfs_pre_landing_location"]="/data/ddd/pre_landing"
    dataSourceDict["dataset_hdfs_landing_location"]="/data/ddd/landing"
    dataSourceDict["dataset_hdfs_pre_dqm_location"]="/data/ddd/pre_dqm"
    dataSourceDict["dataset_hdfs_post_dqm_location"]="/data/ddd/post_dqm"
    dataSourceDict["dataset_hdfs_std_location"]="/data/ddd/std"
    dataSourceDict["dataset_hdfs_staging_location"]="/data/ddd/staging"
    dataSourceDict["dataset_archive_location"]="/data/ddd/archive"
    #dataSourceDict["dataset_hdfs_pre_landing_table"]="ddd_pre_landing"
    #dataSourceDict["dataset_hdfs_landing_table"]="ddd_landing"
    dataSourceDict["dataset_hdfs_pre_dqm_table"]="ddd_pre_dqm"
    dataSourceDict["dataset_hdfs_post_dqm_table"]="ddd_post_dqm"
    dataSourceDict["dataset_hdfs_std_table"]="ddd_std"
    #dataSourceDict["dataset_hdfs_staging_table"]="ddd_staging"
    dataSourceDict["dataset_file_num_of_fields"]=i


    #Set view count to 0 initial
    finalJson['dataset_view_count']=0

    finalJson["dataset_last_update_timestamp"]=''
    finalJson["dataset_last_access_timestamp"]=''
    finalJson["dataset_latest_dataload_time"]=''

    #print (finalJson)
    db = MySQLdb.connect(mysql_host, mysql_audit_username, mysql_audit_password, mysql_audit_database_name)
    db.autocommit=True
    cursor = db.cursor()

    columnsDB = ', '.join(dataSourceDict.keys())
    print (dataSourceDict.values())
    placeholders = ''

    for v in dataSourceDict.values():
        if isinstance(v,int):
            if(v==""):
                placeholders += 'null' + ","
            else:
                placeholders += str(v) + ","
        elif isinstance(v,str):
            if(v==""):
                placeholders += 'null' + ","
            else:
                placeholders += "\"" + v + '\",'

    query = 'INSERT INTO datasource_information (%s) VALUES (%s)' % (columnsDB, placeholders.rstrip(','))
    #print query
    cursor.execute(query, dataSourceDict)
    db.commit()

    dataSetId=cursor.lastrowid

    #Inset vlaues into workflow_scheduling_details Table
    if not finalJson['dataset_arrival_day_of_month']:
        dataset_arrival_day_of_month = 'null'
    else:
        dataset_arrival_day_of_month=str(finalJson['dataset_arrival_day_of_month'])

    if not finalJson['dataset_arrivals_frequency']:
        dataset_arrivals_frequency = 'null'
    else:
        dataset_arrivals_frequency=str(finalJson['dataset_arrivals_frequency'])

    if not finalJson['dataset_arrival_day_of_week']:
        dataset_arrival_day_of_week = 'null'
    else:
        dataset_arrival_day_of_week=str(finalJson['dataset_arrival_day_of_week'])

    if not finalJson['dataset_arrival_time']:
        dataset_arrival_time = 'null'
    else:
        dataset_arrival_time=str(finalJson['dataset_arrival_time'])

    values=str(dataSetId)+",'"+dataset_arrivals_frequency+"',"+dataset_arrival_day_of_month+","+dataset_arrival_day_of_week+",'"+str(dataset_arrival_time)+"'"
    print (values)
    query = 'INSERT INTO workflow_scheduling_details(dataset_id_fk,workflow_run_frequency,workflow_run_day_of_month,workflow_run_day_of_week,workflow_run_time) values(%s)' %values
    print (query)
    cursor.execute(query)
    db.commit()

    #Insert Values in table_metadata
    values = str(dataSetId) + ",'" + str(finalJson['dataset_hdfs_staging_table']) + "','" + str(finalJson['dataset_hdfs_staging_table_desc'])+"'"
    print (values)
    query = 'INSERT INTO table_metadata(datasource_id_fk,`table_name`,table_description) values(%s)' % values
    print (query)
    cursor.execute(query)
    db.commit()

    tableID = int(cursor.lastrowid)
    print (tableID)
    insertSchema=[]

    listOfTableId=[]
    handleComments=[]
    for index in range(len(column_name)):
        listOfTableId.append(tableID)
        if not column_comment_list[index]:
            commnet='null'
        else:
            commnet=column_comment_list[index]

        handleComments.append(commnet)
    tup = map(None, listOfTableId, column_name_list,column_datatype_list, handleComments, column_sequence_list)
    print (tup)


    #Insert Table Schema
    cursor.executemany("""insert into column_metadata(table_id_fk, column_name_pk, column_data_type, column_description, column_sequence_number)
      VALUES (%s, %s, %s, %s, %s)""", tup)
    print (cursor._executed)
    db.commit()

    # setting the id from mysql table as id to elastic search json
    finalJson["dataset_id"] = dataSetId
    print (finalJson)
    es = Elasticsearch([{'host': es_host, 'port': int(es_port)}])
    result = es.index(index=es_index_name, doc_type=es_document_type, id=finalJson["dataset_id"], body=finalJson)
    if result:
        return json.dumps("true")
    else:
        return json.dumps("false")


@app.route('/getCatalogData')
def getCatalogData():
    status_message = "starting function to getCatalogData"
    execution_context.set_context_param({"function_status": 'STARTED'})
    logger.info(status_message, extra=execution_context.get_context())
    try:
        es = Elasticsearch([{'host': es_host, 'port': int(es_port)}])
        res = es.search(index=es_index_name, doc_type=es_document_type, body={
            "query": {
                "match_all": {}
            },
            "size": 10,
            "sort": [
                {
                    "dataset_creation_timestamp": {
                        "order": "desc"
                    }
                }
            ]
        })
        jsonlist = []
        if res['hits']['total'] == 0:
            print('errorNo documents found')
        else:
            for doc in res['hits']['hits']:
                data = doc['_source']
                jsonlist.append(data)

        config_JSON = {"catalogData": jsonlist}
        return json.dumps(config_JSON)
    except:
        error = "ERROR in " + execution_context.get_context_param("current_module") + \
                " while running ERROR MESSAGE: " + str(traceback.format_exc())
        execution_context.set_context_param({"function_status": 'FAILED', "traceback": error})
        logger.error(status_message, extra=execution_context.get_context())
        return "false"

@app.route('/getCatalogSearchData', methods=['POST'])
def getCatalogSearchData():
    status_message = "starting function to getCatalogSearchData"
    execution_context.set_context_param({"function_status": 'STARTED'})
    logger.info(status_message, extra=execution_context.get_context())

    valueTOSearch = str(request.form['searchCriteria']).strip()
    try:
        es = Elasticsearch([{'host': es_host, 'port': es_port}])
        res = es.search(index=es_index_name, doc_type=es_document_type, body={
            "query": {
                "match":
                    {
                        "_all": valueTOSearch
                    }
            }
        })
        jsonlist = []
        if res['hits']['total'] == 0:
            print('errorNo documents found')
        else:
            for doc in res['hits']['hits']:
                data = doc['_source']
                print (type(data))
                jsonlist.append(data)

        print (json.dumps(jsonlist))

        config_JSON = {"catalogData": jsonlist}
        return json.dumps(config_JSON)

    except:
        error = "ERROR in " + execution_context.get_context_param("current_module") + \
                " while running ERROR MESSAGE: " + str(traceback.format_exc())
        execution_context.set_context_param({"function_status": 'FAILED', "traceback": error})
        logger.error(status_message, extra=execution_context.get_context())

        return "false"


@app.route('/display_dqm_config/<path:dataSetId>')
def display_dqm_config(dataSetId):
    status_message = "starting function to display_dqm_config"
    execution_context.set_context_param({"function_status": 'STARTED'})
    logger.info(status_message, extra=execution_context.get_context())
    try:
        status_message = "starting function to display dqm configs"
        execution_context.set_context_param({"function_status": 'STARTED'})
        logger.info(status_message, extra=execution_context.get_context())
        execution_context.set_context_param({"function_status": 'RUNNING'})
        conn = MySQLdb.connect(mysql_host, mysql_dqm_username, mysql_dqm_password, mysql_dqm_database_name)
        cursor = conn.cursor()
        if(dataSetId =="displayAll"):
            query = 'Select qc_id,qc_type,table_name,column_name,param,active_ind,primary_key,filter_column,criticality,create_by,create_ts,update_by,update_ts from dqm_qc_master'
        else:
            query = 'Select qc_id,qc_type,table_name,column_name,param,active_ind,primary_key,filter_column,criticality,create_by,create_ts,update_by,update_ts from dqm_qc_master where dataset_id='+dataSetId

        status_message = "creating mysql query " + query + " for displaying DQM configs"
        logger.debug(status_message, execution_context.get_context())
        final_query = query_db(query, conn)
        config_dict = {}
        config_dict['dqm_config'] = final_query
        config_JSON = json.dumps(config_dict)
        status_message = "Completing function to get DQM configs"
        execution_context.set_context_param({"function_status": 'COMPLETED'})
        logger.info(status_message, extra=execution_context.get_context())
        return config_JSON
    except:
        error = "ERROR in " + execution_context.get_context_param("current_module") + \
                " while running ERROR MESSAGE: " + str(traceback.format_exc())
        execution_context.set_context_param({"function_status": 'FAILED', "traceback": error})
        logger.error(status_message, extra=execution_context.get_context())
        return "flase"

@app.route('/display_dqm_error/<path:dataSetId>')
def display_dqm_error(dataSetId):
    status_message = "starting function to display_dqm_error"
    execution_context.set_context_param({"function_status": 'STARTED'})
    logger.info(status_message, extra=execution_context.get_context())
    try:
        conn = connect(host=impala_host, port=impala_port, auth_mechanism=mechanism,
                       kerberos_service_name=service_name)
        if (dataSetId == "displayAll"):
            query = 'Select qc_id,qc_type,table_name,column_name,param,active_ind,primary_key,filter_column,criticality,create_by,create_ts,update_by,update_ts from dqm_qc_master'
        else:
            query = 'Select qc_id,qc_type,table_name,column_name,param,active_ind,primary_key,filter_column,criticality,create_by,create_ts,update_by,update_ts from dqm_qc_master where dataset_id=' + dataSetId
        final_query = query_db(query, conn)
        error_dict = {}
        error_dict['dqm_error'] = final_query
        error_JSON = json.dumps(error_dict)
        print(error_JSON)
        return error_JSON
    except:
        error = "ERROR in " + execution_context.get_context_param("current_module") + \
                " while running ERROR MESSAGE: " + str(traceback.format_exc())
        execution_context.set_context_param({"function_status": 'FAILED', "traceback": error})
        logger.error(status_message, extra=execution_context.get_context())
        return "flase"

def query_db(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    query_result = [dict((cursor.description[i][0], value) \
                         for i, value in enumerate(row)) for row in cursor.fetchall()]
    cursor.close()
    return query_result

@app.route("/dataDqm/<path:dataSetId>")
def dataDqm(dataSetId):
    return render_template("dqmresults.html")

@app.route("/loadhistory/<path:dataSetId>")
def loadhistory(dataSetId):
    return render_template("loadhistory.html")

@app.route('/display_load_history/<path:dataSetId>')
def display_load_history(dataSetId):
    status_message = "starting function to display_load_history"
    execution_context.set_context_param({"function_status": 'STARTED'})
    logger.info(status_message, extra=execution_context.get_context())
    try:
        status_message = "starting function to display load history"
        conn = MySQLdb.connect(host=mysql_host, user=mysql_audit_username, passwd=mysql_audit_password,
                               db=mysql_audit_database_name)
        cursor = conn.cursor()
        if (dataSetId == "displayAll"):
            query = 'Select dataset_id,dataset_subject_area,data_week,batch_id,file_id,pre_landing_record_count,post_dqm_record_count,dqm_error_count,unified_record_count from audit_information.load_summary'
        else:
            query = 'Select dataset_id,dataset_subject_area,data_week,batch_id,file_id,pre_landing_record_count,post_dqm_record_count,dqm_error_count,unified_record_count from audit_information.load_summary where dataset_id=' + dataSetId
        final_query = query_db(query, conn)
        history_dict = {}
        history_dict['load_data'] = final_query
        history_JSON = json.dumps(history_dict)
        print(history_JSON)
        return history_JSON
    except:
        error = "ERROR in " + execution_context.get_context_param("current_module") + \
                " while running ERROR MESSAGE: " + str(traceback.format_exc())
        execution_context.set_context_param({"function_status": 'FAILED', "traceback": error})
        logger.error(status_message, extra=execution_context.get_context())
        return "flase"

@app.route('/getSourceFile/<path:filename>', methods=['GET', 'POST'])
def download(filename):
    return send_from_directory(directory=file_path, filename=filename)


if __name__ == "__main__":
    app.secret_key = os.urandom(12)
    app.run(debug=True, host=flask_host, port=int(flask_port))