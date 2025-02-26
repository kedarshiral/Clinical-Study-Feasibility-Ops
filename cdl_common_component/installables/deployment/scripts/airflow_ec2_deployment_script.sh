#!/bin/bash
set -e
# Script to clone the git repository and deploy the code in application folders

trap cleanup 0

cleanup()
{
    printf "%s\n--------------------------------------------------------"
    printf "%s\nPerforming post deployment cleanup\n"

    ls -1 $WORKING_DIR/cdl_common_component/configurations/environment_configurations/*/environment_params.json > /dev/null 2>&1; if [ "$?" = "0" ]; then rm $WORKING_DIR/cdl_common_component/configurations/environment_configurations/*/environment_params.json; fi
    ls -1 $WORKING_DIR/code_set/environment_params.json > /dev/null 2>&1; if [ "$?" = "0" ]; then rm $WORKING_DIR/code_set/environment_params.json; fi

    printf "%s\nDone cleanup. Quitting code\n"
    printf "%s\n--------------------------------------------------------\n"
    exit 0
}


printf "%s\n--------------------------------------------------------"
printf "%s\n$(date):$(hostname) - Starting code deployment for Common Components code"
printf "%s\n--------------------------------------------------------"
printf "%s\n *** General Guidelines and Pre-requisites ***"
printf "%s\n  1. Internal Folder Structure for Common Components Application"
printf "%s\n     Please ensure this folder structure is created on your machine before code deployment"
printf "%s\n     APPLICATION DIRECTORY ==> The Parent Root folder for Common Components code. Example: /appdata/dev/common_components"
printf "%s\n     Child Directories ==> "
printf "%s\n     {APPLICATION_DIRECTORY}/code "
printf "%s\n     {APPLICATION_DIRECTORY}/configs "
printf "%s\n--------------------------------------------------------"
printf "%s\n  2. Creation of environment_params.json according to your environment"
printf "%s\n--------------------------------------------------------"

printf "$s\nPlease enter the full path for the WORKING Directory (THIS WILL BE CLEANED BY THE SCRIPT EVERY TIME): "
read WORKING_DIR
printf "$s\nPlease enter the full path for the APPLICATION Directory (THIS WILL BE CLEANED BY THE SCRIPT EVERY TIME AND THE CODES WILL BE DEPLOYED DIRECTLY WITHIN THIS DIRECTORY): "
read APPLICATION_DIR
printf "%s\nCleaning the Working directory - $WORKING_DIR if exists and creating it again"
if [ -d "$WORKING_DIR" ]
    then rm -rf $WORKING_DIR
fi
mkdir $WORKING_DIR
printf "%s\nNavigating into WORKING DIRECTORY for further steps: $WORKING_DIR"
cd $WORKING_DIR
printf "%s\n--------------------------------------------------------"
printf "$s\nCloning Bitbucket repository"
printf "%s\n--------------------------------------------------------"
printf "$s\nPlease enter your Bitbucket username: "
read GIT_USER
printf "$s\nPlease enter the branch or TAG name: "
read GIT_BRANCH
git clone -b "$GIT_BRANCH" https://"$GIT_USER"@bitbucket.org/zs_businesstech/cdl_common_component.git
printf "%s\n--------------------------------------------------------"
printf "$s\nStarting code to extract and prepare the code "
printf "%s\n--------------------------------------------------------"
printf "$s\nPlease enter the environment for which the configurations are to be copied: "
read ENV
if [ ! -d $APPLICATION_DIR/$ENV/configs ]; then
mkdir $APPLICATION_DIR/$ENV/configs/job_executor
fi
if [ -d $APPLICATION_DIR/$$ENV/configs/job_executor ]; then
printf "$s\n Job Executor Configs folder exists, Have you configured the latest configs/rules? (Y/N): "
read RESPONSE
else
printf "$s\n Job Executor Configs folder is empty, Do you still want to continue? (Y/N): "
read RESPONSE
fi
if [ "$RESPONSE" == "N" ]; then
printf "$s\nExiting deployment"
exit 0
else
    printf "$s\nHave you configured the latest 'environment_params.json' at $APPLICATION_DIR/$ENV/code? (Y/N): "
    read ENV_JSON_RESPONSE
    if [ "$ENV_JSON_RESPONSE" == "N" ]
        then
        printf "$s\nPlease configure the environment JSON before deployment"
        exit 0
        else
        printf "$s\nValidating the 'environment_params.json' at $APPLICATION_DIR/$ENV/code "
        if [ ! -f $APPLICATION_DIR/$ENV/code/environment_params.json ]
            then
            printf "$s\nFile $APPLICATION_DIR/$ENV/code/environment_params.json does not exist. Please add the file and rerun this script"
            exit 1
            else
            printf "$s\nHave you configured the latest 'CommonConstants.py' at $APPLICATION_DIR/$ENV/code? (Y/N): "
            read CONSTS_RESPONSE
            if [ "$CONSTS_RESPONSE" == "N" ]
                then
                printf "$s\nPlease configure the constants before deployment"
                exit 0
                else
                printf "$s\nValidating the 'CommonConstants.py' at $APPLICATION_DIR/$ENV/code "
                if [ ! -f $APPLICATION_DIR/$ENV/code/CommonConstants.py ]
                    then
                    printf "$s\nFile $APPLICATION_DIR/$ENV/code/CommonConstants.py does not exist. Please add the file and rerun this script"
                    exit 1
                    else
                    printf "$s\nDo you want to upload the latest EMR Bootstrap script on S3? (Y/N)"
                    read EMR_BOOTSTRAP_RESPONSE
                    if [ "$EMR_BOOTSTRAP_RESPONSE" == "Y" ]
                        then
                        printf "$s\nPlease enter S3 path where Bootstrap script file is to be placed (e.g.s3://<bucket-name>/<path>/: "
                        read EMR_BOOTSTRAP_S3_PATH
                        aws s3 cp $WORKING_DIR/cdl_common_component/installables/emr/python3_emr_bootstrap.sh $EMR_BOOTSTRAP_S3_PATH
                    fi
                    printf "$s\nDo you want to upload the Install Packages.py script on S3 (Needed only for first time setup) (Y/N): "
                    read INSTALL_PKG_RESPONSE
                    if [ "$INSTALL_PKG_RESPONSE" == "Y" ]
                        then
                        printf "$s\nPlease enter S3 path where Install Packages.py script file is to be placed (e.g.s3://<bucket-name>/<path>/: "
                        read INSTALL_PKG_S3_PATH
                        aws s3 cp $WORKING_DIR/cdl_common_component/installables/emr/install_packages.py $INSTALL_PKG_S3_PATH
                    fi
                    printf "$s\nDo you want to upload the S3 Distcp JAR on S3 (Needed only for first time setup) (Y/N): "
                    read S3_DISTCP_RESPONSE
                    if [ "$S3_DISTCP_RESPONSE" == "Y" ]
                        then
                        printf "$s\nPlease enter S3 path where S3 Distcp JAR is to be placed (e.g.s3://<bucket-name>/<path>/: "
                        read S3_DISTCP_S3_PATH
                        aws s3 cp $WORKING_DIR/cdl_common_component/installables/emr/s3-dist-cp.tar.gz $S3_DISTCP_S3_PATH
                    fi
                    printf "$s\nDo you want to upload the Spark.zip package on S3 (Needed only for first time setup) (Y/N): "
                    read SPARK_ZIP_RESPONSE
                    if [ "$SPARK_ZIP_RESPONSE" == "Y" ]
                        then
                        printf "$s\nPlease enter S3 path where Spark.zip package is to be placed (e.g.s3://<bucket-name>/<path>/: "
                        read SPARK_ZIP_S3_PATH
                        aws s3 cp $WORKING_DIR/cdl_common_component/installables/emr/spark.zip $SPARK_ZIP_S3_PATH
                        # AWS COMMAND
                    fi

                    printf "$s\nRemoving the reference environment configuration JSON downloaded from the repository"
                    rm $WORKING_DIR/cdl_common_component/configurations/environment_configurations/*/environment_params.json
#                    rm $WORKING_DIR/cdl_common_component/utilities/common_utilities/code/CommonConstants.py
                    printf "$s\nCreating temporary folder to consolidate the code files to be deployed: $WORKING_DIR/code_set "
                    mkdir $WORKING_DIR/code_set
                    find . -type f -regextype posix-egrep -regex ".*/(standardization|utilities|$ENV|airflow_automation|handlers|engines|common_configurations)/.+" -exec cp '{}' $WORKING_DIR/code_set \;
                    printf "$s\nCompiled the code set at location: $WORKING_DIR/code_set"

                    mkdir -p $APPLICATION_DIR/$ENV/code/logging

                    printf "%s\nRemoving codes and configs in Application directory - $APPLICATION_DIR if exists and creating it again\n"

                    # Add step to take backup of the existing code folder in Application directory

                    shopt -s extglob

                    if [ -d "$APPLICATION_DIR/$ENV/code" ]
                                            echo "inside if"
                        cd $APPLICATION_DIR/$ENV/code
                        then
                        find . -type f -not -name "environment_params.json" -not -name "CommonConstants.py" -print0 | xargs -0  -I {} rm {}
                        cd $WORKING_DIR/cdl_common_component
                    fi

                    printf "$s\nCopying the updated code set to the application folder: $APPLICATION_DIR/$ENV/code \n"
                    cp $WORKING_DIR/code_set/* $APPLICATION_DIR/$ENV/code
                                        cp $WORKING_DIR/code_set/__init__.py $APPLICATION_DIR/$ENV/code/logging/
                                        echo $?
                                        cp $WORKING_DIR/code_set/handlers.py $APPLICATION_DIR/$ENV/code/logging/
                                        echo $?
                                        cp $WORKING_DIR/code_set/config.py  $APPLICATION_DIR/$ENV/code/logging/

                                        cd $WORKING_DIR
                                        printf "$s\nCopying the CommonConstants to the application folder: $APPLICATION_DIR/code \n"
                                        rm -rf $APPLICATION_DIR/code/CommonConstants.py
                                        cp -r $WORKING_DIR/cdl_common_component/configurations/environment_configurations/$ENVIRONEMNT/CommonConstants.py $APPLICATION_DIR/code
                                        cp -r $WORKING_DIR/cdl_common_component/utilities/redshift_utilities/configs/$ENVIRONEMNT/redshiftUtilityInputConfig.json $APPLICATION_DIR/code
                                        mkdir -p $APPLICATION_DIR/$ENV/code/adaptor
                                        cp -r $WORKING_DIR/cdl_common_component/adaptor/code/* $APPLICATION_DIR/$ENV/code/adaptor/
                                        mkdir -p $APPLICATION_DIR/$ENV/code/adaptor/common_utilities
                                        cp -r $WORKING_DIR/cdl_common_component/adaptor/utilities/* $APPLICATION_DIR/$ENV/code/adaptor/common_utilities
                                        cp -r $WORKING_DIR/cdl_common_component/adaptor/configs/$ENV/cluster_variables.json $APPLICATION_DIR/$ENV/code/adaptor/common_utilities
                                        cp -r $WORKING_DIR/cdl_common_component/adaptor/configs/$ENV/RunAdapter.sh $APPLICATION_DIR/$ENV/code/adaptor/common_utilities
                                        echo $APPLICATION_DIR/$ENV/code/adaptor/**/ | xargs -n 1 cp $WORKING_DIR/cdl_common_component/adaptor/configs/$ENV/application_config.json
                                        printf "$s\nPlease enter the branch or TAG name for dw_scripts(master): "
                                        read GIT_BRANCH
                                        git clone -b "$GIT_BRANCH" https://"$GIT_USER"@bitbucket.org/zs_businesstech/cdl_de_ctfo.git
                                        cp -r $WORKING_DIR/cdl_de_ctfo/dw_scripts/**/* $APPLICATION_DIR/$ENV/configs/job_executor/
                                        git clone -b "$GIT_BRANCH" https://"$GIT_USER"@bitbucket.org/zs_businesstech/cdl_data_management.git
                                        cp -r $WORKING_DIR/cdl_data_management/dw_scripts/**/* $APPLICATION_DIR/$ENV/configs/job_executor/
                                                                                cp $WORKING_DIR/cdl_data_management/standardization/* $APPLICATION_DIR/$ENV/code

                    printf "$s\nDo you want to create TAR file for the code set? (Y/N) \n"
                    read TAR_RESPONSE

                    if [ "$TAR_RESPONSE" == "Y" ]
                        then
                        printf "$s\nCreating TAR file with name code.tar.gz at location: $WORKING_DIR/\n"
                        cd $APPLICATION_DIR/$ENV/
                        tar -czvf $WORKING_DIR/code.tar.gz ./code ./configs
                        cd $WORKING_DIR

                        printf "$s\nDo you want to upload the TAR file to S3? (Y/N)"
                        read S3_UPLOAD_RESPONSE
                        if [ "$S3_UPLOAD_RESPONSE" == "Y" ]
                            then
                            printf "$s\nPlease enter the S3 path where TAR file is to be placed (e.g.s3://<bucket-name>/<path>/: "
                            read S3_PATH

                            printf "$s\nUploading the TAR file to S3 : "
                            aws s3 cp $WORKING_DIR/code.tar.gz $S3_PATH
                        fi
                        printf "$s\nDeployment Completed!"
                    fi
                fi
            fi
        fi
    fi
fi






# End my code
