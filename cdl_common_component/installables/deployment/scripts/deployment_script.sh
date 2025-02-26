#!/bin/bash

#This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package and available on bit bucket code repository ( https://sourcecode.jnj.com/projects/ASX-NCKV/repos/jrd_fido_cc/browse ) & all static servers.

set -e
# Script to clone the git repository and deploy the code in application folders

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


if [ -z $WORKING_DIR ]
then
        printf "$s\nPlease enter the full path for the WORKING Directory (THIS WILL BE CLEANED BY THE SCRIPT EVERY TIME): "
        read WORKING_DIR
fi
echo "working directory - "$WORKING_DIR

if [ -z $APPLICATION_DIR ]
then
        printf "$s\nPlease enter the full path for the APPLICATION Directory (THIS WILL BE CLEANED BY THE SCRIPT EVERY TIME AND THE CODES WILL BE DEPLOYED DIRECTLY WITHIN THIS DIRECTORY): "
        read APPLICATION_DIR
fi
echo "application directory - "$APPLICATION_DIR

printf "%s\nCleaning the Working directory - $WORKING_DIR if exists and creating it again"
if [ -d "$WORKING_DIR" ]
    then rm -rf $WORKING_DIR
fi
mkdir -p $WORKING_DIR
printf "%s\nNavigating into WORKING DIRECTORY for further steps: $WORKING_DIR"
cd $WORKING_DIR

printf "\nDeploying code"

printf "%s\n--------------------------------------------------------"
printf "$s\nCloning Bitbucket repository"
printf "%s\n--------------------------------------------------------"
if [ -z $GIT_USER ]
then
        printf "$s\nPlease enter your Bitbucket username: "
        read GIT_USER
fi

if [ -z $GIT_PASSWORD ]
then
        printf "$s\nPlease enter your Bitbucket password: "
        read -s GIT_PASSWORD
fi


if [ -z $CC_GIT_BRANCH ]
then
        printf "$s\nPlease enter the branch or TAG name: "
        read CC_GIT_BRANCH
fi
echo "GIT branch - "$CC_GIT_BRANCH

printf "$s\nCloning Bitbucket repository"
printf "%s\n--------------------------------------------------------"
cd /tmp
if [ -d "/tmp/cdl_common_component" ]
        then rm -rf /tmp/cdl_common_component
fi
cmd='git clone -b '"$GIT_BRANCH"' https://'"$GIT_USER"':"$GIT_PASSWORD"@bitbucket.org/zs_businesstech/cdl_common_component.git'
echo "Executing below command to clone the git repository"
echo $cmd
git clone -b "$CC_GIT_BRANCH" https://"$GIT_USER":"$GIT_PASSWORD"@bitbucket.org/zs_businesstech/cdl_common_component.git 2> /dev/null

printf "%s\n--------------------------------------------------------"
printf "$s\nStarting code to extract and prepare the code "
printf "%s\n--------------------------------------------------------"


if [ -z $ENVIRONMENT ]
then
        printf "$s\nPlease enter the environment for which the configurations are to be copied: "
        read ENVIRONMENT
fi
echo "Environment - "$ENVIRONMENT
cp -r /tmp/cdl_common_component/ $WORKING_DIR
cd $WORKING_DIR

if [ -z $EMR_FLAG ]
then
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
                             aws s3 cp $WORKING_DIR/cdl_common_component/installables/emr/secondstage.sh $INSTALL_PKG_S3_PATH
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
fi



printf "$s\nCreating temporary folder to consolidate the code files to be deployed: $WORKING_DIR/code_set "
mkdir $WORKING_DIR/code_set

find . -type f -regextype posix-egrep -regex ".*/(utilities|$ENVIRONMENT|airflow_automation|handlers|engines|common_configurations)/.+" -exec cp '{}' $WORKING_DIR/code_set \;
printf "$s\nCompiled the code set at location: $WORKING_DIR/code_set"

mkdir -p $APPLICATION_DIR/code/logging

printf "%s\nRemoving codes and configs in Application directory - $APPLICATION_DIR if exists and creating it again\n"

# Add step to take backup of the existing code folder in Application directory

shopt -s extglob

if [ -d "$APPLICATION_DIR/code" ]
                                                                                                                                                                                                          echo "inside if"
              cd $APPLICATION_DIR/code
              then
              find . -type f -not -name "environment_params.json" -not -name "CommonConstants.py" -print0 | xargs -0  -I {} rm {}
              cd $WORKING_DIR/cdl_common_component
fi
printf "$s\nCopying the updated code set to the application folder: $APPLICATION_DIR/code \n"
cp -r $WORKING_DIR/code_set/* $APPLICATION_DIR/code
cp $WORKING_DIR/code_set/__init__.py $APPLICATION_DIR/code/logging/
echo $?
cp $WORKING_DIR/code_set/handlers.py $APPLICATION_DIR/code/logging/
echo $?
cp $WORKING_DIR/code_set/config.py  $APPLICATION_DIR/code/logging/
printf "$s\nCopying the CommonConstants to the application folder: $APPLICATION_DIR/code \n"
rm -rf $APPLICATION_DIR/code/CommonConstants.py
cp -r $WORKING_DIR/cdl_common_component/configurations/environment_configurations/$ENVIRONMENT/CommonConstants.py $APPLICATION_DIR/code
cp -r $WORKING_DIR/cdl_common_component/configurations/environment_configurations/$ENVIRONMENT/environment_params.json $APPLICATION_DIR/code
cd $WORKING_DIR
mkdir -p $APPLICATION_DIR/code/adaptor
cp -r $WORKING_DIR/cdl_common_component/adaptor/code/* $APPLICATION_DIR/code/adaptor/
mkdir -p $APPLICATION_DIR/code/adaptor/common_utilities
cp -r $WORKING_DIR/cdl_common_component/adaptor/utilities/* $APPLICATION_DIR/code/adaptor/common_utilities
echo $APPLICATION_DIR/code/adaptor/**/ | xargs -n 1 cp $WORKING_DIR/cdl_common_component/adaptor/configs/$ENVIRONMENT/application_config.json
echo $APPLICATION_DIR/code/adaptor/**/ | xargs -n 1 cp $WORKING_DIR/cdl_common_component/adaptor/configs/$ENVIRONMENT/CommonConstants.py



if [ -z $DM_DEPLOY_RESPONSE ]
then
        printf "$s\nDo you want to Deploy Data management repo as well? (Y/N)"
        read DM_DEPLOY_RESPONSE
fi
echo "DM_DEPLOY_RESPONSE - "$DM_DEPLOY_RESPONSE

if [ "$DM_DEPLOY_RESPONSE" == "Y" ]
              then

              if [ -z $DM_GIT_BRANCH ]
              then
        printf "$s\nPlease enter the branch or TAG name: "
        read DM_GIT_BRANCH
              fi
              rm -rf $APPLICATION_DIR/configs/
              mkdir $APPLICATION_DIR/configs/
              mkdir $APPLICATION_DIR/configs/job_executor/
              mkdir $APPLICATION_DIR/configs/job_executor/aggregated_custom_template/
              mkdir $APPLICATION_DIR/configs/job_executor/aggregated_spark_code/
              rm -rf $APPLICATION_DIR/dags/
              mkdir $APPLICATION_DIR/dags/
              rm -rf $APPLICATION_DIR/sql/
              mkdir $APPLICATION_DIR/sql/
              cd $WORKING_DIR
#			  cmd='git clone -b "$DM_GIT_BRANCH" https://"$GIT_USER":"$GIT_PASSWORD"@bitbucket.org/zs_businesstech/cdl_de_ctfo.git'
#			  echo "Executing below command to clone the git repository"
#              echo $cmd
#			  git clone -b "$DM_GIT_BRANCH" https://"$GIT_USER":"$GIT_PASSWORD"@bitbucket.org/zs_businesstech/cdl_de_ctfo.git
#              cp -r $WORKING_DIR/cdl_de_ctfo/dw_scripts/**/* $APPLICATION_DIR/configs/job_executor/
              cmd='git clone -b '"$DM_GIT_BRANCH"' https://'"$GIT_USER"':"$GIT_PASSWORD"@bitbucket.org/zs_businesstech/cdl_data_management.git'
              echo "Executing below command to clone the git repository"
              echo $cmd
              git clone -b "$DM_GIT_BRANCH" https://"$GIT_USER":"$GIT_PASSWORD"@bitbucket.org/zs_businesstech/cdl_data_management.git 2> /dev/null
              cp -r $WORKING_DIR/cdl_data_management/dw_scripts/**/* $APPLICATION_DIR/configs/job_executor/
              cp $WORKING_DIR/cdl_data_management/standardization/* $APPLICATION_DIR/code
              #cp -r jrd_fido_de/ctfo_utils/ctl_config/* $APPLICATION_DIR/sql/
              #cp -r jrd_fido_de/ctfo_utils/dags/* $APPLICATION_DIR/dags/
fi
printf "$s\nDeployment Completed!"
