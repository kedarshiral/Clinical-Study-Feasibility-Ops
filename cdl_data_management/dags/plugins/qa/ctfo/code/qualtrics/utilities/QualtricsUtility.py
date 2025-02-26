import uuid

import requests
import json
import traceback
import zipfile
import json
import io, os
import sys
import re
import time
import ast
import copy
import traceback

service_directory_path = os.path.dirname(os.path.abspath(__file__))
utilities_dir_path = os.path.abspath(os.path.join(service_directory_path, "../utilities/"))
sys.path.insert(0, utilities_dir_path)

from LogSetup import get_logger
import CommonServicesConstants
from utils import Utils

LOGGING = get_logger()
UTILS_OBJ = Utils()


class QualtricsUtility():
    def __init__(self):
        self.survey_config_dict = UTILS_OBJ.get_survey_config_json()

    def request_survey_creation(self, parameter_dict, job_id):
        """
                Purpose             : Create a blank survey in Qulatrics
                Input payload       : api_token, data_center, survey_configuration
                Output payload      :

        """
        try:
            LOGGING.info("Starting function request_survey_creation in Qulatrics")
            baseUrl = "https://{data_center}.qualtrics.com/API/v3/survey-definitions".format(
                data_center=self.survey_config_dict["data_center"])
            headers = {
                "x-api-token": parameter_dict["api_token"],
                "Content-Type": "application/json"
            }
            self.response = requests.post(baseUrl, headers=headers, json=parameter_dict["survey_configuration"])
            LOGGING.info("Response after creating blank survey in Qualtrics - %s", str(self.response.text))
            response = self.response.json()
            survey_id = response['result']['SurveyID']
            embeddedurl = "https://ca1.qualtrics.com/API/v3/surveys/{surveyId}/embeddeddatafields".format(
                surveyId=survey_id)
            payload = "{\"name\":\"ContactID\",\"embeddedDataFields\":[{\"key\":\"ContactID\"}]}"
            headers = {
                'x-api-token': parameter_dict["api_token"],
                'content-type': "application/json"
            }
            self.res = requests.request("POST", embeddedurl, data=payload, headers=headers)
            LOGGING.info("Response after inserting embedded data block for  survey in Qualtrics - %s",
                         str(self.res.text))
            # fpi_data = {
            #     "BackButton": True,
            #     "BallotBoxStuffingPrevention": "false",
            #     "Header": "ZS",
            #     "Footer": "ZS",
            #     "NoIndex": "Yes",
            #     "NextButton": " ← ",
            #     "PartialData": "+3 months",
            #     "PreviousButton": " ← ",
            #     "ProgressBarDisplay": "None",
            #     "ResponseSummary": "Yes",
            #     "SaveAndContinue": True,
            #     "SecureResponseFiles": "true",
            #     "SurveyExpiration": "None",
            #     "SurveyProtection": "PublicSurvey",
            #     "SurveyTermination": "DefaultMessage"
            # }
            fpi_data = {
                "BackButton": True,
                "BallotBoxStuffingPrevention": "false",
                "NoIndex": "Yes",
                "NextButton": " ← ",
                "PartialData": "+3 months",
                "PreviousButton": " ← ",
                "ProgressBarDisplay": "None",
                "ResponseSummary": "Yes",
                "SaveAndContinue": True,
                "SecureResponseFiles": "true",
                "SurveyExpiration": "None",
                "SurveyProtection": "PublicSurvey",
                "SurveyTermination": "DefaultMessage"
            }
            aseUrl = "https://{datacenter}.qualtrics.com/API/v3/survey-definitions/{surveyId}/options".format(
                datacenter=self.survey_config_dict["data_center"], surveyId=survey_id)
            headers = {
                "x-api-token": parameter_dict["api_token"],
                "Content-Type": "application/json"
            }
            self.resp = requests.put(aseUrl, headers=headers, json=fpi_data)
            LOGGING.info("Response after inserting back button data block for  survey in Qualtrics - %s",
                         str(self.resp.text))
            if response["meta"]["httpStatus"] == CommonServicesConstants.SURVEY_CREATION_STATUS:
                return response
        except Exception as e:
            LOGGING.error("Error Message : %s " + str(e) + str(traceback.format_exc()), job_id)
            response = self.response.json()
            return response

    def invoke_qualtrics_api(self, method_object, parameters_dict, job_id):
        """
                                Purpose             : Delete Survey
                                Input payload       : survey_id, api_token
                                Output payload      :
                                """
        try:
            """
                Purpose             : To call the qualtrics api in retry logic
                Input payload       : method_object, parameters_dict
                Output payload      :
            """
            # LOGGING.info(" Inside invoke_qualtrics_api method ")
            print(" Inside invoke_qualtrics_api method ")
            count = 0
            while True:
                if count == 3:
                    break
                # LOGGING.info("Inside invoke qualtrics API for {} ".format(str(count)))
                # log_params = {"job_id": job_id}
                # LOGGING.info("method_object - {}".format(str(method_object)))
                # LOGGING.info("parameters_dict - {}".format(str(parameters_dict)))

                response = method_object(parameters_dict, job_id)

                # LOGGING.info("response - {}".format(str(response)))
                # LOGGING.info("type of response object- {}".format(type(response)))

                # response received should be json object
                # have to modify to accomodate response from get_survey_resposes API
                # as get_survey_response do not have response["meta"]
                if method_object.__name__ == "get_survey_response":
                    break

                status_code = int(response["meta"]["httpStatus"].split("-")[0].strip())

                if response["meta"]["httpStatus"] == CommonServicesConstants.SURVEY_CREATION_STATUS:
                    # LOGGING.info("Responses received successfully")
                    print("Responses received successfully")
                    break
                elif status_code >= 400 and status_code <= 428:
                    # LOGGING.info("error code between 400 to 428")
                    print("error code between 400 to 428")
                    break
                count = count + 1
                time.sleep(3)
            # response = response.json()
            return response
        except Exception as e:
            LOGGING.error("Error Message : %s " + str(e) + str(traceback.format_exc()))

    def create_mailing_list(self, mailing_list_name_dict, job_id):
        """
        Purpose     :   Method to create mailing list on Qualtrics
        Input       :
                        parameters_dict -> Contains parameters needed for creating mailing list
                        log_params -> Extra parameters needed for logging
        Output      :   Executes successfully, else raises an exception
        """
        try:
            LOGGING.info("Starting method to create mailing list")
            LOGGING.info("Extracting necessary parameters from parameters dict")

            api_token = mailing_list_name_dict["api_token"]
            data_center = self.survey_config_dict["data_center"]
            directory_id = self.survey_config_dict["directory"]
            mailing_list_name = mailing_list_name_dict["mailing_list_name"]
            # owner_id = self.survey_config_dict["service_account_id"]
            owner_id = mailing_list_name_dict["account_id"]

            #  Create Mailing List
            createMLUrl = "https://{0}.qualtrics.com/API/v3/directories/{1}/mailinglists".format(data_center,
                                                                                                 directory_id)
            LOGGING.info("Mailing List URL - %s", str(createMLUrl))

            headers = {
                "x-api-token": api_token,
                "Content-Type": "application/json"
            }
            data = {
                "name": mailing_list_name,
                "ownerId": owner_id
            }
            LOGGING.info("API payload for create mailing list - %s", str(data))
            response_create_mailing_list = requests.post(createMLUrl, json=data, headers=headers)

            LOGGING.info("Response of creating Mailing List - %s", str(response_create_mailing_list.text))

            create_mailing_list_response = response_create_mailing_list.json()

            return create_mailing_list_response
        except Exception as e:
            LOGGING.error(
                "Error Message : Failed to create Qualtrics mailing list %s " + str(e) + str(traceback.format_exc()),
                job_id)

    def create_qualtrics_contact(self, parameters_dict, job_id):
        """
            Purpose     :   Invokes Qualtrics API to create Qualtrics contact
            Input       :   parameters_dict -> Dictionary containing parameters needed to create a new survey
                            data_center
                            directory_id
                            api_token
                            log_params -> logging parameters dict
            Output      :   Executes successfully, else raises an exception
        """
        try:
            LOGGING.info("Starting method to create Qualtrics contact")
            print(100 * '&')
            print(parameters_dict)
            # extracting parameters from dict
            data_center = self.survey_config_dict["data_center"]
            # api_token = self.survey_config_dict["api_token"]
            api_token = parameters_dict["dynamic_data"]["api_token"]
            directory = self.survey_config_dict["directory"]
            # directory = parameters_dict["dynamic_data"]["account_id"]
            language = self.survey_config_dict["language"]
            pageSize = 100
            offset = 0

            firstName = parameters_dict["first_name"]
            lastName = parameters_dict["last_name"]
            email = parameters_dict["email"]
            # phone = parameters_dict["phone"]
            phone = str(uuid.uuid4())[:10]
            # phone = "0000000000"
            ctfo_site_id = parameters_dict.get("ctfo_site_id", str(uuid.uuid4())[:10])
            extRef = parameters_dict["country_name"] + "#" + str(phone) + '#' + ctfo_site_id
            mailing_list_id = parameters_dict["mailing_list_id"]
            # is_ecda = parameters_dict.get("is_ecda","")
            # is_manual = parameters_dict.get("is_manual","")

            # TODO add ext_ref logic for SipIQ survey
            # ext_ref = parameters_dict["extRef"] if "extRef" in parameters_dict else None
            # if ext_ref is None:
            #     ext_ref = parameters_dict["country"] if "country" in parameters_dict else None

            # Adding contacts to given mailing list id
            create_contact_url = "https://{0}.qualtrics.com/API/v3/directories/{1}/mailinglists/{2}/contacts".format(
                data_center, directory, mailing_list_id)
            LOGGING.info("URL to create contact - %s", create_contact_url)

            headers = {
                "x-api-token": api_token,
                "Content-Type": "application/json"
            }
            LOGGING.info("headers for create contact api - %s", headers)
            # include site_id
            data = {
                "firstName": firstName,
                "lastName": lastName,
                "email": email,
                "phone": phone,
                "language": language,
                "extRef": extRef
            }
            LOGGING.info("payload for create contact api - %s", data)
            create_contact_response = requests.post(create_contact_url, json=data, headers=headers)
            LOGGING.info("Response from create contact API - %s", create_contact_response.text)

            create_contact_response_obj = create_contact_response.json()

            # list_contacts_url = "https://{0}.qualtrics.com/API/v3/directories/{1}/mailinglists/{2}/contacts?pageSize={3}".format(
            #     data_center, directory, mailing_list_id, pageSize)
            #
            # headers = {"x-api-token": api_token}
            #
            # response_list_contact = requests.get(list_contacts_url, headers = headers)
            #
            # LOGGING.info("Response from create contact API - %s", response_list_contact.text)
            #
            # contact_list_json = response_list_contact.json()

            return create_contact_response_obj
        except Exception as e:
            LOGGING.error(
                "Failed to create Qualtrics contact. ERROR - Job ID: %s " + str(e) + str(traceback.format_exc()),
                job_id)

    def create_personalized_link(self, parameter_dict, job_id):
        try:
            LOGGING.info("Starting function create personalized link")

            # headers = {
            #     "x-api-token": self.survey_config_dict["api_token"],
            #     "Content-Type": "application/json"
            # }
            headers = {
                "x-api-token": parameter_dict["api_token"],
                "Content-Type": "application/json"
            }

            LOGGING.info(" parameter_dict - {}".format(str(parameter_dict)))

            #  Step-3: Create a distribution ID for the specified Mailing List
            api_data = {
                "surveyId": parameter_dict["survey_id"],
                # survey id will be retrieved from the FIDO table against the study_id
                "linkType": self.survey_config_dict["link_type"],
                "description": parameter_dict["description"],
                "action": parameter_dict["action"],
                "mailingListId": parameter_dict["mailing_list_id"]
                # mailing list IDs will be genearted based on scenario.Every scenario will have a separate mailing list.
            }

            LOGGING.info(" api_data - %s", str(api_data))

            # Generation distribution_id for creating individual link
            distribution_id_url = "https://{data_center}.qualtrics.com/API/v3/distributions".format(
                data_center=self.survey_config_dict["data_center"])

            response_distribution_id = requests.post(distribution_id_url, headers=headers, json=api_data)

            LOGGING.info("Response from create contact API - %s", response_distribution_id.text)

            response_dstri_id = response_distribution_id.json()

            if response_dstri_id["meta"]["httpStatus"] != CommonServicesConstants.SURVEY_CREATION_STATUS:
                return response_dstri_id

            distribution_id = response_dstri_id["result"]["id"]

            LOGGING.info("Distribution Id  for the mailing list %s:", distribution_id)

            survey_link_responses = {}
            #  Step-2 :Create the distribution link from against the distributionID
            distribution_link_url = "https://{data_center}.qualtrics.com/API/v3/distributions/{distributionid}/links?surveyId={survey_id}".format(
                data_center=self.survey_config_dict["data_center"], distributionid=distribution_id,
                survey_id=parameter_dict["survey_id"])

            response = requests.get(distribution_link_url, headers=headers)

            LOGGING.info("Response from create contact API - %s", response.text)

            response = response.json()
            survey_link_responses = response
            if "result" in response and "nextPage" in response["result"] and response["result"]["nextPage"] is not None:
                resp = None
                resp = response
                while "result" in resp and "nextPage" in resp["result"] and resp["result"]["nextPage"] is not None:
                    LOGGING.debug("Fetching the links other than the first 100 links")
                    resp = requests.get(resp["result"]["nextPage"], headers=headers)
                    resp = resp.json()
                    if "result" in resp and "elements" in resp["result"] and isinstance(resp["result"]["elements"], list):
                        survey_link_responses["result"]["elements"].extend(resp["result"]["elements"])

            if response["meta"]["httpStatus"] == CommonServicesConstants.SURVEY_CREATION_STATUS:
                return response
            else:
                raise Exception('Unable to generate the Personalized Link')
        except Exception as e:
            LOGGING.error(
                "Failed to create Personalized Link. ERROR - Job ID: %s " + str(e) + str(traceback.format_exc()),
                job_id)

    def export_responses(self, parameter_dict, job_id):
        """
        Purpose     :   Exports the responses for the given survey and downloads csv file to target_file_location
        Input       :
                        api_token -> API token for accessing Qualtrics
                        data_center -> Qualtrics server (to be used in API endpoints)
                        survey_id -> Qualtrics survey_id for the survey
                        file_format -> Format in which the file must be downloaded
                        target_file_location -> The location to which the file must be downloaded
        Output      :   Completes successfully if export was successful, else raises an exception
        """
        try:
            file_format = parameter_dict["file_format"]
            downloadRequestPayloads = ['{"format":"' + file_format + '", "useLabels": "true"}',
                                       '{"format":"' + file_format + '", "useLabels": "true",  "exportResponsesInProgress":true}']
            for downloadRequestPayload in downloadRequestPayloads:
                print(100 * '&')
                print(downloadRequestPayload)
                LOGGING.info("Starting method to export survey responses, input - %s", str(parameter_dict))
                api_token = parameter_dict["api_token"]
                data_center = self.survey_config_dict["data_center"]
                survey_id = parameter_dict["survey_id"]
                target_file_location = parameter_dict["target_file_location"]
                requestCheckProgress = 0.0
                progressStatus = "inProgress"

                baseUrl = "https://{data_center}.qualtrics.com/API/v3/surveys/{survey_id}/export-responses/".format(
                    data_center=data_center, survey_id=survey_id)
                if "InProgress" in downloadRequestPayload:
                    survey_id = survey_id + "_InProgress"

                headers = {
                    "x-api-token": api_token,
                    "content-type": "application/json"
                }

                downloadRequestUrl = baseUrl
                downloadRequestResponse = requests.request("POST", downloadRequestUrl, data=downloadRequestPayload,
                                                           headers=headers)
                LOGGING.info("Download Request Response - %s", downloadRequestResponse.text)
                progressId = downloadRequestResponse.json()["result"]["progressId"]

                requestCheckResponse = None
                while progressStatus != "complete" and progressStatus != "failed":
                    LOGGING.info("Progress Status - %s", progressStatus)

                    requestCheckUrl = baseUrl + progressId
                    requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers)
                    LOGGING.info("Request Check Response - %s", requestCheckResponse.text)

                    requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
                    LOGGING.info("Download is %s completed", str(requestCheckProgress))

                    progressStatus = requestCheckResponse.json()["result"]["status"]

                if progressStatus is "failed":
                    raise Exception("Failed to export")

                fileId = requestCheckResponse.json()["result"]["fileId"]
                requestDownloadUrl = baseUrl + fileId + '/file'
                requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)
                # zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall(survey_id + "_" +target_file_location)
                zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall(
                    os.path.join(target_file_location, survey_id))
            LOGGING.info("Download Completed!!")
        except Exception as e:
            LOGGING.error("Failed to Export. ERROR - Job ID: %s " + str(e) + str(traceback.format_exc()), job_id)

    def get_survey_metadata(self, parameter_dict, log_params=None):
        """
                Purpose             :
                Input payload       :
                Output payload      :
        """
        try:
            LOGGING.debug("Starting function get_survey_metadata in Qulatrics ", extra=log_params)
            baseUrl = "https://{data_center}.qualtrics.com/API/v3/survey-definitions/{survey_id}/metadata".format(
                data_center=parameter_dict["data_center"], survey_id=parameter_dict["survey_id"])
            LOGGING.debug("baseUrl - {} ".format(str(baseUrl)), extra=log_params)

            headers = {
                "x-api-token": parameter_dict["api_token"],
                "Content-Type": "application/json"
            }

            response = requests.get(baseUrl, headers=headers)

            # LOGGING.debug("response after calling get survey metadata Qualtrics API - {}".format(str(response.text)),
            #              extra=log_params)

            response = response.json()
            if response["meta"]["httpStatus"] != CommonServicesConstants.SURVEY_CREATION_STATUS:
                return response
            print("Final response for Survey Fetched")
            # LOGGING.debug("Final response for  get survey metadata - {}".format(str(response)), extra=log_params)
            return response

        except:
            raise Exception(traceback.format_exc())

    def get_survey_versions(self, parameter_dict, log_params=None):
        """
                Purpose             :
                Input payload       :
                Output payload      :
        """
        try:
            LOGGING.debug("Starting function get_survey_versions in Qulatrics ", extra=log_params)
            baseUrl = "https://{data_center}.qualtrics.com/API/v3/survey-definitions/{survey_id}/versions".format(
                data_center=parameter_dict["data_center"], survey_id=parameter_dict["survey_id"])
            LOGGING.debug("baseUrl - {} ".format(str(baseUrl)), extra=log_params)

            headers = {
                "x-api-token": parameter_dict["api_token"],
                "Content-Type": "application/json"
            }

            response = requests.get(baseUrl, headers=headers)

            LOGGING.debug("response after calling get_survey_versions Qualtrics API - {}".format(str(response.text)),
                          extra=log_params)

            response = response.json()
            if response["meta"]["httpStatus"] != CommonServicesConstants.SURVEY_CREATION_STATUS:
                return response

            LOGGING.debug("Final response for  get_survey_versions - {}".format(str(response)), extra=log_params)
            return response
        except:
            raise Exception(traceback.format_exc())

    def get_survey(self, parameter_dict, log_params=None):
        """
                Purpose             :
                Input payload       :
                Output payload      :
        """
        try:
            # LOGGING.debug("Starting function get_survey in Qulatrics ", extra=log_params)
            print("Starting function get_survey in Qulatrics ")
            baseUrl = "https://{datacenter}.qualtrics.com/API/v3/survey-definitions/{surveyid}".format(
                datacenter=parameter_dict["data_center"], surveyid=parameter_dict["survey_id"])
            # LOGGING.debug("baseUrl - {} ".format(str(baseUrl)), extra=log_params)

            headers = {
                "x-api-token": parameter_dict["api_token"],
                "Content-Type": "application/json"
            }

            response = requests.get(baseUrl, headers=headers)

            # LOGGING.debug("response after calling get_survey Qualtrics API - {}".format(str(response.text)),
            #              extra=log_params)
            response = response.json()

            if response["meta"]["httpStatus"] != CommonServicesConstants.SURVEY_CREATION_STATUS:
                return response
            # LOGGING.debug("Final response for  get_survey - {}".format(str(response)), extra=log_params)
            print("Got the Final response")
            return response

        except:
            raise Exception(traceback.format_exc())

    def get_survey_response(self, parameter_dict, log_params=None):
        """
        Purpose     :   Exports the responses for the given survey and downloads csv file to target_file_location
        Input       :
                        api_token -> API token for accessing Qualtrics
                        data_center -> Qualtrics server (to be used in API endpoints)
                        survey_id -> Qualtrics survey_id for the survey
                        file_format -> Format in which the file must be downloaded
                        inprogress_flag -> Flag to fetch in-progress/completed suvrey responses
                        start_date -> parameter to fetch responses received after start_time
                        end_date ->  parameter to fetch responses received till end_time
        Output      :   Completes successfully if export was successful, else raises an exception
        """
        try:
            LOGGING.info("Starting method to get survey responses, input - {}".format(parameter_dict))
            api_token = parameter_dict["api_token"]
            data_center = parameter_dict["data_center"]
            survey_id = parameter_dict["survey_id"]
            file_format = parameter_dict["file_format"]
            inprogress_flag = parameter_dict["inprogress_flag"]
            # start_date =parameter_dict["start_date"]
            # end_date = parameter_dict["end_date"]

            r = re.compile('^SV_.*')
            m = r.match(parameter_dict["survey_id"])
            if not m:
                raise Exception("surveyId must match ^SV_.*")

            base_url = "https://{datacenter}.qualtrics.com/API/v3/surveys/{surveyid}/export-responses/".format(
                datacenter=data_center, surveyid=survey_id)
            LOGGING.debug("Base URL for export responses - {}".format(base_url))
            headers = {
                "content-type": "application/json",
                "x-api-token": api_token,
            }
            progress_status = "inProgress"

            download_request_payload = '{"format":"' + file_format + '","compress":false, "exportResponsesInProgress":' + inprogress_flag + '}'
            ## HAVE TO INCLUDE THE START_DATE AND END_DATE LOGIC (have to verify the time format)

            download_request_response = requests.request("POST", base_url, data=download_request_payload,
                                                         headers=headers)

            LOGGING.debug("response after calling get_survey_response STEP -1  Qualtrics API - {}".format(
                str(download_request_response.text)),
                extra=log_params)
            progress_id = None
            request_check_response = None
            if download_request_response.json()["meta"]["httpStatus"] == CommonServicesConstants.SURVEY_CREATION_STATUS:
                progress_id = download_request_response.json()["result"]["progressId"]
                LOGGING.debug("Progress ID received - {}".format(progress_id))
            else:
                raise Exception("Failed to get response. Error - {}".format(download_request_response.text))

            while progress_status != "complete" and progress_status != "failed":
                print("progressStatus=", progress_status)
                request_check_url = base_url + progress_id
                request_check_response = requests.request("GET", request_check_url, headers=headers)
                LOGGING.debug("response after calling get_survey_response STEP -2  Qualtrics API - {}".format(
                    str(request_check_response.text)),
                    extra=log_params)
                request_check_progress = request_check_response.json()["result"]["percentComplete"]
                print("Download is " + str(request_check_progress) + " complete")
                progress_status = request_check_response.json()["result"]["status"]

            if request_check_response is None:
                raise Exception("Failed to check response")

            if progress_status is "failed":
                raise Exception("Export failed. Error - {}".format(request_check_response.text))

            file_id = request_check_response.json()["result"]["fileId"]

            requestDownloadUrl = base_url + file_id + '/file'

            response = requests.get(requestDownloadUrl, headers=headers, stream=True)
            # LOGGING.debug("response after calling get_survey_response STEP -3  Qualtrics API - {}".format(
            #     str(response.text)), extra=log_params)
            response = response.json()

            # if response["meta"]["httpStatus"] != Constants.SURVEY_CREATION_STATUS:
            #    return response
            LOGGING.debug("Final response for  get_survey_response - {}".format(str(response)), extra=log_params)
            return response
        except:
            raise Exception(str(traceback.format_exc()))