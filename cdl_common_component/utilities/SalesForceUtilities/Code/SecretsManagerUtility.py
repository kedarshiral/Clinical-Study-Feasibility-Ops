"""
Module for Retrieving Secrets from Secrets Manager
"""
__AUTHOR__ = 'ZS Associates'

# This file is subject to the terms and conditions defined in file 'LICENSE.txt' which is part of this source code package.

"""
   Module Name         :   SecretsManagerUtility
   Purpose             :   Utility to retrieve secrets stored in Secrets Manager
                           
   Input Parameters    :   secret_name,region_name
   Output              :   JSON with decoded credentials from Secrets Manager
   Execution Steps     :   python SecretsManagerUtility.py
   Predecessor module  :   NA
   Successor module    :   NA
   Pre-requisites      :   NA
   Last changed on     :   14 Dec 2018
   Last changed by     :   Katta Sreeharsha
   Reason for change   :   Incorporated the code review changes
"""


import base64
import boto3
import json
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
hndlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
hndlr.setFormatter(formatter)
logger.setLevel("DEBUG")
logger.addHandler(hndlr)


def get_secret(secret_name, region_name):
    """
    Purpose: This fetches the secret details and decrypts them
    Input: secret_name is the secret to be retrieved from Secrets Manager.
            region_name is the region which the secret is stored.
    Output: JSON with decoded credentials from Secrets Manager
    """

    try:
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        logger.info("Fetching the details for the secret name %s" % secret_name)
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        logger.info("Fetched the Encrypted Secret from Secrets Manager for %s" % secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        else:
            raise e
    except Exception as e:
        logger.exception(e, exc_info=True)

    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            logger.info("Decrypted the Secret")
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            logger.info("Decrypted the Secret")
        return json.loads(secret)


if __name__ == "__main__":
    get_secret("", "")

