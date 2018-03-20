from __future__ import print_function

import os, sys

__location__ = os.path.dirname(os.path.realpath(__file__))
site_pkgs = os.path.join(os.path.split(__location__)[0], "lib", "python2.7", "site-packages")
sys.path.append(site_pkgs)

import logging, datetime, pytz
import boto3
import awscostusageprocessor.consts as consts

import json

log = logging.getLogger()
log.setLevel(logging.INFO)
ddbclient = boto3.client('dynamodb')


"""
This function updates a DDB table with the latest execution timestamp. This
information is used by

- The processes that decide whether to query from Athena or from S3.
- Step Function starter, in order to decide if a new execution should be triggered.
- Any application that consumes Cost and Usage data and needs to know when a new report has been processed
"""

def handler(event, context):

    log.info("Received event {}".format(json.dumps(event)))
    accountid = event['accountId']

    ddbresponse = ddbclient.update_item(TableName=consts.AWS_ACCOUNT_METADATA_DDB_TABLE,
                                Key = {'awsPayerAccountId': {'S': accountid}},
                                AttributeUpdates={
                                    'lastProcessedTimestamp':{'Value': {'S': event['startTimestamp']}},
                                    'status':{'Value': {'S': consts.CUR_PROCESSOR_STATUS_OK}},
                                    'lastUpdateTimestamp':{'Value': {'S': datetime.datetime.now(pytz.utc).strftime(consts.TIMESTAMP_FORMAT)}}
                                },
                                ReturnConsumedCapacity='TOTAL'
                            )

    return event
