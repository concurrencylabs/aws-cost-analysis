from __future__ import print_function
import logging
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
                                    'lastProcessedTimestamp':{
                                        'Value': {
                                            'S': event['startTimestamp']
                                        }
                                    }
                                },
                                ReturnConsumedCapacity='TOTAL'
                            )

    return event
