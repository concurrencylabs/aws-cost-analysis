import os, sys, traceback

__location__ = os.path.dirname(os.path.realpath(__file__))
site_pkgs = os.path.join(os.path.split(__location__)[0], "lib", "python2.7", "site-packages")
sys.path.append(site_pkgs)

import logging, json, time, datetime, hashlib, pytz
import boto3
from botocore.exceptions import ClientError as BotoClientError

import awscostusageprocessor.utils as utils
import awscostusageprocessor.processor as cur
import awscostusageprocessor.consts as consts
from awscostusageprocessor.errors import ManifestNotFoundError, CurBucketNotFoundError

log = logging.getLogger()
log.setLevel(logging.INFO)

sfnclient = boto3.client('stepfunctions')
snsclient= boto3.client('sns')
ddbresource = boto3.resource('dynamodb')
ddbclient = boto3.client('dynamodb')


"""
Processing AWS Cost and Usage reports and preparing them for Athena is a multi-step workflow. That's why it
has been implemented as a State Machine using AWS Step Functions.

This function receives a scheduled event and it searches for new Cost and Usage reports, so they can be processed.
"""


def handler(event, context):

    log.info("Received event {}".format(json.dumps(event, indent=4)))

    #Get accounts that are ready for CUR - the ones with reports older than MINUTE_DELTA
    MINUTE_DELTA = 0
    lastProcessedIncludeTs = (datetime.datetime.now(pytz.utc) + datetime.timedelta(minutes=-MINUTE_DELTA)).strftime(consts.TIMESTAMP_FORMAT)

    log.info("Looking for AwsAccountMetadata items processed before [{}] in table [{}]".format(lastProcessedIncludeTs, consts.AWS_ACCOUNT_METADATA_DDB_TABLE))

    metadatatable = ddbresource.Table(consts.AWS_ACCOUNT_METADATA_DDB_TABLE)
    response = metadatatable.scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=boto3.dynamodb.conditions.Attr('lastProcessedTimestamp').lt(lastProcessedIncludeTs) &
                             boto3.dynamodb.conditions.Attr('dataCollectionStatus').eq(consts.DATA_COLLECTION_STATUS_ACTIVE),
            ReturnConsumedCapacity='TOTAL'
    )
    log.info(json.dumps(response, indent=4))

    sfn_executionlinks = ""
    execnames = []

    #Get metadata for each of those accounts and prepare args for CostUsageProcessor
    for item in response['Items']:

        #Prepare args for CostUsageProcessor
        kwargs = {}
        now = datetime.datetime.now(pytz.utc)
        kwargs['startTimestamp'] = now.strftime(consts.TIMESTAMP_FORMAT)
        year = now.strftime("%Y")
        month = now.strftime("%m")
        kwargs['year'] = year
        kwargs['month'] = month
        kwargs['sourceBucket'] = item['curBucket']
        kwargs['sourcePrefix'] = "{}{}/".format(item['curPrefix'],item['curName']) #TODO: move to a common function
        kwargs['destBucket'] = consts.CUR_PROCESSOR_DEST_S3_BUCKET
        kwargs['destPrefix']= consts.CUR_PROCESSOR_DEST_S3_PREFIX
        kwargs['accountId'] = item['awsPayerAccountId']
        kwargs['xAccountSource']=True
        kwargs['roleArn'] = item['roleArn']

        #See how old is the latest CUR manifest in S3 and compare it against the lastProcessedTimestamp in the AWSAccountMetadata DDB table
        #If the CUR manifest is newer, then start processing
        try:
            log.info("Starting new CUR evaluation for account [{}]".format(kwargs['accountId']))
            lastProcessedTs = datetime.datetime.strptime(item.get('lastProcessedTimestamp',consts.EPOCH_TS), consts.TIMESTAMP_FORMAT).replace(tzinfo=pytz.utc)
            minutesSinceLastCurProcessed = int((now - lastProcessedTs).total_seconds() / 60)
            log.info("minutesSinceLastCurProcessed [{}] - lastProcessedTimestamp [{}]".format(minutesSinceLastCurProcessed, item.get('lastProcessedTimestamp',consts.EPOCH_TS)))
            curprocessorStatus = consts.CUR_PROCESSOR_STATUS_OK
            curprocessorStatusDetails = '-'

            curprocessor = cur.CostUsageProcessor(**kwargs)
            cur_manifest_lastmodified_ts = curprocessor.aws_manifest_lastmodified_ts

            log.info("Found manifest for awsAccountId:[{}] - cur_manifest_lastmodified_ts:[{}] - lastProcessedTimestamp:[{}]".format(curprocessor.accountId, cur_manifest_lastmodified_ts, item['lastProcessedTimestamp']))
            if cur_manifest_lastmodified_ts > lastProcessedTs:
                #Start execution
                period = utils.get_period_prefix(year,month).replace('/','')
                execname = "{}-{}-{}".format(curprocessor.accountId, period, hashlib.md5(str(time.time()).encode("utf-8")).hexdigest()[:8])
                sfnresponse = sfnclient.start_execution(stateMachineArn=consts.STEP_FUNCTION_PREPARE_CUR_ATHENA,
                                                     name=execname,
                                                     input=json.dumps(kwargs))

                #Prepare SNS notification
                sfn_executionarn = sfnresponse['executionArn']
                sfn_executionlink = "https://console.aws.amazon.com/states/home?region={}#/executions/details/{}\n".format(consts.AWS_DEFAULT_REGION, sfn_executionarn)
                sfn_executionlinks += sfn_executionlink
                execnames.append(execname)
                log.info("Started execution - executionArn: {}".format(sfn_executionarn))

        except CurBucketNotFoundError as e:
            log.error("CurBucketNotFoundError [{}]".format(e.message))
            curprocessorStatus = consts.CUR_PROCESSOR_STATUS_ERROR
            curprocessorStatusDetails = e.message

        except ManifestNotFoundError as e:
            log.error("ManifestNotFoundError [{}]".format(e.message))
            curprocessorStatus = consts.CUR_PROCESSOR_STATUS_ERROR
            curprocessorStatusDetails = e.message

        except BotoClientError as be:
            errorType = ''
            if be.response['Error']['Code'] == 'AccessDenied':
                errorType = 'BotoAccessDenied'
            else:
                errorType = 'BotoClientError_'+be.response['Error']['Code']
            log.error("{} awsPayerAccountId [{}] roleArn [{}] [{}]".format(errorType, kwargs['accountId'], kwargs['roleArn'], be.message))
            curprocessorStatus = consts.CUR_PROCESSOR_STATUS_ERROR
            curprocessorStatusDetails = errorType

        except Exception as e:
            log.error("xAcctStepFunctionStarterException awsPayerAccountId [{}] roleArn [{}] [{}]".format(kwargs['accountId'], kwargs['roleArn'], e))
            traceback.print_exc()
            curprocessorStatus = consts.CUR_PROCESSOR_STATUS_ERROR
            curprocessorStatusDetails = e.message


        #If there were errors, update Metadata table with details
        if curprocessorStatus == consts.CUR_PROCESSOR_STATUS_ERROR:
            log.info("Updating DDB table [{}]".format(consts.AWS_ACCOUNT_METADATA_DDB_TABLE))
            ddbclient.update_item(TableName=consts.AWS_ACCOUNT_METADATA_DDB_TABLE,
                Key = {'awsPayerAccountId': {'S': item['awsPayerAccountId']}},
                AttributeUpdates={
                    'status':{'Value': {'S': curprocessorStatus}},
                    'statusDetails':{'Value': {'S': curprocessorStatusDetails}},
                    'lastUpdateTimestamp':{'Value': {'S': kwargs['startTimestamp']}}
                }
            )




    if sfn_executionlinks:
        snsclient.publish(TopicArn=consts.SNS_TOPIC,
            Message='New Cost and Usage report. Started execution: {}'.format(sfn_executionlinks),
            Subject='New incoming Cost and Usage report executions - {}'.format(context.invoked_function_arn.split(':')[4]))

    log.info("Started executions: [{}]".format(execnames))

    return execnames
