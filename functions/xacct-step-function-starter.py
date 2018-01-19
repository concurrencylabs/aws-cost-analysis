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
from awscostusageprocessor.errors import ManifestNotFoundError

log = logging.getLogger()
log.setLevel(logging.INFO)

sfnclient = boto3.client('stepfunctions')
snsclient= boto3.client('sns')
ddbresource = boto3.resource('dynamodb')


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
        kwargs['sourcePrefix'] = "{}{}/".format(item['curPrefix'],item['curName'])
        kwargs['destBucket'] = consts.CUR_PROCESSOR_DEST_S3_BUCKET
        kwargs['destPrefix']= '{}{}/'.format(consts.CUR_PROCESSOR_DEST_S3_PREFIX, item['awsPayerAccountId'])
        kwargs['accountId'] = item['awsPayerAccountId']
        kwargs['xAccountSource']=True
        kwargs['roleArn'] = item['roleArn']

        #See how old is the latest CUR manifest in S3 and compare it against the lastProcessedTimestamp in the AWSAccountMetadata DDB table
        #If the CUR manifest is newer, then start processing
        try:
            log.info("Starting new CUR evaluation for account [{}]".format(kwargs['accountId']))
            curprocessor = cur.CostUsageProcessor(**kwargs)
            cur_manifest_lastmodified_ts = curprocessor.get_aws_manifest_lastmodified_ts()
        except ManifestNotFoundError as e:
            log.info("ManifestNotFoundError [{}]".format(e.message))
            cur_manifest_lastmodified_ts = datetime.datetime.strptime(consts.EPOCH_TS, consts.TIMESTAMP_FORMAT).replace(tzinfo=pytz.utc)
            continue
            #TODO: add CW metric filter and alarm for CURs not found
        except BotoClientError as be:
            errorType = ''
            if be.response['Error']['Code'] == 'AccessDenied':
                errorType = 'BotoAccessDenied'
            else:
                errorType = 'BotoClientError_'+be.response['Error']['Code']
            log.error("{} awsPayerAccountId [{}] roleArn [{}] [{}]".format(errorType, curprocessor.accountId, curprocessor.roleArn, be.message))

            continue

        except Exception as e:
            log.error("xAcctStepFunctionStarterException awsPayerAccountId [{}] roleArn [{}] [{}]".format(kwargs['accountId'], kwargs['roleArn'], e))
            traceback.print_exc()
            continue 

        lastProcessedTs = datetime.datetime.strptime(item['lastProcessedTimestamp'], consts.TIMESTAMP_FORMAT).replace(tzinfo=pytz.utc)
        log.info("cur_manifest_lastmodified_ts:[{}] - lastProcessedTimestamp:[{}]".format(cur_manifest_lastmodified_ts, item['lastProcessedTimestamp']))
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

    if sfn_executionlinks:
        snsclient.publish(TopicArn=consts.SNS_TOPIC,
            Message='New Cost and Usage report. Started execution: {}'.format(sfn_executionlinks),
            Subject='New incoming Cost and Usage report executions')

    log.info("Started executions: [{}]".format(execnames))

    return execnames
