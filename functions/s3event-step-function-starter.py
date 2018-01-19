import sys, os

__location__ = os.path.dirname(os.path.realpath(__file__))
site_pkgs = os.path.join(os.path.split(__location__)[0], "lib", "python2.7", "site-packages")
sys.path.append(site_pkgs)

import logging, traceback, json, time, datetime, hashlib, pytz
import boto3
import awscostusageprocessor.utils as utils
import awscostusageprocessor.processor as cur
import awscostusageprocessor.consts as consts

log = logging.getLogger()
log.setLevel(logging.INFO)

sfnclient = boto3.client('stepfunctions')
snsclient= boto3.client('sns')


"""
Processing AWS Cost and Usage reports and preparing them for Athena is a multi-step workflow. That's why it
has been implemented as a State Machine using AWS Step Functions. This function receives an S3 PUT event
when a new AWS Cost and Usage report is generated and it then starts the Step Functions workflow.
"""


def handler(event, context):

    log.info("Received event {}".format(json.dumps(event)))

    try:

        #Get relevant info from S3 event
        s3eventinfo = event['Records'][0]['s3']
        s3key = s3eventinfo['object']['key']

        #Prepare args for CostUsageProcessor
        kwargs = {}
        sourcePrefix, year, month = utils.extract_period(s3key)
        log.info("year:[{}] - month:[{}]".format(year,month))
        kwargs['startTimestamp'] = datetime.datetime.now(pytz.utc).strftime(consts.TIMESTAMP_FORMAT)
        kwargs['year'] = year
        kwargs['month'] = month
        kwargs['sourceBucket'] = s3eventinfo['bucket']['name']
        kwargs['sourcePrefix'] = sourcePrefix
        kwargs['destBucket'] = consts.CUR_PROCESSOR_DEST_S3_BUCKET
        kwargs['destPrefix']= '{}placeholder/'.format(consts.CUR_PROCESSOR_DEST_S3_PREFIX)#placeholder is to avoid validation error when instantiating CostUsageProcessor

        curprocessor = cur.CostUsageProcessor(**kwargs)
        curprocessor.destPrefix = '{}{}/'.format(consts.CUR_PROCESSOR_DEST_S3_PREFIX, curprocessor.accountId)

        kwargs['accountId'] = curprocessor.accountId

        #Start execution
        period = utils.get_period_prefix(year,month).replace('/','')
        execname = "{}-{}-{}".format(curprocessor.accountId, period, hashlib.md5(str(time.time()).encode("utf-8")).hexdigest()[:8])

        sfnresponse = sfnclient.start_execution(stateMachineArn=consts.STEP_FUNCTION_PREPARE_CUR_ATHENA,
                                             name=execname,
                                             input=json.dumps(kwargs))

        #Prepare SNS notification
        sfn_executionarn = sfnresponse['executionArn']
        sfn_executionlink = "https://console.aws.amazon.com/states/home?region={}#/executions/details/{}\n".format(consts.AWS_DEFAULT_REGION, sfn_executionarn)
        snsclient.publish(TopicArn=consts.SNS_TOPIC,
                          Message='New Cost and Usage report. Started execution. Click here to view status: {}'.format(sfn_executionlink),
                          Subject='New incoming Cost and Usage report - accountid:{} - period:{}'.format(curprocessor.accountId, period))

        log.info("Started execution - executionArn: {}".format(sfn_executionarn))

        return execname


    except Exception as e:
        traceback.print_exc()
        print("Exception message:["+str(e.message)+"]")
