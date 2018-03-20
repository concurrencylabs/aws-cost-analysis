from __future__ import print_function
import sys, os, logging, json
import boto3

__location__ = os.path.dirname(os.path.realpath(__file__))
site_pkgs = os.path.join(os.path.split(__location__)[0], "lib", "python2.7", "site-packages")
sys.path.append(site_pkgs)

import awscostusageprocessor.processor as cur
import awscostusageprocessor.consts as consts



log = logging.getLogger()
log.setLevel(logging.INFO)
ddbclient = boto3.client('dynamodb')

"""
This function starts the process that copies and prepares incoming AWS Cost and Usage reports.
"""

def handler(event, context):

    log.info("Received event {}".format(json.dumps(event)))

    curprocessor = cur.CostUsageProcessor(**event)
    #This function only supports processing files for Athena (for now).
    curprocessor.process_latest_aws_cur(consts.ACTION_PREPARE_ATHENA)
    event.update({'curManifest':curprocessor.curManifestJson})
    if not event.get('accountId',''): event['accountId']=curprocessor.accountId
    log.info("CurKeyCount {}".format(len(curprocessor.curManifestJson.get('reportKeys',[]))))
    log.info("Return object:[{}]".format(event))
    return event
