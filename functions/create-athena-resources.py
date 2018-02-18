import logging
import json
import os, sys

__location__ = os.path.dirname(os.path.realpath(__file__))
sql_path = os.path.join(os.path.split(__location__)[0], "awscostusageprocessor","sql")
site_pkgs = os.path.join(os.path.split(__location__)[0], "lib", "python2.7", "site-packages")
sys.path.append(site_pkgs)

import awscostusageprocessor.utils as utils
import awscostusageprocessor.sql.athena as ath
import awscostusageprocessor.consts as consts
from awscostusageprocessor.errors import AthenaExecutionFailedException

log = logging.getLogger()
log.setLevel(logging.INFO)


"""
This function creates Athena databases and tables based on incoming AWS Cost and Usage reports
"""


def handler(event, context):

    #get event data
    log.info("Received event {}".format(json.dumps(event)))
    accountid = event['accountId']
    year = event['year']
    month = event['month']
    curManifest = event['curManifest']
    curS3Bucket = event['destBucket']

    #Ensure this operation is NOT executed xAcct
    if 'xAccountSource' in event: event['xAccountSource']=False
    if 'roleArn' in event: event['roleArn'] = ''

    try:
        athena = ath.AthenaQueryMgr(consts.ATHENA_BASE_OUTPUT_S3_BUCKET, accountid, year, month)

        #construct database name based on input parameters: costusage-accountid
        athena.create_database()

        #drop table for the current month - 20170601-20170701
        athena.drop_table()

        #TODO: use columnar format, for better performance
        #create new table for the current month
        curS3Prefix = consts.CUR_PROCESSOR_DEST_S3_PREFIX + accountid + "/" + utils.get_period_prefix(year, month)#TODO: move to a method in athena module, so it can be reused
        athena.create_table(curManifest, curS3Bucket, curS3Prefix)

    except AthenaExecutionFailedException as ae:
        log.error(ae.message)
        raise Exception("Failure when creating Athena resources: {}".format(ae.message))



    return event























