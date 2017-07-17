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

    #Ensure this operation is NOT executed xAcct
    if 'xAccountSource' in event: event['xAccountSource']=False
    if 'roleArn' in event: event['roleArn'] = ''

    athena = ath.AthenaQueryMgr(consts.ATHENA_BASE_OUTPUT_S3_BUCKET, accountid, year, month)

    #construct database name based on input parameters: costusage-accountid
    athena.create_database()

    #drop table for the current month - 20170601-20170701
    athena.drop_table()

    #TODO: dynamically create fields based on the latest AWS CUR manifest
    #TODO: use columnar format, for better performance
    #create new table for the current month
    prefix = consts.CUR_PROCESSOR_DEST_S3_PREFIX + accountid + "/" + utils.get_period_prefix(year, month)
    createtablesql = open(sql_path+'/create_athena_table.sql', 'r').read()
    sqlstatement = createtablesql.replace("{dbname}",athena.dbname).\
                                  replace("{tablename}",athena.tablename).\
                                  replace("{bucket}",consts.CUR_PROCESSOR_DEST_S3_BUCKET).\
                                  replace("{prefix}",prefix)
    athena.execute_query('create_table', sqlstatement)

    return event























