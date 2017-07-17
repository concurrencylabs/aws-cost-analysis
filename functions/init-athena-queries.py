import logging
import json
import sys, os

__location__ = os.path.dirname(os.path.realpath(__file__))
site_pkgs = os.path.join(os.path.split(__location__)[0], "lib", "python2.7", "site-packages")
sys.path.append(site_pkgs)

import awscostusageprocessor.api as curapi

log = logging.getLogger()
log.setLevel(logging.INFO)

"""
This function initializes common queries, so the results are available in S3.
The API implementation will search first in S3 before making calls to the Athena API. This will
increase performance and reduce cost.
"""

def handler(event, context):

    #get event data
    log.info("Received event {}".format(json.dumps(event)))
    accountid = event['accountId']
    year = event['year']
    month = event['month']

    apiprocessor = curapi.ApiProcessor(accountid, year, month)

    resultset = {'resultset':[]}
    result_dict = {'getTotalCost':resultset, 'getCostByService':resultset, 'getCostByUsageType':resultset,
                   'getCostByResource':resultset, 'getUsageByResourceId':resultset}

    result_dict['getTotalCost']['resultset'] = apiprocessor.getTotalCost()
    result_dict['getCostByService']['resultset'] = apiprocessor.getCostByService()
    result_dict['getCostByUsageType']['resultset'] = apiprocessor.getCostByUsageType()
    result_dict['getCostByResource']['resultset'] = apiprocessor.getCostByResource()

    log.info("Results:{}".format(json.dumps(result_dict,indent=4)))

    return event












