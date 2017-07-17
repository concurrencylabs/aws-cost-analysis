import logging
import ConfigParser
import sys, os

__location__ = os.path.dirname(os.path.realpath(__file__))
sql_path = os.path.join(os.path.split(__location__)[0],"awscostusageprocessor","sql")
site_pkgs = os.path.join(os.path.split(__location__)[0], "lib", "python2.7", "site-packages")
sys.path.append(sql_path)
sys.path.append(site_pkgs)
config = ConfigParser.RawConfigParser()
config.read(sql_path+'/queries.properties')


from awscostusageprocessor.sql import athena as ath
from awscostusageprocessor import consts as consts


log = logging.getLogger()
log.setLevel(logging.INFO)


"""
This class takes care of API operations. It doesn't integrate with Athena directly
and it doesn't act as a request handler. It serves as a middle layer between an
API request handler (a Lambda function) and the Athena data access class.
"""

class ApiProcessor():

    def __init__(self, accountid, year, month):
        self.accountid = accountid
        self.year = year
        self.month = month
        self.athena = ath.AthenaQueryMgr(consts.ATHENA_BASE_OUTPUT_S3_BUCKET, accountid, year, month)

    def getResultSet(self, action, **kargs):
        result = []
        sqlstatement = self.athena.replace_params(config.get('queries',action),**kargs)
        log.info("\nQuery type: {}".format(action))
        queryexecutionid, querystate = self.athena.execute_query(action, sqlstatement)
        if querystate == consts.ATHENA_QUERY_STATE_SUCCEEDED:
            result = self.athena.get_query_execution_results(queryexecutionid)
        return result

    def getTotalCost(self):
        #TODO: do mapping between SQL columns and API field names that will be returned
        return self.getResultSet(consts.ACTION_GET_TOTAL_COST)

    def getCostByService(self):
        return self.getResultSet(consts.ACTION_GET_COST_BY_SERVICE)

    def getCostByUsageType(self):
        return self.getResultSet(consts.ACTION_GET_COST_BY_USAGE_TYPE)

    def getCostByResource(self):
        return self.getResultSet(consts.ACTION_GET_COST_BY_RESOURCE)

    #TODO:Implement for all resources, when resourceid is empty
    def getUsageByResourceId(self, resourceid):
        return self.getResultSet(consts.ACTION_GET_USAGE_BY_RESOURCE_ID, resourceid=resourceid)

    """
        sqlstatement = athena.adjustsql(config.get('resources',consts.ACTION_GET_ACTIVE_RESOURCES))
        log.info("\nQuery type: {}".format(consts.ACTION_GET_ACTIVE_RESOURCES))
        athena.execute_query(consts.ACTION_GET_ACTIVE_RESOURCES, sqlstatement)

        sqlstatement = athena.adjustsql(config.get('resources',consts.ACTION_GET_RESOURCE_BY_SERVICE))
        log.info("\nQuery type: {}".format(consts.ACTION_GET_RESOURCE_BY_SERVICE, consts.ACTION_GET_RESOURCE_BY_SERVICE))
        athena.execute_query(consts.ACTION_GET_RESOURCE_BY_SERVICE, sqlstatement)
    """





















