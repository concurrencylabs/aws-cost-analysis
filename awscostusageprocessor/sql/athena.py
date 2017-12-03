import os, sys

__location__ = os.path.dirname(os.path.realpath(__file__))
site_pkgs = os.path.join(os.path.split(__location__)[0], "lib", "python2.7", "site-packages")
sys.path.append(site_pkgs)

import time, logging, json, datetime, pytz
import boto3, botocore
import awscostusageprocessor.utils as utils
import awscostusageprocessor.consts as consts

log = logging.getLogger()
log.setLevel(logging.INFO)



athenaclient = boto3.client('athena')
s3resource = boto3.resource('s3')
ddbclient = boto3.client('dynamodb')


QUERY_EXECUTIONS_FOLDER = 'queryexecutions'
QUERY_METADATA_FOLDER = 'querymetadata'



class AthenaQueryMgr():
    def __init__(self,athena_base_output_s3_bucket, accountid, year, month):
        #Athena query output is placed in a bucket and prefix with the account id and month.
        self.athena_output_s3_location = "{}/{}/{}".format(athena_base_output_s3_bucket, accountid, utils.get_period_prefix(year, month))
        self.athena_result_configuration = {'OutputLocation': self.athena_output_s3_location+QUERY_EXECUTIONS_FOLDER+"/", 'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}}
        self.dbname = "costusage_"+accountid
        self.tablename = "hourly_"+utils.get_period_prefix(year, month).replace("-","_").replace("/","")
        self.payerAccountid = accountid


    #TODO: add option to execute synchronously or asynchronously
    def execute_query(self, queryid, querystring):
        log.info("Query: {}".format(querystring))

        #Database management queries such as create database, create table or drop table should always execute fresh
        if queryid in (consts.QUERY_ID_CREATE_DATABASE, consts.QUERY_ID_DROP_TABLE, consts.QUERY_ID_CREATE_TABLE):
            runnew = True
        else:
            runnew, queryexecutionid = self.should_run_fresh(queryid)

        querystate = ''
        if runnew:
            log.info("Running fresh Athena query")
            start_query_response = athenaclient.start_query_execution(QueryString=querystring, ResultConfiguration=self.athena_result_configuration)
            queryexecutionid = self.get_queryexecutionid(start_query_response)
            log.info("QueryExecutionId: {}".format(queryexecutionid))
            self.create_query_metadata(queryid, queryexecutionid)
            querystate = self.poll_query_state(queryexecutionid, 100)
        else:
            log.info("Fetching results for query [{}] based on existing queryExecutionId: [{}]".format(queryid, queryexecutionid))
            querystate = self.poll_query_state(queryexecutionid, 1)
        return queryexecutionid, querystate


    """
    This method determines if the query should be re-executed in Athena or if results should be fetched
    based on the Athena queryid
    """
    def should_run_fresh(self, queryid):
        result = True
        queryexecutionid = ''
        queryexecutionts = ''
        lastProcessedTimestamp = datetime.datetime(2050,01,01)

        #get the last Cost and Usage processed timestamp from DynamoDB based on accountid
        response = ddbclient.get_item(TableName=consts.AWS_ACCOUNT_METADATA_DDB_TABLE,
                                        Key={'awsPayerAccountId': {'S': self.payerAccountid }},
                                        AttributesToGet=['lastProcessedTimestamp'],
                                        ConsistentRead=False)
        if 'Item' in response:
            item = response['Item']
            lastProcessedTimestamp = datetime.datetime.strptime(item['lastProcessedTimestamp']['S'], consts.TIMESTAMP_FORMAT)


        #get latest execution timestamp from S3, based on the query id and accountid
        bucket = self.get_athena_query_output_s3_bucket()
        key = self.get_athena_query_output_s3_key(bucket, queryid)
        querymetadatabody = {}
        try:
            if s3resource.Object(bucket,key).get():
                querymetadatabody = json.loads(s3resource.Object(bucket,key).get()['Body'].read())
            log.info("Looking for existing metadata: {}/{}: {}".format(bucket,key,json.dumps(querymetadatabody, indent=4)))
            if 'queryExecutionId' in querymetadatabody: queryexecutionid = querymetadatabody['queryExecutionId']
            if 'queryExecutionTimestamp' in querymetadatabody: queryexecutionts = querymetadatabody['queryExecutionTimestamp']
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                queryexecutionts = consts.EPOCH_TS

        #if there's a new processed CUR (query execution timestamp < CUR timestamp), then get query fresh
        #else, get queryexecutionid
        if  lastProcessedTimestamp < datetime.datetime.strptime(queryexecutionts, consts.TIMESTAMP_FORMAT):
            result = False

        if not queryexecutionid: result = True

        log.info("queryid: [{}] - lastProcessedTimestamp: [{}] - run query from Athena: [{}] - queryexecutionid: [{}] - queryexecutionts: [{}]".
                 format(queryid, lastProcessedTimestamp.strftime(consts.TIMESTAMP_FORMAT), result,queryexecutionid, queryexecutionts))
        return result, queryexecutionid


    """
    Athena query executions take some time to complete. This method polls the execution state until there is a result (or failure).
    """
    #TODO: send a timeout parameter
    def poll_query_state(self,queryexecutionid, sleep_ms):
        querystate = ''
        while True:
            time.sleep(sleep_ms/1000)
            queryexecution = athenaclient.get_query_execution(QueryExecutionId=queryexecutionid)
            querystate = queryexecution['QueryExecution']['Status']['State']
            if querystate == consts.ATHENA_QUERY_STATE_FAILED:
                if 'StateChangeReason' in queryexecution['QueryExecution']['Status']:
                    querystatereason = queryexecution['QueryExecution']['Status']['StateChangeReason']
                    log.info("querystate [{}] reason [{}]".format(querystate, querystatereason ))
            else:
                log.info("querystate:{}".format(querystate))
            if querystate in [consts.ATHENA_QUERY_STATE_FAILED,consts.ATHENA_QUERY_STATE_CANCELLED,consts.ATHENA_QUERY_STATE_SUCCEEDED]:break
        return querystate


    def get_queryexecutionid(self,response):
        queryexecutionid = ''
        if 'QueryExecutionId' in response: queryexecutionid = response['QueryExecutionId']
        return queryexecutionid

    """
    This method returns query results as an array of dictionaries, which can be converted to a JSON object
    """
    def get_query_execution_results(self, queryexecutionid):
        result = []
        #TODO: validate first that the query execution status is 'SUCCEEDED'
        queryresults = athenaclient.get_query_results(QueryExecutionId=queryexecutionid)#TODO:paginate
        log.debug("queryresults: {}".format(json.dumps(queryresults,indent=4)))
        rowheaders = queryresults['ResultSet']['Rows'][0]['Data']#The first item in ['ResultSet']['Rows'] contains a list of the column names
        rowindex = 0
        for r in queryresults['ResultSet']['Rows']:
            row_dict = {}
            if rowindex > 0:#skip column names
                columnindex = 0
                for columnvalue in r['Data']:
                    row_dict[rowheaders[columnindex]['VarCharValue']] = columnvalue['VarCharValue']
                    columnindex += 1
                result.append(row_dict)
            rowindex += 1
        return result

    def drop_table(self):
        querystring = "DROP TABLE {}.{}".format(self.dbname, self.tablename)
        return self.execute_query(consts.QUERY_ID_DROP_TABLE, querystring)

    def create_table(self, curManifest, curS3Bucket, curS3Prefix):
        return self.execute_query(consts.QUERY_ID_CREATE_TABLE, self.get_create_table_query(curManifest, curS3Bucket, curS3Prefix))

    def get_create_table_query(self, curManifest, curS3Bucket, curS3Prefix):
        querystring = "CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} (\n".format(self.dbname, self.tablename)
        i = 0
        for c in curManifest.get('columns',[]):
            if i: querystring += ",\n"
            column_type = 'string'
            #if c['name'].lower() in ('billingperiodstartdate','billingperiodenddate','usagestartdate','usageenddate'):
            #    column_type = 'timestamp'
            querystring += "`{}_{}` {}".format(c['category'].lower(),c['name'].lower().replace(':','_'),column_type)
            i += 1
        querystring += " )\n" \
                        " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" \
                            "WITH SERDEPROPERTIES ( \n" \
                            "'separatorChar' = ',', \n" \
                            "'quoteChar' = '\\\"', \n" \
                            "'escapeChar' = '\\\\' \n" \
                        ") \n" \
                        "STORED AS TEXTFILE \n" \
                        "LOCATION 's3://{}/{}';".format(curS3Bucket,curS3Prefix)
        return querystring


    def create_database(self):
        querystring = "CREATE DATABASE IF NOT EXISTS {}".format(self.dbname)
        return self.execute_query(consts.QUERY_ID_CREATE_DATABASE, querystring)

    def get_databases(self):
        result = []
        querystring = "SHOW DATABASES"
        queryexecutionid, querystate = self.execute_query('show_databases', querystring)
        if querystate == consts.ATHENA_QUERY_STATE_SUCCEEDED:
            queryresults = athenaclient.get_query_results(QueryExecutionId=queryexecutionid)#TODO:paginate
            for r in queryresults['ResultSet']['Rows']:
                result.append(r['Data'][0]['VarCharValue'])
        return result


    """
    SQL statements in the config file have placeholders for parameters such as dbname and table.
    This function replaces those placeholders with real values.
    """
    def replace_params(self, sqlstatement, **kargs):
        result = sqlstatement.replace("{dbname}", self.dbname).replace("{tablename}", self.tablename)
        if kargs:
            for k in kargs.keys():
                placeholder = '{'+k+'}'
                if placeholder in result:
                    result = result.replace(placeholder,kargs[k])
        return result


    """
    Query metadata is used to find a valid previous execution of a query type and avoid
    querying Athena every time a customer requests data that has already been queried using
    the most recent Cost and Usage report.
    """

    def create_query_metadata(self, queryid, queryexecutionid):
        #create queryid metadata and upload to S3
        bucket = self.get_athena_query_output_s3_bucket()
        key = self.get_athena_query_output_s3_key(bucket, queryid)
        log.info("query metadata s3 location - bucket: [{}] key:[{}]".format(bucket, key))
        querymetadata = s3resource.Object(bucket,key)

        response = athenaclient.get_query_execution(QueryExecutionId=queryexecutionid)

        metadatabody = {
            "queryExecutionId":queryexecutionid,
            "queryExecutionTimestamp":datetime.datetime.now(pytz.utc).strftime(consts.TIMESTAMP_FORMAT)
        }

        querymetadata.put(Body=json.dumps(metadatabody, indent=4), StorageClass='REDUCED_REDUNDANCY')


    def get_athena_query_output_s3_bucket(self):
        return self.athena_output_s3_location.split("/")[2]

    def get_athena_query_output_s3_key(self, bucket, queryid):
        return self.athena_output_s3_location.split(bucket)[1][1:]+QUERY_METADATA_FOLDER+"/"+queryid+".json"


