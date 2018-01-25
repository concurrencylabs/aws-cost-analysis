#!/usr/bin/python
import sys
import json
import gzip
import os
import traceback
import boto3
import utils, consts
from errors import ManifestNotFoundError

from botocore.exceptions import ClientError

class CostUsageProcessor():
    def __init__(self, **args):
        self.s3sourceclient = None
        self.s3destclient = None
        self.s3resource = None

        self.sourceBucket = ''
        if 'sourceBucket' in args: self.sourceBucket = args['sourceBucket']

        self.sourcePrefix = ''
        if 'sourcePrefix' in args: self.sourcePrefix = args['sourcePrefix']

        self.destBucket = ''
        if 'destBucket' in args: self.destBucket = args['destBucket']

        self.destPrefix = ''
        if 'destPrefix' in args: self.destPrefix = args['destPrefix']

        self.year = ''
        if 'year' in args: self.year = args['year']

        self.month = ''
        if 'month' in args: self.month = args['month']

        self.limit = 1000
        if 'limit' in args: self.limit = args['limit']

        self.xAccountSource = False
        if 'xAccountSource' in args: self.xAccountSource = args['xAccountSource']

        self.xAccountDest = False
        if 'xAccountDest' in args: self.xAccountDest = args['xAccountDest']

        self.roleArn = ''
        if 'roleArn' in args: self.roleArn = args['roleArn']

        self.accountId = args.get('accountId','')

        self.validate()
        self.init_clients()

        #self.accountId = args.get('accountId','')

        self.curManifestJson = self.get_aws_manifest_content()

        if not self.accountId:
            self.accountId = self.curManifestJson.get('account','')



    """
    This method copies AWS Cost and Usage report files to an S3 bucket, where they will
    be used by either Athena or QuickSight. There are file preparation activities that
    need to take place before reports can be used by Athena.

    For example:
    - Manifest files must not be included in S3 Athena bucket.
    - No folders must exist underneath partition folders for Athena (i.e. data files should live under 20170201-20170301)
    - First row needs to be removed for Athena
    - Create Athena files in Reduced Redundancy storage class.
    - Define user-metadata that includes the report ID hash (folder AWS creates when it generates new Cost and Usage reports)
    """

    def process_latest_aws_cur(self, action):

      if action in (consts.ACTION_PREPARE_ATHENA, consts.ACTION_PREPARE_QUICKSIGHT):
        if not utils.is_valid_prefix(self.destPrefix):
          raise Exception ("Invalid Destination S3 Bucket prefix: [{}]".format(self.destPrefix))

      period_prefix = utils.get_period_prefix(self.year, self.month)
      monthSourcePrefix = self.sourcePrefix + period_prefix
      monthDestPrefix = self.destPrefix + period_prefix
      report_keys = self.get_latest_aws_cur_keys(self.sourceBucket, monthSourcePrefix, self.s3sourceclient )
      destS3keys = []

      #Get content for all report files
      for rk in report_keys:
        tokens = rk.split("/")
        hash = tokens[len(tokens)-2]
        response = self.s3sourceclient.get_object(Bucket=self.sourceBucket, Key=rk)
        if '/var/task' in os.getcwd(): #executing as a Lambda function
            tmpLocalFolder = '/tmp'
        else:
            tmpLocalFolder = os.getcwd()+'/tmp'

        if not os.path.isdir(tmpLocalFolder): os.mkdir(tmpLocalFolder)
        tmpLocalKey = tmpLocalFolder+'/tmp_'+rk.replace("/","-")+'.csv.gz'#temporary file that is downloaded from S3, before any modifications take place
        finalLocalKey = tmpLocalFolder+'/'+hash+'.csv.gz'#final local file after any modifications take place
        fileToUpload = ''
        finalS3Key = ''

        #Download latest report as a tmp local file
        with open(tmpLocalKey, 'wb') as report:
            self.s3resource.Bucket(self.sourceBucket).download_fileobj(rk, report)

        #Read through the tmp local file and skip first line (for Athena)
        record_count = 0
        if action == consts.ACTION_PREPARE_ATHENA:
            fileToUpload = finalLocalKey
            finalS3Key = monthDestPrefix + "cost-and-usage-athena.csv.gz" #TODO:test for case when multiple files (report_keys) are listed in the manifest
            with gzip.open(tmpLocalKey, 'rb') as f:
                f.next()#skips first line for Athena files
                #Write contents to another tmp file, which will be uploaded to S3
                with gzip.open(finalLocalKey,'ab') as no_header:
                    for line in f:
                        no_header.write(line)
                        record_count = record_count + 1

            print "Number of records: [{}]".format(record_count)

        #TODO:if we're using the files for QuickSight, do a Copy operation and don't download.
        if action == consts.ACTION_PREPARE_QUICKSIGHT:
            fileToUpload = tmpLocalKey
            finalS3Key = monthDestPrefix + "cost-and-usage-quicksight.csv.gz"

        print "Putting: [{}/{}] in [{}/{}]".format(self.sourceBucket,rk,self.destBucket,finalS3Key)

        with open(fileToUpload, 'rb') as data:
            self.s3destclient.upload_fileobj(data, self.destBucket, finalS3Key,
                                    ExtraArgs={
                                        'Metadata':{'reportId':hash},
                                        'StorageClass':'REDUCED_REDUNDANCY'
                                    })
            destS3keys.append(finalS3Key)

      return destS3keys


    """
    Every time a new Cost and Usage report is generated, AWS updates a Manifest file with the S3 keys that
    correspond to the latest report. This method gets the location of that Manifest file.
    """

    def get_latest_aws_manifest_key(self):
        manifestprefix = self.sourcePrefix + utils.get_period_prefix(self.year, self.month)
        print "Getting Manifest key for acccount:[{}] - bucket:[{}] - prefix:[{}]".format(self.accountId, self.sourceBucket,manifestprefix)
        manifest_key = ''
        try:
            response = self.s3sourceclient.list_objects_v2(Bucket=self.sourceBucket,Prefix=manifestprefix)
            #Get the latest manifest
            if 'Contents' in response:
                for o in response['Contents']:
                    key = o['Key']
                    post_prefix = key[key.find(manifestprefix)+len(manifestprefix):]
                    if '-Manifest.json' in key and post_prefix.find("/") < 0:#manifest file is at top level after prefix and not inside one of the folders
                        manifest_key = key
                        break
        except Exception as e:
            print "Error when getting manifest key for acccount:[{}] - bucket:[{}] - key:[{}]".format(self.accountId, self.sourceBucket, manifest_key)
            print e.message
            traceback.print_exc()

        if not manifest_key:
            raise ManifestNotFoundError("Could not find manifest file in bucket:[{}], key:[{}]".format(self.sourceBucket, manifest_key))

        return manifest_key



    """
    Every time a new Cost and Usage report is generated, AWS updates a Manifest file with the S3 keys that
    correspond to the latest report. This method gets those keys.
    """

    def get_latest_aws_cur_keys(self, bucket, prefix, s3client):
        result = []
        print "Getting report keys for bucket:[{}] - prefix:[{}]".format(bucket,prefix)
        manifest_key = self.get_latest_aws_manifest_key()
        response = {}
        try:
            print "Getting contents of manifest [{}]".format(manifest_key)
            response = s3client.get_object(Bucket=bucket, Key=manifest_key)
        except Exception as e:
            print "There was a problem getting object - bucket:[{}] - key [{}]".format(bucket, manifest_key)
            print e.message

        if 'Body' in response:
            manifest_json = json.loads(response['Body'].read())
            if 'reportKeys' in manifest_json:
                result = manifest_json['reportKeys']

        print "Latest Cost and Usage report keys: [{}]".format(result)
        return result


    """
    This method gets the AWS Account ID from the Manifest file. This is used for Athena resource creation and
    Lambda functions that need to know the AWS Account ID.
    NOTE: replaced by get_aws_manifest_content, which can be used to extract not just accountId, but all other fields
    in the CUR manifest
    """
    """
    def get_account_id_from_aws_manifest(self):
        result = ''
        manifest_key = self.get_latest_aws_manifest_key()
        print "Getting accountId from manifest file - bucket: [{}] - key: [{}]".format(self.sourceBucket, manifest_key)
        response = self.s3sourceclient.get_object(Bucket=self.sourceBucket, Key=manifest_key)
        if 'Body' in response:
            manifest_json = json.loads(response['Body'].read())
            if 'account' in manifest_json:
                result = manifest_json['account']
        return result
    """

    #TODO: this function is redundant, we can get the lastmodified_ts from the call to S3 in method get_aws_manifest_content and update the instance of CostUsageProcessor
    def get_aws_manifest_lastmodified_ts(self):
        result = ''
        manifest_key = self.get_latest_aws_manifest_key()
        print "Getting creation timestamp from manifest file - bucket: [{}] - key: [{}]".format(self.sourceBucket, manifest_key)
        response = self.s3sourceclient.get_object(Bucket=self.sourceBucket, Key=manifest_key)
        if 'LastModified' in response:
            result = response['LastModified']
        return result

    """
    Returns a JSON object representing the AWS Cost and Usage Report manifest
    """
    def get_aws_manifest_content(self):
        result = {}
        manifest_key = self.get_latest_aws_manifest_key()
        print "Getting manifest file JSON content - bucket: [{}] - key: [{}]".format(self.sourceBucket, manifest_key)
        response = self.s3sourceclient.get_object(Bucket=self.sourceBucket, Key=manifest_key)
        if 'Body' in response:
            result = json.loads(response['Body'].read())
        return result


    """
    This function is only applicable when the caller of the script has assumed an IAM Role
    in order to access objects in the destination S3 bucket.
    """
    def test_role(self):
        monthly_report_prefix = ""
        if self.year and self.month:monthly_report_prefix = utils.get_period_prefix(self.year, self.month)
        latest_report_keys = self.get_latest_aws_cur_keys(self.sourceBucket,self.sourcePrefix+monthly_report_prefix, self.s3sourceclient)
        if latest_report_keys:
            print "xAccount Source test passed!"


    """
    This operation creates a manifest based on the provided S3 location of Cost and Usage reports.
    It puts the manifest in the same bucket and prefix as indicated.
    It supports the creation of manifest files for Redshift and QuickSight.
    """
    def create_manifest(self, type, bucket,prefix, report_keys):

      monthly_report_prefix = ""
      if self.year and self.month: monthly_report_prefix = utils.get_period_prefix(self.year, self.month)

      manifest = {}

      #report_keys can by any array of keys. If it's not provided, then we get the ones generated by AWS
      if not report_keys:
        report_keys = self.get_latest_aws_cur_keys(bucket,prefix+monthly_report_prefix, self.s3destclient)

      entries = []
      uris = []
      for key in report_keys:
          #TODO: manifest cannot point to more than 1000 files (add validation)
        uris.append("s3://"+bucket+"/"+key)
        if type == consts.MANIFEST_TYPE_REDSHIFT:
          entries.append({"url":"s3://"+bucket+"/"+key,"mandatory":True})
        if len(entries) == self.limit: break

      manifest_file_name = ""
      if type == consts.MANIFEST_TYPE_REDSHIFT:
          manifest['entries']=entries
          manifest_file_name = "billing-redshift-manifest-concurrencylabs.json"

      if type == consts.MANIFEST_TYPE_QUICKSIGHT:
          manifest['fileLocations']=[{"URIs":uris}]
          manifest_file_name = "billing-quicksight-manifest-concurrencylabs.json"

      manifest_body = json.dumps(manifest,indent=4,sort_keys=False)
      print("Manifest ({}):{}".format(type, manifest_body))
      record_count = 0
      if len(uris):record_count = len(uris)
      if len(entries):record_count = len(entries)
      print "Number of files in manifest: [{}]".format(record_count)

      #TODO: validate that no athena files exist in S3 destination, before creating manifest
      manifest_key = prefix+monthly_report_prefix+manifest_file_name
      if record_count:
        self.s3destclient.put_object(Bucket=bucket,Key=manifest_key,ACL='private',Body=manifest_body)
        print "Manifest S3 URL (this is the URL you provide in {}): [https://s3.amazonaws.com/{}/{}]".format(type, bucket,manifest_key)
      else:
        print "No entries found - did not write manifest"


    def validate(self):

        message = ""
        #TODO:dest bucket and dest prefix are mandatory if action is about preparing reports
        #TODO:partition bucket cannot be different for origin and destination
        #TODO:if roleArn is specified, either xAccountSource or xAccountDestination must be specified

        if not self.sourceBucket:
            message += "Source bucket cannot be empty"

        if not self.destBucket:
            message += "Destination bucket cannot be empty"

        if (self.sourceBucket+self.sourcePrefix)==(self.destBucket+self.destPrefix):
            message += "Source and destination locations cannot be the same\n"

        if not (self.limit >= 1 and self.limit <= 1000):
            message += "Limit must be between 1 and 1000\n"

        if message:
            raise Exception(message)
        else:
            return True



    def init_clients(self):

        self.s3sourceclient = boto3.client('s3')
        self.s3resource = boto3.resource('s3') #TODO rename to something that describes whether it's destination or source
        self.s3destclient = boto3.client('s3')

        if self.roleArn and (self.xAccountSource or self.xAccountDest):
            lambda_owner_aws_access_key_id = ''
            lambda_owner_aws_secret_access_key = ''
            if 'LAMBDA_OWNER_AWS_ACCESS_KEY_ID' in os.environ and 'LAMBDA_OWNER_AWS_SECRET_ACCESS_KEY' in os.environ:
                lambda_owner_aws_access_key_id = os.environ['LAMBDA_OWNER_AWS_ACCESS_KEY_ID']
                lambda_owner_aws_secret_access_key = os.environ['LAMBDA_OWNER_AWS_SECRET_ACCESS_KEY']
                #If running from inside a Lambda function, assume role using the function's owner's credentials (and not the temp credentials given to the function)
                masterstsclient = boto3.client('sts', aws_access_key_id=lambda_owner_aws_access_key_id, aws_secret_access_key=lambda_owner_aws_secret_access_key)
                #instead of using the masterstsclient, we get a session token, otherwise we run into AccessDenied exceptions when using the same master credentials for assuming roles for multiple customer accounts
                sessionToken = masterstsclient.get_session_token()
                stsclient = boto3.client('sts', aws_access_key_id=sessionToken['Credentials']['AccessKeyId'],
                                                aws_secret_access_key=sessionToken['Credentials']['SecretAccessKey'],
                                                aws_session_token=sessionToken['Credentials']['SessionToken']
                                                )
                #stsclient = boto3.client('sts', aws_access_key_id=lambda_owner_aws_access_key_id, aws_secret_access_key=lambda_owner_aws_secret_access_key)

            else:
                #Assume role using the AWS credentials configured in the environment
                stsclient = boto3.client('sts')
            print ("Assuming role [{}]".format(self.roleArn))
            stsresponse = stsclient.assume_role(RoleArn=self.roleArn, RoleSessionName='costAnalysis')

            if 'Credentials' in stsresponse:
                accessKeyId = stsresponse['Credentials']['AccessKeyId']
                secretAccessKey = stsresponse['Credentials']['SecretAccessKey']
                sessionToken = stsresponse['Credentials']['SessionToken']
                if self.xAccountSource:
                    print("Getting xAcct S3 source client")
                    self.s3sourceclient = boto3.client('s3',aws_access_key_id=accessKeyId, aws_secret_access_key=secretAccessKey,aws_session_token=sessionToken)
                    self.s3resource = boto3.resource('s3',aws_access_key_id=accessKeyId, aws_secret_access_key=secretAccessKey,aws_session_token=sessionToken)
                if self.xAccountDest:
                    print("Getting xAcct S3 dest client")
                    self.s3destclient = boto3.client('s3',aws_access_key_id=accessKeyId, aws_secret_access_key=secretAccessKey,aws_session_token=sessionToken)
