#!/usr/bin/python
import sys, os, json
import argparse
import traceback
import boto3
import utils
import gzip




s3sourceclient = None
s3destclient = None
s3resource = None
#itemDict = {}


ACTION_PREPARE_ATHENA = 'prepare-athena'
ACTION_PREPARE_QUICKSIGHT = 'prepare-quicksight'
ACTION_CREATE_MANIFEST = 'create-manifest'
ACTION_TEST_ROLE = 'test-role'
MANIFEST_TYPE_REDSHIFT = 'redshift'
MANIFEST_TYPE_QUICKSIGHT = 'quicksight'



def main(argv):

  parser = argparse.ArgumentParser()
  parser.add_argument('--action', help='', required=True)
  parser.add_argument('--manifest-type', help='', required=False)
  parser.add_argument('--year', help='', required=True)
  parser.add_argument('--month', help='', required=True)
  parser.add_argument('--limit', help='', required=False)
  parser.add_argument('--source-bucket', help='', required=True)
  parser.add_argument('--source-prefix', help='', required=True)
  parser.add_argument('--dest-bucket', help='', required=False)
  parser.add_argument('--dest-prefix', help='', required=False)
  parser.add_argument('--role-arn', help='', required=False)
  parser.add_argument('--xacct-source', help='', required=False)
  parser.add_argument('--xacct-dest', help='', required=False)


  if len(sys.argv) == 1:
    parser.print_help()
    sys.exit(1)
  args = parser.parse_args()

  action = ''
  sourceBucket = ''
  sourcePrefix = ''
  destBucket = ''
  destPrefix = ''
  manifestType = ''
  year = ''
  month = ''
  limit = 1000
  roleArn = ''
  xAccountSource = False
  xAccountDest = False

  if args.action: action=args.action
  if args.manifest_type: manifestType=args.manifest_type
  if args.year: year=args.year
  if args.month: month=args.month
  if args.source_bucket: sourceBucket=args.source_bucket
  if args.source_prefix: sourcePrefix= args.source_prefix
  if args.dest_bucket: destBucket= args.dest_bucket
  if args.dest_prefix: destPrefix= args.dest_prefix
  if args.role_arn: roleArn = args.role_arn
  if args.xacct_source: xAccountSource = True
  if args.xacct_dest: xAccountDest = True

  print ("xAccountSource: [{}] - xAccountDest[{}]".format(xAccountSource, xAccountDest))
  validate(action, sourceBucket,sourcePrefix, destBucket, destPrefix, limit)

  try:

    init_clients(xAccountSource, xAccountDest, roleArn)

    if action in (ACTION_PREPARE_ATHENA, ACTION_PREPARE_QUICKSIGHT):
      destS3keys = copy_to_dest(action, sourceBucket,sourcePrefix,destBucket,destPrefix, year, month, limit)
      create_manifest(MANIFEST_TYPE_QUICKSIGHT, destBucket,destPrefix, year, month, limit, destS3keys)
      create_manifest(MANIFEST_TYPE_REDSHIFT, destBucket,destPrefix, year, month, limit, destS3keys)
    if action == ACTION_CREATE_MANIFEST:
      create_manifest(manifestType, sourceBucket,sourcePrefix, year, month, limit, [])
    if action == ACTION_TEST_ROLE:
      test_role(sourceBucket,sourcePrefix, year, month)



  except Exception as e:
    traceback.print_exc()
    print("Exception message:["+str(e.message)+"]")


def init_clients(xAccountSource, xAccountDest, roleArn):
    global s3sourceclient
    global s3destclient
    global s3resource

    s3sourceclient = boto3.client('s3')
    s3resource = boto3.resource('s3')
    s3destclient = boto3.client('s3')

    if roleArn:
        stsclient = boto3.client('sts')
        stsresponse = stsclient.assume_role(RoleArn=roleArn, RoleSessionName='costAnalysis')
        #print ("sts.assume_role:[{}]".format(stsresponse))
        if 'Credentials' in stsresponse:
            accessKeyId = stsresponse['Credentials']['AccessKeyId']
            secretAccessKey = stsresponse['Credentials']['SecretAccessKey']
            sessionToken = stsresponse['Credentials']['SessionToken']
            if xAccountSource:
                s3sourceclient = boto3.client('s3',aws_access_key_id=accessKeyId, aws_secret_access_key=secretAccessKey,aws_session_token=sessionToken)
                s3resource = boto3.resource('s3',aws_access_key_id=accessKeyId, aws_secret_access_key=secretAccessKey,aws_session_token=sessionToken)
            if xAccountDest:
                s3destclient = boto3.client('s3',aws_access_key_id=accessKeyId, aws_secret_access_key=secretAccessKey,aws_session_token=sessionToken)



"""
This method copies AWS Cost and Usage report files to an S3 bucket, where they will
be used by either Athena or QuickSight. There are file preparation activities that
need to take place before reports can be used by Athena.

For example:
- Manifest files must not be included in S3 Athena bucket.
- No folders must exist underneath partition folders for Athena (i.e. 20170201-20170301)
- First row needs to be removed for Athena
- Create Athena files in Reduced Redundancy storage class.
- Define user-metadata that includes the report ID (folder AWS creates when it generates new Cost and Usage reports)
"""


def copy_to_dest(action, sourceBucket,sourcePrefix,destBucket,destPrefix, year, month, limit):

  period_prefix = get_period_prefix(year, month)
  sourcePrefix += period_prefix
  destPrefix += period_prefix

  report_keys = get_latest_report_keys(sourceBucket,sourcePrefix,s3sourceclient)
  destS3keys = []

  #Get content for all report files
  for rk in report_keys:
    tokens = rk.split("/")
    hash = tokens[len(tokens)-2]
    response = s3sourceclient.get_object(Bucket=sourceBucket, Key=rk)
    tmpLocalFolder = os.getcwd()+'/tmp'
    if not os.path.isdir(tmpLocalFolder): os.mkdir(tmpLocalFolder)
    tmpLocalKey = tmpLocalFolder+'/tmp_'+rk.replace("/","-")+'.csv.gz'#temporary file that is downloaded from S3, before any modifications take place
    finalLocalKey = tmpLocalFolder+'/'+hash+'.csv.gz'#final local file after any modifications take place
    fileToUpload = ''
    finalS3Key = ''

    #Download latest report as a tmp local file
    with open(tmpLocalKey, 'wb') as report:
        s3resource.Bucket(sourceBucket).download_fileobj(rk, report)

    #Read through the tmp local file and skip first line (for Athena)
    record_count = 0
    if action == ACTION_PREPARE_ATHENA:
        fileToUpload = finalLocalKey
        finalS3Key = destPrefix + "cost-and-usage-athena.csv.gz" #TODO:fix for the case when multiple files (report_keys) are listed in the manifest
        with gzip.open(tmpLocalKey, 'rb') as f:
            f.next()#skips first line for Athena files
            #Write contents to another tmp file, which will be uploaded to S3
            with gzip.open(finalLocalKey,'wb') as no_header:
                for line in f:
                    no_header.write(line)
                    record_count = record_count + 1

        print "Number of records: [{}]".format(record_count)

    #TODO:in the future, if we're using the files for QuickSight, do a Copy operation and don't download.
    if action == ACTION_PREPARE_QUICKSIGHT:
        fileToUpload = tmpLocalKey
        finalS3Key = destPrefix + "cost-and-usage-quicksight.csv.gz"


    print "Putting: [{}/{}] in [{}/{}]".format(sourceBucket,rk,destBucket,finalS3Key)

    with open(fileToUpload, 'rb') as data:
        s3destclient.upload_fileobj(data, destBucket, finalS3Key,
                                ExtraArgs={
                                    'Metadata':{'reportId':hash},
                                    'StorageClass':'REDUCED_REDUNDANCY'
                                })
        destS3keys.append(finalS3Key)

  return destS3keys



"""
Every time a new Cost and Usage report is generated, AWS updates a Manifest file with the S3 keys that
correspond to the latest report. This method gets those keys.
"""

def get_latest_report_keys(sourceBucket,sourcePrefix,s3client):
    result = []

    print "Getting report keys for bucket:[{}] - prefix:[{}]".format(sourceBucket,sourcePrefix)

    try:
        response = s3client.list_objects_v2(Bucket=sourceBucket,Prefix=sourcePrefix)
        #Get the latest manifest
        manifest_key = ''
        if 'Contents' in response:
            for o in response['Contents']:
                key = o['Key']
                post_prefix = key[key.find(sourcePrefix)+len(sourcePrefix):]
                if '-Manifest.json' in key and post_prefix.find("/") < 0:#manifest file is at top level after prefix and not inside one of the folders
                    manifest_key = key
                    break
    except Exception as e:
        print "There was a problem listing objects for bucket:[{}] and prefix [{}]".format(sourceBucket, sourcePrefix)
        print e.message



    try:
        print "Getting manifest [{}]".format(manifest_key)
        response = s3client.get_object(Bucket=sourceBucket, Key=manifest_key)
    except Exception as e:
        print "There was a problem getting object - bucket:[{}] - key [{}]".format(sourceBucket, manifest_key)
        print e.message


    if 'Body' in response:
        manifest_json = json.loads(response['Body'].read())
        if 'reportKeys' in manifest_json:
            result = manifest_json['reportKeys']

    print "Latest Cost and Usage report keys: [{}]".format(result)
    return result


"""
This function is only applicable when the caller of the script has assumed an IAM Role
in order to access objects in the destination S3 bucket.
"""

def test_role(sourceBucket,sourcePrefix, year, month):
    monthly_report_prefix = ""
    if year and month:monthly_report_prefix = get_period_prefix(year, month)
    latest_report_keys = get_latest_report_keys(sourceBucket,sourcePrefix+monthly_report_prefix, s3sourceclient)
    if latest_report_keys:
        print "xAccount Source test passed!"

"""
This operation creates a manifest based on the provided S3 location of Cost and Usage reports.
It puts the manifest in the same bucket and prefix as indicated.
It supports the creation of manifest files for Redshift and QuickSight.
"""
def create_manifest(type, bucket,prefix, year, month, limit, report_keys):

  monthly_report_prefix = ""
  if year and month:monthly_report_prefix = get_period_prefix(year, month)

  manifest = {}

  if not report_keys:
    report_keys = get_latest_report_keys(bucket,prefix+monthly_report_prefix, s3destclient)

  entries = []
  uris = []
  for key in report_keys:
      #TODO: manifest cannot point to more than 1000 files (add validation)
    uris.append("s3://"+bucket+"/"+key)
    if type == MANIFEST_TYPE_REDSHIFT:
      entries.append({"url":"s3://"+bucket+"/"+key,"mandatory":True})
    if len(entries) == limit: break


  manifest_file_name = ""
  if type == MANIFEST_TYPE_REDSHIFT:
      manifest['entries']=entries
      manifest_file_name = "billing-redshift-manifest-concurrencylabs.json"


  if type == MANIFEST_TYPE_QUICKSIGHT:
      manifest['fileLocations']=[{"URIs":uris}]
      manifest_file_name = "billing-quicksight-manifest-concurrencylabs.json"



  manifest_body = json.dumps(manifest,indent=4,sort_keys=False)
  print("Manifest ({}):{}".format(type, manifest_body))
  record_count = 0
  if len(uris):record_count = len(uris)
  if len(entries):record_count = len(entries)
  print "Number of files in manifest: [{}]".format(record_count)

  manifest_key = prefix+monthly_report_prefix+manifest_file_name
  if record_count:
    s3destclient.put_object(Bucket=bucket,Key=manifest_key,ACL='private',Body=manifest_body)
    print "Manifest S3 URL (this is the URL you provide in {}): [https://s3.amazonaws.com/{}/{}]".format(type, bucket,manifest_key)
  else:
    print "No entries found - did not write manifest"



def get_period_prefix(year, month):
  imonth = int(month)
  return "{}{:02d}01-{}{:02d}01/".format(year,imonth,year,imonth+1)



def validate(action, sourceBucket,sourcePrefix, destBucket, destPrefix, limit):

    valid_actions = [ACTION_PREPARE_ATHENA,ACTION_PREPARE_QUICKSIGHT, ACTION_CREATE_MANIFEST, ACTION_TEST_ROLE]
    message = ""
    validation_ok = True

    #TODO:dest bucket and dest prefix are mandatory if action is about preparing reports
    #TODO:partition bucket cannot be different for origin and destination
    #TODO:if roleArn is specified, either xAccountSource or xAccountDestination must be specified

    if action not in valid_actions:
        validation_ok = False
        message += "Invalid action, valid options are: {}".format(valid_actions)

    if action in (ACTION_PREPARE_ATHENA):
        if not utils.is_valid_prefix(destPrefix):
            validation_ok = False
            message += "Invalid Destination S3 Bucket prefix: [{}]".format(destPrefix)

    if (sourceBucket+sourcePrefix)==(destBucket+destPrefix):
        validation_ok = False
        message += "Source and destination locations cannot be the same\n"

    if not (limit >= 1 and limit <= 1000):
        validation_ok = False
        message += "Limit must be between 1 and 1000\n"

    if not validation_ok:
        print "Validation failed - aborting execution:\n" + message
        sys.exit()

    else:
        return validation_ok


if __name__ == "__main__":
   main(sys.argv[1:])
