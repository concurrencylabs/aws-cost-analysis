#!/usr/bin/python
import sys, os, json
import argparse
import traceback
import boto3
import utils
import gzip


s3client = boto3.client('s3')
s3resource = boto3.resource('s3')
itemDict = {}


ACTION_PREPARE_ATHENA = 'prepare-athena'
ACTION_PREPARE_QUICKSIGHT = 'prepare-quicksight'
ACTION_CREATE_MANIFEST = 'create-manifest'
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

  if args.action: action=args.action
  if args.manifest_type: manifestType=args.manifest_type
  if args.year: year=args.year
  if args.month: month=args.month
  if args.limit: limit=int(args.limit)
  if args.source_bucket: sourceBucket=args.source_bucket
  if args.source_prefix: sourcePrefix= args.source_prefix
  if args.dest_bucket: destBucket= args.dest_bucket
  if args.dest_prefix: destPrefix= args.dest_prefix


  validate(action, sourceBucket,sourcePrefix, destBucket, destPrefix, limit)


  try:

    if action in (ACTION_PREPARE_ATHENA, ACTION_PREPARE_QUICKSIGHT):
      copy_to_dest(action, sourceBucket,sourcePrefix,destBucket,destPrefix, year, month, limit)
      if action == ACTION_PREPARE_QUICKSIGHT:
        create_manifest(MANIFEST_TYPE_REDSHIFT, destBucket,destPrefix, year, month, limit)
        create_manifest(MANIFEST_TYPE_QUICKSIGHT, destBucket,destPrefix, year, month, limit)
    if action == ACTION_CREATE_MANIFEST:
      create_manifest(manifestType, sourceBucket,sourcePrefix, year, month, limit)



  except Exception as e:
    traceback.print_exc()
    print("Exception message:["+str(e.message)+"]")


"""
This method copies AWS Cost and Usage report files to an S3 bucket, where they will
be used by either Athena or QuickSight. There are file preparation activities that
need to take place before reports can be used by Athena or QuickSight.

For example:
- Manifest files must not be included in S3 Athena bucket.
- No folders must not exist underneath partition folders for Athena (i.e. 20170201-20170301)
- There needs to be deduplication of records across multiple files
- First row needs to be removed for Athena
- QuickSight manifest needs to be created
"""


def copy_to_dest(action, sourceBucket,sourcePrefix,destBucket,destPrefix, year, month, limit):

  period_prefix = get_period_prefix(year, month)
  sourcePrefix += period_prefix
  destPrefix += period_prefix

  response = s3client.list_objects_v2(Bucket=sourceBucket,Prefix=sourcePrefix, MaxKeys=limit)

  if not os.path.isdir(os.getcwd()+'/tmp'): os.mkdir(os.getcwd()+'/tmp')

  for o in response['Contents']:
      bkey = o['Key']
      #Ignore manifest files
      if '.csv.gz' in bkey:
        tokens = bkey.split("/")
        hash = tokens[len(tokens)-2]
        zipfile  = tokens[len(tokens)-1]
        #athkey = bkey.replace(billingPrefix,athenaPrefix,1)
        #Remove hash folder from object keys
        destkey = bkey.replace(sourcePrefix,destPrefix,1).replace(hash+"/"+zipfile,hash+"-"+zipfile,1)
        tmpKey = os.getcwd()+'/tmp/tmp_'+hash+'.csv.gz'
        finalKey = os.getcwd()+'/tmp/'+hash+'.csv.gz'
        with open(tmpKey, 'wb') as report:
            s3resource.Bucket(sourceBucket).download_fileobj(bkey, report)
        try:
            with gzip.open(tmpKey, 'rb') as f:
                if action == ACTION_PREPARE_ATHENA: f.next()#skips first line for Athena files
                record_count = 0
                with gzip.open(finalKey,'wb') as no_header:
                    for line in f:
                        tokens = line.split(",")
                        #key consists of: identity/LineItemId+identity/TimeInterval+bill/PayerAccountId+lineItem/ProductCode+lineItem/UsageType+lineItem/Operation+lineItem/ResourceId
                        itemKey = tokens[0]+tokens[1]+tokens[5]+tokens[12]+tokens[13]+tokens[14]+tokens[16]
                        #print("itemKey:[{}]".format(itemKey))
                        #AWS often produces duplicate records in different files. Therefore it's necessary to validate for uniqueness
                        if itemKey not in itemDict:
                            no_header.write(line)
                            itemDict[itemKey]=True
                            record_count = record_count + 1

                if record_count:
                    print("Putting: [{}/{}] in [{}/{}]".format(sourceBucket,bkey,destBucket,destkey))
                    s3resource.Object(destBucket,destkey).upload_file(finalKey)
        except Exception as e:
            print str(e)




def create_manifest(type, sourceBucket,sourcePrefix, year, month, limit):

  monthly_report_prefix = ""
  if year and month:monthly_report_prefix = get_period_prefix(year, month)

  manifest = {}
  entries = []
  response = s3client.list_objects_v2(Bucket=sourceBucket,Prefix=sourcePrefix+monthly_report_prefix, MaxKeys=limit)

  contents = []
  uris = []
  if 'Contents' in response: contents = response['Contents']
  for obj in contents:
      bkey = obj['Key']
      #TODO: manifest cannot point to more than 1000 files (add validation)
      if '.csv' in bkey:
        uris.append("s3://"+sourceBucket+"/"+bkey)
        if type == MANIFEST_TYPE_REDSHIFT:
          entries.append({"url":"s3://"+sourceBucket+"/"+bkey,"mandatory":True})
        if len(entries) == limit: break


  manifest_file_name = ""
  if type == MANIFEST_TYPE_REDSHIFT:
      manifest['entries']=entries
      manifest_file_name = "billing-redshift-manifest-concurrencylabs.json"

  if type == MANIFEST_TYPE_QUICKSIGHT:
      manifest['fileLocations']=[{"URIs":uris}]
      manifest_file_name = "billing-quicksight-manifest-concurrencylabs.json"



  manifest_body = json.dumps(manifest,indent=4,sort_keys=False)
  print("Manifest: ["+manifest_body+"]")
  print "Number of files in manifest: [{}]".format(len(uris))

  manifest_key = sourcePrefix+monthly_report_prefix+manifest_file_name
  s3client.put_object(Bucket=sourceBucket,Key=manifest_key,ACL='public-read',Body=manifest_body)
  print "Manifest S3 URL: [https://s3.amazonaws.com/{}/{}]".format(sourceBucket,manifest_key)



def get_period_prefix(year, month):
  imonth = int(month)
  return "{}{:02d}01-{}{:02d}01/".format(year,imonth,year,imonth+1)



def validate(action, sourceBucket,sourcePrefix, destBucket, destPrefix, limit):

    valid_actions = [ACTION_PREPARE_ATHENA, ACTION_PREPARE_QUICKSIGHT, ACTION_CREATE_MANIFEST]
    message = ""
    validation_ok = True

    #TODO:dest bucket and dest prefix are mandatory if action is about preparing reports
    #TODO:dest bucket must be different than origin bucket
    #TODO:dest bucket + prefix cannot be the same as origin!!!
    #TODO:partition bucket cannot be different for origin and destination

    if action not in valid_actions:
        validation_ok = False
        message += "Invalid action, valid options are: {}".format(valid_actions)

    if action in (ACTION_PREPARE_ATHENA, ACTION_PREPARE_QUICKSIGHT):
        if not utils.is_valid_prefix(destPrefix):
            validation_ok = False
            message += "Invalid Destination S3 Bucket prefix: [{}]".format(destPrefix)

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
