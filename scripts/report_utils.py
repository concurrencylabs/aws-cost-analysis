#!/usr/bin/python
import os, sys, logging
import argparse
import traceback

sys.path.insert(0, os.path.abspath('..'))
import awscostusageprocessor.processor as cur
import awscostusageprocessor.consts as consts
import awscostusageprocessor.sql.athena as ath
import awscostusageprocessor.utils as curutils

log = logging.getLogger()
log.setLevel(logging.INFO)


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

  kwargs = {}

  action = ''
  if args.action: action=args.action

  manifestType = ''
  if args.manifest_type: manifestType=args.manifest_type

  kwargs['limit'] = 1000
  if args.limit: kwargs['limit'] = args.limit

  if args.year: kwargs['year']=args.year
  if args.month: kwargs['month']=args.month
  if args.source_bucket: kwargs['sourceBucket']=args.source_bucket
  if args.source_prefix: kwargs['sourcePrefix']= args.source_prefix
  if args.dest_bucket: kwargs['destBucket']= args.dest_bucket
  if args.dest_prefix: kwargs['destPrefix']= args.dest_prefix
  if args.role_arn: kwargs['roleArn'] = args.role_arn
  if args.xacct_source: kwargs['xAccountSource'] = True
  if args.xacct_dest: kwargs['xAccountDest'] = True


  try:

    if action not in consts.VALID_ACTIONS:
        raise Exception("Invalid action, valid options are: {}".format(consts.VALID_ACTIONS))

    if action == consts.ACTION_CREATE_MANIFEST:
      kwargs['destBucket'] = kwargs['sourceBucket'] + '-dest'
      kwargs['destPrefix'] = kwargs['sourcePrefix'] + 'dest/'

    curprocessor = cur.CostUsageProcessor(**kwargs)

    if action in (consts.ACTION_PREPARE_ATHENA, consts.ACTION_PREPARE_QUICKSIGHT):
      #Process Cost and Usage Report
      destS3keys = curprocessor.process_latest_aws_cur(action)

      #Then create Athena table for the current month
      athena = ath.AthenaQueryMgr("s3://"+curprocessor.destBucket, curprocessor.accountId, curprocessor.year, curprocessor.month)
      athena.create_database()
      athena.drop_table()#drops the table for the current month (before creating a new one)
      curS3Prefix = curprocessor.destPrefix + "/" + curutils.get_period_prefix(curprocessor.year, curprocessor.month)
      print ("Creating Athena table for S3 location [s3://{}/{}]".format(curprocessor.destBucket,curS3Prefix))
      athena.create_table(curprocessor.curManifestJson, curprocessor.destBucket, curS3Prefix)


      if action == consts.ACTION_PREPARE_QUICKSIGHT:
        curprocessor.create_manifest(consts.MANIFEST_TYPE_QUICKSIGHT, kwargs['destBucket'],kwargs['destPrefix'], destS3keys)
        curprocessor.create_manifest(consts.MANIFEST_TYPE_REDSHIFT, kwargs['destBucket'],kwargs['destPrefix'], destS3keys)

    if action == consts.ACTION_CREATE_MANIFEST:
      curprocessor.create_manifest(manifestType, kwargs['sourceBucket'],kwargs['sourcePrefix'], [])

    if action == consts.ACTION_TEST_ROLE:
      curprocessor.test_role()


  except Exception as e:
    traceback.print_exc()
    print("Exception message:["+str(e.message)+"]")




if __name__ == "__main__":
   main(sys.argv[1:])
