# AWS Cost AnalysisTools


This repo has utilities for AWS cost analysis and optimization. In its initial version, it provides tools that prepare AWS Cost and Usage data, so it can be analyzed using AWS Athena and QuickSight.

For more details, refer to this article in the Concurrency Labs blog:

https://www.concurrencylabs.com/blog/aws-cost-reduction-athena



## What's in this repo

The repo contains the following files:


### awscostusageprocessor

The modules in this package execute a number of operations that are required
in order to analyze Cost and Usage report files by Athena or QuickSight.

**processor.py**

Performs the following operations:

* Execute required data preparations before putting files in an S3 bucket that will be queried by Athena.
  * Athena doesn't like manifest files or anything that is not a data file, therefore this module only
      copies .csv files to the destination S3 bucket.
  * Place files in Athena S3 bucket using a partition that corresponds to the report date:
           {aws-cost-usage-bucket}/{prefix}/{period}/{csv-file}.
  * Remove 'hash' folder from the object key structure that is used in the Athena S3 bucket. AWS Cost and Usage creates
           files with the following structure: {aws-cost-usage-bucket}/{prefix}/{period}/{reportID-hash}/{csv-file}.
           This implementation removes the 'hash' folder when copying the file to the destination S3 bucket, since it interferes with Athena partitions.
  * Remove first row in every single file. For some reason, Athena ignores OpenCSVSerde's option to skip first rows.


If you have large Cost and Usage reports, it is recommended that you execute this script from an EC2 instance in the same region as the S3 buckets
where you have the AWS Cost and Usage Reports as well as the destination Athena S3 bucket. This
way you won't pay data transfer cost out of S3 and out of the EC2 instance into S3. You will also get
much better performance when transferring data. 

If you run this processes outside of an EC2 instance, you will incur in data transfer costs from S3 out to the internet.


### Scripts

**scripts/report_utils.py**

This script instantiates class CostUsageProcessor, which executes the operations in 
`awscostusageprocessor/processor.py`


## Installation Instructions

### 1. Enable AWS Cost and Usage Reports

The first step is to make sure that AWS Cost and Usage reports are enabled in your AWS account.
These are the necessary steps to enable them:

1. Create an S3 bucket to store the reports.
3. Enable Cost and Usage Reports
	* Go to the "Billing Dashboard" section in the AWS Management Console, navigate to "Reports"
	* Click on "Create Report" under "AWS Cost and Usage Reports". A new screen will be displayed.
	* Set "Time Unit" to "Hourly".
	* Check "Include Resource IDs"
	* Make sure support for QuickSight and Redshift is enabled.

This is what the "Select Content" screen should look like:

![AWS Cost Usage Report](https://www.concurrencylabs.com/img/posts/19-athena-reduce-aws-cost/setup-reports.png)

Click on "Next", that should take you to "Select delivery options"	
	
1. Specify the S3 bucket where reports will be stored.
2. Assign the right permissions to the S3 bucket (there is a link in the console that shows
you the bucket policy)
	
Once you complete these steps, AWS will generate and put Cost and Usage reports in the
S3 bucket you specified. These reports are generated approximately once per day and it's
hard to predict when exactly they will be copied to S3. This means you'll have to wait 
approximately one day before you see any reports in your S3 bucket.


### 2. Install this repo

There are 3 steps that need to happen before you can run this script the first time:

**1. Create a new S3 bucket or folder**.
First, create a new S3 bucket, or create a folder in your existing bucket where you will
place the modified Cost and Usage files.

**2. Install the script and create IAM Permissions**.
 
* Clone this repo
* Create a virtualenv (recommended) and activate it. Please note the current codebase only supports Python 2.7.
* Install dependencies ```pip install -r requirements.txt```
* Make sure the AWS CLI is installed in your system, including your AWS credentials.
* Make sure your IAM user or EC2 Instance profile has the required S3 permissions
 to read files from the source buckets and to put objects into the destination buckets.


**3. Setting environment variables**

Before executing any script, make sure the following environment variables are set:

```export AWS_DEFAULT_PROFILE=<my-aws-default-credentials-profile>```

```export AWS_DEFAULT_REGION=<us-east-1,eu-west-1, etc.>```


### 3. Executing the scripts

Go to the ```scripts``` folder.

There are 3 different operations available:

**Prepare files for Athena**

This operation copies files from a destination S3 bucket, prepares the files and creates
an Athena database and table, so they can be queried using Athena (remove reportId hash, remove manifest files, etc.). 

```bash
python report_utils.py --action=prepare-athena --source-bucket=<s3-bucket-with-cost-usage-reports> --source-prefix=<folder>/ --dest-bucket=<s3-bucket-athena-will-read-files-from> --dest-prefix=<folder>/ --year=<year-in-4-digits> --month=<month-in-1-or-2-digits>
```

Keep in mind that AWS creates Cost and Usage files daily, therefore you must execute this
script daily if you want to have the latest billing data in Athena.

**Prepare QuickSight manifest**

If you want to upload files to QuickSight, you must provide a manifest file. The manifest file essentially
lists the location of the data files, so QuickSight can find them and load them. The source-bucket and source-prefix parameter
indicate the S3 location of the Cost and Usage reports that will be used to create the QuickSight manifest.

AWS gives you the option to create QuickSight manifests when you configure Cost and Usage reports in the Billing console. One issue is that
AWS creates the QuickSight manifest only at the end of the month. That's why I added this option in the script, so you can generate a QuickSight
manifest anytime you want.

```bash
python report_utils.py --source-bucket=<s3-bucket-for-quicksight-files> --source-prefix=<folder>/ --action=create-manifest --manifest-type=quicksight --year=<year-in-4-digits> --month=<month-in-1-or-2-digits>
```

**Prepare Redshift manifest**

As a bonus, if you want to upload files in QuickSight using a Redshift manifest, the script creates one for you.

```bash
python report_utils.py --source-bucket=<s3-bucket-for-quicksight-files> --source-prefix=<folder>/ --action=create-manifest --manifest-type=redshift --year=<year-in-4-digits> --month=<month-in-1-or-2-digits>
```

## Example: Using Athena for AWS Cost and Usage report analysis

**1. Enable AWS Cost and Usage reports**
[see instructions above]. Wait at least one day for the first report to be available in S3.

**2. Prepare AWS Cost and Usage data for Athena**

Make sure you have IAM permissions, environment variables are set and AWS Cost and Usage reports are ready in source-bucket. Then execute:

```bash
python report_utils.py --action=prepare-athena --source-bucket=<s3-bucket-with-cost-usage-reports> --source-prefix=<folder>/ --dest-bucket=<s3-bucket-athena-will-read-files-from> --dest-prefix=<folder>/ --year=<year-in-4-digits> --month=<month-in-1-or-2-digits>
```

The script also creates an Athena database with the format `costusage_<awsaccountid>` and a table with the format
`hourly_<month_range>`

**3. Execute queries against your AWS Cost and Usage data!**
That's it! Now you can query your AWS Cost and Usage data! You can use the sample queries in [**athena_queries.sql**](https://github.com/ConcurrenyLabs/aws-cost-analysis/blob/master/awscostusageprocessor/sql/athena_queries.sql)

## Serverless Application Model Stack(optional)

This repo also has a number of Lambda functions designed to automate the
daily processing of Cost and Usage reports for Athena. These functions can
be orchestrated by an AWS Step Functions State Machine.

Under the `functions` folder:

**process-cur.py**
Starts the process that copies and prepares incoming AWS Cost and Usage reports.

**create-athena-resources.py**
Creates Athena databases and tables based on incoming AWS Cost and Usage reports

**init-athena-queries.py**
Initializes common queries, so the results are available in S3. This 
increases performance and reduces cost.

**update-metadata.py**
Updates a DDB table with the latest execution timestamp. This
information is used by:

* The processes that decide whether to query from Athena or from S3.
* Step Function starter, in order to decide if a new execution should be triggered.
* Any application that consumes Cost and Usage data and needs to know when a new report has been processed

**s3event-step-function-starter.py**
This function receives an S3 PUT event when a new AWS Cost and Usage
report is generated and it then starts the Step Function workflow. You'll
have to manually configure the S3 event so it points to this function.

Under the `cloudformation` folder:

**cloudformation/process-cur-sam.yml**
Serverless Application Model definition to deploy all Lambda functions. This template automates the creation of:

* S3 bucket where AWS Cost and Usage reports will be placed
* All relevant Lambda functions
* Step Function that executes Lambda functions in the right order.
* S3 Event that will automatically trigger the Step Function as soon as a new Cost
and Usage report is placed by AWS in the S3 bucket.


### Automated Deployment using Serverless Application Model (recommended)

Before you can deploy the Serverless Application Model Stack you need to create a virtual environment and install all the requirements:

```bash
# Create a virtual environment and install the requirements
virtualenv ./
source ./bin/activate
pip install -r requirements.txt
```

You can deploy the AWS cost analysis and optimization in your AWS Account using the following commands:

```bash
# Set the environment variable correct for your environment
AWS_REGION=us-east-1
BUCKET=my-s3-bucket # Used for the package command not the billing bucket
NAME=curprocessor-sam
BILLING_BUCKET_NAME=[BUCKET_NAME]

# Package the template and functions
sam package \
  --template-file cloudformation/curprocessor-sam.yml \
  --output-template-file cloudformation/curprocessor-sam-packaged.yml \
  --s3-bucket ${BUCKET} \
  --s3-prefix ${NAME}

# Deploy the Serverless AWS Cost Analysis in your account
sam deploy \
  --template-file cloudformation/curprocessor-sam-packaged.yml \
  --region ${AWS_REGION} \
  --stack-name ${NAME} \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
      StackTag=${NAME} \
      BucketName=${BILLING_BUCKET_NAME} \
      CloudWatchRetention=7 \
      ReportPathPrefix=aws-reports/ \
      CreateLogGroups=Enabled
```

> NOTE: If you are updating your existing stack and it fails due to the fact that you already have existing LogGroups you can delete them and re-deploy if possible or set the `CreateLogGroups` parameter to `Disabled`.



### Manual Deployment

If you prefer to setup all components manually, you can arrange the Lambda functions above and automate their execution. All you have
to do is to create a State Function that calls the relevant Lambda functions.

The Step Function is defined in the following file:

**functions/step-function-athena.json**
Step Function definition to automate the daily processing of CUR files and
creation of Athena resources.

**Starting the Step Function**

Configure a S3 Event that invokes the `S3EventStepFunctionStarter` function when a PUT is done on the S3 bucket that receives the CUR reports.
  * Events: Put
  * Prefix: Match your path
  * Suffix: `Manifest.json`

 
If you prefer an alternate method to start the Step Function, just make sure
that it sends a dictionary with the following values in it: `year`, `month`,
`sourceBucket`, `sourcePrefix` (with a '/' at the end), `destBucket`, `destPrefix`, `accountId` and
optionally `xAccountSource` (Boolean), and `roleArn` if you're analyzing
Cost and Usage reports cross-account.

