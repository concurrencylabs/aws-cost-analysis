# AWS Cost AnalysisTools


This repo has utilities for AWS cost analysis and optimization. In its initial version, it provides tools that prepare AWS Cost and Usage data, so it can be analyzed using AWS Athena and QuickSight.

For more details, refer to this article in the Concurrency Labs blog:

https://www.concurrencylabs.com/blog/aws-cost-reduction-athena

The repo contains the following files:

### report_utils.py

This module implements operations that are required to prepare AWS Cost and Usage data before it can
be analyzed using AWS Athena or AWS QuickSight. It copies data from the Cost and Usage Reports S3 bucket (source)
into a separate S3 Bucket, where they can be queried by Athena, or loaded onto QuickSight.

The scripts performs the following operations:

* Execute required data preparations before putting files in an S3 bucket that will be queried by Athena.
  * Athena doesn't like manifest files or anything that is not a data file, therefore this script only
      copies .csv files to the destination S3 bucket.
  * Place files in Athena S3 bucket using a partition that corresponds to the report date:
           <aws-cost-usage-bucket>/<prefix>/><period>/<csv-file>.
  * Remove 'hash' folder from the object key structure that is used in the Athena S3 bucket. AWS Cost and Usage creates
           files with the following structure: <aws-cost-usage-bucket>/<prefix>/<period>/<hash>/<csv-file>. This script removes the 'hash' folder, since
           it interferes with Athena partitions.
  * Remove first row in every single file. For some reason, Athena ignores OpenCSVSerde's option to skip first rows.
  * De-duplicate records. AWS often generates duplicate records across different files. The script makes sure that
    only unique records are stored in the destination S3 bucket.


It is recommended that you execute this script from an EC2 instance in the same region as the S3 buckets
where you have the AWS Cost and Usage Reports as well as the destination Athena S3 bucket. This
way you won't pay data transfer cost out of S3 and out of the EC2 instance into S3. You will also get
much better performance when transferring data. 

If you run this script outside of an EC2 instance, you will incur in data transfer costs from S3 out to the internet.


### create_athena_table.sql

This file contains the SQL statement to create an Athena table. Make sure you update the ```LOCATION``` statement with the S3 bucket where you've
placed the Athena files.

After you run the CREATE TABLE statement, you'll have to run the following for each partition in your dataset:

```
ALTER TABLE hourly ADD PARTITION (period='<period>') location 's3://<athena-s3-bucket>/<period>/'
```



### athena_queries.sql

Contains a list of SQL queries you can use to find relevant cost and usage information in the Athena table. 
It's recommended that you filter by partition, in order to avoid querying the whole database.
This will result in better performance and lower Athena cost.


## Installation Instructions

### Enable AWS Cost and Usage reports

Refer to <a href="https://www.concurrencylabs.com/blog/aws-cost-reduction-athena/#enable" target="new">this section</a> in the Concurrency Labs article.


### Install this repo


There are 3 steps that need to happen before you can run this script the first time:

**1. Create a new S3 bucket or folder**.
First, create a new S3 bucket, or create a folder in your existing bucket where you will
place the modified Cost and Usage files.

**2. Install the script and create IAM Permissions**.
 
* Clone this repo
* Create a virtualenv (recommended) and activate it.
* Install dependencies ```pip install -r requirements.txt```
* Make sure the AWS CLI is installed in your system, including your AWS credentials.
* Make sure your IAM user or EC2 Instance profile has the required S3 permissions
 to read files from the source buckets and to put objects into the destination buckets.


### Running the scripts

Make sure the following environment variables are set, which are required by Boto:

```export AWS_DEFAULT_PROFILE=<my-aws-default-credentials-profile>```
```export AWS_DEFAULT_REGION=<us-east-1,eu-west-1, etc.>```


There are 4 different operations you can do:

** Prepare files for Athena **

This operation copies files from a destination S3 bucket and prepare the files so they
can be queried using Athena (de-duplication, remove manifest files, etc.)

```
python report_utils.py --action=prepare-athena --source-bucket=<s3-bucket-with-cost-usage-reports> --source-prefix=<folder>/ --dest-bucket=<s3-bucket-for-athena-files> --dest-prefix=<folder>/ --year=<year-in-4-digits> --month=<month-in-1-or-2-digits>
```

Keep in mind that AWS creates Cost and Usage files daily, therefore you must execute this
script if you want to have the latest billing data in Athena.  

** Prepare files for QuickSight **

Same as Athena, except file headers must be kept.

```
python report_utils.py --action=prepare-quicksight --source-bucket=<s3-bucket-with-cost-usage-reports> --source-prefix=<folder>/ --dest-bucket=<s3-bucket-for-athena-files> --dest-prefix=<folder>/ --year=<year-in-4-digits> --month=<month-in-1-or-2-digits>
```

** Prepare QuickSight manifest**

If you want to upload files to QuickSight, you must provide a manifest file. The manifest file essentially
lists the location of the data files, so QuickSight can find them and load them. In this case, the source-bucket parameter
is the S3 bucket that contains files that are ready to be loaded.

```
python report_utils.py --source-bucket=<s3-bucket-for-quicksight-files> --source-prefix=<folder>/ --action=create-manifest --manifest-type=quicksight --year=2017 --month=3
```

** Prepare Redshigt manifest**

As a bonus, if you want to upload files in QuickSight using a Redshift manifest, the script creates one for you.

```
python report_utils.py --source-bucket=<s3-bucket-for-quicksight-files> --source-prefix=<folder>/ --action=create-manifest --manifest-type=redshift --year=2017 --month=3
```

Tip: if you will be copying large amounts of data, it is recommended to run this script from an EC2
instance located in the same region as your S3 buckets. This way you won't pay for data
transfer cost out to the internet.



And that's it. Now you can query your AWS Cost and Usage data! You can use the sample queries in **athena_queries.sql**





