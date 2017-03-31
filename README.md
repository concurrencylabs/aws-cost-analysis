# AWS Cost AnalysisTools


This repo has utilities for AWS cost analysis and optimization. In its initial version, it provides tools that prepare AWS Cost and Usage data, so it can be analyzed using AWS Athena and QuickSight.

For more details, refer to this article in the Concurrency Labs blog:

https://www.concurrencylabs.com/blog/aws-cost-reduction-athena



## What's in this repo

The repo contains the following files:

### report_utils.py

This module implements operations that are required to prepare AWS Cost and Usage data before it can
be analyzed using AWS Athena. It copies data from the Cost and Usage Reports S3 bucket (source)
into a separate S3 Bucket, where they can be queried by Athena.

The script performs the following operations:

* Execute required data preparations before putting files in an S3 bucket that will be queried by Athena.
  * Athena doesn't like manifest files or anything that is not a data file, therefore this script only
      copies .csv files to the destination S3 bucket.
  * Place files in Athena S3 bucket using a partition that corresponds to the report date:
           {aws-cost-usage-bucket}/{prefix}/{period}/{csv-file}.
  * Remove 'hash' folder from the object key structure that is used in the Athena S3 bucket. AWS Cost and Usage creates
           files with the following structure: {aws-cost-usage-bucket}/{prefix}/{period}/{reportID-hash}/{csv-file}. This script removes the 'hash' folder when
           copying the file to the destination S3 bucket, since it interferes with Athena partitions.
  * Remove first row in every single file. For some reason, Athena ignores OpenCSVSerde's option to skip first rows.


If you have large Cost and Usage reports, it is recommended that you execute this script from an EC2 instance in the same region as the S3 buckets
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
* Create a virtualenv (recommended) and activate it.
* Install dependencies ```pip install -r requirements.txt```
* Make sure the AWS CLI is installed in your system, including your AWS credentials.
* Make sure your IAM user or EC2 Instance profile has the required S3 permissions
 to read files from the source buckets and to put objects into the destination buckets.


**3. Setting environment variables**

Before executing any script, make sure the following environment variables are set, which are required by Boto:

```export AWS_DEFAULT_PROFILE=<my-aws-default-credentials-profile>```
```export AWS_DEFAULT_REGION=<us-east-1,eu-west-1, etc.>```


### 3. Executing the scripts

There are 4 different operations available:

**Prepare files for Athena **

This operation copies files from a destination S3 bucket and prepare the files so they
can be queried using Athena (remove reportId hash, remove manifest files, etc.)

```
python report_utils.py --action=prepare-athena --source-bucket=<s3-bucket-with-cost-usage-reports> --source-prefix=<folder>/ --dest-bucket=<s3-bucket-for-athena-files> --dest-prefix=<folder>/ --year=<year-in-4-digits> --month=<month-in-1-or-2-digits>
```

Keep in mind that AWS creates Cost and Usage files daily, therefore you must execute this
script if you want to have the latest billing data in Athena.  


**Prepare QuickSight manifest**

If you want to upload files to QuickSight, you must provide a manifest file. The manifest file essentially
lists the location of the data files, so QuickSight can find them and load them. In this case, the source-bucket parameter
is the S3 bucket where AWS puts Cost and Usage reports.

```
python report_utils.py --source-bucket=<s3-bucket-for-quicksight-files> --source-prefix=<folder>/ --action=create-manifest --manifest-type=quicksight --year=2017 --month=3
```

**Prepare Redshift manifest**

As a bonus, if you want to upload files in QuickSight using a Redshift manifest, the script creates one for you.

```
python report_utils.py --source-bucket=<s3-bucket-for-quicksight-files> --source-prefix=<folder>/ --action=create-manifest --manifest-type=redshift --year=2017 --month=3
```



## Example: Using Athena for AWS Cost and Usage report analysis

**1. Enable AWS Cost and Usage reports**
[see instructions above]. Wait at least one day for the first report to be available in S3.

**2. Prepare AWS Cost and Usage data for Athena**

```
python report_utils.py --action=prepare-athena --source-bucket=<s3-bucket-with-cost-usage-reports> --source-prefix=<folder>/ --dest-bucket=<s3-bucket-for-athena-files> --dest-prefix=<folder>/ --year=<year-in-4-digits> --month=<month-in-1-or-2-digits>
```

**3. Create Athena database and table**

Once your cleaned-up files are in S3, you need to have an Athena database. You can create one from the Athena console by running a SQL statement:

```
CREATE DATABASE billing;
```

You can use an existing database if you want, that's up to you. For this example we'll use a new database named 'billing'.

The next step is to create an Athena table. You can do this by running the statement in <a href="https://github.com/ConcurrenyLabs/aws-cost-analysis/blob/master/create_athena_table.sql" target="new">**create_athena_table.sql**</a> from the Athena console.
 
Note this Athena table is partitioned by month. This is a natural option, since AWS already partitions cost and usage data by month. For example, data files are placed in an S3 folder
with the format ```<year><startMonth>01-<year><endMonth>01```. It's recommended to use the same partition format, for example ```period='20170301-20170401'```.
For more on Athena partitions, <a href="http://docs.aws.amazon.com/athena/latest/ug/partitions.html" target="new">read this</a>.

For each partition in your Athena table, you'll have to execute the following script:

```
ALTER TABLE hourly ADD PARTITION (period='<period>') location 's3://<athena-s3-bucket>/<period>/'
```


**4. Execute queries against your AWS Cost and Usage data!**
And that's it. Now you can query your AWS Cost and Usage data! You can use the sample queries in <a href="https://github.com/ConcurrenyLabs/aws-cost-analysis/blob/master/athena_queries.sql" target="new">**athena_queries.sql**</a>





