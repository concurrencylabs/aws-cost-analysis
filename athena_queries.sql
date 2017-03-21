
/* Add all partitions at once */
MSCK REPAIR TABLE hourly



/* Add individual Partition  - make sure that table is created with partition=period */
ALTER TABLE hourly ADD PARTITION (period='<period>') location 's3://<athena-s3-bucket>/<period>/'

/* List all the partitions */
SHOW PARTITIONS billing.hourly

/* Make sure the table has been created successfully */
select * from hourly limit 10


/* Just a sanity check to see which time intervals are included in your queries */
SELECT distinct(identity_timeinterval)
FROM billing.hourly
ORDER BY  identity_timeinterval


/* View total cost */
SELECT round(sum(cast(lineitem_unblendedcost AS double)),2) AS sum_unblendedcost
FROM billing.hourly
WHERE period = ''


/* View cost by AWS Service */
SELECT lineitem_productcode,
         round(sum(cast(lineitem_unblendedcost AS double)),2) AS sum_unblendedcost
FROM billing.hourly
WHERE period='<period>'
GROUP BY  lineitem_productcode
ORDER BY  sum_unblendedcost DESC


/*
View cost by AWS Service and Usage Type
This view is partially available in Cost Explorer. CE shows usage type but not the service. The service
is inferred based on usage, but it's not always clear'
*/

SELECT lineitem_productcode, lineItem_UsageType,
         round(sum(cast(lineitem_unblendedcost AS double)),2) AS sum_unblendedcost
FROM billing.hourly
WHERE period='<period>'
GROUP BY  lineitem_productcode, lineItem_UsageType
ORDER BY  sum_unblendedcost DESC



/*
Find worst offenders in your account (EC2 instances, S3 buckets, etc.)
This information is not available using AWS Cost Explorer. Cost Explorer only shows cost by Service, not by Resource
*/

SELECT lineitem_productCode,
         lineitem_resourceId,
         sum(cast(lineitem_unblendedcost AS double)) AS sum_unblendedcost
FROM billing.hourly
WHERE period='<period>'
GROUP BY  lineitem_productCode,lineitem_resourceId
ORDER BY  sum_unblendedcost desc
LIMIT 10


/* Query the usage type incurred by a specific resourceId. This  one is useful to find 'death by a thousand cuts' situations. */
SELECT DISTINCT lineitem_usagetype, lineitem_lineitemdescription,
  sum(cast(lineitem_usageamount AS double)) AS sum_usageamount,
  sum(cast(lineitem_unblendedcost AS double)) AS sum_unblendedcost
FROM billing.hourly
WHERE lineitem_resourceId = '<resourceID>'
AND period='<period>'
GROUP BY lineitem_usagetype, lineitem_lineitemdescription
ORDER BY sum_unblendedcost DESC


/* See all the active resources in your AWS account */
SELECT DISTINCT lineitem_resourceId FROM billing.hourly limit 100


/* The search by a particular resource */
SELECT identity_lineitemid,
         identity_timeinterval,
         lineitem_usagetype,
         lineitem_operation,
         lineitem_lineitemdescription,
         lineitem_usageamount,
         lineitem_unblendedcost,
         lineItem_ResourceId
FROM billing.hourly
WHERE lineItem_ResourceId ='<resourceID>'
ORDER BY identity_timeinterval



/*
See all your resources grouped by AWS Service
This view is not available in AWS Cost Explorer
*/
SELECT lineitem_productcode, lineitem_resourceId
FROM billing.hourly
WHERE lineitem_resourceId <> ''

group by lineitem_productcode, lineitem_resourceId
order by lineitem_productcode


/*
See cost incurred chronologically, with hourly granularity
*/

SELECT lineitem_usagestartdate, round(sum(cast(lineitem_unblendedcost AS double)),2) AS sum_unblendedcost
FROM billing.hourly
WHERE period='<period>'
GROUP BY  lineitem_usagestartdate
ORDER BY  lineitem_usagestartdate



/* TODO: View EC2 Instance cost by EC2 instance type */


/* TODO: View Data Transfer FROM and TO */


/*
View latest time interval that is avaiable in Athena
If it's too old, you might want to refresh the Cost and Usage data in your S3 bucket'
*/

SELECT MAX(identity_timeinterval) as latest_interval
FROM billing.hourly
WHERE period='<period>'






