#!/usr/bin/python


def is_valid_prefix(prefix):
    result = True
    if prefix[len(prefix)-1] != "/":
        print("Invalid prefix value:[{}] - trailing '/' expected ".format(prefix))
        result = False

    return result


"""
Converts year and month to the format used by AWS Cost and Usage reports (i.e. 20170101-20170201)
"""
def get_period_prefix(year, month):
  imonth = int(month)
  return "{}{:02d}01-{}{:02d}01/".format(year,imonth,year,imonth+1)


"""
This method extracts the year and month of a Cost and Usage report,
as well as the prefix, based on the S3 key of the report.
For example, period comes in the format: 20170501-20170601.

A report key comes in the following format:
'/dir/dir/20170601-20170701/hash/{reportname}.csv.gz'

"""

def extract_period(s3key):
    dirs = s3key.split('/')
    period = dirs[len(dirs)-3]

    prefix = ""
    for d in dirs[0:len(dirs)-3]:
        prefix += d+"/"

    year = period[0:4]
    month = period[4:6]
    return prefix, year, month






