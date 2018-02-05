#!/usr/bin/python
import re

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
  month = int(month)
  year = int(year)
  nextYear = year
  nextMonth = month + 1

  if month == 12:
      nextMonth = 1
      nextYear = year + 1

  return "{}{:02d}01-{}{:02d}01/".format(year,month,nextYear,nextMonth)


"""
This method extracts the year and month of a Cost and Usage report,
as well as the prefix, based on the S3 key of the report.
For example, period comes in the format: 20170501-20170601.

A report key comes in the following format:
'<prefix>/<period>/<hash>/<reportname>.csv.gz'

"""

def extract_period(s3key):
    dirs = s3key.split('/')
    prefix = ""
    year = ""
    month = ""
    periodregex = r"[0-9]{8}-[0-9]{8}"

    for d in dirs:
        if re.search(periodregex, d):
            period = d
            year = period[0:4]
            month = period[4:6]
            break
        prefix += d+"/"

    print("s3 key [{}]".format(s3key))
    print("prefix [{}] year [{}] month [{}]".format(prefix, year, month))

    return prefix, year, month





