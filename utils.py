#!/usr/bin/python
import sys



def validate(sourcePrefix):
  if not is_valid_prefix(sourcePrefix):
      sys.exit()


def is_valid_prefix(prefix):
    result = True
    if prefix[len(prefix)-1] != "/":
        print("Invalid prefix value:[{}] - trailing '/' expected ".format(prefix))
        result = False

    return result
