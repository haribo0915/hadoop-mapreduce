#!/usr/bin/python3
import sys
from scanf import scanf
from datetime import datetime

def convert_month_str_to_int(month_str):
  return {
        'jan': 1,
        'feb': 2,
        'mar': 3,
        'apr': 4,
        'may': 5,
        'jun': 6,
        'jul': 7,
        'aug': 8,
        'sep': 9,
        'oct': 10,
        'nov': 11,
        'dec': 12,
    }.get(month_str.lower(), 1)

def preprocess_timestamp(timestamp_with_timezone_offset):
  timestamp_without_timezone_offset = timestamp_with_timezone_offset.split(' ')[0]

  (day, month, year, hour, minute, second,) = scanf("%d/%s/%d:%d:%d:%d", timestamp_without_timezone_offset)
  month = convert_month_str_to_int(month)

  # extract target value from timestamp_without_timezone_offset to create datetime object
  timestamp = datetime(year, month, day, hour)

  # return a date string with milliseconds (3 decimal places behind seconds)
  return timestamp.strftime("%Y-%m-%d T %H:%M:%S.%f")[:-3]

def main(argv):
  line = sys.stdin.readline()
  try:
    while line:

      # remove trailing whitespace
      line = line.rstrip()

      open_bracket = line.index("[")
      close_bracket = line.index("]")

      timestamp_with_timezone_offset = line[(open_bracket+1):close_bracket]

      key = preprocess_timestamp(timestamp_with_timezone_offset)

      print(key + "\t" + "1")

      line = sys.stdin.readline()

  except ValueError as e:
    print("Line format is not valid: " + e)
    return
  except EOFError:
    return

if __name__ == "__main__":
  main(sys.argv)