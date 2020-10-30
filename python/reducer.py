#!/usr/bin/python3
import sys


def main(argv):
  line = sys.stdin.readline()

  current_key = None
  current_counter = 0

  try:
    while line:

      # remove trailing whitespace
      line = line.rstrip()

      # parse the input from mapper. input format: "2004-03-07 T 16:00:00.000       1"
      (key, counter,) = line.split("\t", 1)

      try:
        counter = int(counter)
      except ValueError:
        # discard the line if counter isn't number
        continue

      if current_key == key:
        current_counter += counter
      else:
        # ouput the counter when the key changed
        if current_key:
          print(f"{current_key}\t{current_counter}")

        # set the new key and init counter
        current_key = key
        current_counter = counter

      line = sys.stdin.readline()
  except EOFError:
    pass 
  finally:
    # output the last key's result
    print(f"{current_key}\t{current_counter}")

if __name__ == "__main__":
  main(sys.argv)