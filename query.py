import sys
import os
import re
from pymongo import MongoClient

PAREN_OPEN = "("
PAREN_CLOSE = ")"
QUOTE = "\""
AND = "&"
OR = "|"

def remove_bounding_chars(string, ch1, ch2):
  if (string[0] == ch1 and string[len(string)-1] == ch2):
    return string[1:len(string)-1]
  else:
    return string

# Validate the number of input args
if len(sys.argv) != 2:
  print("Please provide 1 argument for the query string")
  exit()

# Get the query string
query = remove_bounding_chars(sys.argv[1], QUOTE, QUOTE)

# Validate the input format
if (re.match(r"(\([a-zA-Z]+(\|[a-zA-Z]+)+\)|[a-zA-Z]+)(\&(\([a-zA-Z]+(\|[a-zA-Z]+)+\)|[a-zA-Z]+))*$", query) == None):
  print("Please provide query string in format: (keywords|ored)&other&keywords&anded")
  exit()

# Parse the query string into a list of lists to disjunct and conjunct
groupsStrs = query.split(AND)
groups = list(map(lambda group: remove_bounding_chars(group, PAREN_OPEN, PAREN_CLOSE).split(OR), groupsStrs)) 

print("Processing query: " + str(groups))
