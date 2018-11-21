import sys
import os
import re
from pymongo import MongoClient
from pprint import pprint

PAREN_OPEN = "("
PAREN_CLOSE = ")"
QUOTE = "\""
AND = "&"
OR = "|"

MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PORT = os.environ["MONGO_PORT"]
MONGO_USER = os.environ["MONGO_USER"]
MONGO_PASS = os.environ["MONGO_PASS"]

MONGO_NAME = "dki"
MONGO_COLLECTION = "Keywords"

def query_keyword(keyword, db_collection):
  # Perform query on the db
  results = list(db_collection.find({"word": keyword})) 
  
  # Extract the urls lists from all returned entries (dictionaries)
  urls = list(map(lambda entry: entry["urls"], results))

  # Flatten the list
  urls = [url for sublist in urls for url in sublist]
  
  return urls

def remove_bounding_chars(string, ch1, ch2):
  if (string[0] == ch1 and string[len(string)-1] == ch2):
    return string[1:len(string)-1]
  else:
    return string

# Take query (list of lists) and return list of urls
def get_urls(query, db_collection):
  # Query and perform unions and intersections as needed
  urls = None
  for group in groups:
    # build list of urls for the current disjoint group
    group_urls = []
    for word in group:
      group_urls = set(group_urls) | set(query_keyword(word, urls_collection))
  
    # If urls is defined, set it to take intersection of current group urls with the accumulated urls
    # Otherwise set it to the current group urls
    if urls != None:
      urls = set(urls) & set(group_urls)
    else:
      urls = group_urls

  return list(urls)

## End of function definitions, start of code ##

# Setup database connection
client = MongoClient(MONGO_HOST, int(MONGO_PORT))
db = client[MONGO_NAME]
db.authenticate(MONGO_USER, MONGO_PASS)
urls_collection = db[MONGO_COLLECTION]

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

urls = get_urls(groups, urls_collection)

print("Results: " + str(urls))
