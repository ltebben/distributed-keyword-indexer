import sys
import os
import time
import urllib.robotparser as roboparser
from mpi4py import MPI
from pymongo import MongoClient
from random import randint

# Import project modules
from scrape import Scrape
from status import Status

# Define constants for config files
LOG_PATH = os.environ["LOG_PATH"]
SOURCES_LOC = "sources.txt"

MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PORT = os.environ["MONGO_PORT"]
MONGO_USER = os.environ["MONGO_USER"]
MONGO_PASS = os.environ["MONGO_PASS"]
MONGO_NAME = os.environ["MONGO_NAME"]
MONGO_COLLECTION = os.environ["MONGO_COLLECTION"]

# Initialize database connection
client = MongoClient(MONGO_HOST, int(MONGO_PORT))
db = client[MONGO_NAME]
db.authenticate(MONGO_USER, MONGO_PASS)
urls_collection = db[MONGO_COLLECTION]

# Initialize scraper
s = Scrape(collection=urls_collection)

# Initialize mpi cluster variables
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Initialize root set of sources
sources_file = open(SOURCES_LOC, "r")
sources = sources_file.readlines()
explored = set()

# Initialize stopTime
if len(sys.argv) == 2: 
    stopTime = time.time() + int(sys.argv[1])
else:
    stopTime = -1

# Initialize the status
status = Status(LOG_PATH)

# Select the initial source
rootIdx = rank % len(sources)
source = sources[rootIdx]

while stopTime < 0 or time.time() < stopTime:
    source = source.strip()
    parts = source.split('/')
    baseurl = '/'.join(parts[0:3])

    # Respect robots.txt
    rp = roboparser.RobotFileParser()
    rp.set_url(baseurl + '/robots.txt')
    rp.read()

    # Process the url if allowed
    if rp.can_fetch('*', source):
        s.setUrl(source)
        keywords, links = s.scrape() 

        # Persist keywords to the database
        s.submitWords(keywords)
            
        # randomly select a next link to explore next
        # with 1/20 chance of starting again from root
        if randint(1,20) == 1:
          source = source[rootIdx]
        else:
          linkIdx = randint(0, len(links)-1)
          source = links[linkIdx]
    else:
        # If robots.txt disallows reading, shift the root source
        rootIdx = rootIdx + 1
            source = sources[rootIdx]

        time.sleep(1)
