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
from measurements import *

# Define constants for config files
LOG_PATH = os.environ["LOG_PATH"]
SOURCES_LOC = "sources.txt"

#MONGO_HOST = os.environ["MONGO_HOST"]
#MONGO_PORT = os.environ["MONGO_PORT"]
#MONGO_USER = os.environ["MONGO_USER"]
#MONGO_PASS = os.environ["MONGO_PASS"]
#MONGO_NAME = os.environ["MONGO_NAME"]
#MONGO_COLLECTION = os.environ["MONGO_COLLECTION"]

# Initialize database connection
#client = MongoClient(MONGO_HOST, int(MONGO_PORT))
#db = client[MONGO_NAME]
#db.authenticate(MONGO_USER, MONGO_PASS)
#urls_collection = db[MONGO_COLLECTION]

# Initialize scraper
s = Scrape()

# Initialize mpi cluster variables
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Initialize root set of sources
sources_file = open(SOURCES_LOC, "r")
sources = sources_file.readlines()

# Initialize stopTime
if len(sys.argv) == 2: 
    stopTime = time.time() + int(sys.argv[1])
else:
    stopTime = -1

# Initialize the status
if rank == 0:
    # Initialize status
    status = Status(LOG_PATH)

    # Initialize sets to track words and links
    words = set()
    links = set()
  
    receiveMsgs = [comm.irecv(source=idx+1) for idx in range(size-1)]

    while True: 
        for idx, req in enumerate(receiveMsgs): 
            res = (True, (set(), set()))
            try:
                res = req.test()
            except Exception:
                pass 
            
            if res[0]: 
                if res[1][0] and len(res[1][0]) > 0:
                    words = words | res[1][0]
                if res[1][1] and len(res[1][1]) > 0: 
                    links = links | res[1][1]
                
                # Start a new request for this worker
                receiveMsgs[idx] = comm.irecv(source=idx+1)
                
                # Update stats
                status.updateStats({"Number of links": len(links), "Number of words": len(words)})
                
        
else:
    # Select the initial source
    rootIdx = rank % len(sources)
    source = sources[rootIdx]

    # Loop for surfer action
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
 
            # Submit the words for this link and count this link as explored
            if not keywords:
                keywords = list()
            if not links:
                links = list()
            
            # randomly select a next link to explore next
            # with 1/20 chance of starting again from root
            if links and len(links) > 0 and randint(1,20) != 1:
                linkIdx = randint(0, len(links)-1)
                source = links[linkIdx]
            else:
                rootIdx = (rootIdx + 1) % len(sources)
                source = sources[rootIdx]
            
            comm.send((set(keywords), set(links)), dest=0)
            
            # Sleep to respect rate limits
            time.sleep(1)
        else:
            # If robots.txt disallows reading, shift the root source
            rootIdx = (rootIdx + 1) % len(sources)
            source = sources[rootIdx]
