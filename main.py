import sys
import os
import time
import urllib.robotparser as roboparser
from mpi4py import MPI
from pymongo import MongoClient

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
explored = set()
uniqueKeywords = set()

# Initialize stopTime
if len(sys.argv) == 2: 
    stopTime = time.time() + int(sys.argv[1])
else:
    stopTime = -1


if rank == 0:
    # Initialize status object
    status = Status(LOG_PATH)

    # Distribute sources to each worker. If more workers than sources, give same
    # sources to multiple workers so they can take different walks

    # outstandingReqs tracks the number of outstanding requests
    outstandingReqs = 0

    # The given index in this request object list corresponds to the worker of id index + 1
    receiveMessages = list()
    for idx in range(size - 1):
        receiveMessages.append(comm.irecv(source=idx+1))
        # Send links to workers
        if len(sources) > 0:
            # If there's a link to send, send it
            comm.isend(sources.pop(0), dest=idx+1)
            outstandingReqs += 1
        else:
            # If there is no link, send a blank string and the worker will wait
            comm.isend('', dest=idx+1)
            
    # do stuff 
    while outstandingReqs > 0 and (stopTime < 0 or time.time() < stopTime): 
        for idx, req in enumerate(receiveMessages):
            # Check to see if the request has come back yet
            res = (True, (list(), list()))
            try:
                res = req.test()
            except Exception:
                pass
            if res[0]:
                # Got a response, so deduct from outstandingReqs
                outstandingReqs -= 1
                
                # Get the keywords from the worker 
                keywords = res[1][0]
                if not keywords:
                    keywords = list()
                
                # Get the links the worker found
                links = res[1][1]
                if not links:
                    links = list()
                
                # Send the worker the next link to work on
                if len(sources) > 0:
                    # If there's a link to send, send it
                    nextLink = sources.pop(0)
                    status.count("Links sent to workers")
                    comm.isend(nextLink, dest=idx+1)
                    outstandingReqs += 1
                else:
                    # If there is no link, send a blank string and the worker will wait
                    comm.isend('', dest=idx+1)
                # start a new receive message from workers
                receiveMessages[idx] = comm.irecv(source=idx+1)

                # Add keywords to unique keywords
                uniqueKeywords.update(keywords)
                status.updateStats({"Unique keywords" : len(uniqueKeywords)})
                
                # add the new links to the sources
                if links is None:
                    continue
                for link in links: 
                    if link not in explored:
                        status.count("Number of unique links")
                        sources.append(link)
                        explored.add(link)
                        status.updateStats({"Queue length": len(sources)})
                    else:
                        status.count("Number of repeated links")

    # Clean up and terminate
    clearCollection(urls_collection)
    status.end()                   
else:  
    while stopTime < 0 or time.time() < stopTime: 
        

        # Wait to receive a source from the master
        source = comm.recv(source=0)
        links = list()
        keywords = list()

        if source == '':
            # We got a blank link, wait for a while then ask again
            time.sleep(1)
        else:
            source = source.strip()
            parts = source.split('/')
            baseurl = '/'.join(parts[0:3])
            rp = roboparser.RobotFileParser()
            rp.set_url(baseurl + '/robots.txt')

            rp.read()

            if rp.can_fetch('*', source):
                s.setUrl(source)
                keywords, links = s.scrape() 

                # Persist keywords to the database
                # commenting out because this is such a bottleneck
                # s.submitWords(keywords)

        # Send new links back to the master queue
        comm.send((keywords, links), dest=0) 
        time.sleep(1)
