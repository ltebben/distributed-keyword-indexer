import os
import time
from mpi4py import MPI
from pymongo import MongoClient

# Import project modules
from scrape import Scrape
from status import Status

# Define constants for config files
SOURCES_LOC = "sources.txt"
MONGO_HOST = os.environ["MONGO_HOST"]
MONGO_PORT = os.environ["MONGO_PORT"]
MONGO_USER = os.environ["MONGO_USER"]
MONGO_PASS = os.environ["MONGO_PASS"]

MONGO_NAME = "dki"
MONGO_COLLECTION = "Keywords"

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

if rank == 0:
    # Initialize status object
    status = Status()

    # Distribute sources to each worker. If more workers than sources, give same
    # sources to multiple workers so they can take different walks
    i = 1
    
    # TODO: remove i<10 and put this stuff in the db instead of a local array

    # The given index in this request object list corresponds to the worker of id index + 1
    receiveMessages = list()
    for idx in range(size - 1):
        receiveMessages.append(comm.irecv(source=idx+1))
        # Send links to workers
        if len(sources) > 0:
            # If there's a link to send, send it
            comm.isend(sources.pop(0), dest=idx+1)
        else:
            # If there is no link, send a blank string and the worker will wait
            comm.isend('', dest=idx+1)
            
    # do stuff
    while len(sources) > 0 and i < 10:
        for idx, req in enumerate(receiveMessages):
            # Check to see if the request has come back yet
            print("req.test(): " + str(req.test()))
            if req.test()[0]:
                # Get the links the worker found
                links = req.wait()
                # Send the worker the next link to work on
                if len(sources) > 0:
                    # If there's a link to send, send it
                    comm.isend(sources.pop(0), dest=idx+1)
                else:
                    # If there is no link, send a blank string and the worker will wait
                    comm.isend('', dest=idx+1)
                # start a new receive message from workers
                receiveMessages[idx] = comm.irecv(source=idx+1)
                status.count("Number of discovered links")
                # add the new links to the sources
                if links is None:
                    continue
                for link in links:
                    if link not in explored:
                        sources.append(link)
                        explored.add(link)
                    else:
                        status.count("Number of repeated links")
                       
                i+=1
else: 
    while True:
        # Wait to receive a source from the master
        source = comm.recv(source=0)
        links = list()

        if source == '':
            # We got a blank link, wait for a while then ask again
            time.sleep(1)
        else:

            s.setUrl(source.strip())
            keywords, links = s.scrape()

            print("number of links: " + str(len(links)))

            # Persist keywords to the database
            s.submitWords(keywords)

        # Send new links back to the master queue
        comm.send(links, dest=0)  
