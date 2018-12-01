import os
import copy
from mpi4py import MPI
from scrape import Scrape
from pymongo import MongoClient

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
  # Distribute sources to each worker. If more workers than sources, give same
  # sources to multiple workers so they can take different walks
  i = 1
  
  # TODO: remove i<10 and put this stuff in the db instead of a local array

  # Create lists of messages/lists
  # The given index in these lists corresponds to the worker of id index + 1
  receiveMessages = list()
  receiveLinks = list()
  sendMessages = list()
  for idx in range(size - 1):
    receiveLinks.append(None)
    receiveMessages.append(comm.Irecv(receiveLinks[idx], source=idx+1))
  # do stuff
  while source and i < 10:
    for idx, req in enumerate(receiveMessages):
      # Check to see if the request has come back yet
      if req.test():
        links = copy.copy(receiveLinks[idx])
        sendMessages.append(comm.Isend(sources.pop(0), dest=idx+1))
        receiveLinks[idx] = None
        receiveMessages[idx] = comm.Irecv(receiveLinks[idx], source=idx+1);
        for link in links:
          if link not in explored:
            sources.append(link)
            explored.add(link)
        i+=1
else: 
  while True:
    # Wait to receive a source from the master
    source = comm.recv(source=0)

    s.setUrl(source.strip())
    keywords, links = s.scrape()

    print("number of links: " + str(len(links)))

    # Persist keywords to the database
    s.submitWords(keywords)

    # Send new links back to the master queue
    comm.send(links, dest=0)  
