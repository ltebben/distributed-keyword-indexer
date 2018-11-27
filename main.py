import os
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
  while sources and i<10:
    ind = i%size
    link = sources.pop(0)
    comm.send(link, dest=ind)
    newlinks = comm.recv(source=ind)
    
    for link in newlinks:
      if link not in explored:
        sources.append(link)
        explored.add(link)
    # with open(SOURCES_LOC, 'a') as w:
    #   w.write("\n")
    #   w.write('\n'.join(newlinks))
    i += 1
else: 
  # Wait to receive a source from the master
  source = comm.recv(source=0)

  s.setUrl(source.strip())
  keywords, links = s.scrape()

  print("number of links: " + str(len(links)))

  # Persist keywords to the database
  s.submitWords(keywords)

  # Send new links back to the master queue
  comm.send(links, dest=0)  
