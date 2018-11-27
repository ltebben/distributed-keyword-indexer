from mpi4py import MPI
from scrape import Scrape
# Define constants for config files
SOURCES_LOC = "sources.txt"
s = Scrape()
# Get cluster variables
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
    print(newlinks)
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
  comm.send(links, dest=0)
