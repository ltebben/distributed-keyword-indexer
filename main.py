from mpi4py import MPI

# Define constants for config files
SOURCES_LOC = "sources.txt"

# Get cluster variables
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Initialize root set of sources
sources_file = open(SOURCES_LOC, "r")
sources = sources_file.readlines()

if rank == 0:
  # Distribute sources to each worker. If more workers than sources, give same
  # sources to multiple workers so they can take different walks
  for i in range(1, size):
    comm.send(sources[(i-1) % len(sources)], dest=i)
else:
  # Wait to receive a source from the master 
  source = comm.recv(source=0) 
