# distributed-keyword-indexer
Parallel Computing final project

# OSC Environment Setup
* Use python3: `module load python/3.4.1`
* Create a virtual environment: `cd <workdir>; python -m venv .`
* Activate the environment: `source bin/activate`
* Install mpi4py: `pip install mpi4py`

# To run
* run: `mpiexec -n <total number of nodes> python main.py`
