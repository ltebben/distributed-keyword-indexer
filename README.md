# distributed-keyword-indexer
Parallel Computing final project

# OSC Environment Setup
* Use python3: `module load python/3.4.2`
* Create a virtual environment: `cd <workdir>; python -m venv .`
* Activate the environment: `source bin/activate`
* Install dependencies: `pip install -r requirements.txt`

# To run (from within the project directory)
* Activate the environment: `source bin/activate`
* Set env vars using: `.env`
* run: `mpiexec -n <total number of nodes> python main.py`

# Attributions
`stop-words.txt` copied from [Alireza Savand](https://github.com/Alir3z4/stop-words/tree/bd8cc1434faeb3449735ed570a4a392ab5d35291) and is licensed under [Creative Commons Attribution 4.0 International License](https://github.com/Alir3z4/stop-words/blob/master/LICENSE).
