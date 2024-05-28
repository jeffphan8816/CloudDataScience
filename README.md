# Team 69
## Dillon Blake 1524907, Andrea Delahaye 1424289, Yue Peng 958289, Jeff Phan 1577799, Alistair Wilcox 212544

# Basic Instructions 
Make sure you are on the UniMelb network or a VPN. Our bastion node is 172.26.135.52

# Installation
The deployment specifications can be found in the Makefile in the deployment directory. With the architecture setup, the software can be installed my running ```make deploy``` in the backend/api and backend/fission/functions directories. There are also teardown commands in these makefiles. The Elasticseach indices were created using the bash scripts in the development directory. Various python functions in the backend/data_functions directory were used to upload static data from the data directory.

# Testing
Our GitLab repository has a CI/CD pipeline, but to run tests locally the following commands must be run
```
pip install ./
pip install -r requirements.txt
python -m unittest discover test
```

# Documentation
To generate documentation first run ```make doc``` in the backend directory. To deploy a web server to view the generated HTML, run ```make deploy``` in the docs directory. The documentation can then be accessed at the following urls:

http://172.26.135.52:9090/docs/api.html for the REST API
http://172.26.135.52:9090/docs/fission.html for the Fission harvester functions
http://172.26.135.52:9090/docs/data_functions.html for the static data functions

# Run Jupyter Notebooks
There are two ways to run the frontend notebooks. The first option is to run them locally from the frontend directory and the second is to run them on the bastion at http://172.26.135.52:8888/lab/workspaces/auto-J/tree/cloudcompa2. Running them locally has better performance and can be done with the following commands:
```
pip install requirements.txt
cd frontend
jupyter notebook
```
The notebooks are as follows:
* Crime_Dashboard.ipynb: The main interface for analyzing crime data
* EPA_Dashboard.ipynb: The main interface for analyzing EPA data
* weathervscrash_peer.ipynb: The main interface for analyzing Crash data
* epa_model_trainer.ipynb: The interface to design models with EPA data
* weathervscrashmodel.ipynb: The interface to design models with crash data

More detailed descriptions of how to use these notebooks can be found in section 4 of our report.