# Retail-Case-Study
## 

## Running on Airflow on your machine
### .ENV file
- Once you have cloned the repository, move into the deliverables folder, then the airflow-project folder within that
- All files needed to run the airflow project are there, except for a `.env` file
- Create the .env file, then add the following lines
  - `AIRFLOW_UID=50000`
  - `FERNET_KEY=Replace_With_Your_Fernet_Key_Here` (this one is optional, but it gets rid of some of the warnings)
  - `_PIP_ADDITIONAL_REQUIREMENTS=kafka_python apache-airflow-providers-databricks`
- Once you have completed this, you have all the files you need

### Setting up your connection
- This project connects to and reads from the Gold table within your project
- The filepath within our DAG is set up specifically to read from our project, so if you do not have the same filespace/paths, it may not work on your system
- If all is well, then you are still missing a connection. This must be set up in airflow
- Once you have your connection set up in airflow, that is all you need to run the program. All should run well