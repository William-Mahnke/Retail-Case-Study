# Retail-Case-Study
## Deliverables
- This project was made and is run with a docker container, to ensure compatibility across multiple systems.
- The full source code for the Airflow + Kafka portion is within the "deliverables" folder. The "airflow-project" folder contains all source code for the project.
  - The dag is named `retailpulse_dag.py` and contained within the dags
  - The code for the kafka consumer is contained within the "scripts" folder, in the `kafka_consumer.py` file
  - The code for the kafka producer is contained within a function in the dag `retailpulse_dag.py` file.
    - There is a separate file in the scripts folder named "kafka_producer.py". It is a left over file from testing, and contains the same code found within the dag. This file is never executed
- The `retail_sales_pipeline.ipynb` contains the code we used in our databricks instance

## Running on Airflow on your machine
- Note: This project relies on our Databricks cloud project. Without connection credentials to it, this project will not run

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