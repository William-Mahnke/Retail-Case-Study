import json
from datetime import datetime
from kafka import KafkaProducer
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# The temp section here should be replaced with the databricks query portion once that is complete

######################
# TEMP SECTION START #
######################

# Read the JSON file created by mock_etl.py
with open("/opt/airflow/output/mock_etl_summary.json", "r") as f:
    data = json.load(f)

# Add
data["kafka_timestamp"] = datetime.now().isoformat()

####################
# TEMP SECTION END #
####################

# write file
producer.send(
    "retailpulse",
    value=data
)

# Ensure the message is sent before exiting
producer.flush()

# This signals to the consumer that we have finished transmitting all data
producer.send(
    "retailpulse",
    {
        "event_type": "END_OF_BATCH"
    }
)

print("Message sent to Kafka.")