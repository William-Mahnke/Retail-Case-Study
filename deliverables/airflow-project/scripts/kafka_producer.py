import json
from datetime import datetime
from kafka import KafkaProducer
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from decimal import Decimal

def kafka_producer():
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # The temp section here should be replaced with the databricks query portion once that is complete

    ######################
    # TEMP SECTION START #
    ######################

    ## Read the JSON file created by mock_etl.py
    #with open("/opt/airflow/output/mock_etl_summary.json", "r") as f:
    #    data = json.load(f)
    #
    ## Add
    #data["kafka_timestamp"] = datetime.now().isoformat()

    ####################
    # TEMP SECTION END #
    ####################

    ############################
    # DataBricks section start #
    ############################

    def fetch_gold_rows():
            hook = DatabricksSqlHook(databricks_conn_id="DatabricksDefault")
            sql = "SELECT event_id, event_type, sales_month, category, order_count, total_quantity, total_revenue FROM workspace.retail_fresher.gold_events"
            with hook.get_conn() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    columns = [desc[0] for desc in cursor.description]
                    rows = []
                    for row in cursor.fetchall():
                        row_dict = dict(zip(columns, row))
                        row_dict = {k: (float(v) if isinstance(v, Decimal) else v) for k, v in row_dict.items()}
                        rows.append(row_dict)
            return rows

    rows = fetch_gold_rows()
    for row in rows:
        row["producer_timestamp"] = datetime.now().isoformat()

        producer.send(
            "retailpulse",
            value=row
        )

    ##########################
    # DataBricks section end #
    ##########################

    # This signals to the consumer that we have finished transmitting all data
    producer.send(
        "retailpulse",
        {
            "event_type": "END_OF_BATCH"
        }
    )

    # Ensure the message is sent before exiting
    producer.flush()

    print("Message sent to Kafka.")