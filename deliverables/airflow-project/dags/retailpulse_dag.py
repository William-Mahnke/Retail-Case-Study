from datetime import datetime
from airflow import DAG
import pendulum
import json
from decimal import Decimal
from kafka import KafkaProducer
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

local_tz = pendulum.timezone("America/Los_Angeles")

def kafka_producer():
    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

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
            "retail-sales-summary",
            value=row
        )

    # This signals to the consumer that we have finished transmitting all data
    producer.send(
        "retail-sales-summary",
        {
            "event_type": "END_OF_BATCH"
        }
    )

    # Ensure the message is sent before exiting
    producer.flush()

    print("Message sent to Kafka.")


with DAG(
    dag_id="retailpulse_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule="0 9 * * 3",
    catchup=False,
) as dag:
    ## The following runs the databricks job, using the connection set up in airflow
    run_job = DatabricksRunNowOperator(
        task_id="trigger_etl_job",
        databricks_conn_id="DatabricksDefault",
        job_id="409613695793073",
    )
    run_producer = PythonOperator(
        task_id="run_kafka_producer",
        python_callable= kafka_producer,
    )
    run_consumer = BashOperator(
        task_id="run_kafka_consumer",
        bash_command="python /opt/airflow/scripts/kafka_consumer.py"
    )

run_job >> run_producer >> run_consumer
