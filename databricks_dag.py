from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json

with DAG(
    dag_id="test_databricks_job",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:

    run_job = DatabricksRunNowOperator(
        task_id="trigger_etl_job",
        databricks_conn_id="databricks_default",
        job_id="409613695793073",
    )

    fetch_gold_rows = DatabricksSqlOperator(
        task_id="fetch_gold_rows",
        databricks_conn_id="databricks_sql",
        sql="SELECT sales_month, category, order_count, total_quantity, total_revenue FROM retail_fresher.monthly_category_sales",
        do_xcom_push=True,
    )

    def publish_events(**context):
        rows = context["ti"].xcom_pull(task_ids="fetch_gold_rows")
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        for row in rows:
            event = {
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "sales_month": row["sales_month"],
                "category": row["category"],
                "order_count": row["order_count"],
                "total_quantity": row["total_quantity"],
                "total_revenue": row["total_revenue"],
            }
            producer.send("retail-sales-summary", value=event)
        producer.flush()

    def consume_sample(**context):
        consumer = KafkaConsumer(
            "retail-sales-summary",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        with open("/opt/airflow/dags/consumed_events.jsonl", "w") as f:
            for msg in consumer:
                f.write(json.dumps(msg.value) + "\n")

    publish_events_task = PythonOperator(
        task_id="publish_events",
        python_callable=publish_events,
    )

    consume_sample_task = PythonOperator(
        task_id="consume_sample",
        python_callable=consume_sample,
    )

    run_job >> fetch_gold_rows >> publish_events_task >> consume_sample_task  # pyright: ignore[reportUnusedExpression, reportOperatorIssue]