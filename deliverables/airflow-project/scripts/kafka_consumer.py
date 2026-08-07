import json
from kafka import KafkaConsumer

# Creating a consumer
consumer = KafkaConsumer(
    "retail-sales-summary",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

# Store all received rows
events = []

# Read messages until END_OF_BATCH
for message in consumer:
    event = message.value
    if event.get("event_type") == "END_OF_BATCH":
        break
    events.append(event)

with open("/opt/airflow/output/consumed_events.json", "w") as f:
    json.dump(events, f, indent=4)

consumer.close()

print(f"Wrote {len(events)} events to kafka_summary.json")