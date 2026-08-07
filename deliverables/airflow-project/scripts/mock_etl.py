import json
from datetime import datetime

summary = {
    "generated_at": datetime.now().isoformat(),
    "message": "Message in json"
}

# Note: the opt/airflow portion there is a docker thing that references mounted voulumes
with  open("/opt/airflow/output/mock_etl_summary.json", "w") as f: #TODO: fix filepath
    json.dump(summary, f, indent=4)

print("summary.json created")