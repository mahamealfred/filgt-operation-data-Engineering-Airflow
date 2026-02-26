import requests
import json
from datetime import datetime
from pathlib import Path

URL= "https://opensky-network.org/api/states/all"
 

def run_bronze_ingestion(**context):
    response = requests.get(URL, timeout=30)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data = response.json()
    
    # Create a directory for the current date if it doesn't exist and save the data with a timestamped filename
    timestamp = datetime.utcnow().strftime("%Y-%m-%d%H%M%S")
    output_dir = Path("/opt/airflow/data/bronze")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"flight_bronze_{timestamp}.json"
    
    with open(output_file, 'w') as f:
        json.dump(data, f)

    context['ti'].xcom_push(key='bronze_file', value=str(output_file))
     
    print(f"Data ingested and saved to {output_file}")