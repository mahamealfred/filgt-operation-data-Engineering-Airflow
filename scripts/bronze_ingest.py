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
    output_dir = Path(f"/opt/airflow/data/bronze/flight_{timestamp}.json")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir, 'w') as f:
        json.dump(data, f)
    
    print(f"Data ingested and saved to {output_dir}")