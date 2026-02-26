import json
import pandas as pd
from pathlib import Path

def run_silver_transformation(**context):
    execution_date = context['ds_nodash']
    run_timestamp = context.get('ts_nodash', execution_date)

    bronze_file= context['ti'].xcom_pull(key='bronze_file', task_ids='bronze_ingestion')
    if not bronze_file:
        raise ValueError("No bronze file found in XCom")
    silver_file_path = Path("/opt/airflow/data/silver") 
    silver_file_path.mkdir(parents=True, exist_ok=True)
    with open(bronze_file, 'r') as f:
        data = json.load(f)

    states = data.get('states')
    if not states:
        raise ValueError("No flight states found in bronze file")

    df_raw = pd.DataFrame(states)

    df_raw.columns = [
        'icao24', 'callsign', 'origin_country', 'time_position', 'last_contact',
        'longitude', 'latitude', 'baro_altitude', 'on_ground', 'velocity',
        'true_track', 'vertical_rate', 'sensors', 'geo_altitude',
        'squawk', 'spi', 'position_source'
    ]

    df = df_raw[['icao24', 'origin_country', 'velocity', 'on_ground']]
    output_file = silver_file_path / f"flight_silver_{run_timestamp}.csv"
    df.to_csv(output_file, index=False)

    context['ti'].xcom_push(key='silver_file', value=str(output_file))
    print(f"Data transformed and saved to {output_file}")