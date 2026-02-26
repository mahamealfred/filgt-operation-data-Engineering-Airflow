import pandas   as pd
from pathlib import Path


def run_gold_aggregation(**context):
    silver_file = context['ti'].xcom_pull(key='silver_file', task_ids='silver_transformation')
    if not silver_file:
        raise ValueError("No silver file found in XCom")
    df = pd.read_csv(silver_file)

    agg = (
        df.groupby('origin_country')
        .agg(
            total_flights=('icao24', 'count'),
            avg_velocity=('velocity', 'mean'),
            on_ground=('on_ground', 'sum')
        )
        .reset_index()
    )

    gold_file_path = Path(silver_file.replace('silver', 'gold'))
    context['ti'].xcom_push(key='gold_file', value=str(gold_file_path))  
    agg.to_csv(gold_file_path, index=False)
    print(f"Aggregated data saved to {gold_file_path}") 