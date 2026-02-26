import pandas as pd
import snowflake.connector
from airflow.hooks.base import BaseHook


def load_gold_to_snowflake(**context):
    gold_file = context['ti'].xcom_pull(
        key='gold_file', task_ids='gold_aggregation')
    if not gold_file:
        raise ValueError("No gold file found in XCom")

    execution_date = context['data_interval_start'].strftime(
        "%Y-%m-%d %H:%M:%S")

    df = pd.read_csv(gold_file)

    # Get Snowflake connection details from Airflow connections
    conn = BaseHook.get_connection('snowflake_conn')
    snowflake_user = conn.login
    snowflake_password = conn.password
    snowflake_account = conn.extra_dejson.get('account')
    snowflake_database = conn.extra_dejson.get('database')
    snowflake_warehouse = conn.extra_dejson.get('warehouse')
    # Default to PUBLIC schema if not specified
    snowflake_schema = conn.extra_dejson.get('schema', 'KPI')
    snowflake_role = conn.extra_dejson.get('role')  # Optional role

    # Connect to Snowflake and load data
    ctx = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        warehouse=snowflake_warehouse,
        schema=snowflake_schema,
        role=snowflake_role
    )

    cs = ctx.cursor()
    try:
        # Create table if it doesn't exist
        cs.execute("""
            CREATE TABLE IF NOT EXISTS FILGHTS_KPIS(
    window_Start TIMESTAMP,
    origin_country TEXT(250),
    total_filghts INT,
    avg_velocity FLOAT,
    on_ground INT,
    load_time TIMESTAMP DEFAULT     CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, origin_country)
            )  
        """)

        # Load data into Snowflake
        for _, row in df.iterrows():
            cs.execute("""
                INSERT INTO FILGHTS_KPIS (window_start, origin_country, total_filghts, avg_velocity, on_ground)
                VALUES (%s, %s, %s, %s, %s)
            """, (execution_date,  row['origin_country'], row['total_flights'], row['avg_velocity'], row['on_ground']))

        print(f"Data from {gold_file} loaded into Snowflake successfully.")
    finally:
        cs.close()
        ctx.close()
