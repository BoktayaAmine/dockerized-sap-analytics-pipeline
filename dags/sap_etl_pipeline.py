from __future__ import annotations
import pendulum
import os
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from logging import getLogger

log = getLogger(__name__)

# --- CONFIGURATION ---
SNOWFLAKE_CONN_ID = "snowflake_default"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DATA_DIR = "/opt/airflow/data" 
SNOWFLAKE_STAGE = "SAP_CSV_STAGE"
SNOWFLAKE_DATABASE = "SAP_ANALYTICS_DB" # Added for clarity
SNOWFLAKE_SCHEMA = "RAW_STAGING" 

# --- PYTHON FUNCTION FOR UPLOADING (Task 1) ---
def upload_files_to_stage():
    """
    Scans the local data directory and uploads each CSV file to the Snowflake stage.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    if not csv_files:
        raise ValueError("No CSV files found in the data directory.")
    log.info(f"Found {len(csv_files)} CSV files to upload: {csv_files}")
    for filename in csv_files:
        local_file_path = os.path.join(DATA_DIR, filename)
        put_sql = (
            f"PUT file://{local_file_path} @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE} "
            f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
        )
        log.info(f"Executing PUT command for {filename}...")
        hook.run(put_sql)
        log.info(f"Successfully uploaded {filename} to stage {SNOWFLAKE_STAGE}.")

# --- NEW PYTHON FUNCTION FOR COPYING (Task 2) ---
def load_stage_to_raw_tables():
    """
    TRUNCATES the raw tables and then executes a COPY INTO command for each to
    load data from the stage. This ensures the process is idempotent.
    """
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    table_map = {
        "KNA1": "RAW_KNA1",
        "MARA": "RAW_MARA",
        "VBAK": "RAW_VBAK",
        "VBAP": "RAW_VBAP",
        "T005T": "RAW_T005T"
    }
    
    log.info("Starting idempotent load from stage into raw tables...")

    for file_prefix, table_name in table_map.items():
        full_table_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}"
        staged_file = f"{file_prefix}.csv.gz"
        
        # --- NEW ---
        # Step 1: Truncate the table to ensure a clean load
        truncate_sql = f"TRUNCATE TABLE {full_table_name};"
        log.info(f"Executing TRUNCATE command for {table_name}...")
        hook.run(truncate_sql)
        log.info(f"Successfully truncated {table_name}.")
        
        # Step 2: Load data from the stage
        copy_sql = f"""
            COPY INTO {full_table_name}
            FROM @{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_STAGE}/{staged_file}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1, NULL_IF = ('NULL', ''))
            ON_ERROR = 'CONTINUE';
        """
        
        log.info(f"Executing COPY command for {table_name} from {staged_file}...")
        hook.run(copy_sql)
        log.info(f"Successfully loaded data into {table_name}.")
        
# --- DAG DEFINITION ---
with DAG(
    dag_id="sap_elt_pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["sap", "snowflake", "dbt"],
) as dag:
    
    task_1_upload_to_stage = PythonOperator(
        task_id="upload_csv_to_snowflake_stage",
        python_callable=upload_files_to_stage,
    )

    # THIS IS THE UPDATED TASK
    task_2_load_to_raw_tables = PythonOperator(
        task_id="load_stage_to_raw_tables",
        python_callable=load_stage_to_raw_tables,
    )

    task_3_run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )

    task_4_test_dbt_models = BashOperator(
        task_id="test_dbt_models",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )

    task_1_upload_to_stage >> task_2_load_to_raw_tables >> task_3_run_dbt_models >> task_4_test_dbt_models