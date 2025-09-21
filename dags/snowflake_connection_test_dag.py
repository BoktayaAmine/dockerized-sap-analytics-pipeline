from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

with DAG(
    dag_id="snowflake_connection_test",
    start_date=pendulum.datetime(2025, 8, 26, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="A simple DAG to test the connection to Snowflake.",
    tags=["test", "snowflake"],
) as dag:
    
    test_snowflake_connection = SnowflakeOperator(
        task_id="test_snowflake_connection",
        # This must match the Connection ID you created in the Airflow UI
        snowflake_conn_id="snowflake_default", 
        
        # This simple query is perfect for a connection test.
        # It doesn't modify any data and confirms which user, role, and warehouse
        # Airflow is using to connect.
        sql="SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();",
        
        # You can add a handler for success/failure if you want
        # handler=lambda cur: print(cur.fetchall()) 
    )