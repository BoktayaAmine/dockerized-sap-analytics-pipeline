# End-to-End SAP ELT Pipeline with Docker, Airflow, Snowflake, and dbt

This project demonstrates a complete, production-style ELT (Extract, Load, Transform) data pipeline. It simulates a real-world scenario of extracting raw SAP sales data, loading it into a cloud data warehouse, transforming it into an analysis-ready format, and ensuring data quality through automated testing.

The entire environment is containerized with Docker, orchestrated with Apache Airflow, and built on a modern data stack with Snowflake and dbt.

## Project Architecture

![Pipeline Architecture](path/to/your/diagram.png)
*(**Action:** Replace this line with a screenshot of your Eraser.io diagram!)*

---

## Tech Stack

*   **Containerization:** Docker & Docker Compose
*   **Orchestration:** Apache Airflow
*   **Data Warehouse:** Snowflake
*   **Transformation:** dbt (Data Build Tool)
*   **Language:** Python & SQL

---

## Pipeline Stages Explained

The pipeline is orchestrated as an Airflow DAG that executes a sequence of tasks to process the data from its raw state to a clean, analysis-ready format.

### Stage 1: Extract & Load

The first phase reliably ingests raw data into our Snowflake data warehouse.

1.  **Extract (Simulation):** The process begins with five mock CSV files representing core SAP sales tables (`KNA1` - Customers, `MARA` - Products, `VBAK`/`VBAP` - Orders).
2.  **Upload to Stage:** An Airflow task uploads these files to a Snowflake internal stage. This step is **idempotent**, meaning re-running it safely overwrites existing files.
3.  **Load into Raw Tables:** A second Airflow task first **truncates** the destination tables to prevent duplication, then uses the `COPY INTO` command to load the data from the stage. All raw tables are designed with `VARCHAR` columns to ensure 100% of the source data is captured without load failures.

### Stage 2: Transform with dbt

This is where raw data is refined into valuable assets. A single `dbt run` command, triggered by Airflow, executes a multi-layered transformation workflow.

1.  **Staging Layer:** Raw tables are cleaned, with columns renamed to be business-friendly (e.g., `KUNNR` -> `customer_id`) and data types safely cast to their proper formats (e.g., `VARCHAR` -> `DATE`).
2.  **Intermediate Layer:** Staging models are joined to create more complex logical units, such as a unified view of all order items with their parent order details.
3.  **Data Mart Layer:** The final layer creates wide, de-normalized tables optimized for business intelligence. The primary model, `fct_sales_orders`, joins all relevant entities (customers, products, orders) into a single, comprehensive table ready for analysis.

### Stage 3: Data Quality Assurance

An automated pipeline requires automated trust. An Airflow task runs `dbt test` to execute a suite of data quality tests against the final models, including:
*   **Uniqueness & Not-Null Tests:** Ensuring primary keys are valid.
*   **Referential Integrity:** Verifying that every `customer_id` in the sales table exists in the customers table.

If any test fails, the pipeline fails, preventing bad data from reaching analysts.

---

## How to Run This Project

### Prerequisites

*   Git
*   Docker and Docker Compose
*   A free Snowflake account

### 1. Setup

**a. Clone the repository:**
```bash
git clone <my-repository-url>
cd end-to-end-sap-elt-pipeline
```

**b. Configure your environment:**
Create a `.env` file by copying the example file.
```bash
cp .env.example .env
```
Now, edit the `.env` file and fill in your Snowflake credentials and desired Airflow UID.
```env
# Airflow basic config
AIRFLOW_UID=50000

# Snowflake Credentials for dbt & Airflow
SNOWFLAKE_ACCOUNT=<your_account_identifier> # e.g., org-account
SNOWFLAKE_USER=<your_snowflake_user>
SNOWFLAKE_PASSWORD=<your_snowflake_password>
SNOWFLAKE_ROLE=ETL_ROLE
SNOWFLAKE_DATABASE=SAP_ANALYTICS_DB
SNOWFLAKE_WAREHOUSE=ETL_WH
SNOWFLAKE_SCHEMA=RAW_STAGING # Default schema for connections
```

**c. Set up your Snowflake environment:**
Log in to your Snowflake account and run the SQL script located in `snowflake_setup/setup.sql` to create the necessary roles, warehouse, database, and schemas.

### 2. Execution

**a. Launch the environment:**
From the project's root directory, start all services using Docker Compose.
```bash
docker-compose up -d
```

**b. Access Airflow:**
The Airflow UI will be available at `http://localhost:8084`. Log in with the default credentials (`admin`/`admin`).

**c. Trigger the pipeline:**
In the Airflow UI, find the `sap_elt_pipeline` DAG, un-pause it, and trigger a manual run. You can monitor the progress of each task in the "Grid" view.

**d. Shut down the environment:**
When you are finished, you can stop all services.
```bash
docker-compose down
```
