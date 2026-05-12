import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# =========================
# CONFIG
# =========================
PROJECT_ID = "healthcare-496102"
LOCATION = "asia-southeast1"

SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/gold.sql"

# Read SQL query from file
def read_sql_file(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as file:
        return file.read()

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Define default arguments
default_args = {
    "owner": "MinhLD",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["minhld0210@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}


# =========================
# DAG
# =========================
with DAG(
    dag_id="bigquery_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    schedule="0 9 * * *",
    catchup=False,
    description="DAG to run the bigquery jobs",
    default_args=default_args,
    tags=["gcs", "bq", "etl/elt"],
) as dag:

    # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        project_id=PROJECT_ID,
        location=LOCATION,
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

# Task dependencies
bronze_tables >> silver_tables >> gold_tables
