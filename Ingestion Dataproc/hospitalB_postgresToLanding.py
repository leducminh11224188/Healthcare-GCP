from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

# Initialize GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Initialize Spark Session
spark = SparkSession.builder.appName("HospitalBToLanding").getOrCreate()

# Google Cloud Storage Configuration
GCS_BUCKET = "healthcare-bucket-minhld"
HOSPITAL_NAME = "hospital-b"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

# BigQuery Configuration
BQ_PROJECT = "healthcare-496102"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

# Postgresql Configuration
POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://10.126.0.5:5432/hospital_b_db",
    "driver": "org.postgresql.Driver",
    "user": "minhld40",
    "password": "Leducminh1406@"
}

# -------------------------------------------------------------------------------------------------------------------------------------
# Logging Mechanism
# Stores logs before writing to GCS
log_entries = []

def log_event(event_type, message, table=None):
    """Log an event and store it in the log list"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")  # Print for visibility

# logs to GCS
def save_logs_to_gcs():
    """Save logs to a JSON file and upload to GCS"""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"  
    
    json_data = json.dumps(log_entries, indent=4)

    # Get GCS bucket
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    
    # Upload JSON data as a file
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

# logs to BQ
def save_logs_to_bigquery():
    """Save logs to BigQuery"""
    if log_entries:
        # Tạo Spark DataFrame từ danh sách log_entries
        log_df = spark.createDataFrame(log_entries)
        
        # Ghi dữ liệu vào bảng BigQuery
        log_df.write.format("bigquery") \
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()
            
        print("Logs stored in BigQuery for future analysis")

# -------------------------------------------------------------------------------------------------------------------------------------
# Function to Move Existing Files to Archive
# Function to Move Existing Files to Archive
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return

    run_ts = datetime.datetime.now().strftime("%H%M%S%f")
    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        # Extract Date from File Name
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        filename = file.split("/")[-1]

        # Move to Archive
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{run_ts}/{filename}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        # Copy file to archive and delete original
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()

        log_event("INFO", f"Moved {file} to {archive_path}", table=table)

# -------------------------------------------------------------------------------------------------------------------------------------
# Function to Get Latest Watermark from BigQuery Audit Table
# Function to Get Latest Watermark from BigQuery Audit Table
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}' and data_source = 'hospital_b_db'
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"

# -------------------------------------------------------------------------------------------------------------------------------------
# Function to Extract Data from Postgresql and Save to GCS
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        clean_load_type = load_type.strip().lower()

        # Get watermark only for incremental
        if clean_load_type == "incremental":
            last_watermark = get_latest_watermark(table)
        else:
            last_watermark = None

        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)

        # Build query
        if clean_load_type == "full":
            query = f"(SELECT * FROM {table}) AS t"
        else:
            query = f"""
                (SELECT * FROM {table}
                 WHERE {watermark_col} > TIMESTAMP '{last_watermark}'
                ) AS t
            """

        log_event("DEBUG", f"Query: {query}", table=table)

        # Read from Postgres
        df = (spark.read.format("jdbc")
              .option("url", POSTGRES_CONFIG["url"])
              .option("user", POSTGRES_CONFIG["user"])
              .option("password", POSTGRES_CONFIG["password"])
              .option("driver", POSTGRES_CONFIG["driver"])
              .option("dbtable", query)
              .load())

        record_count = df.count()
        log_event("INFO", f"Record count for {table}: {record_count}", table=table)

        # IMPORTANT: Skip if no data
        if record_count == 0:
            log_event("INFO", f"No new records for {table}. Skipping.", table=table)
            return 0

        # Write JSON
        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"

        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)

        blob.upload_from_string(
            df.toPandas().to_json(orient="records", lines=True),
            content_type="application/json"
        )

        log_event("SUCCESS", f"JSON written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)

        # Insert Audit ONLY when data exists
        audit_df = spark.createDataFrame([
            ("hospital_a_db", table, clean_load_type,
             record_count, datetime.datetime.now(), "SUCCESS")],
            ["data_source", "tablename", "load_type",
             "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
         .option("table", BQ_AUDIT_TABLE)
         .option("temporaryGcsBucket", GCS_BUCKET)
         .mode("append")
         .save())

        log_event("SUCCESS", f"Audit log updated for {table}", table=table)

        return record_count

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)
        return 0
    
# -------------------------------------------------------------------------------------------------------------------------------------
# Function to Read Config File from GCS
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "Successfully read the config file")
    return df

# -------------------------------------------------------------------------------------------------------------------------------------
# read config file
config_df = read_config_file()

for row in config_df.collect():
    if row["is_active"] == '1' and row["datasource"] == "hospital_b_db":
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()