set -e  # Exit on first error

# Configuration
COMPOSER_BUCKET="${COMPOSER_BUCKET:-asia-southeast1-healthcare--325dbf4b-bucket}"

echo "Deploy Pipeline to GCS"
echo "Bucket: ${COMPOSER_BUCKET}"

# 1. Upload Ingestion Scripts
echo ""
echo "Uploading Ingestion scripts"

INGESTION_FILES=(
    "Ingestion Dataproc/hospitalA_postgresToLanding.py"
    "Ingestion Dataproc/hospitalB_postgresToLanding.py"
    "Ingestion Dataproc/claims.py"
    "Ingestion Dataproc/cpt_codes.py"
)

for file in "${INGESTION_FILES[@]}"; do
    filename=$(basename "$file")
    gsutil cp "$file" "gs://${COMPOSER_BUCKET}/data/Ingestion/${filename}"
    echo "Uploaded: ${filename}"
done

# 2. Upload SQL Files
echo ""
echo "Uploading SQL files"

SQL_FILES=(
    "Medallion BigQuery/bronze.sql"
    "Medallion BigQuery/silver.sql"
    "Medallion BigQuery/gold.sql"
)

for file in "${SQL_FILES[@]}"; do
    filename=$(basename "$file")
    gsutil cp "$file" "gs://${COMPOSER_BUCKET}/data/BQ/${filename}"
    echo "Uploaded: ${filename}"
done

# 3. Deploy DAG Files (child DAGs first, parent DAG last)
echo ""
echo "Deploying DAG files"

# Child DAGs first
gsutil cp Airflow/pyspark_dag.py "gs://${COMPOSER_BUCKET}/dags/pyspark_dag.py"
echo "Deployed: pyspark_dag.py"

gsutil cp Airflow/bq_dag.py "gs://${COMPOSER_BUCKET}/dags/bq_dag.py"
echo "Deployed: bq_dag.py"

# Wait for child DAGs to register
echo "Waiting 10s for child DAGs to register..."
sleep 10

# Parent DAG last
gsutil cp Airflow/parent_dag.py "gs://${COMPOSER_BUCKET}/dags/parent_dag.py"
echo "Deployed: parent_dag.py"

# Summary
echo ""
echo "All components deployed successfully!"
