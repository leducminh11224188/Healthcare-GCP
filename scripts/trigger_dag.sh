set -e  # Exit on first error

# Configuration
PROJECT_ID="${PROJECT_ID:-healthcare-gcp-495708}"
COMPOSER_ENV="${COMPOSER_ENV:-healthcare-airflow}"
COMPOSER_REGION="${COMPOSER_REGION:-asia-southeast1}"
DAG_ID="${DAG_ID:-parent_dag}"

echo "Trigger Airflow DAG"
echo "Environment: ${COMPOSER_ENV}"
echo "Region: ${COMPOSER_REGION}"
echo "DAG: ${DAG_ID}"

# 1. Wait for DAG to be fully parsed
echo ""
echo "Waiting 60s for DAGs to be fully parsed"
sleep 60

# 2. Trigger the DAG
echo ""
echo "Triggering ${DAG_ID}"

gcloud composer environments run "${COMPOSER_ENV}" \
    --location="${COMPOSER_REGION}" \
    --project="${PROJECT_ID}" \
    dags trigger -- "${DAG_ID}"

echo ""
echo "${DAG_ID} triggered successfully!"
echo ""
echo "Monitor the DAG run at:"
echo "Airflow UI → DAGs → ${DAG_ID} → Runs"
