set -e  # Exit on first error

echo "DAG Validation Script"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

ERRORS=0

# 1. Python Syntax Check
echo ""
echo "Checking Python syntax"

DAG_FILES=(
    "Airflow/parent_dag.py"
    "Airflow/pyspark_dag.py"
    "Airflow/bq_dag.py"
)

SCRIPT_FILES=(
    "Ingestion Dataproc/hospitalA_postgresToLanding.py"
    "Ingestion Dataproc/hospitalB_postgresToLanding.py"
    "Ingestion Dataproc/claims.py"
    "Ingestion Dataproc/cpt_codes.py"
)

for file in "${DAG_FILES[@]}" "${SCRIPT_FILES[@]}"; do
    if python -m py_compile "$file" 2>/dev/null; then
        echo -e "  ${GREEN} PASS${NC}: $file"
    else
        echo -e "  ${RED} FAIL${NC}: $file"
        ERRORS=$((ERRORS + 1))
    fi
done

# 2. DAG Import Check (using DagBag)
echo ""
echo "Checking DAG imports"

python3 -c "
import sys
sys.path.insert(0, '.')
from airflow.models import DagBag

dag_bag = DagBag(dag_folder='Airflow/', include_examples=False)

if dag_bag.import_errors:
    print('DAG Import Errors:')
    for dag_id, error in dag_bag.import_errors.items():
        print(f'  - {dag_id}: {error}')
    sys.exit(1)
else:
    print(f'{len(dag_bag.dags)} DAGs loaded successfully')
    for dag_id in dag_bag.dags:
        print(f'  - {dag_id}')
" 2>/dev/null

if [ $? -ne 0 ]; then
    echo -e "  ${RED} DAG import check failed${NC}"
    ERRORS=$((ERRORS + 1))
else
    echo -e "  ${GREEN} DAG import check passed${NC}"
fi

# 3. SQL File Check
echo ""
echo "Checking SQL files exist"

SQL_FILES=(
    "Medallion BigQuery/bronze.sql"
    "Medallion BigQuery/silver.sql"
    "Medallion BigQuery/gold.sql"
)

for file in "${SQL_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo -e "  ${GREEN} FOUND${NC}: $file"
    else
        echo -e "  ${RED} MISSING${NC}: $file"
        ERRORS=$((ERRORS + 1))
    fi
done

# Summary
echo ""
if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN} All validations passed! Safe to deploy.${NC}"
    exit 0
else
    echo -e "${RED} $ERRORS validation(s) failed. Deployment blocked.${NC}"
    exit 1
fi
