# Healthcare Data Pipeline — CI/CD with Airflow & Cloud Build

## Tổng quan

Pipeline ETL tự động xử lý dữ liệu y tế từ nhiều nguồn (Hospital A, Hospital B) thông qua kiến trúc Medallion (Bronze → Silver → Gold) trên Google Cloud Platform.

## Kiến trúc hệ thống

```
GitHub (push) → Cloud Build (CI/CD) → Cloud Composer (Airflow)
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    ▼                      ▼                      ▼
              Dataproc (PySpark)     BigQuery (SQL)          GCS (Storage)
              - hospitalA.py         - bronze.sql            - Landing Zone
              - hospitalB.py         - silver.sql            - Archive
              - claims.py           - gold.sql
              - cpt_codes.py
```

## Cấu trúc thư mục

```
Healthcare-GCP/
├── Airflow/                    # DAG files
│   ├── parent_dag.py           # Orchestrator DAG
│   ├── pyspark_dag.py          # PySpark ingestion DAG
│   └── bq_dag.py               # BigQuery transformation DAG
├── Ingestion Dataproc/         # PySpark scripts
│   ├── hospitalA_postgresToLanding.py
│   ├── hospitalB_postgresToLanding.py
│   ├── claims.py
│   └── cpt_codes.py
├── Medallion BigQuery/         # SQL transformations
│   ├── bronze.sql
│   ├── silver.sql
│   └── gold.sql
├── scripts/                    # CI/CD scripts
│   ├── validate_dag.sh
│   ├── deploy_dag.sh
│   └── trigger_dag.sh
├── tests/                      # Unit tests
│   └── test_dag_import.py
├── cloudbuild.yaml             # Cloud Build pipeline
├── requirements.txt
└── README.md
```

## Quy trình CI/CD

### Luồng tự động khi push code:

1. **Validate** — Kiểm tra syntax Python & DAG imports
2. **Test** — Chạy unit tests (`pytest`)
3. **Upload Scripts** — Copy Ingestion & SQL files lên GCS
4. **Deploy DAGs** — Deploy DAG files lên Cloud Composer
5. **Trigger** — Trigger `parent_dag` chạy pipeline

### Cách setup:

1. Push code lên GitHub repository
2. Tạo Cloud Build Trigger:
   - GCP Console → Cloud Build → Triggers → Create Trigger
   - Source: GitHub repository
   - Event: Push to branch `main`
   - Config: `cloudbuild.yaml`
3. Mỗi lần `git push` → pipeline CI/CD tự động chạy

## GCP Services

| Service | Mục đích |
|---------|----------|
| Cloud Composer | Orchestrate DAGs (Airflow) |
| Dataproc | Chạy PySpark jobs |
| Cloud SQL | PostgreSQL databases (Hospital A & B) |
| BigQuery | Data warehouse (Bronze/Silver/Gold) |
| GCS | Landing zone, archive, configs |
| Cloud Build | CI/CD pipeline |


Dòng này kiểm chứng sự chính xác của CI/CD
