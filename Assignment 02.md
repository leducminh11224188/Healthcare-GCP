# Assignment: Thực hành CI/CD cho Airflow DAG với GitHub và Google Cloud Build

## 1. Bối cảnh

Bạn là Data Engineer trong một team phân tích dữ liệu. Team đang sử dụng Google Airflow Composer để điều phối các pipeline ETL. Hiện tại, mỗi khi có thay đổi trong source code của DAG hoặc các module liên quan, việc deploy và trigger lại DAG vẫn đang làm thủ công, dễ gây sai sót và mất thời gian.

Nhiệm vụ của bạn là xây dựng một quy trình CI/CD tự động sử dụng:

- GitHub để quản lý source code
- Google Cloud Build để build, test và deploy
- Apache Airflow để orchestrate workflow

Hệ thống cần đảm bảo rằng khi lập trình viên push code mới lên repository, pipeline sẽ tự động kiểm tra, deploy DAG mới và trigger chạy lại DAG tương ứng.

---

## 2. Mục tiêu assignment

Sau khi hoàn thành assignment, bạn cần đạt được các mục tiêu sau:

1. Thiết lập repository GitHub chứa source code Airflow DAG/Script Pyspark, BigQuery,...
2. Viết pipeline CI/CD bằng Google Cloud Build.
3. Tự động thực hiện các bước:
   - checkout source code
   - kiểm tra cú pháp và quality cơ bản
   - cập nhật script, code lên môi trường cần thiết, nếu có (Cloud Storage, BigQuery,...)
   - deploy DAG lên môi trường Airflow
   - trigger chạy lại DAG sau khi deploy thành công
4. Đảm bảo chỉ khi code hợp lệ thì DAG mới được triển khai.
5. Ghi nhận log và bằng chứng cho toàn bộ quá trình CI/CD.

---

## 3. Yêu cầu bài toán

### 3.1. Mô tả hệ thống cần xây dựng

Tham khảo xây dựng một repository có cấu trúc như sau:

```bash
airflow-cicd-assignment/
├── dags/
│   └── example_etl_dag.py
├── scripts/
│   ├── validate_dag.sh
│   ├── deploy_dag.sh
│   └── trigger_dag.sh
├── tests/
│   └── test_dag_import.py
├── cloudbuild.yaml
├── requirements.txt
└── README.md