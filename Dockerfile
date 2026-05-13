FROM python:3.11-slim

WORKDIR /workspace

# Cài đặt các công cụ cần thiết cho môi trường test
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt pytest

# Thiết lập biến môi trường Airflow để chạy test không bị lỗi
ENV AIRFLOW_HOME=/tmp/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Command mặc định
CMD ["python", "-m", "pytest"]