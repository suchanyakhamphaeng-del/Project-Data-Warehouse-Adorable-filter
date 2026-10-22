# ===================================================================
#  Airflow image สำหรับ DW stack
#  - ต่อยอดจาก official apache/airflow
#  - ติดตั้ง dbt-postgres ใน virtualenv แยก (/opt/dbt_venv)
#    เพื่อไม่ให้ dependency ของ dbt ชนกับของ Airflow
#  - ติดตั้ง apache-airflow-providers-postgres สำหรับ hook/operator
#  - ติดตั้ง astronomer-cosmos สำหรับ orchestrate dbt ผ่าน Airflow
# ===================================================================
ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5-python3.11
FROM ${AIRFLOW_IMAGE_NAME}

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
ARG DBT_VERSION=1.9.0
ARG COSMOS_VERSION=1.15.1

# ---------- ส่วนที่ต้องใช้สิทธิ์ root ----------
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        git \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# dbt อยู่ใน venv แยกต่างหาก เรียกใช้ด้วย /opt/dbt_venv/bin/dbt
# (ไม่ใส่ /opt/dbt_venv/bin ลงใน PATH เพราะจะไปทับ python ของ Airflow)
ENV DBT_VENV=/opt/dbt_venv
RUN python -m venv ${DBT_VENV} \
    && ${DBT_VENV}/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
    && ${DBT_VENV}/bin/pip install --no-cache-dir "dbt-postgres==${DBT_VERSION}" \
    && chmod -R a+rX ${DBT_VENV} \
    && ${DBT_VENV}/bin/dbt --version

# ---------- ส่วนที่ติดตั้งใน environment ของ Airflow ----------
USER airflow

# ใช้ constraint ของ Airflow เพื่อไม่ให้ provider ไปดึง dependency ที่ชนกัน
# astronomer-cosmos = ตัวแปลง dbt project เป็น task ของ Airflow (1 model = 1 task)
#   ติดตั้งใน env ของ Airflow ส่วนตัว dbt ยังเรียกจาก /opt/dbt_venv เหมือนเดิม
RUN pip install --no-cache-dir \
      "apache-airflow-providers-postgres" \
      "astronomer-cosmos==${COSMOS_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

WORKDIR /opt/airflow
