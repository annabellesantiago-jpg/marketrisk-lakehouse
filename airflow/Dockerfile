FROM apache/airflow:2.9.1

# Switch to root to install system dependencies
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user — never run Airflow as root
USER airflow

# Copy requirements file — better than hardcoding in RUN
COPY airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    -r /requirements.txt 
