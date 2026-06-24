FROM apache/airflow:2.6.2-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libsasl2-dev \
    libkrb5-dev \
    krb5-user \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set permissions for krb5-config
RUN chmod 755 /usr/bin/krb5-config

# Switch to airflow user
USER airflow

# Upgrade pip
RUN pip install --upgrade pip

# Install additional Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install specific Airflow providers
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-hdfs==3.2.1 \
    apache-airflow-providers-apache-hive==5.1.3 \
    apache-airflow[postgres]==2.6.2

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/opt/airflow