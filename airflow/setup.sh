#!/bin/bash
set -e

# Tạo các thư mục cần thiết
mkdir -p airflow/dags airflow/logs airflow/plugins

# Kiểm tra và tạo file Dockerfile nếu chưa tồn tại
if [ ! -f airflow/Dockerfile ]; then
    cat > airflow/Dockerfile << 'EOF'
FROM apache/airflow:2.8.1

USER root

# Cài đặt OpenJDK cho tương thích với Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        procps \
        wget \
        git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Cài đặt các gói Python cho phân tích dữ liệu thời gian và machine learning
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.0 \
    pyspark==3.2.0 \
    pandas \
    numpy \
    scikit-learn \
    statsmodels \
    matplotlib \
    plotly \
    pyarrow \
    iceberg-python \
    boto3 \
    jupyter

# Quay lại người dùng airflow
USER airflow
EOF
    echo "Created Dockerfile for Airflow"
fi

echo "Airflow environment setup complete. Now you can build with docker-compose."