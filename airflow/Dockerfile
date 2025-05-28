FROM apache/airflow:2.8.1

USER root

# Thêm repository để cài đặt OpenJDK 11
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget apt-transport-https gnupg \
    && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - \
    && echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        temurin-11-jdk \
        procps \
        wget \
        git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập JAVA_HOME cho Adoptium Temurin JDK 11
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Tạo thư mục logs và đảm bảo quyền
RUN mkdir -p /opt/airflow/logs /opt/airflow/logs/scheduler /opt/airflow/logs/dag_processor /opt/airflow/logs/task \
    && chown -R airflow:root /opt/airflow/logs \
    && chmod -R 775 /opt/airflow/logs

# Chuyển sang người dùng airflow
USER airflow

# Cài đặt các gói Python cần thiết với người dùng airflow
RUN pip install --user --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.0 \
    pyspark==3.5.5 \
    pandas \
    numpy \
    scikit-learn \
    statsmodels \
    matplotlib \
    boto3 \
    pyarrow \
    tensorflow \
    selenium

# Thiết lập biến môi trường để Airflow có thể tìm thấy Java
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"