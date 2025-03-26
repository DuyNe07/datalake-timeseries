FROM python:3.9-bullseye

# Optional version env variables
ENV SPARK_VERSION=3.5.5
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.6.1
ENV AWSSDK_VERSION=2.20.18

# Optional path env variables
ENV SPARK_HOME=/opt/spark
ENV BIN_DIR=/usr/bin
ENV DBT_DIR=/src/dbt
ENV NOTEBOOKS_DIR=/src/notebooks
ENV SPARK_EVENTS_DIR=/tmp/spark-events

# Add iceberg spark runtime jar to IJava classpath
ENV IJAVA_CLASSPATH=/opt/spark/jars/*
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV INSTALL_DIR=/tmp/install

# Create dirs
RUN mkdir -p ${SPARK_HOME} ${DBT_DIR} ${NOTEBOOKS_DIR} \
             ${SPARK_EVENTS_DIR} ${INSTALL_DIR} \
            /root/.ipython/profile_default/startup

# install core packages 
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      unzip \
      make \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      libpq-dev \
      gcc \
      g++ \
      libsasl2-dev \
		  libsasl2-2\ 
		  libsasl2-modules-gssapi-mit\
      unixodbc-dev \
      ssh \
      postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR ${INSTALL_DIR}

# Install python deps
COPY conf/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt \
    && rm requirements.txt

# Download and install IJava jupyter kernel
RUN curl https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip -Lo ijava-1.3.0.zip \
    && unzip ijava-1.3.0.zip \
    && python3 install.py --sys-prefix \
    && rm -rf ijava-1.3.0.zip	install.py java

# Install spark

COPY bin/spark-3.5.5-bin-without-hadoop.tgz spark-3.5.5-bin-without-hadoop.tgz
RUN tar xvzf spark-3.5.5-bin-without-hadoop.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-3.5.5-bin-without-hadoop.tgz

# Install AWS CLI
RUN curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm awscliv2.zip \
    && rm -rf aws/

WORKDIR ${SPARK_HOME}    

COPY jars/*.jar ${SPARK_HOME}/jars
COPY scripts/entrypoint.sh ${BIN_DIR}
COPY conf/notebook ${BIN_DIR}/notebook
COPY conf/notebook ${BIN_DIR}/pyspark-notebook
COPY conf/.pyiceberg.yaml /root/.pyiceberg.yaml
COPY conf/ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup

RUN chmod u+x ${SPARK_HOME}/* \
  && chmod u+x ${SPARK_HOME}/bin/* \
  && chmod u+x ${BIN_DIR}/notebook \
  && chmod u+x ${BIN_DIR}/pyspark-notebook \
  && chmod u+x ${BIN_DIR}/entrypoint.sh


EXPOSE 3070
EXPOSE 8888
EXPOSE 7077
EXPOSE 8061
EXPOSE 8062
EXPOSE 10000
EXPOSE 10009
EXPOSE 18080  

ENTRYPOINT ["/spark-iceberg/scripts/entrypoint.sh"]
CMD ["notebook"]