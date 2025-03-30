#!/bin/bash

# Lưu các biến môi trường Spark ban đầu
ORIGINAL_CLASSPATH=$CLASSPATH
ORIGINAL_PYTHONPATH=$PYTHONPATH

# Khởi tạo Nessie
if [ -f "/usr/bin/init-nessie.sh" ]; then
  chmod +x /usr/bin/init-nessie.sh
  echo "Starting Nessie initialization script..."
  bash /usr/bin/init-nessie.sh
else
  echo "Warning: Nessie initialization script not found"
fi

# Thiết lập PYTHONPATH cho Nessie
if [[ ":$PYTHONPATH:" != *":/nessie:"* ]]; then
  export PYTHONPATH="/nessie:$PYTHONPATH"
  echo "Added /nessie to PYTHONPATH"
fi

# Kiểm tra modules Nessie
if [ -d "/nessie/modules" ]; then
  echo "Verifying Nessie modules..."
  python -m nessie.modules.verify_nessie || echo "Verification returned non-zero exit code"
fi

# Khôi phục môi trường ban đầu cho Spark
export CLASSPATH=$ORIGINAL_CLASSPATH
export JAVA_HOME="$(jrunscript -e 'java.lang.System.out.println(java.lang.System.getProperty("java.home"));')"
export KYUUBI_HOME=/opt/kyuubi

# Khởi động Spark
echo "Starting Spark services..."
start-master.sh -p 7077 --webui-port 8061
start-worker.sh spark://spark:7077 --webui-port 8062
start-history-server.sh

# Đảm bảo biến môi trường cho ThriftServer
export SPARK_CLASSPATH=$ORIGINAL_CLASSPATH

# Khởi động ThriftServer
echo "Starting ThriftServer..."
start-thriftserver.sh --hiveconf hive.server2.thrift.port 10000 --hiveconf hive.server2.authentication NOSASL

# Uncomment to start Kyuubi if needed
# ${KYUUBI_HOME}/bin/kyuubi start

# Khôi phục PYTHONPATH bao gồm /nessie cho các ứng dụng người dùng
export PYTHONPATH="/nessie:$ORIGINAL_PYTHONPATH"

# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi