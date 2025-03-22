#!/bin/sh

# Load biến môi trường từ file .env (chỉ cần nếu không được Docker tự động load)

# Kiểm tra xem MinIO đã sẵn sàng chưa
until (/usr/bin/mc alias set minio http://minio:9000 $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY); do
    echo "Waiting for MinIO to be ready..."
    sleep 2
done

# Danh sách các bucket cần tạo
# BUCKETS=("warehouse" "datalake")
BUCKETS=("datalake")

for BUCKET in "${BUCKETS[@]}"; do
    # Kiểm tra xem bucket đã tồn tại chưa
    if /usr/bin/mc stat minio/$BUCKET >/dev/null 2>&1; then
        echo "Bucket '$BUCKET' đã tồn tại, bỏ qua bước tạo bucket."
    else
        echo "Tạo bucket '$BUCKET'..."
        /usr/bin/mc mb minio/$BUCKET && \
        /usr/bin/mc anonymous set public minio/$BUCKET && \
        echo "Bucket '$BUCKET' đã được tạo và đặt public."
    fi
done
exit 0
# Giữ container chạy (nếu cần)
# tail -f /dev/null
