#!/bin/bash
# This script will be executed once Metabase has started up
# It configures the Trino data source connection and Cube.js connection

# Wait for Metabase to be ready
echo "Waiting for Metabase to be ready..."
while ! curl -s http://metabase:3000/api/health >/dev/null; do
  sleep 10
  echo "Still waiting for Metabase..."
done

echo "Metabase is ready! Setting up data sources..."

# Get the setup token
SETUP_TOKEN=$(curl -s http://metabase:3000/api/session/properties | grep -o '"setup-token":"[^"]*"' | cut -d'"' -f4)

# Create admin user
curl -X POST http://metabase:3000/api/setup \
  -H "Content-Type: application/json" \
  -d '{
    "token": "'$SETUP_TOKEN'",
    "user": {
      "first_name": "Admin",
      "last_name": "User",
      "email": "admin@example.com",
      "password": "metabase123",
      "site_name": "Data Analytics Platform"
    },
    "prefs": {
      "allow_tracking": false
    }
  }'

# Login to get session token
SESSION_TOKEN=$(curl -s -X POST http://metabase:3000/api/session \
  -H "Content-Type: application/json" \
  -d '{
    "username": "admin@example.com",
    "password": "metabase123"
  }' | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

# Create Trino data source for direct access to data lake
curl -X POST http://metabase:3000/api/database \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION_TOKEN" \
  -d '{
    "name": "Datalake Trino",
    "engine": "presto-jdbc",
    "details": {
      "host": "trino",
      "port": 8060,
      "catalog": "datalake",
      "schema": "gold",
      "user": "",
      "ssl": false,
      "additional-options": ""
    },
    "is_full_sync": true,
    "is_on_demand": false
  }'

echo "Trino data source configuration complete!"

# Wait for Cube to be ready
echo "Waiting for Cube.js API to be ready..."
while ! curl -s http://cube:4000/cubejs-api/v1/meta >/dev/null; do
  sleep 5
  echo "Still waiting for Cube.js..."
done

# Create Cube.js data source
curl -X POST http://metabase:3000/api/database \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION_TOKEN" \
  -d '{
    "name": "Cube.js Models",
    "engine": "googleanalytics",
    "details": {
      "service-account-json": "",
      "dataset-id": "",
      "client-id": "",
      "client-secret": "",
      "auth-code": "",
      "use-service-account": false
    },
    "is_full_sync": false,
    "is_on_demand": true
  }'

echo "Base configuration complete. Cube.js will need to be manually configured through UI."

# Create a reference document for manual Cube.js configuration
cat > /tmp/cube_instructions.txt << EOF
=== Cấu hình kết nối Cube.js trong Metabase ===

Bước 1: Đăng nhập vào Metabase tại http://localhost:3030 với tài khoản:
- Email: admin@example.com
- Mật khẩu: metabase123

Bước 2: Vào trang Admin > Databases > Add database

Bước 3: Chọn "URL" làm engine

Bước 4: Điền các thông tin sau:
- Tên: Cube.js Models
- URL: http://cube:4000/cubejs-api/v1
- API Key: DuyBao21133 (đây là CUBEJS_API_SECRET trong docker-compose)

Bước 5: Bấm "Save" để lưu kết nối

Bước 6: Sau khi kết nối, Metabase sẽ hiển thị các model từ Cube.js
EOF

echo "Cube.js connection instructions created in /tmp/cube_instructions.txt"
