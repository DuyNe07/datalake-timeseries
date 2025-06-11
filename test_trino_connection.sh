#!/bin/bash

# Test script to validate Trino connectivity and Iceberg tables
echo "Testing connection to Trino and listing tables in the datalake.gold schema..."

# Connect to Trino container
docker exec -it trino trino --catalog datalake --schema gold --execute "SHOW TABLES;"

echo "Testing query on indices_predict table..."
docker exec -it trino trino --catalog datalake --schema gold --execute "SELECT * FROM indices_predict LIMIT 5;"

echo "Done testing Trino connection."
