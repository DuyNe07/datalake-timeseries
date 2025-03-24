#!/bin/bash

# Submit Spark job to transform Silver to Gold
docker exec spark spark-submit \
  --master local[*] \
  --conf spark.executor.memory=2g \
  /src/silver_to_gold.py

echo "Data transformation completed"
