# Metabase Integration

This directory contains configuration files and scripts for the Metabase Business Intelligence tool integration with the data lakehouse.

## Overview

Metabase is connected to the data lakehouse through Trino, which allows it to query data stored in MinIO. The data is organized in a gold schema within the datalake catalog.

## Connection Details

- **Metabase URL**: http://localhost:3030
- **Default Login**: admin@example.com
- **Default Password**: metabase123

## Data Sources

Metabase is pre-configured with the following data source:

- **Name**: Datalake Trino
- **Engine**: Presto (Trino)
- **Host**: trino
- **Port**: 8060
- **Catalog**: datalake
- **Schema**: gold

## Using with Cube.js Models

Metabase can access the same data that Cube.js is modeling. While Metabase doesn't directly read Cube.js models, you can:

1. Use the same tables/views in Trino that Cube.js accesses
2. Create SQL queries in Metabase that match your Cube.js dimensions and metrics
3. Build dashboards and visualizations in Metabase based on your Cube.js data model structure

## Troubleshooting

If the automatic configuration script fails, you can manually set up the data source:

1. Log in to Metabase at http://localhost:3030
2. Go to Admin > Databases
3. Click "Add Database"
4. Select "Presto" as the database type
5. Enter the connection details listed above
6. Click "Save"

## Data Location

Metabase stores its application data in a PostgreSQL database in the `./metabase/db` directory.
