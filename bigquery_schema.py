#!/usr/bin/env python3
"""
BigQuery Schema Definition for Real-time Analytics
This script creates the BigQuery dataset and table with the appropriate schema.
"""

from google.cloud import bigquery

# GCP Configuration
PROJECT_ID = 'your-gcp-project-id'  # Replace with your GCP project ID
DATASET_NAME = 'real_time_analytics'
TABLE_NAME = 'events'

def create_dataset_and_table():
    """Create BigQuery dataset and table"""
    client = bigquery.Client(project=PROJECT_ID)

    # Create dataset
    dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"  # Choose appropriate location

    try:
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}")
    except Exception as e:
        print(f"Dataset {dataset_id} already exists or error: {e}")

    # Define table schema
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("user_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("value", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("device", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("hour", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("day", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("value_category", "STRING", mode="REQUIRED"),
    ]

    # Create table
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
    table = bigquery.Table(table_id, schema=schema)

    try:
        table = client.create_table(table)
        print(f"Created table {table_id}")
    except Exception as e:
        print(f"Table {table_id} already exists or error: {e}")

if __name__ == '__main__':
    create_dataset_and_table()