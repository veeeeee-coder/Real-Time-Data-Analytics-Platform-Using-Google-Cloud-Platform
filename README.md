# Real-Time-Data-Analytics-Platform-Using-Google-Cloud-Platform

This project demonstrates a real-time data analytics platform built using Google Cloud Platform (GCP) services. It showcases how to ingest, process, and analyze streaming data in real-time.

## Architecture

- **Data Ingestion**: Google Cloud Pub/Sub for real-time message ingestion
- **Data Processing**: Google Cloud Dataflow for stream processing
- **Data Storage**: Google Cloud BigQuery for analytical queries
- **Visualization**: Google Data Studio for dashboards

## Prerequisites

- Google Cloud Project with billing enabled
- GCP services enabled: Pub/Sub, Dataflow, BigQuery
- Python 3.7+
- Google Cloud SDK installed

## Setup

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up GCP authentication: `gcloud auth application-default login`
4. Configure your GCP project ID in the scripts

## Visualization

View the real-time analytics dashboard: [Looker Studio Dashboard](https://lookerstudio.google.com/s/qaizWpKSmv0)

## Files

- `producer.py`: Simulates real-time data generation and publishing to Pub/Sub
- `dataflow_pipeline.py`: Apache Beam pipeline for processing streaming data
- `bigquery_schema.py`: BigQuery table schema definition
- `requirements.txt`: Python dependencies