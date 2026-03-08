#!/usr/bin/env python3
"""
Apache Beam Dataflow Pipeline for Real-time Data Processing
This pipeline reads from Pub/Sub, processes the data, and writes to BigQuery.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
import json
from datetime import datetime

# GCP Configuration
PROJECT_ID = 'your-gcp-project-id'  # Replace with your GCP project ID
TOPIC_NAME = 'real-time-analytics-topic'
DATASET_NAME = 'real_time_analytics'
TABLE_NAME = 'events'

class ParseMessage(beam.DoFn):
    """Parse JSON messages from Pub/Sub"""
    def process(self, element):
        try:
            data = json.loads(element.decode('utf-8'))
            # Add processing timestamp
            data['processed_at'] = datetime.utcnow().isoformat()
            yield data
        except json.JSONDecodeError:
            # Skip invalid JSON messages
            pass

class TransformData(beam.DoFn):
    """Transform and enrich the data"""
    def process(self, element):
        # Add derived fields
        element['hour'] = datetime.fromisoformat(element['timestamp']).hour
        element['day'] = datetime.fromisoformat(element['timestamp']).day
        element['month'] = datetime.fromisoformat(element['timestamp']).month
        element['year'] = datetime.fromisoformat(element['timestamp']).year

        # Calculate value categories
        if element['value'] < 25:
            element['value_category'] = 'low'
        elif element['value'] < 75:
            element['value_category'] = 'medium'
        else:
            element['value_category'] = 'high'

        yield element

def run_pipeline():
    """Run the Dataflow pipeline"""
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        # Read from Pub/Sub
        messages = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                topic=f'projects/{PROJECT_ID}/topics/{TOPIC_NAME}'
            )
            | 'Parse JSON' >> beam.ParDo(ParseMessage())
            | 'Transform Data' >> beam.ParDo(TransformData())
        )

        # Write to BigQuery
        messages | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=f'{PROJECT_ID}:{DATASET_NAME}.{TABLE_NAME}',
            schema='timestamp:TIMESTAMP,user_id:INTEGER,event_type:STRING,value:FLOAT,device:STRING,location:STRING,processed_at:TIMESTAMP,hour:INTEGER,day:INTEGER,month:INTEGER,year:INTEGER,value_category:STRING',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    run_pipeline()