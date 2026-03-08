#!/usr/bin/env python3
"""
Real-time Data Producer for Pub/Sub
This script simulates real-time data generation and publishes messages to Google Cloud Pub/Sub.
"""

import json
import time
import random
from datetime import datetime
from google.cloud import pubsub_v1

# GCP Configuration
PROJECT_ID = 'your-gcp-project-id'  # Replace with your GCP project ID
TOPIC_NAME = 'real-time-analytics-topic'

def create_sample_data():
    """Generate sample real-time data"""
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'user_id': random.randint(1, 1000),
        'event_type': random.choice(['page_view', 'purchase', 'login', 'logout']),
        'value': random.uniform(1, 100),
        'device': random.choice(['mobile', 'desktop', 'tablet']),
        'location': random.choice(['US', 'EU', 'ASIA', 'OTHER'])
    }

def publish_messages():
    """Publish messages to Pub/Sub topic"""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    print(f"Publishing messages to {topic_path}")

    try:
        # Create topic if it doesn't exist
        publisher.create_topic(request={"name": topic_path})
        print("Topic created or already exists")
    except Exception as e:
        print(f"Topic creation failed or already exists: {e}")

    while True:
        # Generate and publish sample data
        data = create_sample_data()
        message_data = json.dumps(data).encode('utf-8')

        future = publisher.publish(topic_path, message_data)
        print(f"Published message: {data}")

        # Wait for the publish to complete
        future.result()

        # Simulate real-time data (adjust sleep time as needed)
        time.sleep(1)

if __name__ == '__main__':
    publish_messages()