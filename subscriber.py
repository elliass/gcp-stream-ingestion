import os
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

# Service account credentials
credentials_path = '/Users/elliass/Desktop/gcp-data-ingestion/geolocation-service-account-key.json' # create service account and get key
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Subscriber and subscription
subscriber = pubsub_v1.SubscriberClient()
subscription_path = 'projects/training-328919/subscriptions/geolocation-stream-sub'
timeout = 5.0


def callback(message):
    print(f'received message: {message}')
    print(f'data: {message.data}')

    if message.attributes:
        print(f'Attributes:')
        for key in message.attributes:
            value = message.attributes.get(key)
            print(f'{key}: {value}')
    
    print(f'message_id: {message.message_id}')

    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f'listening for messages on {subscription_path}')

with subscriber:
    try:
        # streaming_pull_future.result(timeout=timeout)
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()