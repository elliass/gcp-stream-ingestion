from faker import Faker
import time
from random import randint
import json
import os
from google.cloud import pubsub_v1


def write_data(output_file, key, data):
    with open(output_file, 'r+') as file:

        # Read input file
        input = json.load(file)

        # Append new records 
        input[key].append(data)

        # Write output
        file.seek(0)
        json.dump(input, file, indent=4)


fake = Faker()

# Service account credentials
credentials_path = '/Users/elliass/Desktop/gcp-data-ingestion/geolocation-service-account-key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Publisher and topic
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/training-328919/topics/geolocation-stream'


def generate_data():
    while True:
        # Init empty object
        data = {}

        # Generate fake data
        data['id'] = str(fake.ean(length=8))
        data['latitude'] = str(fake.latitude())  #'en-US'
        data['longitude'] = str(fake.longitude())
        data['timestamp'] = str(time.time())

        # Write data to file
        # write_data('geodata.json', 'geo_location', data)

        # Publish data to topic
        title = 'Geolocation data'
        title = title.encode('utf-8')
        attributes = data

        future = publisher.publish(topic_path, title, **attributes)
        print(f'published message id {future.result()}')

        # Wait
        time.sleep(2)


def main():
    generate_data()


if __name__ == '__main__':
    main()

