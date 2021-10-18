# Title

## generator.py

Generator script creates fake geo location data en publishes a topic every 2 seconds

### Instructions

install faker library: pip install Faker

install pubsub: pip install --upgrade google-cloud-pubsub

## subscriber.py

Subscriber script subscribes to that topic and stores the message in a queue

### Instructions

install pubsub: pip install --upgrade google-cloud-pubsub

## pipeline.py

Pipeline script uses the Pub/Sub as source and writes the data to a storage and/or big query table

### Instructions

install apache beam: pip install apache-beam

pipeline arguments:
./pipeline.py \
--input uscities.csv \
--input_topic projects/training-328919/topics/geolocation-stream \
--output output.txt \
--streaming
