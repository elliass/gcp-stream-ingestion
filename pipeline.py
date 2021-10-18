import argparse
import logging
import os


import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import pubsub_v1


# Service account credentials
credentials_path = '/Users/elliass/Desktop/gcp-data-ingestion/geolocation-service-account-key.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Big query information
table_id = 'streaming.geolocation'
table_schema = 'id:STRING, latitude:STRING, longitude:STRING'


class PubsubMessage(object):
    def __init__(self, data, attributes, message_id=None, publish_time=None, ordering_key=""):
        if data is None and not attributes:
            raise ValueError(
                'Either data (%r) or attributes (%r) must be set.', data, attributes)
        self.data = data
        self.attributes = attributes
        self.message_id = message_id
        self.publish_time = publish_time
        self.ordering_key = ordering_key
    
    def get_attributes(msg):
        return msg.attributes

def make_list(line):
    return line.split(',')

def clean(line):
    new_line = []
    for element in line:
        new_line.append(element.strip('"'))
    return new_line
    
def select(line):
    return [{
        'city': str(line[0]),
        'state_id': str(line[2]),
        'state_name': str(line[3]),
        'county_fips': str(line[4]),
        'county_name': str(line[5]),
        'lat': str(line[6]),
        'lng': str(line[7])
    }]   

def merge():
    return ""


def run(argv=None, save_main_session=True):
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input text file to process.'
        )
    parser.add_argument(
        '--input_topic',
        dest='input_topic',
        required=True,
        help='Input pubsub message to process.'
        )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.'
        )
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


    with beam.Pipeline(options=pipeline_options) as p:

        # Read from text into a PCollection.
        input_csv = (p
            | 'Read' >> ReadFromText(known_args.input, skip_header_lines=True)
            | 'Split' >> beam.Map(make_list)
            | 'Clean' >> beam.Map(clean)
            | 'Select' >> beam.Map(select)
        )

        # Read from pubsub into a PCollection
        input_stream = (p 
            | 'Read' >> beam.io.ReadFromPubSub(topic=known_args.input_topic, with_attributes=True, id_label=None) #set id_label='id' to avoid deduplication of message, unfortunately not supported for direct runner
            | 'Get Attributes' >> beam.Map(PubsubMessage.get_attributes)
        )

        # Merge Pcollections to enrich streamin data
        # output = (input_csv, input_stream) | 'Merge' >> beam.Map(merge)

        # Write output to local file or gcs storage
        input_stream | 'Write to storage' >> WriteToText(known_args.output)

        # Write output to biq query table 
        # input_stream | 'Write to bq' >> beam.io.WriteToBigQuery(
        #     table_id,
        #     schema=table_schema,
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        # )
        

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()



# dataflow runner arguments
# ./pipeline.py
# --input=pubsub
# --output=gs://geolocationdata                       # optional for bigquery
# --runner=DataflowRunner
# --project=training-328919
# --job_name=
# --temp_location=                                    # required gcs location
# --region=us-central1