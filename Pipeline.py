import argparse
import logging
#from matplotlib import image
from PIL import Image
import io
import tensorflow as tf
import numpy as np
from io import BytesIO
import apache_beam as beam
from apache_beam import coders
from apache_beam.io import fileio

from apache_beam.io import filesystemio
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.io.fileio import WriteToFiles
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class ConvertToTF(beam.DoFn):

    def process(self, element):
        def _int64_feature(value):
            return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))

        def _bytes_feature(value):
            return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))

        load_bytes = BytesIO(element)
        loaded_np = np.load(load_bytes, allow_pickle=True).tolist()
        #print(type(loaded_np))
       # print(type(loaded_np['image']))
       # print(type(loaded_np['label']))
        data = loaded_np['image']
        labels = loaded_np['label']
        example = tf.train.Example(features=tf.train.Features(
            feature={
                 'image': tf.train.Feature(bytes_list=tf.train.BytesList(value=[data.tobytes()])),
                 'label': tf.train.Feature(int64_list=tf.train.Int64List(value=[labels]))
              }
         ))
        return [example]


        #print(element)
        #print(images)



def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        # default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    p = beam.Pipeline(options=pipeline_options)
    # The pipeline will be run on exiting the with block.
    # with beam.Pipeline(options=pipeline_options) as p:
    # Read the text file[pattern] into a PCollection.
   #

    images = p | 'Read' >> beam.io.fileio.MatchFiles(known_args.input+"\\Data\\")\
               | beam.io.fileio.ReadMatches()\
               | beam.Map(lambda file: file.read())\
               | beam.ParDo(ConvertToTF()) \
               |"Serialize" >> beam.Map(lambda x: x.SerializeToString())\
               | 'Write' >> beam.io.WriteToTFRecord(known_args.output+"\\data-tfrecords-cifar-10", file_name_suffix='.tfrecord')


# | beam.io.fileio.ReadMatches() \


    #  | 'Write' >> beam.io.fileio.WriteToFiles(path=known_args.output)

    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
