from pyspark import SparkConf, SparkContext

from operator import add

import sys

from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

import json

from kafka import SimpleProducer, KafkaClient

from kafka import KafkaProducer

import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'

try:

    import json

except ImportError:

    import simplejson as json



from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

def read_credentials():

    file_name = "/home/carlos_theran/project2/credentials.json"

    try:

        with open(file_name) as data_file:

            return json.load(data_file)

    except:

        print ("Cannot load credentials.json")

        return None





def read_tweets():

    sc = SparkContext(appName="Project2")
    ssc = StreamingContext(sc,3600)  #cambiar por 600 = 10 min.
    brokers = "localhost:9092"
    kvs = KafkaUtils.createDirectStream(ssc, ["twitter_60min"], {"metadata.broker.list": brokers})
    kvs.foreachRDD(create_format)
    producer.flush()
    ssc.start()
    ssc.awaitTermination()


def create_format(messages):

    iterator = twitter_stream.statuses.sample()
    count=0

    for tweet in iterator:
        producer.send('twitter_60min', bytes(json.dumps(tweet), "ascii"))
        count+=1

        if(count==90000):
           break


if __name__ == "__main__":

    print("Stating to read tweets")
    credentials = read_credentials()
    oauth = OAuth(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'], credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
    twitter_stream = TwitterStream(auth=oauth)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    read_tweets()
