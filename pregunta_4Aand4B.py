from pyspark import SparkConf, SparkContext

from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

from kafka import SimpleProducer, KafkaClient

from kafka import KafkaProducer

from operator import add

import sys

from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream

from pyspark.sql import Row, SparkSession

import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar pyspark-shell'

try:

    import json

except ImportError:

    import simplejson as json





def read_credentials():
   
 file_name = "/home/carlos_theran/project2/credentials.json"
 try:
      with open(file_name) as data_file:
          return json.load(data_file)

 except:
      print ("Cannot load credentials.json")
      return None



def getSparkSessionInstance(sparkConf):

    if ('sparkSessionSingletonInstance' not in globals()):

        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()

    return globals()['sparkSessionSingletonInstance']



def consumer():

    context = StreamingContext(sc, 10)
    dStream = KafkaUtils.createDirectStream(context, ["twitter_10min"], {"metadata.broker.list": "localhost:9092"})

    dStream.foreachRDD(question_4Aand4B)

    context.start()

    context.awaitTermination()



def keywordsCount(time,rdd):

    #remove field [0] -> none and convert data str in dict
    rdd=rdd.map(lambda x: json.loads(x[1]))

    #convert RDD to Array
    records=rdd.collect()

    #records = [element for element in records if "delete" not in element] #remove delete tweets
    records = [element["text"] for element in records if "text" in element] #select only text part


    if records:  #verify non empty list
        rdd = sc.parallelize(records)
        rdd = rdd.map(lambda x: x.split(" "))
        rdd = rdd.flatMap(lambda x: x)
        rdd = rdd.filter(lambda x: x.startswith(('RT','@','#','http','"','a','an','are','as','at','be','by','for','from','has','he','in','is','it','its','of','on','that','the','to','was','were','will','with')) == False).filter(lambda x: len(x)>2)
       # for n in rdd.take(100):
         # print(n)

        #if rdd.count()>0:
        spark = getSparkSessionInstance(rdd.context.getConf())
       # Convert RDD[String] to RDD[Row] to DataFrame
        keywordsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(text=x, date=time)))
       # create a temporal table for quaries in memory
        keywordsDataFrame.createOrReplaceTempView("spark_pregunta_4b_13")
        keywordsDataFrame = spark.sql("select text, date,  count(*) as total_text from spark_pregunta_4b_13 group by text, date order by total_text desc limit 10")
      # save table into hive.
        keywordsDataFrame.write.mode("append").saveAsTable("hive_pregunta_4b_13")
           # ds=spark.sql("select text, sum(total) as suma from hive_pregunta_4b_13 group by text order by suma desc limit 5")
           # ds.show()
def hashtag_consumer(time,rdd):

    #remove field [0] -> none and convert data str in dict

    rdd=rdd.map(lambda x: json.loads(x[1]))

    records=rdd.collect()

    records = [element for element in records if "delete" not in element] #remove delete tweets

    records = [element["entities"]["hashtags"] for element in records if "entities" in element] #select only hashtags part

    records = [x for x in records if x] #remove empty hashtags

    records = [element[0]["text"] for element in records]

    if not records:

        print("Lista Vacia")

    else:

        rdd = sc.parallelize(records)

        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame

        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(hashtag=x, date = time)))
        hashtagsDataFrame.createOrReplaceTempView("spark_pregunta_4a_13")
        hashtagsDataFrame = spark.sql("select hashtag, date, count(*) as total_hashtag from spark_pregunta_4a_13 group by hashtag, date order by total_hashtag desc limit 10")
        hashtagsDataFrame.write.mode("append").saveAsTable("hive_pregunta_4a_13")

       # ds=spark.sql("select hashtag, sum(total) as suma from hashtag1  group by hashtag order by suma desc limit 5")
       # ds.show()

def question_4Aand4B(time, rdd):
 
    hashtag_consumer(time,rdd)
    keywordsCount(time,rdd)


def f(x):print(x)

if __name__ == "__main__":

    print("Stating to read tweets")

    sc = SparkContext(appName="Project2_pregunta4Aand4B")

    consumer()
