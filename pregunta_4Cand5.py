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

    context = StreamingContext(sc, 240)
    dStream = KafkaUtils.createDirectStream(context, ["twitter_60min"], {"metadata.broker.list": "localhost:9092"})

    dStream.foreachRDD(pregunta_4Cand5)

    context.start()

    context.awaitTermination()



def username(time,rdd):

    #remove field [0] -> none and convert data str in dict
    rdd=rdd.map(lambda x: json.loads(x[1]))

    #convert RDD to Array
    user=rdd.collect()

    #records = [element for element in records if "delete" not in element] #remove delete tweets
    user = [element["user"]["name"] for element in user if "user" in element] #select only text part


    if user:  #verify non empty list
        rdd = sc.parallelize(user)
        rdd = rdd.filter(lambda x: len(x)>2)
        #if rdd.count()>0:
        spark = getSparkSessionInstance(rdd.context.getConf())
       # Convert RDD[String] to RDD[Row] to DataFrame
        hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(user_name=x, date=time)))
       # create a temporal table for quaries in memory
        hashtagsDataFrame.createOrReplaceTempView("spark_pregunta_4c_13")
        hashtagsDataFrame = spark.sql("select user_name, date, count(*) as total_participant from spark_pregunta_4c_13 group by user_name, date order by total_participant desc limit 10")
      # save table into hive.
        hashtagsDataFrame.write.mode("append").saveAsTable("hive_pregunta_4c_13")
           # ds=spark.sql("select user_name, sum(total) as suma from hive_pregunta_c group by user_name order by suma desc limit 5")
           # ds.show()

def clasification_words(time,rdd):

    #remove field [0] -> none and convert data str in dict
    rdd=rdd.map(lambda x: json.loads(x[1]))

    #convert RDD to Array
    text_array=rdd.collect()

    #records = [element for element in records if "delete" not in element] #remove delete tweets
    text_array = [element["text"].upper() for element in text_array if "text" in element] #select only text part
    for text in text_array:
      print(text)

      if text_array:  #verify non empty list
        rdd = sc.parallelize(text_array)
        rdd = rdd.map(lambda x: x.split(" "))
        rdd = rdd.flatMap(lambda x: x)
        rdd = rdd.filter(lambda x: x == 'TRUMP' or x== 'MAGA'or x=='DICTATOR' or x=='IMPEACH' or x=='DRAIN' or x=='SWAMP')
       #.filter(lambda x: len(x)>2)
       # for n in rdd.take(100):
         # print(n)

        if rdd.count()>0:
            spark = getSparkSessionInstance(rdd.context.getConf())
       # Convert RDD[String] to RDD[Row] to DataFrame
            hashtagsDataFrame = spark.createDataFrame(rdd.map(lambda x: Row(words=x, date=time)))
       # create a temporal table for quaries in memory
            hashtagsDataFrame.createOrReplaceTempView("spark_pregunta_5_13")
            hashtagsDataFrame = spark.sql("select words, date, count(*) as total_word from spark_pregunta_5_13 group by words, date order by total_word desc limit 10")
      # save table into hive.
            hashtagsDataFrame.write.mode("append").saveAsTable("hive_pregunta_5_13")
           # ds=spark.sql("select words, sum(total) as suma from hive_pregunta_5 group by words order by suma desc limit 10")
           # ds.show()

def pregunta_4Cand5(time,rdd):
    username(time,rdd)
    clasification_words(time,rdd)
    


def f(x):print(x)

if __name__ == "__main__":

    print("Stating to read tweets")

    sc = SparkContext(appName="Project2_pregunta4Cand5")

    consumer()
