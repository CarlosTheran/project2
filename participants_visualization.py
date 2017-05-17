from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np


def sparksession(sparkConf):
    if ('sparksessioninit' not in globals()):
       globals()['sparksessioninit']=SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparksessioninit']

def username_visualization():
   spark = sparksession(sc.getConf())
   tiempo, fecha = sys.argv[1:]
   interval_time =("%s %s"%(fecha,tiempo))
   query = "select user_name, sum(total_participant) as total_global from hive_pregunta_4c_13 \
            where date between cast('{}' as timestamp) - INTERVAL 1 DAY and cast('{}' as timestamp) \
            group by user_name order by total_global desc limit 10".format(interval_time,interval_time)

   data=spark.sql(query)
   data.show()
   df = data.toPandas()
   index = np.arange(10)
   bar_width = 0.35
   ax = df[['total_global']].plot(kind='bar', title="User Name", figsize=(15, 10), legend=True, fontsize=8)
   ax.set_xlabel('hashtag', fontsize=12)
   plt.xticks(index + bar_width / 2, df['user_name'])
   ax.set_ylabel("total", fontsize=12)
   fig = plt.gcf()
   fig.savefig('/home/carlos_theran/project2/participant.pdf', bbox_inches='tight')
   plt.show()


if __name__ == "__main__":
    sc = SparkContext(appName="username_Visualization")
    username_visualization()
