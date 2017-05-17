from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np


def sparksessioninit(sparkConf):
    if ('sparksessio' not in globals()):
       globals()['sparksession']=SparkSession.builder.config(conf=sparkConf). \
       enableHiveSupport().getOrCreate()
    return globals()['sparksession']

def wordsclasification_visualization():
   spark = sparksessioninit(sc.getConf())
   tiempo, fecha = sys.argv[1:]
   interval_time =("%s %s"%(fecha,tiempo))
   query = "select words, sum(total_word) as total_global from hive_pregunta_5 \
            where date between cast('{}' as timestamp) - INTERVAL 1 DAY and cast('{}' as timestamp) \
            group by words order by total_global desc limit 10".format(interval_time,interval_time)

   data = spark.sql(query)
   df = data.toPandas()
   index = np.arange(10)
   bar_width = 0.35
   ax = df[['total_global']].plot(kind='bar', title="Words Clasification Twitter", figsize=(15, 10), legend=True, fontsize=8)
   ax.set_xlabel('Words', fontsize=12)
   plt.xticks(index + bar_width / 2, df['words'])
   ax.set_ylabel("total", fontsize=12)
   plt.tight_layout()
   fig = plt.gcf()
   fig.savefig('/home/carlos_theran/project2/wordsClasification.pdf', bbox_inches='tight')
   plt.show()


  
if  __name__ == "__main__":
    sc = SparkContext(appName="wordsclasification_Visualization")
    wordsclasification_visualization()
