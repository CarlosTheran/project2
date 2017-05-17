from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import sys
import numpy as np
def sparksessioninit(sparkConf):
    if ('sparksession' not in globals()):
       globals()['sparksession']=SparkSession.builder.config(conf=sparkConf). \
       enableHiveSupport().getOrCreate()
    return globals()['sparksession']

def keyword_vizual():
   spark = sparksessioninit(sc.getConf())
   tiempo, fecha = sys.argv[1:]
   interval_time =("%s %s"%(fecha,tiempo))
   query = "select text, sum(total_text) as total_global from hive_pregunta_4b_13 \
            where date between cast('{}' as timestamp) - INTERVAL 1 HOUR and cast('{}' as timestamp) \
            group by text order by total_global desc limit 10".format(interval_time,interval_time)

   data = spark.sql(query)
   df = data.toPandas()
   index = np.arange(10)
   bar_width = 0.35
   ax = df[['total_global']].plot(kind='bar', title="Keywords Twitter", figsize=(15, 10), legend=True, fontsize=8)
   ax.set_xlabel('keywords', fontsize=12)
   plt.xticks(index + bar_width / 2, df['text'])
   ax.set_ylabel("total", fontsize=12)
   fig = plt.gcf()
   fig.savefig('/home/carlos_theran/project2/keywords.pdf', bbox_inches='tight')
   plt.show()


if __name__ == "__main__":
    sc = SparkContext(appName="keyword_Visualization")
    keyword_vizual()
