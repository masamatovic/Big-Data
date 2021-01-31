r"""
 Run the example
    `spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark/stream/AQI-stream.py zoo1:2181 air-pollution`
"""
from __future__ import print_function                                 
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *


def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="Air Quality Index")
    quiet_logs(sc)

    ssc = StreamingContext(sc, 180)


    zooKeeper, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zooKeeper, "spark-streaming-consumer", {topic: 1})
    # stize mi lista ovakvih linija sa kafke: ("1/29/2021 9:55", City: Beograd Vračar, Serbia, AQI: 53)    
    lines = kvs \
        .map(lambda x: "{0},{1},{2}".format(x[0], x[1].split()[1], x[1].split()[-1]))
    
  
    def process(time, rdd):
        print("========= %s USAO =========" % str(time))

        try:

            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFra
            rowRdd = rdd.map(lambda line: Row(time = line.split(',')[0], city = line.split(',')[1], aqi = line.split(',')[2]))
            df = spark.createDataFrame(rowRdd)
            df.show()

            df.createOrReplaceTempView("aqis")
            #Upit koji racuna procesni aqi u oviru tok paketa
            newDF = spark.sql("select city, avg(aqi) as avgAQI from aqis group by city")
            #newDF.show()
            
            #ispisivanje posruke na osnovu prosecne vrednosti
            newDF.createOrReplaceTempView("aqis")
            finalDF = spark.sql("select avgAQI, \
                                    case when avgAQI <= 50 then 'GOOD' \
                                        when avgAQI >= 51 and avgAQI <= 100 then 'MODERATE' \
                                        when avgAQI >= 101 and avgAQI <= 150 then 'UNHEALTHY FOR SENSITIVE GROUPS' \
                                        when avgAQI >= 151 and avgAQI <= 200 then 'UNHEALTHY' \
                                        when avgAQI >= 201 and avgAQI <= 300 then 'VERY UNHEALTHY' \
                                        else 'HAZARDOUS' end as aqiMessage from aqis")
            finalDF.show()
        except:
            pass

    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()