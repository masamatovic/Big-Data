#!/usr/bin/python

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, expr, when, coalesce, greatest, udf, substring, regexp_replace
from pyspark.sql.types import *


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def get_max_row_with_None_(*cols):
      br = cols.count(None)
      if br == 6:
            return 0
      else:
            return float(max(x for x in cols if x is not None))
            
      
def get_aqi_pm25(dataframe):
      return dataframe.withColumn("pm25", when(col("pm25") <= 30, 1)
      .when((col("pm25") >= 31) & (col("pm25") <= 60), 2)
      .when((col("pm25") >= 61) & (col("pm25") <= 90), 3)
      .when((col("pm25") >= 91) & (col("pm25") <= 120), 4)
      .when((col("pm25") >= 121) & (col("pm25") <= 250), 5)
      .when(col("pm25") >= 251, 6)
      .otherwise(None))

def get_aqi_pm10(dataframe):
      return dataframe.withColumn("pm10", when(col("pm10") <= 50, 1)
      .when((col("pm10") >= 51) & (col("pm10") <= 100), 2)
      .when((col("pm10") >= 101) & (col("pm10") <= 250), 3)
      .when((col("pm10") >= 251) & (col("pm10") <= 350), 4)
      .when((col("pm10") >= 351) & (col("pm10") <= 430), 5)
      .when(col("pm10") >= 430, 6)
      .otherwise(None))

def get_aqi_no2(dataframe):
      return dataframe.withColumn("no2", when(col("no2") <= 40, 1)
      .when((col("no2") >= 41) & (col("no2") <= 80), 2)
      .when((col("no2") >= 81) & (col("no2") <= 180), 3)
      .when((col("no2") >= 181) & (col("no2") <= 280), 4)
      .when((col("no2") >= 281) & (col("no2") <= 400), 5)
      .when(col("no2") >= 400, 6)
      .otherwise(None))

def get_aqi_so2(dataframe):
      return dataframe.withColumn("so2", when(col("so2") <= 40, 1)
      .when((col("so2") >= 41) & (col("so2") <= 80), 2)
      .when((col("so2") >= 81) & (col("so2") <= 380), 3)
      .when((col("so2") >= 381) & (col("so2") <= 800), 4)
      .when((col("so2") >= 801) & (col("so2") <= 1600), 5)
      .when(col("so2") >= 1600, 6)
      .otherwise(None))

def get_aqi_o3(dataframe):
      return dataframe.withColumn("o3", when(col("o3") <= 50, 1)
      .when((col("o3") >= 51) & (col("o3") <= 100), 2)
      .when((col("o3") >= 101) & (col("o3") <= 168), 3)
      .when((col("o3") >= 169) & (col("o3") <= 208), 4)
      .when((col("o3") >= 209) & (col("o3") <= 748), 5)
      .when(col("o3") >= 748, 6)
      .otherwise(None))

def get_aqi_co(dataframe):
      return dataframe.withColumn("co", when(col("co") <= 1000, 1)
      .when((col("co") >= 1001) & (col("co") <= 2000), 2)
      .when((col("co") >= 2001) & (col("co") <= 10000), 3)
      .when((col("co") >= 10001) & (col("co") <= 17000), 4)
      .when((col("co") >= 17001) & (col("co") <= 34000), 5)
      .when(col("co") >= 34000, 6)
      .otherwise(None))

spark = SparkSession \
    .builder \
    .appName("Air Pollution") \
    .getOrCreate()

quiet_logs(spark)

schemaString = "location city country utc local parameter value unit latitude longitude attribution"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

#ucitavanje dataseta sa hdfsa
df = spark.read.csv("hdfs://namenode:9000/air-pollution/2017_india.csv", header=True, mode="DROPMALFORMED", schema=schema )
df = df.withColumn("value", df["value"].cast(DoubleType()))

df_2016 = spark.read.csv("hdfs://namenode:9000/air-pollution/2016_india.csv", header=True, mode="DROPMALFORMED", schema=schema)
df_2016 = df_2016.withColumn("value", df_2016["value"].cast(DoubleType()))

#izdvajanje datuma iz npr:  2017-01-01T00:00:00.000Z
df = df.withColumn("date", df["utc"].substr(1, 10))

df_2016 = df_2016.withColumn("date", df_2016["utc"].substr(1, 10))

#trazenje prosecne vradnosti svih parametra na dnevnom niovu za svaki grad
print("====================================== pivot table 2017 =========================================")
pivotDF = df.groupBy("city", "date").pivot("parameter").avg("value")
pivotDF.show()
print("====================================== pivot table 2016 =========================================")
pivotDF_2016 = df_2016.groupBy("city", "date").pivot("parameter").avg("value")
pivotDF_2016.show()

#provera broja dana merenja za grad
# pr = pivotDF.groupBy("city").count()
# pr1 = pr.orderBy("city", ascending=True)
# pr1.repartition(1).write.csv("hdfs://namenode:9000/air-pollution/dani", sep='|')
# pr1.show()

#Racunanje AQI-a sa svaki parametar  

aqiDF = get_aqi_pm10(pivotDF)
aqiDF = get_aqi_pm25(aqiDF)
aqiDF = get_aqi_no2(aqiDF)
aqiDF = get_aqi_so2(aqiDF) 
aqiDF = get_aqi_co(aqiDF) 
aqiDF = get_aqi_o3(aqiDF)
print("====================================== aqi for each  parameter in 2017 =========================================")
aqiDF.show()

aqiDF_2016 = get_aqi_pm10(pivotDF_2016)
aqiDF_2016 = get_aqi_pm25(aqiDF_2016)
aqiDF_2016 = get_aqi_no2(aqiDF_2016)
aqiDF_2016 = get_aqi_so2(aqiDF_2016) 
aqiDF_2016 = get_aqi_co(aqiDF_2016) 
aqiDF_2016 = get_aqi_o3(aqiDF_2016)
print("====================================== aqi for each parameter in 2016 =========================================")
aqiDF_2016.show()

# Racunanje dnevnog AQI-a
# Maksimalan AQI parametra
get_max_row_with_None = udf(get_max_row_with_None_, DoubleType())

finalAQI = aqiDF.withColumn("aqi", get_max_row_with_None('no2', 'pm10', 'pm25', 'so2', 'o3', 'co'))
print("=============================== final aqi for each day in 2017 ===============================")
finalAQI.orderBy('city', ascending=True).show()

finalAQI_2016 = aqiDF_2016.withColumn("aqi", get_max_row_with_None('no2', 'pm10', 'pm25', 'so2', 'o3', 'co'))
print("=============================== final aqi for each day in 2016 ===============================")
finalAQI_2016.orderBy('city', ascending=True).show()

#broj dana za svkai nivo AQI-a u 4 izabrana grada 
filte_2017 = finalAQI.filter((finalAQI.city == "Chennai") | (finalAQI.city == "Delhi") | (finalAQI.city == "Hyderabad") | (finalAQI.city == "Mumbai"))
final = filte_2017.groupBy("city", "aqi").count()
final = final.withColumn("aqi", final["aqi"].cast(IntegerType()))
finalOrderd = final.orderBy("city", ascending=True)
finalOrderd.coalesce(1).write.mode("overwrite").option("header","true").csv("r2017")
print("=============================== number of days with each index value 2017  ===============================")
finalOrderd.show()



filte_2016 = finalAQI_2016.filter((finalAQI_2016.city == "Chennai"))
filte_2016 = filte_2016.withColumn("month", filte_2016["date"].substr(6, 2))
new = filte_2016.groupBy("city", "month").pivot("aqi").count()
b = new.drop("null")
#new.show()
setDate = b.withColumn("month", when((col("month").substr(1,1) == '0'), regexp_replace("month", '0', '')).otherwise(col('month')))
setDate.show()

# new = new.withColumn("month", regexp_replace("month", '0', ''))
# new.orderBy("month", ascending=True).show()
# new.show()
# final_2016 =  filte_2016.withColumn("aqi", when(col("aqi") == None, 6))
# finalOrderd_2016 = final_2016.orderBy("city", ascending=True)
# finalOrderd_2016.coalesce(1).write.mode("overwrite").option("header","true").csv("r2016")
# print("=============================== number of days with each index value 2016  ===============================")
# finalOrderd_2016.show()

#MORTALITET U 2016 SA DANIMA
d = spark.read.csv("hdfs://namenode:9000/air-pollution/deaths.csv", header=True, mode="DROPMALFORMED")
d = d.withColumn("year", substring(d['Date'], -4, 4))
d = d.withColumn("month", substring(d['Date'], 1, 2))
d = d.withColumn('month', regexp_replace('month', '/', ''))
d1 = d.filter((d.year == "2016"))
d1 = d1.withColumn("Count", d1["Count"].cast(DoubleType()))
d2 = d1.groupBy('month').sum('Count')
d2.orderBy("month", ascending=True).show()

final = setDate.join(d2, on=["month"], how='inner')
final.show()


#finalOrderd.repartition(1).write.csv("hdfs://namenode:9000/air-pollution/final_2017", sep=',')
#finalOrderd_2016.repartition(1).write.csv("hdfs://namenode:9000/air-pollution/final_2016", sep=',')


