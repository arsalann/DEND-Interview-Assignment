#%%

import configparser
from datetime import datetime, date
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType
from pyspark import SparkFiles








# def create_spark_session():
#     print('\n\Spark Session Starting...\n\n')
#     '''
#     Function: 
#         1) initialize spark session builder
#         2) configure session parameters
#         3) create or get session if exists
#     Parameters:
#         None
    
#     Returns:
#         spark session curser
#     '''
#     spark = SparkSession \
#         .builder \
#         .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
#         .getOrCreate()
#     return spark


#%%

def create_spark_session():
    



#%%


create_spark_session()


url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=14&Year=2020&Month=11&Day=1&timeframe=2&submit=Download+Data"
from pyspark import SparkFiles
spark.sparkContext.addFile(url)

df = spark.read.csv("file://"+SparkFiles.get("en_climate_daily_BC_1010066_2020_P1D.csv"), header=True, inferSchema= True)


df