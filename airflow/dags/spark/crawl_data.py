from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from delta import *
from delta.pip_utils import configure_spark_with_delta_pip
from pyspark.sql import functions as func
from pyspark.sql import Window
from datetime import date, timedelta, datetime
import requests

def path_exists(spark, path):
        # spark is a SparkSession
        sc = spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jvm.java.net.URI.create(path),
                sc._jsc.hadoopConfiguration(),
        )
        return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
    
    
def get_crash_data(spark, days_ago=1):
    limit, offset = 10000, 0
    current_time = datetime.now()
    filter_date = current_time - timedelta(days=days_ago)
    watermarkTable = DeltaTable.forPath(spark, 'hdfs://namenode:9000/chicago-crash/raw/watermark')
    while True:
        response = requests.get(f"https://data.cityofchicago.org/resource/85ca-t3if.json?$where=crash_date>'{filter_date.date()}'&$limit={limit}&$offset={offset}")
        offset += 10000
        if len(response.json()) == 0:
            break
        if response.status_code != 200:
            break
        crashes_df = spark.createDataFrame(response.json()) \
            .withColumn('year', func.year(func.to_date(func.col('crash_date')))) \
            .withColumn('month', func.month(func.to_date(func.col('crash_date')))) \
            .withColumn('day', func.month(func.to_date(func.col('crash_date')))) \
            .na.fill('N/A')
        
        
        max_timestamp = crashes_df.select(func.max(func.to_timestamp("crash_date")).alias('max_timestamp')) \
                .collect()[0][0]
                
        insert_watermark = spark.createDataFrame(data=[['Crashes', max_timestamp]], schema=['TableName', 'WatermarkValue'])
        watermarkTable.alias('a') \
                .merge(insert_watermark.alias('b'), condition='a.TableName = b.TableName') \
                .whenMatchedUpdate(condition='b.WatermarkValue > a.WatermarkValue', set={'WatermarkValue': 'b.WatermarkValue'}) \
                .whenNotMatchedInsertAll() \
                .execute()
                
        if path_exists('hdfs://namenode:9000/chicago-crash/raw/crashes'):
            old_crashes_table = DeltaTable.forPath(spark, path='hdfs://namenode:9000/chicago-crash/raw/crashes')
            old_crashes_table.alias('a') \
                .merge(crashes_df.alias('b'), 'a.crash_record_id = b.crash_record_id') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            crashes_df.write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/chicago-crash/raw/crashes').option('mergeSchema', True).partitionBy('year', 'month', 'day').save()

def get_people_data(spark, days_ago=1):
    limit, offset = 10000, 0
    current_time = datetime.now()
    filter_date = current_time - timedelta(days=days_ago)
    watermarkTable = DeltaTable.forPath(spark, 'hdfs://namenode:9000/chicago-crash/raw/watermark')
    while True:
        response = requests.get(f"https://data.cityofchicago.org/resource/u6pd-qa9d.json?$where=crash_date>'{filter_date.date()}'&$limit={limit}&$offset={offset}")
        offset += 10000
        if len(response.json()) == 0:
            break
        if response.status_code != 200:
            break
        people_df = spark.createDataFrame(response.json()) \
            .withColumn('year', func.year(func.to_date(func.col('crash_date')))) \
            .withColumn('month', func.month(func.to_date(func.col('crash_date')))) \
            .withColumn('day', func.month(func.to_date(func.col('crash_date')))) \
            .na.fill('N/A')
        
        max_timestamp = people_df.select(func.max(func.to_timestamp("crash_date")).alias('max_timestamp')) \
                .collect()[0][0]
        insert_watermark = spark.createDataFrame(data=[['People', max_timestamp]], schema=['TableName', 'WatermarkValue'])
        watermarkTable.alias('a') \
                .merge(insert_watermark.alias('b'), condition='a.TableName = b.TableName') \
                .whenMatchedUpdate(condition='b.WatermarkValue > a.WatermarkValue', set={'WatermarkValue': 'b.WatermarkValue'}) \
                .whenNotMatchedInsertAll() \
                .execute()
                
        if path_exists('hdfs://namenode:9000/chicago-crash/raw/people'):
            old_people_table = DeltaTable.forPath(spark, path='hdfs://namenode:9000/chicago-crash/raw/people')
            old_people_table.alias('a') \
                .merge(people_df.alias('b'), 'a.person_id = b.person_id AND a.crash_record_id = b.crash_record_id') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            people_df.write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/chicago-crash/raw/people').option('mergeSchema', True).partitionBy('year', 'month', 'day').save()

def get_vehicle_data(spark, days_ago=1):
    limit, offset = 10000, 0
    current_time = datetime.now()
    filter_date = current_time - timedelta(days=days_ago)
    watermarkTable = DeltaTable.forPath(spark, 'hdfs://namenode:9000/chicago-crash/raw/watermark')
    while True:
        response = requests.get(f"https://data.cityofchicago.org/resource/68nd-jvt3.json?$where=crash_date>'{filter_date.date()}'&$limit={limit}&$offset={offset}")
        offset += 10000
        if len(response.json()) == 0:
            break
        if response.status_code != 200:
            break
        vehicles_df = spark.createDataFrame(response.json()) \
            .withColumn('year', func.year(func.to_date(func.col('crash_date')))) \
            .withColumn('month', func.month(func.to_date(func.col('crash_date')))) \
            .withColumn('day', func.month(func.to_date(func.col('crash_date')))) \
            .na.fill('N/A') \
            .filter(func.col('unit_type') != 'PEDESTRIAN')
        
        max_timestamp = vehicles_df.select(func.max(func.to_timestamp("crash_date")).alias('max_timestamp')) \
                .collect()[0][0]
        insert_watermark = spark.createDataFrame(data=[['Vehicles', max_timestamp]], schema=['TableName', 'WatermarkValue'])
        watermarkTable.alias('a') \
                .merge(insert_watermark.alias('b'), condition='a.TableName = b.TableName') \
                .whenMatchedUpdate(condition='b.WatermarkValue > a.WatermarkValue', set={'WatermarkValue': 'b.WatermarkValue'}) \
                .whenNotMatchedInsertAll() \
                .execute()
                
        if path_exists('hdfs://namenode:9000/chicago-crash/raw/vehicles'):
            old_vehicles_table = DeltaTable.forPath(spark, path='hdfs://namenode:9000/chicago-crash/raw/vehicles')
            old_vehicles_table.alias('a') \
                .merge(vehicles_df.alias('b'), condition='a.vehicle_id = b.vehicle_id AND a.crash_record_id = b.crash_record_id') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        else:
            vehicles_df.write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/chicago-crash/raw/vehicles').save()
