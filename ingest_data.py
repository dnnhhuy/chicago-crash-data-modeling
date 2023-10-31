from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from delta import *
from delta.pip_utils import configure_spark_with_delta_pip
from crawl_data import *

conf = SparkConf()
conf.set('spark.jars.packages', "io.delta:delta-core_2.12:2.3.0,org.postgresql:postgresql:42.6.0")
conf.set("spark.sql.warehouse.dir", "hdfs://namenode:9000/data")
conf.set("spark.cores.max", 2)
conf.set("spark.driver.memory", "4g")
conf.set("spark.executor.memory", "4g")
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = SparkSession.builder \
    .master('spark://spark-master:7077') \
    .appName('DeltaLake') \
    .config(conf=conf)
    
spark = configure_spark_with_delta_pip(spark).getOrCreate()

if __name__ == '__main__':
    crashes_data = get_crash_data(7)
    crashes_df = spark.createDataFrame(data=crashes_data).na.fill('empty')
    
    try:
        old_crashes_table = DeltaTable.forPath(spark, path='hdfs://namenode:9000/data/crashes_table')
        old_crashes_table.alias('a') \
            .merge(crashes_df.alias('b'), 'a.crash_record_id = b.crash_record_id') \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    except:
        crashes_df.write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/data/crashes_table').save()
        
    people_data = get_people_data(7)
    people_df = spark.createDataFrame(data=people_data).na.fill('empty')
    try:
        old_people_table = DeltaTable.forPath(spark, path='hdfs://namenode:9000/data/people_table')
        old_people_table.alias('a') \
            .merge(people_df.alias('b'), 'a.person_id = b.person_id') \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    except:
        people_df.write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/data/people_table').save()
        
    vehicles_data = get_vehicle_data(7)
    vehicles_df = spark.createDataFrame(data=vehicles_data).na.fill('empty')
    try:
        old_vehicles_table = DeltaTable.forPath(spark, path='hdfs://namenode:9000/data/vehicles_table')
        old_vehicles_table.alias('a') \
            .merge(vehicles_df.alias('b'), 'a.vehicle_id = b.vehicle_id') \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    except:
        vehicles_df.write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/data/vehicles_table').save()


    

