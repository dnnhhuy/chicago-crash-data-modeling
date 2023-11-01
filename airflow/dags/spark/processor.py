from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from delta import *
from delta.pip_utils import configure_spark_with_delta_pip
import itertools
from pyspark.sql import functions as func
from pyspark.sql import Window
from spark.crawl_data import get_crash_data, get_people_data, get_vehicle_data
from datetime import date, timedelta
class DataProcessor:
    def __init__(self) -> None:
        conf = SparkConf()
        conf.set('spark.jars.packages', "io.delta:delta-iceberg_2.12:2.3.0.0")
        conf.set("spark.sql.spark-warehouse.dir", "hdfs://namenode:9000/spark-warehouse")
        conf.set("spark.cores.max", 2)
        conf.set("spark.driver.memory", "4g")
        conf.set("spark.executor.memory", "4g")
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        self.spark = SparkSession.builder \
            .master('spark://spark-master:7077') \
            .appName('deltalake') \
            .config(conf=conf)
            
        self.spark = configure_spark_with_delta_pip(self.spark).getOrCreate()

    def stop(self):
        self.spark.stop()

    def api_to_delta(self):
        crashes_data = get_crash_data(days_ago=7)
        crashes_df = self.spark.createDataFrame(data=crashes_data).na.fill('empty')
        
        try:
            old_crashes_table = DeltaTable.forPath(self.spark, path='hdfs://namenode:9000/data/crashes_table')
            old_crashes_table.alias('a') \
                .merge(crashes_df.alias('b'), 'a.crash_record_id = b.crash_record_id') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        except:
            crashes_df.withColumn('date', func.to_date(func.col('crash_date'))).write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/data/crashes_table').partitionBy('date').save()
            
        people_data = get_people_data(days_ago=7)
        people_df = self.spark.createDataFrame(data=people_data).na.fill('empty')
        try:
            old_people_table = DeltaTable.forPath(self.spark, path='hdfs://namenode:9000/data/people_table')
            old_people_table.alias('a') \
                .merge(people_df.alias('b'), 'a.person_id = b.person_id') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        except:
            people_df.withColumn('date', func.to_date(func.col('crash_date'))).write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/data/people_table').partitionBy('date').save()
            
        vehicles_data = get_vehicle_data(days_ago=7)
        vehicles_df = self.spark.createDataFrame(data=vehicles_data).na.fill('empty')
        try:
            old_vehicles_table = DeltaTable.forPath(self.spark, path='hdfs://namenode:9000/data/vehicles_table')
            old_vehicles_table.alias('a') \
                .merge(vehicles_df.alias('b'), 'a.vehicle_id = b.vehicle_id') \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        except:
            vehicles_df.withColumn('date', func.to_date(func.col('crash_date'))).write.mode('overwrite').format('delta').option('path', 'hdfs://namenode:9000/data/vehicles_table').partitionBy('date').save()
    
    def path_exists(self, path):
        # spark is a SparkSession
        sc = self.spark.sparkContext
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
                sc._jvm.java.net.URI.create(path),
                sc._jsc.hadoopConfiguration(),
        )
        return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
    
    def extract_crash_table(self, days_ago=0):
        paths = []
        for day in range(days_ago + 1):
            path = f'hdfs://namenode:9000/data/crashes_table/date={(date.today() - timedelta(days=day))}'
            if self.path_exists(path):
                paths.append(path)
        crash_df = self.spark.read.parquet(*paths) \
            .withColumn('hour', func.hour(func.col('crash_date'))) \
            .withColumn('minute', func.minute(func.col('crash_date'))) \
            .withColumn('second', func.second(func.col('crash_date'))) \
            .withColumn('day', func.dayofmonth(func.col('crash_date'))) \
            .withColumn('dayofweek', func.dayofweek(func.col('crash_date'))) \
            .withColumn('month', func.month(func.col('crash_date'))) \
            .withColumn('week', func.weekofyear(func.col('crash_date'))) \
            .withColumn('year', func.year(func.col('crash_date'))) \
            .withColumn('quarter', func.quarter(func.col('crash_date'))) \
            .drop('location')
        return crash_df

    def extract_people_table(self, days_ago=0):
        paths = []
        for day in range(days_ago + 1):
            path = f'hdfs://namenode:9000/data/people_table/date={(date.today() - timedelta(days=day))}'
            if self.path_exists(path):
                paths.append(path)
        people_df = self.spark.read.parquet(*paths)
        return people_df
    
    def extract_vehicle_table(self, days_ago=0):
        paths = []
        for day in range(days_ago + 1):
            path = f'hdfs://namenode:9000/data/vehicles_table/date={(date.today() - timedelta(days=day))}'
            if self.path_exists(path):
                paths.append(path)
        vehicle_df = self.spark.read.parquet(*paths)
        return vehicle_df

    def transform(self, crash_df, people_df, vehicle_df):

        dim_vehicle = vehicle_df.select('vehicle_id', 'num_passengers', 'make', 'model', 'lic_plate_state', 'vehicle_year', 'vehicle_defect', 'vehicle_type', 'vehicle_use', 'travel_direction', 'maneuver', 'towed_i', 'fire_i', 'occupant_cnt', 'towed_by', 'towed_to', 'first_contact_point', 'commercial_src', 'carrier_name', 'carrier_state', 'carrier_city', 'total_vehicle_length', 'axle_cnt', 'vehicle_config', 'cargo_body_type', 'load_type')
        
        dim_person = people_df.select('person_id', 'person_type', 'seat_no', 'city', 'state', 'zipcode', 'sex', 'age', 'drivers_license_state', 'drivers_license_class', 'safety_equipment', 'airbag_deployed', 'ejection', 'injury_classification', 'hospital', 'driver_action', 'driver_vision', 'physical_condition', 'pedpedal_action', 'pedpedal_visibility', 'pedpedal_location', 'bac_result')

        dim_location = crash_df.select('street_no', 'street_direction', 'street_name', 'alignment', 'posted_speed_limit', 'trafficway_type', 'longitude', 'latitude') \
            .dropDuplicates() \
            .withColumn('location_id', func.expr('uuid()')) \
            .select('location_id', 'street_no', 'street_direction', 'street_name', 'alignment', 'posted_speed_limit', 'trafficway_type', 'longitude', 'latitude')

        time = [[x for x in range(24)], [x for x in range(60)], [x for x in range(60)]]
        combination = itertools.product(*time)

        dim_time = self.spark.createDataFrame(combination, ['hour', 'minute', 'second']) \
            .withColumn('time_id', func.expr('uuid()')) \
            .select('time_id', 'hour', 'minute', 'second')

        dim_date = crash_df.select('day', 'dayofweek', 'month', 'week', 'year', 'quarter') \
            .dropDuplicates() \
            .withColumn('date_id', func.expr('uuid()')) \
            .select('date_id', 'day', 'dayofweek', 'month', 'week', 'year', 'quarter')


        dim_weather = crash_df.select('weather_condition', 'lighting_condition') \
            .na.fill('empty') \
            .dropDuplicates() \
            .withColumn('weather_id', func.expr('uuid()')) \
            .select('weather_id', 'weather_condition', 'lighting_condition')
            

        dim_junk = crash_df.select('intersection_related_i', 'hit_and_run_i', 'photos_taken_i', 'statements_taken_i', 'dooring_i', 'work_zone_i', 'workers_present_i') \
            .na.fill('empty') \
            .dropDuplicates() \
            .withColumn('junk_id', func.expr('uuid()'))
            
        dim_cause = crash_df.select('prim_contributory_cause', 'sec_contributory_cause') \
            .na.fill('empty') \
            .dropDuplicates() \
            .withColumn('cause_id', func.expr('uuid()')) \
            .select('cause_id', 'prim_contributory_cause', 'sec_contributory_cause')


        dim_crash_type = crash_df.select('crash_type') \
            .na.fill('empty') \
            .dropDuplicates() \
            .withColumn('crash_type_id', func.expr('uuid()')) \
            .select('crash_type_id', 'crash_type')

        dim_report_type = crash_df.select('report_type') \
            .na.fill('empty') \
            .dropDuplicates() \
            .withColumn('report_type_id', func.expr('uuid()')) \
            .select('report_type_id', 'report_type')

        dim_collision = crash_df.select('first_crash_type') \
            .na.fill('empty') \
            .dropDuplicates() \
            .withColumnRenamed('first_crash_type', 'collision_type') \
            .withColumn('collision_type_id', func.expr('uuid()')) \
            .select('collision_type_id', 'collision_type')

        dim_road_cond = crash_df.select('roadway_surface_cond', 'road_defect') \
            .na.fill('empty')\
            .dropDuplicates() \
            .withColumn ('road_cond_key', func.expr('uuid()')) \
            .select('road_cond_key', 'roadway_surface_cond', 'road_defect')

        dim_control_device_cond = crash_df.select('traffic_control_device', 'device_condition') \
            .na.fill('empty') \
            .dropDuplicates() \
            .withColumn('device_cond_key', func.expr('uuid()')) \
            .select('device_cond_key', 'traffic_control_device', 'device_condition')


        bridge_vehicle_group = vehicle_df.select('crash_record_id', 'vehicle_id') \
            .withColumnRenamed('crash_record_id', 'vehicle_group_key')

        bridge_person_group = people_df.select('crash_record_id', 'person_id') \
            .withColumnRenamed('crash_record_id', 'person_group_key')


        windowspec = Window.partitionBy(func.col('location_id')).orderBy(func.col('crash_date'))
        road_cond_mini_dim = crash_df.join(dim_location, (crash_df['street_no'] == dim_location['street_no'])
                                & (crash_df['street_direction'] == dim_location['street_direction'])
                                & (crash_df['street_name'] == dim_location['street_name'])
                                & (crash_df['alignment'] == dim_location['alignment'])
                                & (crash_df['posted_speed_limit'] == dim_location['posted_speed_limit'])
                                & (crash_df['trafficway_type'] == dim_location['trafficway_type'])
                                & (crash_df['longitude'] == dim_location['longitude'])
                                & (crash_df['latitude'] == dim_location['latitude']), 'inner') \
                            .join(dim_road_cond, (crash_df['roadway_surface_cond'] == dim_road_cond['roadway_surface_cond'])
                                                & (crash_df['road_defect'] == dim_road_cond['road_defect']), 'inner') \
                            .dropDuplicates() \
                            .select('crash_date', 'location_id', 'road_cond_key') \
                            .withColumn('start_date', func.to_date(func.col('crash_date')).cast(StringType())) \
                            .withColumn('end_date', func.to_date(func.lead(func.col('crash_date'), 1).over(windowspec)).cast(StringType())) \
                            .sort(func.col('location_id'), func.col('crash_date')) \
                            .select('location_id', 'road_cond_key', 'start_date', 'end_date') \
                            .na.fill("")
        
        control_device_cond_mini_dim = crash_df.join(dim_location, (crash_df['street_no'] == dim_location['street_no'])
                                & (crash_df['street_direction'] == dim_location['street_direction'])
                                & (crash_df['street_name'] == dim_location['street_name'])
                                & (crash_df['alignment'] == dim_location['alignment'])
                                & (crash_df['posted_speed_limit'] == dim_location['posted_speed_limit'])
                                & (crash_df['trafficway_type'] == dim_location['trafficway_type'])
                                & (crash_df['longitude'] == dim_location['longitude'])
                                & (crash_df['latitude'] == dim_location['latitude']), 'inner') \
                            .join(dim_control_device_cond, (crash_df['traffic_control_device'] == dim_control_device_cond['traffic_control_device'])
                                                & (crash_df['device_condition'] == dim_control_device_cond['device_condition']), 'inner') \
                            .dropDuplicates() \
                            .select('crash_date', 'location_id', 'device_cond_key') \
                            .withColumn('start_date', func.to_date(func.col('crash_date')).cast(StringType())) \
                            .withColumn('end_date', func.to_date(func.lead(func.col('crash_date'), 1).over(windowspec)).cast(StringType())) \
                            .sort(func.col('location_id'), func.col('crash_date')) \
                            .select('location_id', 'device_cond_key', 'start_date', 'end_date') \
                            .na.fill("")
        
        fact_crash = crash_df.join(dim_location, (crash_df['street_no'] == dim_location['street_no'])
                                & (crash_df['street_direction'] == dim_location['street_direction'])
                                & (crash_df['street_name'] == dim_location['street_name'])
                                & (crash_df['alignment'] == dim_location['alignment'])
                                & (crash_df['posted_speed_limit'] == dim_location['posted_speed_limit'])
                                & (crash_df['trafficway_type'] == dim_location['trafficway_type'])
                                & (crash_df['longitude'] == dim_location['longitude'])
                                & (crash_df['latitude'] == dim_location['latitude']), 'inner') \
                            .join(dim_time, (crash_df['hour'] == dim_time['hour'])
                                            & (crash_df['minute'] == dim_time['minute'])
                                            & (crash_df['second'] == dim_time['second']), 'inner') \
                            .join(dim_date, (crash_df['day'] == dim_date['day'])
                                            & (crash_df['dayofweek'] == dim_date['dayofweek'])
                                            & (crash_df['month'] == dim_date['month'])
                                            & (crash_df['week'] == dim_date['week'])
                                            & (crash_df['year'] == dim_date['year']), 'inner') \
                            .join(dim_collision, (crash_df['first_crash_type'] == dim_collision['collision_type']), 'inner') \
                            .join(dim_report_type, (crash_df['report_type'] == dim_report_type['report_type']), 'inner') \
                            .join(dim_weather, (crash_df['weather_condition'] == dim_weather['weather_condition']) & (crash_df['lighting_condition'] == dim_weather['lighting_condition']), 'inner') \
                            .join(dim_junk, (crash_df['intersection_related_i'] == dim_junk['intersection_related_i'])
                                            & (crash_df['hit_and_run_i'] == dim_junk['hit_and_run_i'])
                                            & (crash_df['photos_taken_i'] == dim_junk['photos_taken_i'])
                                            & (crash_df['statements_taken_i'] == dim_junk['statements_taken_i'])
                                            & (crash_df['dooring_i'] == dim_junk['dooring_i'])
                                            & (crash_df['work_zone_i'] == dim_junk['work_zone_i'])
                                            & (crash_df['workers_present_i'] == dim_junk['workers_present_i']), 'inner') \
                            .join(dim_cause, (crash_df['prim_contributory_cause'] == dim_cause['prim_contributory_cause'])
                                            & (crash_df['sec_contributory_cause'] ==  dim_cause['sec_contributory_cause']), 'inner') \
                            .join(dim_crash_type, (crash_df['crash_type'] == dim_crash_type['crash_type']), 'inner') \
                            .withColumn('person_group_key', func.col('crash_record_id')) \
                            .withColumn('vehicle_group_key', func.col('crash_record_id')) \
                            .select('location_id', 'time_id', 'date_id', 'person_group_key', 'vehicle_group_key', 'weather_id', 'junk_id', 'cause_id', 'collision_type_id', 'report_type_id', 'crash_type_id', 'damage', 'num_units', 'injuries_total', 'injuries_fatal', 'injuries_incapacitating', 'injuries_non_incapacitating', 'injuries_reported_not_evident', 'injuries_no_indication', 'injuries_unknown')
        fact_crash.show()
        return {'dim_location': dim_location,
                'road_cond_mini_dim': road_cond_mini_dim,
                'control_device_cond_mini_dim': control_device_cond_mini_dim,
                'dim_road_cond': dim_road_cond,
                'dim_control_device_cond': dim_control_device_cond,
                'dim_time': dim_time,
                'dim_date': dim_date,
                'bridge_vehicle_group': bridge_vehicle_group,
                'dim_vehicle': dim_vehicle,
                'dim_collision': dim_collision,
                'dim_report_type': dim_report_type,
                'bridge_person_group': bridge_person_group,
                'dim_person': dim_person,
                'dim_weather': dim_weather,
                'dim_junk': dim_junk,
                'dim_cause': dim_cause,
                'dim_crash_type': dim_crash_type,
                'fact_crash': fact_crash}
    

    # Merge to old table
    def load_star_schema(self, table_dict):
        for table_name, table in table_dict.items():
            table.write.mode('overwrite').format('parquet').option('path', f'hdfs://namenode:9000/spark-warehouse/{table_name}').save()
        