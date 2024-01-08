from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from delta import *
from delta.pip_utils import configure_spark_with_delta_pip
import itertools
from pyspark.sql import functions as func
from pyspark.sql import Window
from spark.crawl_data import get_crash_data, get_people_data, get_vehicle_data, path_exists
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

    def api_to_delta(self, days_ago=1):
        get_crash_data(spark=self.spark, days_ago=days_ago)
        get_people_data(spark=self.spark, days_ago=days_ago)
        get_vehicle_data(spark=self.spark, days_ago=days_ago)
    
    def extract_crash_table(self, days_ago=0):
        crash_table = DeltaTable.forPath(self.spark, 'hdfs://namenode:9000/chicago-crash/raw/crashes')
        crash_df = crash_table.toDF() \
            .filter((func.to_date(func.col('crash_date')) <= date.today()) & (func.to_date(func.col('crash_date')) >= date.today() - timedelta(days=days_ago))) \
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
        people_table = DeltaTable.forPath(self.spark, 'hdfs://namenode:9000/chicago-crash/raw/people')
        people_df = people_table.toDF() \
            .filter((func.to_date(func.col('crash_date')) <= date.today()) & (func.to_date(func.col('crash_date')) >= date.today() - timedelta(days=days_ago)))
        return people_df

    def extract_vehicle_table(self, days_ago=0):
        vehicles_table = DeltaTable.forPath(self.spark, 'hdfs://namenode:9000/chicago-crash/raw/vehicles')
        vehicles_df = vehicles_table.toDF() \
            .filter((func.to_date(func.col('crash_date')) <= date.today()) & (func.to_date(func.col('crash_date')) >= date.today() - timedelta(days=days_ago))) \
            .filter(func.col('vehicle_id') != 'N/A')
        return vehicles_df

    def transform(self, crash_df, people_df, vehicle_df):

        dim_vehicle = vehicle_df.select('vehicle_id', 'num_passengers', 'make', 'model', 'lic_plate_state', 'vehicle_year', 'vehicle_defect', 'vehicle_type', 'vehicle_use', 'travel_direction', 'maneuver', 'towed_i', 'fire_i', 'occupant_cnt', 'towed_by', 'towed_to', 'first_contact_point', 'commercial_src', 'carrier_name', 'carrier_state', 'carrier_city', 'total_vehicle_length', 'axle_cnt', 'vehicle_config', 'cargo_body_type', 'load_type')
    
        dim_person = people_df.select('person_id', 'person_type', 'seat_no', 'city', 'state', 'zipcode', 'sex', 'age', 'drivers_license_state', 'drivers_license_class', 'safety_equipment', 'airbag_deployed', 'ejection', 'injury_classification', 'hospital', 'driver_action', 'driver_vision', 'physical_condition', 'pedpedal_action', 'pedpedal_visibility', 'pedpedal_location', 'bac_result')

        dim_location = crash_df.select('street_no', 'street_direction', 'street_name', 'alignment', 'posted_speed_limit', 'trafficway_type', 'longitude', 'latitude') \
            .dropDuplicates()
            
        dim_location = dim_location.withColumn('location_id', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_location.columns
                )),
                512
            )) \
            .select('location_id', 'street_no', 'street_direction', 'street_name', 'alignment', 'posted_speed_limit', 'trafficway_type', 'longitude', 'latitude')

        time = [["{:02d}".format(x) for x in range(24)], ["{:02d}".format(x) for x in range(60)], ["{:02d}".format(x) for x in range(60)]]
        combination = itertools.product(*time)

        dim_time = self.spark.createDataFrame(combination, ['hour', 'minute', 'second'])
        dim_time = dim_time \
            .withColumn('time_id', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_time.columns
                )),
                512
            )) \
            .select('time_id', 'hour', 'minute', 'second')

        dim_date = crash_df.select('day', 'dayofweek', 'month', 'week', 'year', 'quarter')
        dim_date = dim_date \
            .dropDuplicates() \
            .withColumn('date_id', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_date.columns
                )),
                512
            )) \
            .select('date_id', 'day', 'dayofweek', 'month', 'week', 'year', 'quarter')


        dim_weather = crash_df.select('weather_condition', 'lighting_condition') \
            .na.fill('empty') \
            .dropDuplicates()
        dim_weather = dim_weather \
            .withColumn('weather_id', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_weather.columns
                )),
                512
            )) \
            .select('weather_id', 'weather_condition', 'lighting_condition')
            

        dim_junk = crash_df.select('intersection_related_i', 'hit_and_run_i', 'photos_taken_i', 'statements_taken_i', 'dooring_i', 'work_zone_i', 'workers_present_i') \
            .na.fill('empty') \
            .dropDuplicates()
        dim_junk = dim_junk \
            .withColumn('junk_id', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_junk.columns
                )),
                512
            ))
            
        dim_cause = crash_df.select('prim_contributory_cause', 'sec_contributory_cause') \
            .na.fill('empty') \
            .dropDuplicates()
        dim_cause = dim_cause \
            .withColumn('cause_id', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_cause.columns
                )),
                512
            )) \
            .select('cause_id', 'prim_contributory_cause', 'sec_contributory_cause')


        dim_crash_type = crash_df.select('crash_type') \
            .na.fill('empty') \
            .dropDuplicates()
        dim_crash_type = dim_crash_type \
            .withColumn('crash_type_id', func.sha2(
                func.concat(*(
                    func.col(col).cast("string")
                    for col 
                    in dim_crash_type.columns
                )),
                512
            )) \
            .select('crash_type_id', 'crash_type')

        dim_report_type = crash_df.select('report_type') \
            .na.fill('empty') \
            .dropDuplicates()
        dim_report_type = dim_report_type \
            .withColumn('report_type_id', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_report_type.columns
                )),
                512
            )) \
            .select('report_type_id', 'report_type')

        dim_collision = crash_df.select('first_crash_type') \
            .na.fill('empty') \
            .dropDuplicates() \
            .withColumnRenamed('first_crash_type', 'collision_type')
        dim_collision = dim_collision \
            .withColumn('collision_type_id', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_collision.columns
                )),
                512
            )) \
            .select('collision_type_id', 'collision_type')

        dim_road_cond = crash_df.select('roadway_surface_cond', 'road_defect') \
            .na.fill('empty')\
            .dropDuplicates()
        dim_road_cond = dim_road_cond \
            .withColumn ('road_cond_key', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_road_cond.columns
                )),
                512
            )) \
            .select('road_cond_key', 'roadway_surface_cond', 'road_defect')

        dim_control_device_cond = crash_df.select('traffic_control_device', 'device_condition') \
            .na.fill('empty') \
            .dropDuplicates()
        dim_control_device_cond = dim_control_device_cond \
            .withColumn('device_cond_key', func.sha2(
            func.concat(*(
                func.col(col).cast("string")
                for col 
                in dim_control_device_cond.columns
                )),
                512
            )) \
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
        spark = self.spark
        for table_name, table in table_dict.items():
            if not path_exists(f'hdfs://namenode:9000/chicago-crash/serving/{table_name}'):
                table.write.mode('overwrite').format('delta').option('path', f'hdfs://namenode:9000/chicago-crash/serving/{table_name}').option('mergeSchema', True).save()
            else:
                old_table = DeltaTable.forPath(spark, path=f'hdfs://namenode:9000/chicago-crash/serving/{table_name}')
                if table_name == 'dim_location':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.location_id = b.location_id') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'dim_road_cond':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.road_cond_key = b.road_cond_key') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'dim_control_device_cond':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.device_cond_key = b.device_cond_key') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'road_cond_mini_dim':
                    old_table_df = old_table.toDF()
                    old_table_df.createOrReplaceTempView('OldTableTempView')
                    # Get max start date from old table
                    max_StartDate = spark.sql('SELECT location_id, max(start_date) as max_start_date FROM OldTableTempView GROUP BY location_id')

                    # Filter only new records having start_date > max start date from old table
                    new_record = table.join(max_StartDate, (table['location_id'] == max_StartDate['location_id']), 'left_outer') \
                        .filter((func.col('start_date') > func.col('max_start_date')) | (func.isnull(func.col('max_start_date')))) \
                        .select(table['location_id'], 'road_cond_key', 'start_date', 'end_date')
                    
                    new_record.createOrReplaceTempView('NewTableTempView')
                    
                    # Get min start date from new records
                    min_StartDate_newRecord = spark.sql('SELECT location_id, min(start_date) as min_start_date FROM NewTableTempView GROUP BY location_id')

                    # Filter neededly updated rows from old table using min start date from above
                    update_rows = old_table_df.join(min_StartDate_newRecord, (old_table_df['location_id'] == min_StartDate_newRecord['location_id']), 'inner') \
                        .filter(func.col('end_date') == '') \
                        .select(old_table_df['location_id'], 'road_cond_key', 'start_date', 'min_start_date') \
                        .withColumnRenamed('min_start_date', 'end_date')
                    
                    insert_rows = update_rows.union(new_record)
                    
                    old_table.alias('a') \
                        .merge(insert_rows.alias('b'), condition='a.location_id = b.location_id AND a.road_cond_key = b.road_cond_key AND a.start_date = b.start_date') \
                        .whenMatchedUpdate(set={'end_date': 'b.end_date'}) \
                        .whenNotMatchedInsertAll() \
                        .execute()
                        
                elif table_name == 'control_device_cond_mini_dim':
                    old_table_df = old_table.toDF()
                    old_table_df.createOrReplaceTempView('OldTableTempView')
                    # Get max start date from old table
                    max_StartDate = spark.sql('SELECT location_id, max(start_date) as max_start_date FROM OldTableTempView GROUP BY location_id')

                    # Filter only new records
                    new_record = table.join(max_StartDate, (table['location_id'] == max_StartDate['location_id']), 'left_outer') \
                        .filter((func.col('start_date') > func.col('max_start_date')) | (func.isnull(func.col('max_start_date')))) \
                        .select(table['location_id'], 'device_cond_key', 'start_date', 'end_date')
                    
                    new_record.createOrReplaceTempView('NewTableTempView')
                    
                    # Get min start date from new records
                    min_StartDate_newRecord = spark.sql('SELECT location_id, min(start_date) as min_start_date FROM NewTableTempView GROUP BY location_id')

                    # Filter neededly updated rows from old table using min start date from above
                    update_rows = old_table_df.join(min_StartDate_newRecord, (old_table_df['location_id'] == min_StartDate_newRecord['location_id']), 'inner') \
                        .filter(func.col('end_date') == '') \
                        .select(old_table_df['location_id'], 'device_cond_key', 'start_date', 'min_start_date') \
                        .withColumnRenamed('min_start_date', 'end_date')
                    
                    insert_rows = update_rows.union(new_record)
                    
                    old_table.alias('a') \
                        .merge(insert_rows.alias('b'), condition='a.location_id = b.location_id AND a.device_cond_key = b.device_cond_key AND a.start_date = b.start_date') \
                        .whenMatchedUpdate(set={'end_date': 'b.end_date'}) \
                        .whenNotMatchedInsertAll() \
                        .execute()
                        
                elif table_name == 'dim_time':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.time_id = b.time_id') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'dim_date':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.date_id = b.date_id') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'bridge_vehicle_group':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.vehicle_group_key = b.vehicle_group_key AND a.vehicle_id = b.vehicle_id') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'dim_vehicle':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.vehicle_id = b.vehicle_id') \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'dim_collision':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.collision_type_id = b.collision_type_id') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'dim_report_type':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.report_type_id = b.report_type_id') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'bridge_person_group':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.person_group_key = b.person_group_key and a.person_id = b.person_id') \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'dim_person':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.person_id = b.person_id') \
                        .whenMatchedUpdateAll() \
                        .whenNotMatchedInsertAll() \
                        .execute()
                elif table_name == 'dim_weather':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.weather_id = b.weather_id') \
                        .whenNotMatchedInsertAll()\
                        .execute()
                elif table_name == 'dim_junk':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.junk_id = b.junk_id') \
                        .whenNotMatchedInsertAll()\
                        .execute()
                elif table_name == 'dim_cause':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.cause_id = b.cause_id') \
                        .whenNotMatchedInsertAll()\
                        .execute()
                elif table_name == 'dim_crash_type':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.crash_type_id = b.crash_type_id')\
                        .whenNotMatchedInsertAll()\
                        .execute()
                elif table_name == 'fact_crash':
                    old_table.alias('a') \
                        .merge(table.alias('b'), condition='a.location_id = b.location_id AND a.time_id = b.time_id AND a.date_id = b.date_id AND a.person_group_key = b.person_group_key AND a.vehicle_group_key = b.vehicle_group_key AND a.weather_id = b.weather_id AND a.junk_id = b.junk_id AND a.cause_id = b.cause_id AND a.collision_type_id = b.collision_type_id AND a.report_type_id = b.report_type_id AND a.crash_type_id = b.crash_type_id') \
                        .whenNotMatchedInsertAll() \
                        .execute()