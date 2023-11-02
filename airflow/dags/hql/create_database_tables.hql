create database if not exists chicago_crash;
use chicago_crash;

create external table if not exists dim_time (
    time_id string,
    hour int,
    minute int,
    second int
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_time';

create external table if not exists dim_date (
    date_id string,
    day int,
    dayOfWeek int,
    week int,
    month int,
    year int,
    quarter int
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_date';

create external table if not exists dim_location (
    location_id string,
    street_no string,
    street_direction string,
    street_name string,
    alignment string,
    posted_speed_limit string,
    trafficway_type string,
    longitude string,
    latitude string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_location';

create external table if not exists road_cond_mini_dim (
    location_id string,
    road_cond_key string,
    start_date string,
    end_date string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/road_cond_mini_dim';

create external table if not exists control_device_cond_mini_dim (
    location_id string,
    device_cond_key string,
    start_date string,
    end_date string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/control_device_cond_mini_dim';

create external table if not exists dim_road_cond (
    road_cond_key string,
    roadway_surface_cond string,
    road_defect string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_road_cond';

create external table if not exists dim_control_device_cond (
    device_cond_key string,
    traffic_control_device string,
    device_condition string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_control_device_cond';


create external table if not exists bridge_vehicle_group(
    vehicle_group_key string,
    vehicle_id string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/bridge_vehicle_group';

create external table if not exists dim_vehicle(
    vehicle_id string,
    num_passengers string,
    make string,
    model string,
    lic_plate_state string,
    vehicle_year string,
    vehicle_defect string,
    vehicle_type string,
    vehicle_use string,
    travel_direction string,
    maneuver string,
    towed_i string,
    fire_i string,
    occupant_cnt string,
    towed_by string,
    towed_to string,
    first_contact_point string,
    commercial_src string,
    carrier_name string,
    carrier_state string,
    carrier_city string,
    total_vehicle_length string,
    axle_cnt string,
    vehicle_config string,
    cargo_body_type string,
    load_type string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_vehicle';

create external table if not exists dim_collision(
    collision_type_id string,
    collision_type string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_collision';

create external table if not exists dim_report_type(
    report_type_id string,
    report_type string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_report_type';

create external table if not exists bridge_person_group(
    person_group_key string,
    person_id string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/bridge_person_group';

create external table if not exists dim_person(
    person_id string,
    person_type string,
    seat_no string,
    city string,
    state string,
    zipcode string,
    sex string, 
    age string,
    drivers_license_state string,
    drivers_license_class string,
    safety_equipment string,
    airbag_deployed string,
    ejection string,
    injury_classification string,
    hospital string,
    driver_action string,
    driver_vision string,
    physical_condition string,
    pedpedal_action string,
    pedpedal_visibility string,
    pedpedal_location string,
    bac_result string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_person';

create external table if not exists dim_weather(
    weather_id string,
    weather_condition string,
    lighting_condition string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_weather';

create external table if not exists dim_junk(
    junk_id string,
    intersection_related_i string,
    hit_and_run_i string,
    photos_taken_i string,
    statements_taken_i string,
    dooring_i string,
    work_zone_i string,
    workers_present_i string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_junk';

create external table if not exists dim_cause(
    cause_id string,
    prim_contributory_cause string,
    sec_contributory_cause string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_cause';

create external table if not exists dim_crash_type(
    crash_type_id string,
    crash_type string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/dim_crash_type';

create external table if not exists fact_crash(
    location_id string,
    time_id string,
    date_id string,
    person_group_key string,
    vehicle_group_key string,
    weather_id string,
    junk_id string,
    cause_id string,
    collision_type_id string,
    report_type_id string,
    crash_type_id string,
    damage string,
    num_units string,
    injuries_total string,
    injuries_fatal string,
    injuries_incapacitating string,
    injuries_non_incapacitating string,
    injuries_reported_not_evident string,
    injuries_no_indication string,
    injuries_unknown string
)
stored as parquet location 'hdfs://namenode:9000/spark-warehouse/fact_crash';