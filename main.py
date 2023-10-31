from spark.processor import DataProcessor
if __name__ == '__main__':
    processor = DataProcessor()
    processor.api_to_delta()
    crash_df = processor.extract_crash_table()
    people_df = processor.extract_people_table()
    vehicle_df = processor.extract_vehicle_table()
    table_dict = processor.transform(crash_df, people_df, vehicle_df)
    for table_name, table in table_dict.items():
        processor.load_star_schema(table, table_name)
    
    