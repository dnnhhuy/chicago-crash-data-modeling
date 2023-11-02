from spark.processor import DataProcessor
def run():
    processor = DataProcessor()
    crash_df = processor.extract_crash_table(days_ago=7)
    people_df = processor.extract_people_table(days_ago=7)
    vehicle_df = processor.extract_vehicle_table(days_ago=7)
    table_dict = processor.transform(crash_df, people_df, vehicle_df)
    processor.load_star_schema(table_dict)
    processor.stop()
if __name__ == '__main__':
    run()
    