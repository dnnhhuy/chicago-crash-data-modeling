from spark.processor import DataProcessor
def run():
    processor = DataProcessor()
    processor.api_to_delta()
    processor.stop()

if __name__ == '__main__':
    run()
    
    