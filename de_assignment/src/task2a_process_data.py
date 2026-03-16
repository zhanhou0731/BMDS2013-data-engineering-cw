#author: Desmond Oon Kai Quan
from utils.spark_manager import SparkManager
from utils.data_processor import DataProcessor
from configs.path import HDFS_RAW_PATH, HDFS_PROCESSED_PATH


def main():
    spark_manager = SparkManager(app_name="DataProcessingPipeline")
    spark = spark_manager.get_session()

    print(f"Loading raw data from {HDFS_RAW_PATH}...")
    raw_df = spark.read.parquet(HDFS_RAW_PATH)

    processor = DataProcessor()
    processed_df = processor.clean_and_transform(raw_df)
    
    print("\nShowing a sample of the final processed data:")
    processed_df.show(5)

    print(f"Saving processed data to {HDFS_PROCESSED_PATH}...")
    processed_df.write.mode("overwrite").parquet(HDFS_PROCESSED_PATH)
    print("Data successfully saved.")

    spark_manager.stop_session()

if __name__ == "__main__":
    main()
