#author: Phung Jian Thong
import json
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import struct, col
from utils.spark_manager import SparkManager
from connectors.mongo_connector import MongoConnector
from dotenv import load_dotenv
from configs.path import HDFS_PROCESSED_PATH

class MongoLoader:
    def __init__(self, hdfs_path, mongo_uri, mongo_db, mongo_collection):

        self.hdfs_path = hdfs_path
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.spark_manager = SparkManager(app_name="LoadToMongoDB")
        self.db_manager = MongoConnector(mongo_uri=self.mongo_uri)

    def _restructure_for_mongo(self, df: DataFrame):
        print("Restructuring data for MongoDB...")
        return df.select(
            col("event_timestamp"),
            struct(
                col("carbon_monoxide"), 
                col("benzene"), 
                col("nitrogen_oxides"),
                col("nitrogen_dioxide"), 
                col("non_methane_hydrocarbon")
            ).alias("pollutants"),
            struct(
                col("year"), 
                col("month"), 
                col("day"),
                col("day_of_week"), 
                col("hour_of_day")
            ).alias("time_features")
        )

    def run(self):
        spark = self.spark_manager.get_session()

        print(f" Loading processed data from {self.hdfs_path}...")
        processed_df = spark.read.parquet(self.hdfs_path)
        
        mongo_df = self._restructure_for_mongo(processed_df)
        
        data_to_insert = [json.loads(row) for row in mongo_df.toJSON().collect()]
        
        if not data_to_insert:
            print("No data to insert.")
        else:
            print(f" Collected {len(data_to_insert)} records.")
            collection = self.db_manager.get_mongo_collection(self.mongo_db, self.mongo_collection)
            if collection is not None:
                print(f" Writing {len(data_to_insert)} records to MongoDB...")
                collection.delete_many({})  # Clear old data for a fresh load
                collection.insert_many(data_to_insert)
                print(" Successfully wrote data to MongoDB!")

        self.db_manager.close_connections()
        self.spark_manager.stop_session()
        print("Loader finished and resources are closed.")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "configs", "mongo_config.env")
    load_dotenv(dotenv_path=config_path)

    
    loader = MongoLoader(
        hdfs_path=HDFS_PROCESSED_PATH,
        mongo_uri=os.getenv("MONGO_URI"),
        mongo_db=os.getenv("MONGO_DB"),
        mongo_collection=os.getenv("MONGO_COLLECTION")
    )
    loader.run()
