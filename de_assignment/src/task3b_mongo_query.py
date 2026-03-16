#author: Phung Jian Thong
import pprint
import os
from connectors.mongo_connector import MongoConnector
from dotenv import load_dotenv
from utils.spark_manager import SparkManager
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, format_string


class MongoQueryAnalyzer:
    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.db_manager = MongoConnector(mongo_uri=mongo_uri)
        self.collection = self.db_manager.get_mongo_collection(mongo_db, mongo_collection)
        self.spark_manager = SparkManager(app_name="MongoQuery")
        self.spark = self.spark_manager.get_session()

    def run_daily_analysis(self):
        if self.collection is None:
            print(" Cannot run daily analysis, collection not available.")
            return
        daily_pipeline = [
            {"$group": {
                "_id": "$time_features.day_of_week",
                "avg_carbon_monoxide": {"$avg": "$pollutants.carbon_monoxide"},
                "avg_benzene": {"$avg": "$pollutants.benzene"},
                "avg_nitrogen_dioxide": {"$avg": "$pollutants.nitrogen_dioxide"}
            }},
            {"$sort": {"_id": 1}}
        ]
        results = list(self.collection.aggregate(daily_pipeline))
        return results

    def daily_analysis_df(self):
        result = self.run_daily_analysis()
        
        schema = StructType([
        StructField("_id", IntegerType(), True),
        StructField("avg_carbon_monoxide", DoubleType(), True),
        StructField("avg_benzene", DoubleType(), True),
        StructField("avg_nitrogen_dioxide", DoubleType(), True)
        ])
        
        df = self.spark.createDataFrame(result, schema=schema)
        
        df = df.withColumnRenamed("_id", "Day")

        day_lookup = [
            (1, "Sunday"), (2, "Monday"), (3, "Tuesday"),
            (4, "Wednesday"), (5, "Thursday"), (6, "Friday"), (7, "Saturday")
        ]
        day_df = self.spark.createDataFrame(day_lookup, ["Day", "day_name"])
        
        df = df.join(day_df, on="Day", how="left")
        return df

    def run_hourly_analysis(self):
        if self.collection is None:
            print(" Cannot run hourly analysis, collection not available.")
            return
        hourly_pipeline = [
            {"$group": {
                "_id": "$time_features.hour_of_day",
                "avg_carbon_monoxide": {"$avg": "$pollutants.carbon_monoxide"},
                "peak_carbon_monoxide": {"$max": "$pollutants.carbon_monoxide"},
            }},
            {"$sort": {"avg_carbon_monoxide": -1}},
            {"$limit": 5},
            {"$project": {
                "_id": 0, "hour_of_day": "$_id", "avg_co": "$avg_carbon_monoxide",
                "peak_co": "$peak_carbon_monoxide"
            }}
        ]
        results = list(self.collection.aggregate(hourly_pipeline))
        return results

    def hourly_analysis_df(self):
        result = self.run_hourly_analysis()

        schema = StructType([
        StructField("hour_of_day", IntegerType(), True),
        StructField("avg_co", DoubleType(), True),
        StructField("peak_co", DoubleType(), True)
        ])

        df = self.spark.createDataFrame(result, schema=schema)
        df = df.select(format_string("%02d:00", col("hour_of_day")).alias("hour_of_day"),
                    col("avg_co").alias("avg_carbon_monoxide"),
                    col("peak_co").alias("peak_carbon_monoxide")
)


        return df

    def print_results(self):
        result1 = self.run_daily_analysis()
        result2 = self.run_hourly_analysis()
        print("\n--- QUERY 1: Daily Average Pollution Levels (1=Sun, 7=Sat) ---")
        pprint.pprint(result1)
        print("\n--- QUERY 2: Top 5 Most Polluted Hours of the Day ---")
        pprint.pprint(result2)

    def run_all_queries(self):
        self.run_daily_analysis()
        self.run_hourly_analysis()
        self.print_results()

    def close(self):
        self.db_manager.close_connections()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "configs", "mongo_config.env")
    load_dotenv(dotenv_path=config_path)

    analyzer = MongoQueryAnalyzer(
        mongo_uri = os.getenv("MONGO_URI"),
        mongo_db = os.getenv("MONGO_DB"),
        mongo_collection = os.getenv("MONGO_COLLECTION")
    )

    print("\n--- QUERY 1: Daily Average Pollution Levels (1=Sun, 7=Sat) ---")
    query1_df = analyzer.daily_analysis_df()
    query1_df.show(n=query1_df.count(), truncate=False)

    print("\n--- QUERY 2: Top 5 Most Polluted Hours of the Day ---")
    query2_df = analyzer.hourly_analysis_df()
    query2_df.show()
    analyzer.close()

