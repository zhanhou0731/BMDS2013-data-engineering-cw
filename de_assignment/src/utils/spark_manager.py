from pyspark.sql import SparkSession

class SparkManager:
    def __init__(self, app_name="DefaultSparkApp", log_level="WARN"):

        print(f" Initializing Spark session for '{app_name}'...")
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel(log_level)
        print("Spark session created successfully.")


    def get_session(self):
        return self.spark

    def stop_session(self):
        if self.spark:
            self.spark.stop()
            print("Spark session stopped.")
