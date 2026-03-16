#author: Lee Kay Yang
from pyspark.sql.functions import window, avg, expr, col, concat_ws, to_timestamp
from utils.spark_manager import SparkManager
from configs.path import HDFS_RAW_PATH
from time import sleep

class StructuredStreaming():
    def __init__(self):
        self.spark_manager = SparkManager(app_name="StructuredStreaming")
        self.spark = self.spark_manager.get_session()

    def readFile(self, PATH=None):
        self.path = HDFS_RAW_PATH or PATH
        df = self.spark.read.parquet(self.path)
        dataSchema = df.schema
        self.streaming = self.spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1)\
        .parquet(HDFS_RAW_PATH)

    def data_transform(self):
        cols_to_drop = ["PT08S1CO", "PT08S2NMHC", "PT08S3NOx", "PT08S4NO2", "PT08S5O3", "T", "RH", "AH"]
        streaming = self.streaming.drop(*cols_to_drop)
        
        streaming = streaming.withColumn(
            "event_timestamp",
            to_timestamp(concat_ws(" ", col("Date"), col("Time")), "dd/MM/yyyy HH.mm.ss")
        )
        
        streaming = streaming.filter(
            (col("COGT") != -200) &
            (col("NMHCGT") != -200) &
            (col("C6H6GT") != -200) &
            (col("NOxGT") != -200) &
            (col("NO2GT") != -200)
        )
        
        self.cleaned = streaming.withColumnRenamed("COGT", "CO").withColumnRenamed("NMHCGT", "NMHC").withColumnRenamed("C6H6GT", "C6H6").withColumnRenamed("NOxGT", "NOx").withColumnRenamed("NO2GT", "NO2")
            
        

    def aggregate(self):
        pollutant_long = self.cleaned.selectExpr(
        "event_timestamp",
        "stack(5, " 
        "'CO', CO, "
        "'NMHC', NMHC, "
        "'Benzene', C6H6, "
        "'NOx', NOx, "
        "'NO2', NO2) "
        "as (pollutant, reading)"
        )
        
        self.pollutant_avg = pollutant_long.groupBy("pollutant").agg(
            avg("reading").alias("avg_reading")
        )

    def struc_streaming(self):
        self.cleaned_query = self.cleaned.writeStream.queryName("cleaned_pollutants") \
            .format("memory").outputMode("append") \
            .trigger(processingTime="3 seconds") \
            .start()
        
        self.avg_query = self.pollutant_avg.writeStream.queryName("pollutant_avg") \
            .format("memory").outputMode("complete") \
            .trigger(processingTime="3 seconds") \
            .start()

    def demo(self):
        for i in range(10):
            print(f"\n===== Snapshot {i+1} =====")
            
            print("--- Cleaned Pollutants ---")
            self.spark.sql("""
                SELECT *
                FROM cleaned_pollutants
                ORDER BY event_timestamp
                LIMIT 10
            """).show(truncate=False)
        
            print("--- Pollutant Averages ---")
            self.spark.sql("""
                SELECT *
                FROM pollutant_avg
                ORDER BY pollutant
            """).show(truncate=False)
        
            sleep(3)

        self.cleaned_query.stop()
        self.avg_query.stop()

    def stop(self):
        self.cleaned_query.stop()
        self.avg_query.stop()
        

