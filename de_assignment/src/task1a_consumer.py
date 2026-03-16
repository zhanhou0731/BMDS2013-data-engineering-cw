#author: Lim Jun Jie
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from utils.spark_manager import SparkManager
from configs.path import HDFS_RAW_PATH, CHECKPOINT_PATH

class DataConsumer:
    def __init__(self, kafka_broker, kafka_topic):
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.spark_manager = SparkManager(app_name="AirQualityStreamConsumerOOP")
        self.spark = self.spark_manager.get_session()
        self.schema = self._define_schema()


    def _define_schema(self):
        return StructType([
            StructField("Date", StringType(), True),
            StructField("Time", StringType(), True),
            StructField("COGT", DoubleType(), True),
            StructField("PT08S1CO", DoubleType(), True),
            StructField("NMHCGT", DoubleType(), True),
            StructField("C6H6GT", DoubleType(), True),
            StructField("PT08S2NMHC", DoubleType(), True),
            StructField("NOxGT", DoubleType(), True),
            StructField("PT08S3NOx", DoubleType(), True),
            StructField("NO2GT", DoubleType(), True),
            StructField("PT08S4NO2", DoubleType(), True),
            StructField("PT08S5O3", DoubleType(), True),
            StructField("T", DoubleType(), True),
            StructField("RH", DoubleType(), True),
            StructField("AH", DoubleType(), True)
        ])

    def start(self):

        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .load()
            

        parsed_stream = kafka_df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")

        hdfs_writer = parsed_stream.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", HDFS_RAW_PATH) \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .start()

        console_writer = parsed_stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        print(f"Listening to Kafka topic '{self.kafka_topic}'. Writing to HDFS and console...")

        self.spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    KAFKA_BROKER = 'localhost:9092'
    KAFKA_TOPIC = 'air_quality_stream'
    consumer = DataConsumer(kafka_broker=KAFKA_BROKER, kafka_topic=KAFKA_TOPIC)
    consumer.start()

