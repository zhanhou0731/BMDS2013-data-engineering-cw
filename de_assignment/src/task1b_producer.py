#author: Lim Jun Jie
import json
import time
import re
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType
from kafka import KafkaProducer as KafkaClient
from utils.spark_manager import SparkManager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = "file://" + os.path.join(BASE_DIR, "data/AirQuality.csv")

class DataProducer:
    def __init__(self, csv_path, kafka_broker, kafka_topic):
        self.csv_path = csv_path
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.spark_manager = SparkManager(app_name="DataProcessingPipeline")
        self.spark = self.spark_manager.get_session()
        self.kafka_producer = self._create_kafka_producer()

    def _create_kafka_producer(self):
        print("Connecting to Kafka broker...")
        try:
            producer = KafkaClient(
                bootstrap_servers=[self.kafka_broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(" Kafka producer connected successfully.")
            return producer
        except Exception as e:
            print(f" Could not connect to Kafka broker: {e}")
            return None

    def _read_and_clean_data(self) -> DataFrame:
        print(f"Reading data from CSV file: {self.csv_path}...")
        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("delimiter", ";") \
            .load(self.csv_path)

        df_renamed = df
        for col_name in df.columns:
            if col_name is not None:
                new_col_name = re.sub(r'[^a-zA-Z0-9]', '', col_name)
                df_renamed = df_renamed.withColumnRenamed(col_name, new_col_name)

        final_cols = [c for c in df_renamed.columns if c]
        df_clean_cols = df_renamed.select(final_cols)

        df_processed = df_clean_cols
        numeric_cols = [c for c in df_processed.columns if c not in ['Date', 'Time']]
        for col_name in numeric_cols:
            df_processed = df_processed.withColumn(col_name, regexp_replace(col(col_name), ",", "."))
            df_processed = df_processed.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        print("Cleaned Columns:", df_processed.columns)
        return df_processed

    def run(self):
        if not self.kafka_producer:
            self.spark.stop()
            return

        data_to_send = self._read_and_clean_data()
        records = data_to_send.collect()

        if not records:
            print("No records found to send.")
        else:
            print(f"Found {len(records)} records. Starting stream to '{self.kafka_topic}'...")
            for row in records:
                record_dict = row.asDict(recursive=True)
                self.kafka_producer.send(self.kafka_topic, record_dict)
                print(f"Sent: {record_dict}")
                time.sleep(1)
            self.kafka_producer.flush()
            print(" All data has been published.")

        self.kafka_producer.close()
        self.spark.stop()
        print("Producer finished and resources are closed.")

if __name__ == "__main__":
    KAFKA_BROKER = 'localhost:9092'
    KAFKA_TOPIC = 'air_quality_stream'
    
    producer = DataProducer(csv_path=CSV_PATH, kafka_broker=KAFKA_BROKER, kafka_topic=KAFKA_TOPIC)
    producer.run()
