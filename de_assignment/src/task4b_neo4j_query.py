#author: Seow Zhan Hou
from connectors.neo4j_connector import Neo4jConnection
from neo4j import GraphDatabase
from utils.spark_manager import SparkManager
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import functions as F

class  AirQualityQuery:

    def __init__(self, neo=None, spark=None):
        self.neo = neo or Neo4jConnection()
        self.spark_manager = SparkManager(app_name="Neo4jPollutionQuery")
        self.spark = spark or self.spark_manager.get_session()

    def query_1(self):
        query1 = """
        MATCH (h:HourOfDay)-[:HAS_READING]->(r:Reading)-[:READING_OF]->(p:PollutionType)
        WITH p.name AS pollutant, h.name AS hour, avg(r.value) AS avg_level
        
        WITH pollutant,
             collect({hour: hour, avg_level: avg_level}) AS hourly_data,
             min(avg_level) AS min_avg,
             max(avg_level) AS max_avg
        
        UNWIND hourly_data AS data
        RETURN pollutant,
               data.hour AS hour,
               data.avg_level AS avg_level,
               (data.avg_level - min_avg) / (max_avg - min_avg) AS normalized_level
        ORDER BY pollutant, hour
        """

        result = self.neo.execute_query(query1)
        return result

    def df_query_1(self):
        result = self.query_1()
        schema = StructType([
        StructField("Pollutant", StringType(), True),
        StructField("HourOfDay", StringType(), True),
        StructField("AvgValue", DoubleType(), True),
        StructField("NormalizedValue", DoubleType(), True)
        ])
        df = self.spark.createDataFrame(result, schema=schema)
        return df
        


    def query_2(self):
        query2 = """
        MATCH (d:Day)-[:DAY_OF_WEEK]->(dow:DayOfWeek),
        (d)-[:HAS_HOUR]->(h:HourOfDay)-[:HAS_READING]->(r:Reading)-[:READING_OF]->(p:PollutionType)
        WITH p.name AS pollutant,
            dow.name AS day_of_week,
            avg(r.value) AS avg_level
        WITH pollutant, collect({day: day_of_week, val: avg_level}) AS day_vals,
             min(avg_level) AS min_avg,
             max(avg_level) AS max_avg
        UNWIND day_vals AS dv
            WITH pollutant, dv.day AS day_of_week, dv.val AS avg_level, min_avg, max_avg
        RETURN pollutant, day_of_week, avg_level,
               (avg_level - min_avg) / (max_avg - min_avg) AS normalized_level
        ORDER BY pollutant, day_of_week;

        """
        result = self.neo.execute_query(query2)
        return result

    def df_query_2(self):
        result = self.query_2()
        schema = StructType([
        StructField("Pollutant", StringType(), True),
        StructField("DayOfWeek", StringType(), True),
        StructField("AvgValue", DoubleType(), True),
        StructField("NormalizedValue", DoubleType(), True)
        ])
        df = self.spark.createDataFrame(result, schema=schema)
        df = df.withColumn(
        "DayOrder",
        F.when(F.col("DayOfWeek")=="Monday",1)
         .when(F.col("DayOfWeek")=="Tuesday",2)
         .when(F.col("DayOfWeek")=="Wednesday",3)
         .when(F.col("DayOfWeek")=="Thursday",4)
         .when(F.col("DayOfWeek")=="Friday",5)
         .when(F.col("DayOfWeek")=="Saturday",6)
         .when(F.col("DayOfWeek")=="Sunday",7)
        )

        df = df.orderBy("Pollutant", "DayOrder").drop("DayOrder")
        return df


    def print_df(self, df):
        df.show(n=df.count(), truncate=False)



def main():
    try:
        dashboard = AirQualityQuery()

        print("\nQuery 1: Average Pollution Level by Hour\n")
        df_query1 = dashboard.df_query_1()
        dashboard.print_df(df_query1)

        print("\nQuery 2: Average Pollution Levels by Day of Week\n")
        df_query2 = dashboard.df_query_2()
        dashboard.print_df(df_query2)

    except Exception as e:
        print(f"An error occurred: {e}")
         
    finally:
        if 'neo' in locals():
            neo.close()
            print("Neo4j connection closed.")
        

if __name__ == "__main__":
    main()
