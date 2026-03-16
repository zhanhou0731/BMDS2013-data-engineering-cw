#author: Seow Zhan Hou
from connectors.neo4j_connector import Neo4jConnection
from utils.spark_manager import SparkManager
from configs.path import HDFS_PROCESSED_PATH

class Neo4jLoader():

    def __init__(self):
        self.neo = Neo4jConnection()

    def create_time_hierarchy_contraints(self):
        constraints = [
            """
            CREATE CONSTRAINT year_unique IF NOT EXISTS
            FOR (y:Year) REQUIRE (y.year) IS UNIQUE
            """,
            """
            CREATE CONSTRAINT month_unique IF NOT EXISTS
            FOR (m:Month) REQUIRE (m.month, m.year) IS UNIQUE
            """,
            """
            CREATE CONSTRAINT day_unique IF NOT EXISTS
            FOR (d:Day) REQUIRE (d.day, d.month, d.year) IS UNIQUE
            """,
            """
            CREATE CONSTRAINT hour_unique IF NOT EXISTS
            FOR (h:HourOfDay) REQUIRE (h.hour, h.day, h.month, h.year) IS UNIQUE
            """,
            """
            CREATE CONSTRAINT dow_unique IF NOT EXISTS
            FOR (dow:DayOfWeek) REQUIRE (dow.code) IS UNIQUE
            """
        ]
        for constraint in constraints:
            self.neo.query(constraint)

    def create_pollution_constraints(self):
        constraints = """
        CREATE CONSTRAINT pollution_type_unique IF NOT EXISTS
        FOR (p:PollutionType) REQUIRE (p.name) IS UNIQUE
        """
        self.neo.query(constraints)

    def create_index(self):
        indeces =["""
        CREATE INDEX hour_name_index IF NOT EXISTS 
        FOR (h:HourOfDay) ON (h.name)"""
        ,
        """        
        CREATE INDEX pollution_index IF NOT EXISTS 
        FOR (p:PollutionType) ON (p.name)
        """
        ]
        for index in indeces:
            self.neo.query(index)

    def create_time_hierarchy(self, data):
        query = """
        UNWIND $rows AS row
        MERGE (y:Year {year: row.year})
        MERGE (m:Month {month: row.month, year: row.year})
            ON CREATE SET m.name = CASE m.month
                WHEN 1 THEN 'January'  WHEN 2 THEN 'February'  WHEN 3 THEN 'March'
                WHEN 4 THEN 'April'    WHEN 5 THEN 'May'       WHEN 6 THEN 'June'
                WHEN 7 THEN 'July'     WHEN 8 THEN 'August'    WHEN 9 THEN 'September'
                WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
            END
        MERGE (d:Day {day: row.day, month: row.month, year: row.year})
        MERGE (h:HourOfDay {hour: row.hour_of_day, day: row.day, month: row.month, year: row.year})
            ON CREATE SET h.name = toString(CASE 
            WHEN row.hour_of_day < 10 THEN '0' + row.hour_of_day 
            ELSE toString(row.hour_of_day) END) + ':00'
            
        MERGE (dow:DayOfWeek {code: row.day_of_week})
            ON CREATE SET dow.name = CASE row.day_of_week
                WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday' WHEN 7 THEN 'Sunday'
            END
        MERGE (y)-[:HAS_MONTH]->(m)
        MERGE (m)-[:HAS_DAY]->(d)
        MERGE (d)-[:HAS_HOUR]->(h)
        MERGE (d)-[:DAY_OF_WEEK]->(dow)
        """
        self.neo.query(query, {"rows": data}) 

    def create_pollution_readings(self, data):
        query = """
        UNWIND $rows AS row
        MATCH (h:HourOfDay {hour: row.hour_of_day, day: row.day, month: row.month, year: row.year})
        UNWIND [
          {name:'Benzene', value: row.benzene},
          {name:'NO2', value: row.nitrogen_dioxide},
          {name:'NOx', value: row.nitrogen_oxides},
          {name:'CO', value: row.carbon_monoxide},
          {name:'NMHC', value: row.non_methane_hydrocarbon}
        ] AS pollution_record
        MERGE (p:PollutionType {name: pollution_record.name})
        CREATE (r:Reading {value: pollution_record.value})
        MERGE (h)-[:HAS_READING]->(r)
        MERGE (r)-[:READING_OF]->(p)
        """
        self.neo.query(query, {"rows": data})

    def add_pollutant_type_unit(self):
        query = """
        UNWIND $rows AS row
        MATCH (p:PollutionType { name: row.name })
        SET p.unit = row.unit
        """
        pollutants = [
            {"name": "NO2", "unit": "µg/m³"},
            {"name": "NOx", "unit": "ppm"},
            {"name": "CO", "unit": "µg/m³"},
            {"name": "NMHC", "unit": "µg/m³"},
            {"name": "Benzene", "unit": "µg/m³"}
        ]
        self.neo.query(query, {"rows": pollutants})

    def close(self):
        self.neo.close()

def main():
    spark_manager = SparkManager(app_name="HDFStoNeo4j")
    spark = spark_manager.get_session()
    df = spark.read.parquet(HDFS_PROCESSED_PATH)
    data_to_load = [row.asDict() for row in df.collect()]
    spark_manager.stop_session()
    
    if not data_to_load:
        print(" No data found in HDFS. Exiting.")
        exit()

    print(f" Collected {len(data_to_load)} records from Spark.")

    loader = Neo4jLoader()
    loader.create_time_hierarchy_contraints()
    loader.create_pollution_constraints()
    loader.create_index()
    loader.create_time_hierarchy(data_to_load)
    loader.create_pollution_readings(data_to_load)
    loader.add_pollutant_type_unit()
    print("Records successfully inserted.")
    loader.close()
    print("Neo4j connection closed.")

if __name__ == "__main__":
    main()