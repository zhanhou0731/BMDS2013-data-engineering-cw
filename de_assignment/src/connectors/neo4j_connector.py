from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

class Neo4jConnection:

    def __init__(self, config_path=None):
        if config_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(base_dir, "configs", "neo4j_config.env")
        
        load_dotenv(dotenv_path=config_path)
            
        uri = os.getenv("NEO4J_URI")
        user = os.getenv("NEO4J_USER")
        password = os.getenv("NEO4J_PASSWORD")

        if not all([uri, user, password]):
            raise ValueError("NEO4J_URI, NEO4J_USER, or NEO4J_PASSWORD not found in environment or .env file.")
        
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None

        try:
            self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
        except Exception as e:
            print("Failed to create the driver:", e)
        print("Connected to Neo4j")


    def close(self):
        if self._driver is not None:
            self._driver.close()

    def query(self, query, parameters=None):
        with self._driver.session() as session:
            result = session.run(query, parameters)
            return result
            
    def execute_query(self, query, parameters=None):
        with self._driver.session() as session:
            return list(session.run(query, parameters))