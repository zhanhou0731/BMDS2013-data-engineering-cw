===========================================================
BMDS2013 Data Engineering Assignment README
===========================================================

Team Reference: RDS2S3G2-4
Submission Date: 29 August 2025

### 1. Project Title:
End-to-End Air Quality Data Engineering Pipeline


##  Project Architecture

The pipeline is designed with a clear separation of concerns, following a producer-consumer-processor model:

1.  Data Ingestion (Producer): A Python script ('producer.py') reads a source CSV file, performs initial cleaning, and streams each record as a message to a Kafka topic.
2.  Real-time Streaming (Consumer): A Spark Structured Streaming application (`consumer.py`) subscribes to the Kafka topic, validates the incoming data against a schema, and saves the raw data in Parquet format to HDFS.
3.  Batch Processing (Processor): A batch Spark job ('process_data.py') reads the raw data from HDFS, applies a series of complex transformations (cleaning, imputation, feature enrichment) using the 'DataProcessor' class, and saves the clean data back to HDFS.
4.  Data Warehousing & Analysis:
     The processed data is loaded into a MongoDB collection for flexible document-based storage and querying.
     Relationships between pollutants and time features are modeled and stored in a Neo4j graph database.
5.  Visualization: A Jupyter Notebook (`task3_mongo_dashboard.ipynb`) connects to the databases, runs analytical queries, and presents the findings through interactive charts and graphs using Seaborn and Plotly.


### 2. Project Folder Structure:
/de_assignment/
├── src/
|      └── configs/
|       |   └── mongo_config.env
|       |   └── neo4j_config.env
|       │   └── path.py
|       |
|       └── connectors/
│       |   └── mongo_connector.py
│       |   └── neo4j_connector.py
|       └── data/
│       |   └── AirQuality.csv
|       |
|       └── utils/
│       |    ├── data_processor.py
│       |    └── spark_manager.py
|       |
|       └── task1a_consumer.py
|       └── task1b_producer.py
|       └── task2a_process_data.py
|       └── task2b_process_data.ipynb
|       └── task3a_load_to_mongo.py
|       └── task3b_mongo_query.py
|       └── task3c_mongo_visualisation_seaborn.py
|       └── task3d_mongo_dashboard.ipynb
|       └── task4a_load_to_neo4j.py
|       └── task4b_neo4j_query.py
|       └── task4c_neo4j_visualisation.py
|       └── task4d_neo4j_visualisation.ipynb
|       └── task5a_structured_streaming.py
|       └── task5b_structured_streaming.ipynb
├── requirements.txt
└── readme.txt


### 3. Setup Instructions:
1.  **Prerequisites**:
    * Java 8 or 11
    * Python 3.8+ and Pip
    * Apache Spark 
    * Apache Kafka 
    * Hadoop (for HDFS)
    * MongoDB
    * Neo4j

2.  **Install Dependencies**:
    Navigate to the project's root directory and run:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment**:
    * Ensure all services (Zookeeper, Kafka, HDFS, MongoDB, Neo4j) are running.
    * In the project root, create a file named `.env` and populate it with your database credentials based on the `configs/.env.example` template.

### 4. How to Run the Demo:

#### Task 1 & 2: Data Ingestion and Processing
Execute the following scripts in separate terminals, in the specified order.

**a. Navigate to de-prj folder and Activate the virtual environment**
```bash
cd de-prj
source de-venv/bin/activate

**b. Create Kafka Topic**
```bash
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic air_quality_stream --partitions 1 --replication-factor 1

**c. Open one terminal and Run the Kafka Consumer**
This script listens to the subscribed topic, and write it to HDFS
```bash
spark-submit --jars spark-sql-kafka-0-10_2.13-3.5.1.jar,kafka-clients-3.5.1.jar,spark-token-provider-kafka-0-10_2.13-3.5.1.jar,commons-pool2-2.11.1.jar de_assignment/src/task1.1_consumer.py


**d. Open a new terminal and Run the Kafka Producer**
This script reads the source CSV, cleans it, and streams it to Kafka.
```bash
spark-submit de_assignment/src/task1.2_producer.py

**e. Checks whether the data has already been written to HDFS**
```bash
hdfs dfs -ls /user/air_quality/raw

**f. Run the Python file to process the data**
This script performs data processing such as Data Cleaning, Data Standardisation and etc.
```bash
spark-submit de_assignment/src/task2.1_process_data.py

**g. Open Jupyter Lab and navigate to task2.2_process_data.ipynb to show how the data is being processed

### Task 3: Load Data to Atlas MongoDB and Create Query for Data Visualization and Analysis
**a. Configure the credentials to connect to Atlas MongoDB**
```bash
nano de_assignment/src/configs/mongo_config.env

**b. Load data from HDFS to MongoDB
```bash
spark-submit de_assignment/src/task3.1_load_to_mongo.py

**c. Open jupyter lab and navigate to 'task3.2_mongo_dashboard.ipynb' to see the visualization and data analysis

### Task 4: Load Data to Aura Neo4j and Create Query for Data Visualization and Analysis
**a. Configure the credentials to connect to Aura Neo4j**
```bash
nano de_assignment/src/configs/neo4j_config.env

**b. Load data from HDFS to MongoDB
```bash
spark-submit de_assignment/src/task4.1_load_to_neo4j.py

**c. Open jupyter lab and navigate to 'task4.2_neo4j_visualise.ipynb' to see the visualization and data analysis

### Task 5: Spark Structured Streaming Demo
**a. Open Jupyter lab and navigate to "task5.1_structured_streaming.ipynb"**

**b. Run each cell and show how the dataframes are being updated during streaming.**




