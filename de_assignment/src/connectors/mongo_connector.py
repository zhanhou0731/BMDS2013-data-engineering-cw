from pymongo import MongoClient

class MongoConnector:

    def __init__(self, mongo_uri=None):
        self.mongo_uri = mongo_uri
        self.mongo_client = None

    def get_mongo_collection(self, db_name, collection_name):

        if not self.mongo_uri:
            raise ValueError("MongoDB URI not provided.")
        try:
            print("Connecting to MongoDB...")
            self.mongo_client = MongoClient(self.mongo_uri)
            db = self.mongo_client[db_name]
            collection = db[collection_name]
            self.mongo_client.admin.command('ping')
            print("MongoDB connection successful.")
            return collection
        except Exception as e:
            print(f"MongoDB connection failed: {e}")
            return None


    def close_connections(self):
        self.mongo_client.close()
        print("MongoDB connection closed.")

