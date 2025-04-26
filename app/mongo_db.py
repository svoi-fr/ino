import os
import json
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBWrapper:
    """
    A wrapper class for MongoDB operations
    """
    
    def __init__(self, host="localhost", port=27017, username=None, password=None, db_name="ino_db"):
        """
        Initialize MongoDB connection
        
        Args:
            host: MongoDB host
            port: MongoDB port
            username: MongoDB username (optional)
            password: MongoDB password (optional)
            db_name: Name of the database
        """
        # Use environment variables if credentials not provided
        if username is None:
            username = os.environ.get("MONGO_USERNAME", "root")
        if password is None:
            password = os.environ.get("MONGO_PASSWORD", "example")
            
        # Build connection URI
        connection_string = f"mongodb://{username}:{password}@{host}:{port}/"
        
        try:
            # Connect to MongoDB
            self.client = MongoClient(connection_string)
            self.db = self.client[db_name]
            logger.info(f"Connected to MongoDB at {host}:{port}")
            
            # Test connection
            self.client.server_info()
            self.connected = True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            self.connected = False
            raise
    
    def insert_document(self, collection_name, document):
        """
        Insert a document into a collection
        
        Args:
            collection_name: Name of the collection
            document: Document to insert
            
        Returns:
            ID of the inserted document
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            logger.info(f"Inserted document with ID: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            logger.error(f"Failed to insert document: {str(e)}")
            return None
    
    def insert_many_documents(self, collection_name, documents):
        """
        Insert multiple documents into a collection
        
        Args:
            collection_name: Name of the collection
            documents: List of documents to insert
            
        Returns:
            List of IDs of the inserted documents
        """
        try:
            collection = self.db[collection_name]
            result = collection.insert_many(documents)
            logger.info(f"Inserted {len(result.inserted_ids)} documents")
            return result.inserted_ids
        except Exception as e:
            logger.error(f"Failed to insert documents: {str(e)}")
            return []
    
    def find_document(self, collection_name, query):
        """
        Find a document in a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to find the document
            
        Returns:
            Matching document or None
        """
        try:
            collection = self.db[collection_name]
            document = collection.find_one(query)
            return document
        except Exception as e:
            logger.error(f"Failed to find document: {str(e)}")
            return None
    
    def find_documents(self, collection_name, query, limit=0, sort_field=None, sort_direction=1):
        """
        Find multiple documents in a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to find the documents
            limit: Maximum number of documents to return (0 for all)
            sort_field: Field to sort by (optional)
            sort_direction: Sort direction (1 for ascending, -1 for descending)
            
        Returns:
            List of matching documents
        """
        try:
            collection = self.db[collection_name]
            cursor = collection.find(query)
            
            # Apply sort if specified
            if sort_field:
                cursor = cursor.sort(sort_field, sort_direction)
                
            # Apply limit if specified
            if limit > 0:
                cursor = cursor.limit(limit)
                
            return list(cursor)
        except Exception as e:
            logger.error(f"Failed to find documents: {str(e)}")
            return []
    
    def update_document(self, collection_name, query, update):
        """
        Update a document in a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to find the document
            update: Update to apply
            
        Returns:
            Number of documents updated
        """
        try:
            collection = self.db[collection_name]
            result = collection.update_one(query, update)
            logger.info(f"Updated {result.modified_count} document(s)")
            return result.modified_count
        except Exception as e:
            logger.error(f"Failed to update document: {str(e)}")
            return 0
    
    def update_documents(self, collection_name, query, update):
        """
        Update multiple documents in a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to find the documents
            update: Update to apply
            
        Returns:
            Number of documents updated
        """
        try:
            collection = self.db[collection_name]
            result = collection.update_many(query, update)
            logger.info(f"Updated {result.modified_count} document(s)")
            return result.modified_count
        except Exception as e:
            logger.error(f"Failed to update documents: {str(e)}")
            return 0
    
    def delete_document(self, collection_name, query):
        """
        Delete a document from a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to find the document
            
        Returns:
            Number of documents deleted
        """
        try:
            collection = self.db[collection_name]
            result = collection.delete_one(query)
            logger.info(f"Deleted {result.deleted_count} document(s)")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Failed to delete document: {str(e)}")
            return 0
    
    def delete_documents(self, collection_name, query):
        """
        Delete multiple documents from a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to find the documents
            
        Returns:
            Number of documents deleted
        """
        try:
            collection = self.db[collection_name]
            result = collection.delete_many(query)
            logger.info(f"Deleted {result.deleted_count} document(s)")
            return result.deleted_count
        except Exception as e:
            logger.error(f"Failed to delete documents: {str(e)}")
            return 0
    
    def count_documents(self, collection_name, query=None):
        """
        Count documents in a collection
        
        Args:
            collection_name: Name of the collection
            query: Query to filter documents (optional)
            
        Returns:
            Number of matching documents
        """
        try:
            collection = self.db[collection_name]
            query = query or {}
            count = collection.count_documents(query)
            return count
        except Exception as e:
            logger.error(f"Failed to count documents: {str(e)}")
            return 0
    
    def load_json_to_collection(self, collection_name, json_file, id_field=None):
        """
        Load documents from a JSON file into a collection
        
        Args:
            collection_name: Name of the collection
            json_file: Path to the JSON file
            id_field: Field to use as document ID (optional)
            
        Returns:
            Number of documents loaded
        """
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different input formats
            documents = data
            if isinstance(data, dict):
                # If it's a dict with items, use values
                if "items" in data:
                    documents = data["items"]
                # If it's a dict with no items field, maybe it's a dict of entities
                else:
                    documents = list(data.values())
            
            # Process documents if ID field is specified
            if id_field:
                for doc in documents:
                    if id_field in doc:
                        doc["_id"] = doc[id_field]
            
            # Insert documents
            if documents:
                result = self.insert_many_documents(collection_name, documents)
                return len(result)
            else:
                logger.warning(f"No documents found in {json_file}")
                return 0
        except Exception as e:
            logger.error(f"Failed to load JSON to collection: {str(e)}")
            return 0
    
    def close(self):
        """
        Close the MongoDB connection
        """
        if self.connected:
            self.client.close()
            self.connected = False
            logger.info("Closed MongoDB connection")
    
    def __enter__(self):
        """
        Context manager entry
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit
        """
        self.close()