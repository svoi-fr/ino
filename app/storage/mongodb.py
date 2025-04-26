"""
MongoDB adapter for document storage.

This module provides functions for storing and retrieving documents from MongoDB.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from functools import wraps

import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Type definitions
Document = Dict[str, Any]
DocumentID = str
QueryFilter = Dict[str, Any]

# Module-level client variable for connection pooling
_mongo_client = None
_mongo_db = None


def get_connection_params() -> Dict[str, Any]:
    """Get MongoDB connection parameters from environment variables.
    
    Returns:
        Dict with host, port, username, password, and db_name
    """
    return {
        "host": os.environ.get("MONGO_HOST", "localhost"),
        "port": int(os.environ.get("MONGO_PORT", 27017)),
        "username": os.environ.get("MONGO_USERNAME", "root"),
        "password": os.environ.get("MONGO_PASSWORD", "example"),
        "db_name": os.environ.get("MONGO_DB", "ino_db")
    }


def with_mongo_client(func: Callable):
    """Decorator to ensure a MongoDB client is available.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function that ensures a MongoDB client is available
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        global _mongo_client, _mongo_db
        
        if _mongo_client is None:
            connect()
            
        if _mongo_client is None:
            raise RuntimeError("Failed to connect to MongoDB")
            
        return func(*args, **kwargs)
    
    return wrapper


def connect() -> bool:
    """Connect to MongoDB.
    
    Returns:
        bool: Whether the connection was successful
    """
    global _mongo_client, _mongo_db
    
    if _mongo_client is not None:
        return True
    
    params = get_connection_params()
    
    try:
        # Build connection URI
        connection_string = f"mongodb://{params['username']}:{params['password']}@{params['host']}:{params['port']}/"
        
        # Connect to MongoDB
        _mongo_client = MongoClient(connection_string)
        _mongo_db = _mongo_client[params['db_name']]
        
        # Test connection
        _mongo_client.server_info()
        
        logger.info(f"Connected to MongoDB at {params['host']}:{params['port']}")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        _mongo_client = None
        _mongo_db = None
        return False


def disconnect() -> None:
    """Disconnect from MongoDB."""
    global _mongo_client, _mongo_db
    
    if _mongo_client is not None:
        _mongo_client.close()
        _mongo_client = None
        _mongo_db = None
        logger.info("Disconnected from MongoDB")


def get_collection(collection_name: str) -> Collection:
    """Get a MongoDB collection.
    
    Args:
        collection_name: Name of the collection
        
    Returns:
        PyMongo Collection object
    
    Raises:
        RuntimeError: If not connected to MongoDB
    """
    global _mongo_db
    
    if _mongo_db is None:
        raise RuntimeError("Not connected to MongoDB")
        
    return _mongo_db[collection_name]


@with_mongo_client
def store_document(collection: str, document: Document) -> DocumentID:
    """Store a document in MongoDB.
    
    Args:
        collection: Collection name
        document: Document to store
        
    Returns:
        ID of the stored document
    """
    try:
        result = get_collection(collection).insert_one(document)
        logger.debug(f"Inserted document with ID: {result.inserted_id}")
        return str(result.inserted_id)
    except PyMongoError as e:
        logger.error(f"Failed to insert document: {str(e)}")
        return ""


@with_mongo_client
def store_documents(collection: str, documents: List[Document]) -> List[DocumentID]:
    """Store multiple documents in MongoDB.
    
    Args:
        collection: Collection name
        documents: List of documents to store
        
    Returns:
        List of IDs of the stored documents
    """
    if not documents:
        return []
        
    try:
        result = get_collection(collection).insert_many(documents)
        return [str(id) for id in result.inserted_ids]
    except PyMongoError as e:
        logger.error(f"Failed to insert documents: {str(e)}")
        return []


@with_mongo_client
def get_document(collection: str, doc_id: DocumentID) -> Optional[Document]:
    """Get a document by its ID.
    
    Args:
        collection: Collection name
        doc_id: Document ID
        
    Returns:
        Document or None if not found
    """
    try:
        # Convert string ID to ObjectId if needed
        query_id = ObjectId(doc_id) if ObjectId.is_valid(doc_id) else doc_id
        
        document = get_collection(collection).find_one({"_id": query_id})
        
        # Convert ObjectId to string for JSON serialization
        if document and "_id" in document and isinstance(document["_id"], ObjectId):
            document["_id"] = str(document["_id"])
            
        return document
    except PyMongoError as e:
        logger.error(f"Failed to retrieve document: {str(e)}")
        return None


@with_mongo_client
def query_documents(collection: str, query: QueryFilter, limit: int = 0, 
                   offset: int = 0, sort_by: Optional[List[Tuple[str, int]]] = None) -> List[Document]:
    """Query documents based on criteria.
    
    Args:
        collection: Collection name
        query: Query filter
        limit: Maximum number of documents to return (0 for all)
        offset: Number of documents to skip
        sort_by: List of (field, direction) tuples for sorting
        
    Returns:
        List of matching documents
    """
    try:
        cursor = get_collection(collection).find(query)
        
        # Apply skip/offset
        if offset > 0:
            cursor = cursor.skip(offset)
        
        # Apply limit
        if limit > 0:
            cursor = cursor.limit(limit)
        
        # Apply sort
        if sort_by:
            cursor = cursor.sort(sort_by)
        
        # Convert ObjectId to string for JSON serialization
        documents = list(cursor)
        for doc in documents:
            if "_id" in doc and isinstance(doc["_id"], ObjectId):
                doc["_id"] = str(doc["_id"])
        
        return documents
    except PyMongoError as e:
        logger.error(f"Failed to query documents: {str(e)}")
        return []


@with_mongo_client
def update_document(collection: str, doc_id: DocumentID, updates: Dict[str, Any]) -> bool:
    """Update a document.
    
    Args:
        collection: Collection name
        doc_id: Document ID
        updates: Fields to update (using $set operator)
        
    Returns:
        Whether the update was successful
    """
    try:
        # Convert string ID to ObjectId if needed
        query_id = ObjectId(doc_id) if ObjectId.is_valid(doc_id) else doc_id
        
        # Ensure updates use $set operator if not already present
        if not any(k.startswith('$') for k in updates.keys()):
            updates = {"$set": updates}
        
        result = get_collection(collection).update_one({"_id": query_id}, updates)
        return result.modified_count > 0
    except PyMongoError as e:
        logger.error(f"Failed to update document: {str(e)}")
        return False


@with_mongo_client
def delete_document(collection: str, doc_id: DocumentID) -> bool:
    """Delete a document.
    
    Args:
        collection: Collection name
        doc_id: Document ID
        
    Returns:
        Whether the deletion was successful
    """
    try:
        # Convert string ID to ObjectId if needed
        query_id = ObjectId(doc_id) if ObjectId.is_valid(doc_id) else doc_id
        
        result = get_collection(collection).delete_one({"_id": query_id})
        return result.deleted_count > 0
    except PyMongoError as e:
        logger.error(f"Failed to delete document: {str(e)}")
        return False


@with_mongo_client
def count_documents(collection: str, query: Optional[QueryFilter] = None) -> int:
    """Count documents matching a query.
    
    Args:
        collection: Collection name
        query: Query filter (None for all documents)
        
    Returns:
        Number of matching documents
    """
    try:
        return get_collection(collection).count_documents(query or {})
    except PyMongoError as e:
        logger.error(f"Failed to count documents: {str(e)}")
        return 0


@with_mongo_client
def load_json_to_collection(collection: str, json_file: str, id_field: Optional[str] = None) -> int:
    """Load documents from a JSON file into a collection.
    
    Args:
        collection: Collection name
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
            ids = store_documents(collection, documents)
            return len(ids)
        else:
            logger.warning(f"No documents found in {json_file}")
            return 0
    except Exception as e:
        logger.error(f"Failed to load JSON to collection: {str(e)}")
        return 0