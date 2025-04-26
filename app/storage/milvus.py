"""
Milvus adapter for vector storage.

This module provides functions for storing and retrieving vectors from Milvus.
"""

import os
import json
import logging
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Union, Callable
from functools import wraps

from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Type definitions
Vector = List[float]
VectorID = str
Metadata = Dict[str, Any]

# Module-level connection state
_connected = False


def get_connection_params() -> Dict[str, Any]:
    """Get Milvus connection parameters from environment variables.
    
    Returns:
        Dict with host and port
    """
    return {
        "host": os.environ.get("MILVUS_HOST", "localhost"),
        "port": os.environ.get("MILVUS_PORT", "19530")
    }


def with_milvus_connection(func: Callable):
    """Decorator to ensure a Milvus connection is available.
    
    Args:
        func: Function to wrap
        
    Returns:
        Wrapped function that ensures a Milvus connection is available
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        global _connected
        
        if not _connected:
            _connected = connect()
            
        if not _connected:
            raise RuntimeError("Failed to connect to Milvus")
            
        return func(*args, **kwargs)
    
    return wrapper


def connect() -> bool:
    """Connect to Milvus.
    
    Returns:
        bool: Whether the connection was successful
    """
    global _connected
    
    if _connected:
        return True
    
    params = get_connection_params()
    
    try:
        connections.connect("default", host=params["host"], port=params["port"])
        logger.info(f"Connected to Milvus at {params['host']}:{params['port']}")
        _connected = True
        return True
    except Exception as e:
        logger.error(f"Failed to connect to Milvus: {str(e)}")
        _connected = False
        return False


def disconnect() -> None:
    """Disconnect from Milvus."""
    global _connected
    
    if _connected:
        try:
            connections.disconnect("default")
            logger.info("Disconnected from Milvus")
        except Exception as e:
            logger.error(f"Error disconnecting from Milvus: {str(e)}")
        finally:
            _connected = False


def collection_exists(name: str) -> bool:
    """Check if a collection exists.
    
    Args:
        name: Collection name
        
    Returns:
        Whether the collection exists
    """
    try:
        return utility.has_collection(name)
    except Exception as e:
        logger.error(f"Error checking if collection exists: {str(e)}")
        return False


@with_milvus_connection
def create_collection(name: str, dimension: int, recreate: bool = False) -> bool:
    """Create a collection for storing vectors.
    
    Args:
        name: Collection name
        dimension: Vector dimension
        recreate: Whether to recreate the collection if it exists
        
    Returns:
        Whether the operation was successful
    """
    try:
        # Check if collection exists
        if collection_exists(name):
            if recreate:
                utility.drop_collection(name)
                logger.info(f"Dropped existing collection: {name}")
            else:
                logger.info(f"Collection {name} already exists")
                return True
        
        # Define collection schema
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="vector_id", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dimension),
            FieldSchema(name="metadata", dtype=DataType.JSON)
        ]
        
        schema = CollectionSchema(fields)
        collection = Collection(name=name, schema=schema)
        
        # Create index for efficient search
        index_params = {
            "metric_type": "COSINE",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024}
        }
        collection.create_index(field_name="embedding", index_params=index_params)
        collection.load()
        
        logger.info(f"Created collection: {name}")
        return True
    except Exception as e:
        logger.error(f"Failed to create collection: {str(e)}")
        return False


@with_milvus_connection
def store_vectors(collection: str, vectors: List[Vector], 
                 ids: List[str], metadata: Optional[List[Metadata]] = None) -> List[str]:
    """Store vectors in a collection.
    
    Args:
        collection: Collection name
        vectors: Vectors to store
        ids: Vector IDs
        metadata: Optional metadata for each vector
        
    Returns:
        List of internal IDs for the stored vectors
    """
    try:
        # Get collection
        coll = Collection(collection)
        
        # Prepare metadata if not provided
        if metadata is None:
            metadata = [{} for _ in range(len(vectors))]
        
        # Convert numpy arrays to lists if needed
        if isinstance(vectors, np.ndarray):
            vectors = vectors.tolist()
        
        # Prepare data for insertion
        entities = [
            ids,
            vectors,
            metadata
        ]
        
        # Insert data
        insert_result = coll.insert(entities)
        
        # Ensure data is flushed to disk
        coll.flush()
        
        # Get the IDs of inserted entities
        internal_ids = insert_result.primary_keys
        
        logger.info(f"Inserted {len(internal_ids)} vectors into {collection}")
        return [str(id_) for id_ in internal_ids]
    except Exception as e:
        logger.error(f"Failed to store vectors: {str(e)}")
        return []


@with_milvus_connection
def get_vector(collection: str, vector_id: str) -> Optional[Vector]:
    """Get a vector by its ID.
    
    Args:
        collection: Collection name
        vector_id: Vector ID
        
    Returns:
        Vector or None if not found
    """
    try:
        # Get collection
        coll = Collection(collection)
        coll.load()
        
        # Query for the vector
        results = coll.query(
            expr=f'vector_id == "{vector_id}"',
            output_fields=["embedding"]
        )
        
        if results:
            return results[0].get("embedding")
        
        return None
    except Exception as e:
        logger.error(f"Failed to get vector: {str(e)}")
        return None


@with_milvus_connection
def search_vectors(collection: str, query_vector: Vector, top_k: int = 10) -> List[Dict[str, Any]]:
    """Search for similar vectors.
    
    Args:
        collection: Collection name
        query_vector: Query vector
        top_k: Number of results to return
        
    Returns:
        List of dictionaries with vector_id, score, and metadata
    """
    try:
        # Get collection
        coll = Collection(collection)
        coll.load()
        
        # Convert numpy array to list if needed
        if isinstance(query_vector, np.ndarray):
            query_vector = query_vector.tolist()
        
        # Wrap single query in list if needed
        if isinstance(query_vector[0], (int, float)):
            query_vector = [query_vector]
        
        # Define search parameters
        search_params = {
            "metric_type": "COSINE",
            "params": {"nprobe": 16}
        }
        
        # Perform search
        results = coll.search(
            query_vector,
            "embedding",
            search_params,
            top_k,
            output_fields=["vector_id", "metadata"]
        )
        
        # Format results
        formatted_results = []
        for hits in results:
            for hit in hits:
                formatted_results.append({
                    "id": str(hit.id),
                    "vector_id": hit.entity.get("vector_id"),
                    "score": hit.score,
                    "metadata": hit.entity.get("metadata")
                })
        
        return formatted_results
    except Exception as e:
        logger.error(f"Failed to search vectors: {str(e)}")
        return []


@with_milvus_connection
def delete_vector(collection: str, vector_id: str) -> bool:
    """Delete a vector.
    
    Args:
        collection: Collection name
        vector_id: Vector ID
        
    Returns:
        Whether the deletion was successful
    """
    try:
        # Get collection
        coll = Collection(collection)
        
        # Delete the vector
        expr = f'vector_id == "{vector_id}"'
        result = coll.delete(expr)
        
        # Check if any vectors were deleted
        return result and result.delete_count > 0
    except Exception as e:
        logger.error(f"Failed to delete vector: {str(e)}")
        return False


@with_milvus_connection
def collection_stats(name: str) -> Dict[str, Any]:
    """Get statistics about a collection.
    
    Args:
        name: Collection name
        
    Returns:
        Dictionary with collection statistics
    """
    try:
        if not collection_exists(name):
            return {"exists": False}
        
        coll = Collection(name)
        
        return {
            "exists": True,
            "name": name,
            "entities": coll.num_entities,
            "schema": str(coll.schema)
        }
    except Exception as e:
        logger.error(f"Failed to get collection stats: {str(e)}")
        return {"exists": False, "error": str(e)}


@with_milvus_connection
def get_all_vectors(collection: str, batch_size: int = 1000) -> Dict[str, Vector]:
    """Get all vectors in a collection.
    
    Args:
        collection: Collection name
        batch_size: Number of vectors to retrieve per batch
        
    Returns:
        Dictionary mapping vector IDs to vectors
    """
    try:
        # Get collection
        coll = Collection(collection)
        coll.load()
        
        # Check if collection has entities
        if coll.num_entities == 0:
            logger.info(f"Collection {collection} is empty")
            return {}
        
        # Query all entities with pagination
        offset = 0
        all_results = []
        
        while True:
            results = coll.query(
                expr="id > 0", 
                output_fields=["vector_id", "embedding"],
                limit=batch_size,
                offset=offset
            )
            
            if not results:
                break
            
            all_results.extend(results)
            offset += batch_size
            
            if len(results) < batch_size:
                break
        
        # Format results
        vectors_dict = {}
        
        for entity in all_results:
            vector_id = entity.get("vector_id")
            embedding = entity.get("embedding")
            
            if vector_id and embedding:
                vectors_dict[vector_id] = embedding
        
        logger.info(f"Retrieved {len(vectors_dict)} vectors from {collection}")
        return vectors_dict
    except Exception as e:
        logger.error(f"Failed to get all vectors: {str(e)}")
        return {}