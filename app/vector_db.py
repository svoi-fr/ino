import numpy as np
import json
import os
from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)


class MilvusWrapper:
    """
    A wrapper class for Milvus operations to store and retrieve embeddings
    """
    
    def __init__(self, host="localhost", port="19530"):
        """
        Initialize Milvus connection
        """
        # Connect to Milvus server
        connections.connect("default", host=host, port=port)
        self.connected = True
        print(f"Connected to Milvus server at {host}:{port}")
        
    def create_collection(self, collection_name, dim=4096, drop_existing=False):
        """
        Create a collection for storing embeddings
        
        Args:
            collection_name: Name of the collection
            dim: Dimensionality of embeddings
            drop_existing: Whether to drop existing collection
        """
        # Check if collection exists
        if utility.has_collection(collection_name):
            if drop_existing:
                utility.drop_collection(collection_name)
                print(f"Dropped existing collection: {collection_name}")
            else:
                print(f"Collection {collection_name} already exists")
                return Collection(collection_name)
        
        # Define collection schema
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="original_id", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="metadata", dtype=DataType.JSON)
        ]
        
        schema = CollectionSchema(fields)
        collection = Collection(name=collection_name, schema=schema)
        
        # Create index for efficient search
        index_params = {
            "metric_type": "COSINE",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 1024}
        }
        collection.create_index(field_name="embedding", index_params=index_params)
        collection.load()
        
        print(f"Created collection: {collection_name}")
        return collection
    
    def insert_embeddings(self, collection_name, embeddings, original_ids, metadata_list=None):
        """
        Insert embeddings into collection
        
        Args:
            collection_name: Name of the collection
            embeddings: List of embedding vectors
            original_ids: List of original IDs to reference content
            metadata_list: List of metadata dicts for each embedding
        
        Returns:
            List of Milvus IDs for inserted entities
        """
        # Get collection
        collection = Collection(collection_name)
        
        # Prepare metadata if not provided
        if metadata_list is None:
            metadata_list = [{} for _ in range(len(embeddings))]
        
        # Convert numpy arrays to lists if needed
        if isinstance(embeddings, np.ndarray):
            embeddings = embeddings.tolist()
        
        # Prepare data for insertion
        entities = [
            original_ids,
            embeddings,
            metadata_list
        ]
        
        # Insert data
        insert_result = collection.insert(entities)
        
        # Ensure data is flushed to disk
        collection.flush()
        
        # Get the IDs of inserted entities
        milvus_ids = insert_result.primary_keys
        
        print(f"Inserted {len(milvus_ids)} embeddings into {collection_name}")
        return milvus_ids
    
    def search_similar(self, collection_name, query_embedding, top_k=5):
        """
        Search for similar embeddings
        
        Args:
            collection_name: Name of the collection
            query_embedding: Query embedding vector
            top_k: Number of similar embeddings to return
        
        Returns:
            List of results with IDs, distances and metadata
        """
        try:
            # Get collection
            collection = Collection(collection_name)
            collection.load()
            
            # Check if collection has entities
            if collection.num_entities == 0:
                print(f"Collection {collection_name} is empty")
                return []
            
            # Convert numpy array to list if needed
            if isinstance(query_embedding, np.ndarray):
                query_embedding = query_embedding.tolist()
            
            # Wrap single query in list if needed
            if isinstance(query_embedding[0], (int, float)):
                query_embedding = [query_embedding]
            
            # Define search parameters
            search_params = {
                "metric_type": "COSINE",
                "params": {"nprobe": 16}
            }
            
            # Perform search
            results = collection.search(
                query_embedding,
                "embedding",
                search_params,
                top_k,
                output_fields=["original_id", "metadata"]
            )
            
            # Format results
            formatted_results = []
            for hits in results:
                hit_results = []
                for hit in hits:
                    hit_results.append({
                        "milvus_id": hit.id,
                        "original_id": hit.entity.get("original_id"),
                        "similarity": hit.score,
                        "metadata": hit.entity.get("metadata")
                    })
                formatted_results.append(hit_results)
            
            # If single query, return just one result list
            if len(formatted_results) == 1:
                return formatted_results[0]
            
            return formatted_results
            
        except Exception as e:
            print(f"Error searching in {collection_name}: {str(e)}")
            return []
    
    def get_all_embeddings(self, collection_name):
        """
        Get all embeddings from a collection
        
        Args:
            collection_name: Name of the collection
        
        Returns:
            Dictionary with original_ids as keys and embeddings as values,
            and a list of milvus_ids
        """
        try:
            # Get collection
            collection = Collection(collection_name)
            collection.load()
            
            # Check if collection has entities
            if collection.num_entities == 0:
                print(f"Collection {collection_name} is empty")
                return {}, []
            
            # Query all entities with pagination to handle potentially large result sets
            limit = 1000
            offset = 0
            all_results = []
            
            while True:
                results = collection.query(
                    expr="id > 0", 
                    output_fields=["id", "original_id", "embedding", "metadata"],
                    limit=limit,
                    offset=offset
                )
                
                if not results:
                    break
                
                all_results.extend(results)
                offset += limit
                
                if len(results) < limit:
                    break
            
            # Format results
            embeddings_dict = {}
            milvus_ids = []
            
            for entity in all_results:
                original_id = entity.get("original_id")
                embedding = entity.get("embedding")
                milvus_id = entity.get("id")
                
                if original_id and embedding:
                    embeddings_dict[original_id] = embedding
                    milvus_ids.append(milvus_id)
            
            print(f"Retrieved {len(embeddings_dict)} embeddings from {collection_name}")
            return embeddings_dict, milvus_ids
            
        except Exception as e:
            print(f"Error retrieving embeddings from {collection_name}: {str(e)}")
            return {}, []
    
    def get_embeddings_by_ids(self, collection_name, original_ids):
        """
        Get embeddings by original IDs
        
        Args:
            collection_name: Name of the collection
            original_ids: List of original IDs
            
        Returns:
            Dictionary with original_ids as keys and embeddings as values
        """
        # Get collection
        collection = Collection(collection_name)
        collection.load()
        
        # Prepare query expression
        # We need to handle multiple IDs with OR conditions
        expr_parts = [f'original_id == "{id_}"' for id_ in original_ids]
        expr = " || ".join(expr_parts)
        
        # Query entities
        results = collection.query(
            expr=expr,
            output_fields=["original_id", "embedding"]
        )
        
        # Format results
        embeddings_dict = {}
        for entity in results:
            original_id = entity.get("original_id")
            embedding = entity.get("embedding")
            embeddings_dict[original_id] = embedding
        
        return embeddings_dict
    
    def delete_by_ids(self, collection_name, milvus_ids):
        """
        Delete entities by Milvus IDs
        
        Args:
            collection_name: Name of the collection
            milvus_ids: List of Milvus IDs to delete
        """
        # Get collection
        collection = Collection(collection_name)
        
        # Convert single ID to list if needed
        if isinstance(milvus_ids, (int, str)):
            milvus_ids = [milvus_ids]
        
        # Delete entities
        expr = f"id in {milvus_ids}"
        collection.delete(expr)
        
        print(f"Deleted {len(milvus_ids)} entities from {collection_name}")
    
    def close(self):
        """
        Close connection to Milvus
        """
        if self.connected:
            connections.disconnect("default")
            self.connected = False
            print("Disconnected from Milvus server")


def create_embedding_storage(drop_existing=False):
    """
    Create the necessary collections for the project
    
    Args:
        drop_existing: Whether to drop existing collections
    """
    # Initialize Milvus wrapper
    milvus = MilvusWrapper()
    
    # Create collections for different embedding types
    milvus.create_collection("conversations", drop_existing=drop_existing)
    milvus.create_collection("category_embeddings", drop_existing=drop_existing)
    milvus.create_collection("cluster_centroids", drop_existing=drop_existing)
    
    milvus.close()
    return "Created embedding storage collections"