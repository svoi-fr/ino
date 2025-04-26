"""
Document indexing module.

This module provides functions for indexing documents in MongoDB and Milvus.
"""

import os
import logging
import json
from typing import List, Dict, Any, Optional, Tuple, Union

from app.storage import mongodb, milvus
from app.processing import chunking, embedding

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_collections(recreate: bool = False) -> bool:
    """Set up the MongoDB and Milvus collections.
    
    Args:
        recreate: Whether to recreate the collections if they exist
        
    Returns:
        Whether the setup was successful
    """
    # Connect to both databases
    mongo_connected = mongodb.connect()
    milvus_connected = milvus.connect()
    
    if not mongo_connected or not milvus_connected:
        logger.error("Failed to connect to one or both databases")
        return False
    
    # Create Milvus collections
    # Use the embedding dimension from the model we'll be using
    emb_dim = embedding.get_embedding_dimension("voyage-3-large")
    
    milvus.create_collection("documents", emb_dim, recreate=recreate)
    milvus.create_collection("chunks", emb_dim, recreate=recreate)
    milvus.create_collection("categories", emb_dim, recreate=recreate)
    
    return True


def disconnect():
    """Disconnect from MongoDB and Milvus."""
    mongodb.disconnect()
    milvus.disconnect()


def index_document(document: Dict[str, Any], text_field: str, 
                 chunk_size: int = 1000, model: str = "voyage-3-large") -> str:
    """Index a document in MongoDB and Milvus.
    
    Args:
        document: The document to index
        text_field: The field containing the text to index
        chunk_size: The maximum chunk size in characters
        model: The embedding model to use
        
    Returns:
        The ID of the indexed document
    """
    # Store the original document in MongoDB
    doc_id = mongodb.store_document("documents", document)
    
    if not doc_id:
        logger.error("Failed to store document in MongoDB")
        return ""
    
    # Update the document with its ID
    document["_id"] = doc_id
    
    # Generate an embedding for the document
    doc_embedding = embedding.generate_embedding(
        document.get(text_field, ""), 
        model=model, 
        input_type="document"
    )
    
    if doc_embedding:
        # Store the document embedding in Milvus
        milvus.store_vectors(
            "documents",
            [doc_embedding],
            [doc_id],
            [{"title": document.get("title", ""), "type": document.get("type", "")}]
        )
    
    # Chunk the document
    chunks = chunking.chunk_document(
        document,
        text_field=text_field,
        max_chunk_size=chunk_size
    )
    
    # Store chunks in MongoDB
    chunk_ids = mongodb.store_documents("chunks", chunks)
    
    # Update chunks with their IDs
    for i, chunk_id in enumerate(chunk_ids):
        chunks[i]["_id"] = chunk_id
    
    # Generate embeddings for chunks
    chunk_texts = [chunk.get(text_field, "") for chunk in chunks]
    chunk_embeddings = embedding.generate_embeddings_batch(
        chunk_texts,
        model=model,
        input_type="document"
    )
    
    # Store valid chunk embeddings in Milvus
    valid_embeddings = []
    valid_ids = []
    valid_metadata = []
    
    for i, (chunk, chunk_emb) in enumerate(zip(chunks, chunk_embeddings)):
        if chunk_emb:
            valid_embeddings.append(chunk_emb)
            valid_ids.append(chunk["_id"])
            valid_metadata.append({
                "parent_id": doc_id,
                "chunk_index": chunk.get("chunk_index", i),
                "title": document.get("title", "")
            })
    
    if valid_embeddings:
        milvus.store_vectors(
            "chunks",
            valid_embeddings,
            valid_ids,
            valid_metadata
        )
    
    logger.info(f"Indexed document with ID {doc_id} and {len(valid_embeddings)} chunks")
    return doc_id


def index_documents(documents: List[Dict[str, Any]], text_field: str,
                  chunk_size: int = 1000, model: str = "voyage-3-large") -> List[str]:
    """Index multiple documents in MongoDB and Milvus.
    
    Args:
        documents: The documents to index
        text_field: The field containing the text to index
        chunk_size: The maximum chunk size in characters
        model: The embedding model to use
        
    Returns:
        The IDs of the indexed documents
    """
    doc_ids = []
    
    for doc in documents:
        doc_id = index_document(
            doc,
            text_field=text_field,
            chunk_size=chunk_size,
            model=model
        )
        
        if doc_id:
            doc_ids.append(doc_id)
    
    return doc_ids


def index_from_file(file_path: str, text_field: str, chunk_size: int = 1000, 
                  model: str = "voyage-3-large", id_field: Optional[str] = None) -> List[str]:
    """Index documents from a JSON file.
    
    Args:
        file_path: Path to the JSON file
        text_field: The field containing the text to index
        chunk_size: The maximum chunk size in characters
        model: The embedding model to use
        id_field: The field to use as the document ID
        
    Returns:
        The IDs of the indexed documents
    """
    try:
        # Load documents from the file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Handle different input formats
        documents = data
        if isinstance(data, dict):
            # If it's a dict with items, use values
            if "items" in data:
                documents = data["items"]
            # If it's a dict with no items field, maybe it's a dict of entities
            elif not any(k.startswith("_") for k in data.keys()):
                documents = list(data.values())
        
        # Process documents if ID field is specified
        if id_field:
            for doc in documents:
                if id_field in doc:
                    doc["_id"] = doc[id_field]
        
        # Index the documents
        if documents:
            return index_documents(
                documents,
                text_field=text_field,
                chunk_size=chunk_size,
                model=model
            )
        else:
            logger.warning(f"No documents found in {file_path}")
            return []
    except Exception as e:
        logger.error(f"Failed to index documents from file: {str(e)}")
        return []


def search(query: str, top_k: int = 10, model: str = "voyage-3-large") -> List[Dict[str, Any]]:
    """Search for documents matching a query.
    
    Args:
        query: The search query
        top_k: The number of results to return
        model: The embedding model to use
        
    Returns:
        The search results with document information
    """
    # Generate an embedding for the query
    query_embedding = embedding.generate_embedding(
        query,
        model=model,
        input_type="query"
    )
    
    if not query_embedding:
        logger.error("Failed to generate query embedding")
        return []
    
    # Search for similar chunks
    chunk_results = milvus.search_vectors("chunks", query_embedding, top_k=top_k)
    
    # Fetch the documents and chunks from MongoDB
    results = []
    
    for result in chunk_results:
        chunk_id = result.get("vector_id")
        
        # Get the chunk from MongoDB
        chunk = mongodb.get_document("chunks", chunk_id)
        
        if not chunk:
            continue
        
        # Get the parent document
        parent_id = chunk.get("parent_id")
        document = mongodb.get_document("documents", parent_id) if parent_id else None
        
        # Create a result entry
        result_entry = {
            "score": result.get("score", 0),
            "chunk": chunk,
            "document": document
        }
        
        results.append(result_entry)
    
    return results