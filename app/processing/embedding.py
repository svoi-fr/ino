"""
Embedding functions for text.

This module provides functions for generating embeddings from text.
"""

import os
import time
import logging
import numpy as np
from typing import List, Dict, Any, Optional, Union, Callable

import voyageai

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize VoyageAI client (module level)
_voyage_client = None


def get_voyage_client():
    """Get or initialize the VoyageAI client.
    
    Returns:
        VoyageAI client instance
    """
    global _voyage_client
    
    if _voyage_client is None:
        _voyage_client = voyageai.Client()  # Will automatically use VOYAGEAI_API_KEY from env
        
    return _voyage_client


def get_embedding_dimension(model: str = "voyage-3-large") -> int:
    """Get the dimension of embeddings for a given model.
    
    Args:
        model: The model to use
        
    Returns:
        The dimension of the model's embeddings
    """
    # Map models to their dimensions
    dimensions = {
        "voyage-3-large": 4096,
        "voyage-3-mini": 1024,
        "voyage-2": 1024,
        "voyage-large-2": 1536,
        "voyage-code-2": 1536
    }
    
    return dimensions.get(model, 4096)


def generate_embedding(text: str, model: str = "voyage-3-large", 
                      input_type: str = "query") -> Optional[List[float]]:
    """Generate an embedding for a single text.
    
    Args:
        text: The text to embed
        model: The model to use
        input_type: The type of input text ('query' or 'document')
        
    Returns:
        The embedding vector or None if failed
    """
    if not text:
        logger.warning("Empty text provided for embedding generation")
        return None
    
    try:
        client = get_voyage_client()
        embedding_result = client.embed([text], model=model, input_type=input_type)
        
        if embedding_result and embedding_result.embeddings:
            return embedding_result.embeddings[0]
        
        return None
    except Exception as e:
        logger.error(f"Failed to generate embedding: {str(e)}")
        return None


def generate_embeddings_batch(texts: List[str], model: str = "voyage-3-large", 
                           input_type: str = "document", batch_size: int = 10) -> List[Optional[List[float]]]:
    """Generate embeddings for a batch of texts.
    
    Args:
        texts: The texts to embed
        model: The model to use
        input_type: The type of input text ('query' or 'document')
        batch_size: The number of texts to process in each API call
        
    Returns:
        List of embedding vectors (None for failed embeddings)
    """
    if not texts:
        logger.warning("Empty text list provided for embedding generation")
        return []
    
    all_embeddings = []
    client = get_voyage_client()
    
    # Process in batches to avoid rate limits
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i+batch_size]
        
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(texts) + batch_size - 1)//batch_size}")
        
        # Count tokens for the batch (optional, for monitoring)
        try:
            total_tokens = client.count_tokens(batch, model=model)
            logger.info(f"Batch token count: {total_tokens}")
        except Exception as e:
            logger.warning(f"Failed to count tokens: {str(e)}")
        
        # Get embeddings for the batch
        start_time = time.time()
        try:
            embedding_result = client.embed(batch, model=model, input_type=input_type)
            
            # The embeddings are returned as an EmbeddingsObject with embeddings field
            embedding_list = embedding_result.embeddings
            logger.info(f"Embeddings generated successfully in {time.time() - start_time:.2f} seconds")
            
            all_embeddings.extend(embedding_list)
            
        except Exception as e:
            logger.error(f"Failed to generate embeddings for batch: {str(e)}")
            # Add None placeholders for failed embeddings
            all_embeddings.extend([None] * len(batch))
        
        # Small delay between batches to avoid rate limits
        if i + batch_size < len(texts):
            logger.info("Short pause between batches...")
            time.sleep(1)
    
    return all_embeddings


def embed_documents(documents: List[Dict[str, Any]], text_field: str,
                  model: str = "voyage-3-large", batch_size: int = 10) -> List[Dict[str, Any]]:
    """Embed documents and add embeddings to them.
    
    Args:
        documents: The documents to embed
        text_field: The field containing the text to embed
        model: The model to use
        batch_size: The number of documents to process in each API call
        
    Returns:
        Documents with added 'embedding' field
    """
    # Extract texts to embed
    texts = []
    valid_docs = []
    
    for doc in documents:
        if text_field in doc and doc[text_field]:
            texts.append(doc[text_field])
            valid_docs.append(doc)
        else:
            logger.warning(f"Document missing text field: {text_field}")
    
    # Generate embeddings
    embeddings = generate_embeddings_batch(
        texts, 
        model=model, 
        input_type="document", 
        batch_size=batch_size
    )
    
    # Add embeddings to documents
    for i, (doc, embedding) in enumerate(zip(valid_docs, embeddings)):
        if embedding is not None:
            doc["embedding"] = embedding
            doc["embedding_model"] = model
            doc["embedding_id"] = f"{doc.get('_id', '')}_{i}"
    
    return valid_docs


def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
    """Calculate cosine similarity between two vectors.
    
    Args:
        vec1: First vector
        vec2: Second vector
        
    Returns:
        Cosine similarity (between -1 and 1)
    """
    if not vec1 or not vec2:
        return 0.0
        
    # Convert to numpy arrays if they aren't already
    if not isinstance(vec1, np.ndarray):
        vec1 = np.array(vec1)
    if not isinstance(vec2, np.ndarray):
        vec2 = np.array(vec2)
        
    # Calculate cosine similarity
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
        
    return np.dot(vec1, vec2) / (norm1 * norm2)