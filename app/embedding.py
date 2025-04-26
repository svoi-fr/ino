import os
import time
import logging
import numpy as np
from typing import List, Dict, Union, Optional, Any, Tuple
import voyageai

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

class EmbeddingService:
    """
    Service for generating and managing embeddings
    """
    
    def __init__(self, model="voyage-3-large", batch_size=10):
        """
        Initialize the embedding service
        
        Args:
            model: Embedding model to use
            batch_size: Number of items to process in each batch
        """
        self.model = model
        self.batch_size = batch_size
        self.client = voyageai.Client()  # Will automatically use VOYAGEAI_API_KEY from env
        logger.info(f"Initialized embedding service with model: {model}")
        
    def get_embedding_dimension(self) -> int:
        """
        Get the dimension of embeddings for the current model
        
        Returns:
            Dimension of embeddings
        """
        # Map models to their dimensions
        dimensions = {
            "voyage-3-large": 4096,
            "voyage-3-mini": 1024,
            "voyage-2": 1024,
            "voyage-large-2": 1536,
            "voyage-code-2": 1536
        }
        
        return dimensions.get(self.model, 4096)
    
    def generate_embeddings(self, texts: List[str], input_type: str = "query") -> List[List[float]]:
        """
        Generate embeddings for a list of texts
        
        Args:
            texts: List of texts to embed
            input_type: Type of input text ('query' or 'document')
            
        Returns:
            List of embeddings
        """
        if not texts:
            logger.warning("Empty text list provided for embedding generation")
            return []
            
        all_embeddings = []
        
        # Process in batches to avoid rate limits
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i:i+self.batch_size]
            
            logger.info(f"Processing batch {i//self.batch_size + 1}/{(len(texts) + self.batch_size - 1)//self.batch_size}")
            
            # Count tokens for the batch (optional, for monitoring)
            try:
                total_tokens = self.client.count_tokens(batch, model=self.model)
                logger.info(f"Batch token count: {total_tokens}")
            except Exception as e:
                logger.warning(f"Failed to count tokens: {str(e)}")
            
            # Get embeddings for the batch
            start_time = time.time()
            try:
                embeddings_result = self.client.embed(batch, model=self.model, input_type=input_type)
                # The embeddings are returned as an EmbeddingsObject with embedded field containing the list
                embedding_list = embeddings_result.embeddings
                logger.info(f"Embeddings generated successfully in {time.time() - start_time:.2f} seconds")
                
                all_embeddings.extend(embedding_list)
                
            except Exception as e:
                logger.error(f"Failed to generate embeddings: {str(e)}")
                # Add None placeholders for failed embeddings
                all_embeddings.extend([None] * len(batch))
            
            # Small delay between batches to avoid rate limits
            if i + self.batch_size < len(texts):
                logger.info("Short pause between batches...")
                time.sleep(1)
        
        return all_embeddings
    
    def embed_documents(self, documents: List[Dict[str, Any]], text_field: str) -> Tuple[List[Dict[str, Any]], List[List[float]]]:
        """
        Generate embeddings for a list of documents
        
        Args:
            documents: List of documents to embed
            text_field: Field in the documents containing the text to embed
            
        Returns:
            Tuple of (documents with added embedding IDs, list of embeddings)
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
        embeddings = self.generate_embeddings(texts, input_type="document")
        
        # Filter out None embeddings
        valid_embeddings = []
        result_docs = []
        
        for i, embedding in enumerate(embeddings):
            if embedding is not None:
                valid_embeddings.append(embedding)
                doc = valid_docs[i].copy()
                doc['embedding_id'] = f"emb_{i}"
                result_docs.append(doc)
        
        return result_docs, valid_embeddings