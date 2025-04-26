"""
Storage interfaces defining the contract for different storage backends.

This module contains protocol classes (interfaces) for document storage and
vector storage, allowing for different implementations to be plugged in.
"""

from typing import Dict, List, Any, Optional, Protocol, Tuple, Union, runtime_checkable


@runtime_checkable
class DocumentStore(Protocol):
    """Protocol for document storage operations."""
    
    def connect(self) -> bool:
        """Connect to the document store.
        
        Returns:
            bool: Whether the connection was successful
        """
        ...
    
    def disconnect(self) -> None:
        """Disconnect from the document store."""
        ...
    
    def store_document(self, collection: str, document: Dict[str, Any]) -> str:
        """Store a document in the specified collection.
        
        Args:
            collection: The collection to store the document in
            document: The document to store
            
        Returns:
            str: The ID of the stored document
        """
        ...
    
    def store_documents(self, collection: str, documents: List[Dict[str, Any]]) -> List[str]:
        """Store multiple documents in the specified collection.
        
        Args:
            collection: The collection to store the documents in
            documents: The documents to store
            
        Returns:
            List[str]: The IDs of the stored documents
        """
        ...
    
    def get_document(self, collection: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """Get a document by its ID.
        
        Args:
            collection: The collection to get the document from
            doc_id: The ID of the document
            
        Returns:
            Optional[Dict[str, Any]]: The document, or None if not found
        """
        ...
    
    def query_documents(self, collection: str, query: Dict[str, Any], 
                        limit: int = 0, offset: int = 0) -> List[Dict[str, Any]]:
        """Query documents based on criteria.
        
        Args:
            collection: The collection to query
            query: The query criteria
            limit: Maximum number of documents to return (0 for all)
            offset: Number of documents to skip
            
        Returns:
            List[Dict[str, Any]]: The matching documents
        """
        ...
    
    def update_document(self, collection: str, doc_id: str, 
                        updates: Dict[str, Any]) -> bool:
        """Update a document.
        
        Args:
            collection: The collection containing the document
            doc_id: The ID of the document to update
            updates: The fields to update
            
        Returns:
            bool: Whether the update was successful
        """
        ...
    
    def delete_document(self, collection: str, doc_id: str) -> bool:
        """Delete a document.
        
        Args:
            collection: The collection containing the document
            doc_id: The ID of the document to delete
            
        Returns:
            bool: Whether the deletion was successful
        """
        ...
    
    def count_documents(self, collection: str, query: Optional[Dict[str, Any]] = None) -> int:
        """Count documents matching a query.
        
        Args:
            collection: The collection to count documents in
            query: The query criteria (None for all documents)
            
        Returns:
            int: The number of matching documents
        """
        ...


@runtime_checkable
class VectorStore(Protocol):
    """Protocol for vector storage and retrieval operations."""
    
    def connect(self) -> bool:
        """Connect to the vector store.
        
        Returns:
            bool: Whether the connection was successful
        """
        ...
    
    def disconnect(self) -> None:
        """Disconnect from the vector store."""
        ...
    
    def create_collection(self, name: str, dimension: int, recreate: bool = False) -> bool:
        """Create a collection for storing vectors.
        
        Args:
            name: The name of the collection
            dimension: The dimensionality of the vectors
            recreate: Whether to recreate the collection if it exists
            
        Returns:
            bool: Whether the operation was successful
        """
        ...
    
    def store_vectors(self, collection: str, vectors: List[List[float]], 
                      ids: List[str], metadata: List[Dict[str, Any]]) -> List[str]:
        """Store vectors in a collection.
        
        Args:
            collection: The collection to store the vectors in
            vectors: The vectors to store
            ids: The IDs to associate with the vectors
            metadata: The metadata to associate with the vectors
            
        Returns:
            List[str]: The IDs of the stored vectors
        """
        ...
    
    def get_vector(self, collection: str, vector_id: str) -> Optional[List[float]]:
        """Get a vector by its ID.
        
        Args:
            collection: The collection to get the vector from
            vector_id: The ID of the vector
            
        Returns:
            Optional[List[float]]: The vector, or None if not found
        """
        ...
    
    def search_vectors(self, collection: str, query_vector: List[float], 
                      top_k: int = 10) -> List[Dict[str, Any]]:
        """Search for similar vectors.
        
        Args:
            collection: The collection to search in
            query_vector: The query vector
            top_k: The number of results to return
            
        Returns:
            List[Dict[str, Any]]: The search results, including IDs, scores, and metadata
        """
        ...
    
    def delete_vector(self, collection: str, vector_id: str) -> bool:
        """Delete a vector.
        
        Args:
            collection: The collection containing the vector
            vector_id: The ID of the vector to delete
            
        Returns:
            bool: Whether the deletion was successful
        """
        ...
    
    def collection_exists(self, name: str) -> bool:
        """Check if a collection exists.
        
        Args:
            name: The name of the collection
            
        Returns:
            bool: Whether the collection exists
        """
        ...
    
    def collection_stats(self, name: str) -> Dict[str, Any]:
        """Get statistics about a collection.
        
        Args:
            name: The name of the collection
            
        Returns:
            Dict[str, Any]: Statistics about the collection
        """
        ...