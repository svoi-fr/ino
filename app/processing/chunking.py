"""
Text chunking functions.

This module provides functions for splitting text into chunks for embedding and indexing.
"""

import re
from typing import List, Dict, Any, Optional, Tuple


def split_by_sentence(text: str) -> List[str]:
    """Split text into sentences.
    
    Args:
        text: The text to split
        
    Returns:
        List of sentences
    """
    # Simple sentence splitting - can be improved with a more sophisticated approach
    text = re.sub(r'([\\.\\?\\!])\\s+', r'\1\n', text)
    sentences = [s.strip() for s in text.split('\n') if s.strip()]
    return sentences


def merge_sentences(sentences: List[str], max_chunk_size: int = 1000, 
                  min_chunk_size: int = 50, overlap: int = 1) -> List[str]:
    """Merge sentences into chunks respecting max_chunk_size.
    
    Args:
        sentences: List of sentences to merge
        max_chunk_size: Maximum character length of a chunk
        min_chunk_size: Minimum character length of a chunk
        overlap: Number of sentences to overlap between chunks
        
    Returns:
        List of text chunks
    """
    chunks = []
    current_chunk = []
    current_length = 0
    
    for sentence in sentences:
        # If adding this sentence would exceed the max chunk size and we have at least one sentence
        if current_length + len(sentence) > max_chunk_size and current_chunk:
            # Add the current chunk to the list of chunks
            chunks.append(' '.join(current_chunk))
            
            # Start a new chunk with overlap
            if overlap > 0 and len(current_chunk) >= overlap:
                current_chunk = current_chunk[-overlap:]
                current_length = sum(len(s) for s in current_chunk) + len(current_chunk) - 1
            else:
                current_chunk = []
                current_length = 0
        
        # Add the sentence to the current chunk
        current_chunk.append(sentence)
        current_length += len(sentence) + 1  # +1 for the space
    
    # Add the last chunk if it's not empty and meets minimum size
    if current_chunk and current_length >= min_chunk_size:
        chunks.append(' '.join(current_chunk))
    
    return chunks


def split_text(text: str, max_chunk_size: int = 1000, min_chunk_size: int = 50, 
             sentence_overlap: int = 1) -> List[str]:
    """Split text into chunks.
    
    Args:
        text: The text to split
        max_chunk_size: Maximum character length of a chunk
        min_chunk_size: Minimum character length of a chunk
        sentence_overlap: Number of sentences to overlap between chunks
        
    Returns:
        List of text chunks
    """
    # Handle empty or very short text
    if not text or len(text) < min_chunk_size:
        return [text] if text else []
    
    # Split into sentences
    sentences = split_by_sentence(text)
    
    # Handle long sentences that exceed max_chunk_size
    processed_sentences = []
    for sentence in sentences:
        if len(sentence) > max_chunk_size:
            # Split the sentence by character count with some overlap
            overlap_chars = max(50, int(max_chunk_size * 0.1))
            for i in range(0, len(sentence), max_chunk_size - overlap_chars):
                chunk = sentence[i:i + max_chunk_size]
                processed_sentences.append(chunk)
        else:
            processed_sentences.append(sentence)
    
    # Merge sentences into chunks
    return merge_sentences(
        processed_sentences, 
        max_chunk_size=max_chunk_size, 
        min_chunk_size=min_chunk_size, 
        overlap=sentence_overlap
    )


def chunk_document(doc: Dict[str, Any], text_field: str, max_chunk_size: int = 1000, 
                 min_chunk_size: int = 50, sentence_overlap: int = 1) -> List[Dict[str, Any]]:
    """Split a document into chunks.
    
    Args:
        doc: The document to split
        text_field: The field containing the text to split
        max_chunk_size: Maximum character length of a chunk
        min_chunk_size: Minimum character length of a chunk
        sentence_overlap: Number of sentences to overlap between chunks
        
    Returns:
        List of document chunks
    """
    # Get the text to chunk
    text = doc.get(text_field, '')
    if not text:
        return [doc]  # Return the original document if no text
    
    # Split the text into chunks
    chunks = split_text(
        text, 
        max_chunk_size=max_chunk_size, 
        min_chunk_size=min_chunk_size, 
        sentence_overlap=sentence_overlap
    )
    
    # Create a new document for each chunk
    chunked_docs = []
    for i, chunk in enumerate(chunks):
        # Create a copy of the original document
        chunked_doc = doc.copy()
        
        # Update the text field with the chunk
        chunked_doc[text_field] = chunk
        
        # Add chunk metadata
        chunked_doc['chunk_index'] = i
        chunked_doc['chunk_count'] = len(chunks)
        chunked_doc['parent_id'] = doc.get('_id', '')
        
        # Generate a new ID for the chunk
        chunked_doc['_id'] = f"{doc.get('_id', '')}_{i}"
        
        chunked_docs.append(chunked_doc)
    
    return chunked_docs


def chunk_documents(docs: List[Dict[str, Any]], text_field: str, max_chunk_size: int = 1000, 
                  min_chunk_size: int = 50, sentence_overlap: int = 1) -> List[Dict[str, Any]]:
    """Split multiple documents into chunks.
    
    Args:
        docs: The documents to split
        text_field: The field containing the text to split
        max_chunk_size: Maximum character length of a chunk
        min_chunk_size: Minimum character length of a chunk
        sentence_overlap: Number of sentences to overlap between chunks
        
    Returns:
        List of document chunks
    """
    all_chunks = []
    
    for doc in docs:
        chunks = chunk_document(
            doc, 
            text_field=text_field, 
            max_chunk_size=max_chunk_size, 
            min_chunk_size=min_chunk_size, 
            sentence_overlap=sentence_overlap
        )
        
        all_chunks.extend(chunks)
    
    return all_chunks