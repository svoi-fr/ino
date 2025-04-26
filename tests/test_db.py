import sys
import os
import unittest
import logging
import time

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.vector_db import MilvusWrapper, create_embedding_storage
from app.mongo_db import MongoDBWrapper

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

class TestDatabases(unittest.TestCase):
    """
    Test database connections and basic operations
    """
    
    def test_milvus_connection(self):
        """Test Milvus connection and basic operations"""
        # Create collections
        result = create_embedding_storage(drop_existing=True)
        logger.info(result)
        
        # Initialize wrapper
        milvus = MilvusWrapper()
        
        # Test inserting sample data
        sample_embeddings = [
            [0.1, 0.2, 0.3] * 1365,  # Create 4095-dim vector
            [0.4, 0.5, 0.6] * 1365,
            [0.7, 0.8, 0.9] * 1365
        ]
        sample_ids = ["test_1", "test_2", "test_3"]
        sample_metadata = [
            {"text": "Sample text 1", "source": "test"},
            {"text": "Sample text 2", "source": "test"},
            {"text": "Sample text 3", "source": "test"}
        ]
        
        milvus_ids = milvus.insert_embeddings(
            "conversations", 
            sample_embeddings, 
            sample_ids, 
            sample_metadata
        )
        
        self.assertEqual(len(milvus_ids), 3, "Should insert 3 embeddings")
        
        # Wait for data to be fully processed
        time.sleep(3)
        
        # Test retrieval
        embeddings_dict, milvus_ids = milvus.get_all_embeddings("conversations")
        self.assertEqual(len(embeddings_dict), 3, "Should retrieve 3 embeddings")
        
        # Test search
        query_embedding = sample_embeddings[0]
        search_results = milvus.search_similar("conversations", query_embedding, top_k=2)
        self.assertEqual(len(search_results), 2, "Should return 2 search results")
        
        # Clean up
        milvus.close()
    
    def test_mongodb_connection(self):
        """Test MongoDB connection and basic operations"""
        try:
            # Initialize wrapper
            mongo_db = MongoDBWrapper()
            
            # Test inserting a document
            collection_name = "test_collection"
            document = {"name": "Test", "value": 123}
            doc_id = mongo_db.insert_document(collection_name, document)
            self.assertIsNotNone(doc_id, "Should insert a document")
            
            # Test finding a document
            query = {"name": "Test"}
            result = mongo_db.find_document(collection_name, query)
            self.assertIsNotNone(result, "Should find the document")
            self.assertEqual(result["value"], 123, "Should find the correct document")
            
            # Test updating a document
            update = {"$set": {"value": 456}}
            count = mongo_db.update_document(collection_name, query, update)
            self.assertEqual(count, 1, "Should update 1 document")
            
            # Test finding the updated document
            result = mongo_db.find_document(collection_name, query)
            self.assertEqual(result["value"], 456, "Should find the updated document")
            
            # Test deleting the document
            count = mongo_db.delete_document(collection_name, query)
            self.assertEqual(count, 1, "Should delete 1 document")
            
            # Clean up
            mongo_db.close()
        except Exception as e:
            logger.error(f"MongoDB test failed: {str(e)}")
            self.fail(f"MongoDB test failed: {str(e)}")

if __name__ == "__main__":
    unittest.main()