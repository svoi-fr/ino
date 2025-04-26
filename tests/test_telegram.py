"""
Test module for Telegram fetcher.

Note: Due to the nature of Telegram API, these tests might require authentication
and actual connection to Telegram services. Some tests might be skipped in CI environments.
"""

import sys
import os
import unittest
import asyncio
import json
from datetime import timedelta
from unittest.mock import patch, MagicMock

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.sources.telegram import TelegramFetcher

# Sample data for testing
SAMPLE_MESSAGES = [
    {
        'id': 1,
        'date': '2023-01-01T12:00:00+00:00',
        'sender_id': 100,
        'text': 'Hello everyone!',
        'reply_to_msg_id': None,
        'original_reply_to': None,
        'inferred_reply_type': None
    },
    {
        'id': 2,
        'date': '2023-01-01T12:01:00+00:00',
        'sender_id': 100,
        'text': 'This is a follow-up message.',
        'reply_to_msg_id': None,
        'original_reply_to': None,
        'inferred_reply_type': None
    },
    {
        'id': 3,
        'date': '2023-01-01T12:02:00+00:00',
        'sender_id': 200,
        'text': 'Hi! Nice to meet you.',
        'reply_to_msg_id': 1,
        'original_reply_to': 1,
        'inferred_reply_type': None
    },
    {
        'id': 4,
        'date': '2023-01-01T12:03:00+00:00',
        'sender_id': 100,
        'text': 'Nice to meet you too!',
        'reply_to_msg_id': None,
        'original_reply_to': None,
        'inferred_reply_type': None
    },
    {
        'id': 5,
        'date': '2023-01-01T12:10:00+00:00',
        'sender_id': 300,
        'text': 'Can I join the conversation?',
        'reply_to_msg_id': None,
        'original_reply_to': None,
        'inferred_reply_type': None
    }
]

SAMPLE_GROUP_INFO = {
    'group_name': 'Test Group',
    'group_id': 12345678,
    'export_date': '2023-01-01T13:00:00+00:00',
    'fetch_start_id': None,
    'fetch_message_limit': 100,
    'fetch_reverse': False,
    'total_messages_fetched': 5
}


class TestTelegramFetcher(unittest.TestCase):
    """Test the TelegramFetcher class."""
    
    def setUp(self):
        """Set up test environment."""
        self.output_dir = os.path.join(os.path.dirname(__file__), 'test_output')
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create fetcher with mock credentials
        self.fetcher = TelegramFetcher(
            api_id='123456',
            api_hash='abcdef1234567890abcdef',
            phone_number='+1234567890',
            output_dir=self.output_dir
        )
    
    def tearDown(self):
        """Clean up test environment."""
        # Remove test output directory if needed
        # Commented out to prevent accidental deletion
        # if os.path.exists(self.output_dir):
        #     import shutil
        #     shutil.rmtree(self.output_dir)
        pass
    
    def test_initialization(self):
        """Test proper initialization of the fetcher."""
        self.assertEqual(self.fetcher.api_id, '123456')
        self.assertEqual(self.fetcher.api_hash, 'abcdef1234567890abcdef')
        self.assertEqual(self.fetcher.phone_number, '+1234567890')
        self.assertTrue(self.fetcher.output_dir.endswith('test_output'))
        self.assertIsNone(self.fetcher.client)
    
    @patch('app.sources.telegram.TelegramClient')
    def test_connect_disconnect(self, mock_client_class):
        """Test connection and disconnection."""
        # Set up mock
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.start.return_value = asyncio.Future()
        mock_client.start.return_value.set_result(True)
        mock_client.is_connected.return_value = True
        
        # Run test
        async def test():
            # Test connect
            result = await self.fetcher.connect()
            self.assertTrue(result)
            self.assertIsNotNone(self.fetcher.client)
            mock_client.start.assert_called_once()
            
            # Test disconnect
            await self.fetcher.disconnect()
            mock_client.disconnect.assert_called_once()
        
        # Run the async test
        asyncio.run(test())
    
    def test_process_messages_to_chunks(self):
        """Test processing messages into conversation chunks."""
        # Run test
        results = self.fetcher.process_messages_to_chunks(
            messages=SAMPLE_MESSAGES,
            group_info=SAMPLE_GROUP_INFO,
            time_threshold=timedelta(minutes=5),
            id_threshold=5,
            min_participants=2
        )
        
        # Verify results
        self.assertIn('conversation_chunks', results)
        self.assertIn('processing_stats', results)
        
        chunks = results['conversation_chunks']
        stats = results['processing_stats']
        
        # Check we have the correct number of chunks
        self.assertEqual(stats['total_messages_fetched'], 5)
        self.assertTrue(len(chunks) > 0, "Should have at least one conversation chunk")
        
        # Check first chunk has correct format
        first_chunk = chunks[0]
        self.assertIn('id', first_chunk)
        self.assertIn('content', first_chunk)
        self.assertIn('source', first_chunk)
        self.assertEqual(first_chunk['source'], 'telegram')
        self.assertEqual(first_chunk['group_id'], SAMPLE_GROUP_INFO['group_id'])
        
        # Verify content format with user prefixes
        content = first_chunk['content']
        self.assertTrue('user0:' in content or 'user1:' in content)


# Skip this test in CI environments
@unittest.skipIf(os.environ.get('CI') == 'true', "Skip in CI environment")
class TestTelegramAPI(unittest.TestCase):
    """
    Integration tests that use actual Telegram API.
    
    Note: These tests require proper API credentials in the environment variables:
    - TELEGRAM_API_ID
    - TELEGRAM_API_HASH
    - TELEGRAM_PHONE_NUMBER
    
    Additionally, the Telegram account must be able to access the test group.
    """
    
    def setUp(self):
        """Set up test environment."""
        # Check if credentials are available
        if not all([
            os.environ.get('TELEGRAM_API_ID'),
            os.environ.get('TELEGRAM_API_HASH'),
            os.environ.get('TELEGRAM_PHONE_NUMBER')
        ]):
            self.skipTest("Missing Telegram API credentials in environment variables")
        
        self.output_dir = os.path.join(os.path.dirname(__file__), 'test_output')
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Use real credentials from environment
        self.fetcher = TelegramFetcher(output_dir=self.output_dir)
        
        # Test group username - make sure this group exists and is accessible
        self.test_group = os.environ.get('TELEGRAM_TEST_GROUP', 'pythontelegrambottalk')
    
    def tearDown(self):
        """Clean up test environment."""
        pass
    
    def test_fetch_messages(self):
        """Test fetching messages from a Telegram group."""
        async def test():
            try:
                # Connect to Telegram
                connected = await self.fetcher.connect()
                self.assertTrue(connected)
                
                # Fetch a small number of messages
                messages, group_info, success = await self.fetcher.fetch_messages(
                    group_username=self.test_group,
                    message_limit=5  # Small limit for testing
                )
                
                # Verify results
                self.assertTrue(success)
                self.assertGreater(len(messages), 0)
                self.assertIn('group_name', group_info)
                self.assertIn('group_id', group_info)
                
                print(f"Successfully fetched {len(messages)} messages from {group_info['group_name']}")
                
            finally:
                # Disconnect
                await self.fetcher.disconnect()
        
        # Only run if not in CI
        if os.environ.get('CI') != 'true':
            asyncio.run(test())


if __name__ == '__main__':
    unittest.main()