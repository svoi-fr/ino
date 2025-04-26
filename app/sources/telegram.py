"""
Telegram data source adapter for fetching and processing conversations.

This module provides functions to fetch conversations from Telegram groups,
process them into meaningful chunks, and prepare them for indexing.
"""

import os
import json
import asyncio
import logging
import copy
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
from functools import lru_cache
from dotenv import load_dotenv

# Telethon for Telegram API access
from telethon import TelegramClient, utils

# Configure logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Default parameters
DEFAULT_OUTPUT_DIR = 'data/telegram'
DEFAULT_TIME_THRESHOLD = timedelta(minutes=5)
DEFAULT_ID_THRESHOLD = 5
DEFAULT_MIN_PARTICIPANTS = 2
DEFAULT_MESSAGE_LIMIT = 10000
DEFAULT_SESSION_PREFIX = 'telegram_exporter_session'

class TelegramFetcher:
    """Fetch and process conversations from Telegram groups."""
    
    def __init__(self, 
                 api_id: Optional[str] = None, 
                 api_hash: Optional[str] = None, 
                 phone_number: Optional[str] = None,
                 session_name: Optional[str] = None,
                 output_dir: str = DEFAULT_OUTPUT_DIR):
        """
        Initialize the Telegram fetcher.
        
        Args:
            api_id: Telegram API ID (from environment if None)
            api_hash: Telegram API Hash (from environment if None)
            phone_number: Phone number for Telegram account (from environment if None)
            session_name: Name for session file (defaults to telegram_exporter_session)
            output_dir: Directory to save output files
        """
        # API credentials from params or environment
        self.api_id = api_id or os.getenv('TELEGRAM_API_ID')
        self.api_hash = api_hash or os.getenv('TELEGRAM_API_HASH')
        self.phone_number = phone_number or os.getenv('TELEGRAM_PHONE_NUMBER')
        
        # Validate required credentials
        if not self.api_id or not self.api_hash:
            raise ValueError("API ID and API Hash are required. Set them as parameters or environment variables.")
        
        # Session name and output directory
        self.session_name = session_name or f"{DEFAULT_SESSION_PREFIX}_{datetime.now().strftime('%Y%m%d')}"
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize client (not connected yet)
        self.client = None
        
    async def connect(self):
        """Connect to Telegram API."""
        logger.info("Initializing Telegram client...")
        self.client = TelegramClient(self.session_name, self.api_id, self.api_hash)
        
        try:
            logger.info("Connecting to Telegram...")
            await self.client.start(phone=self.phone_number)
            logger.info("Client connected successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Telegram: {e}")
            return False
            
    async def disconnect(self):
        """Disconnect from Telegram API."""
        if self.client and self.client.is_connected():
            logger.info("Disconnecting client...")
            await self.client.disconnect()
            logger.info("Client disconnected.")
    
    @staticmethod        
    def _safe_get_sender_id(message):
        """Safely get sender ID, handling potential None sender."""
        if message and message.sender_id:
            return message.sender_id
        # Handle cases like deleted accounts or channel posts if needed
        return None
        
    async def fetch_messages(self, 
                          group_username: str, 
                          start_id: Optional[int] = None, 
                          message_limit: int = DEFAULT_MESSAGE_LIMIT,
                          fetch_reverse: bool = False) -> Tuple[List[Dict], Dict, bool]:
        """
        Fetch messages from a Telegram group.
        
        Args:
            group_username: Username or ID of the group
            start_id: Message ID to start from (None for latest)
            message_limit: Maximum number of messages to fetch
            fetch_reverse: Whether to fetch in reverse order (oldest first)
            
        Returns:
            Tuple of (messages list, group info dict, success flag)
        """
        if not self.client or not self.client.is_connected():
            logger.error("Client not connected. Call connect() first.")
            return [], {}, False
            
        logger.info(f"Starting export for group: {group_username}")
        
        try:
            entity = await self.client.get_entity(group_username)
            logger.info(f"Successfully found entity: {utils.get_display_name(entity)} (ID: {entity.id})")
        except ValueError:
            logger.error(f"Could not find the group/channel '{group_username}'. Please check the username/ID.")
            return [], {}, False
        except Exception as e:
            logger.error(f"Error getting entity '{group_username}': {e}")
            return [], {}, False
            
        # Fetch messages
        all_messages_raw = []
        downloaded_count = 0
        processed_ids_fetch = set()  # Track IDs to avoid duplicates if iteration overlaps somehow
        
        logger.info(f"Fetching up to {message_limit or 'all'} messages...")
        try:
            # Determine fetching parameters based on START_ID
            if start_id is not None:
                # Fetch messages *after* START_ID (newer messages)
                messages_iterator = self.client.iter_messages(
                    entity,
                    limit=message_limit,
                    min_id=start_id,  # Fetch messages with ID > START_ID
                    reverse=True     # Start from min_id and go upwards (older to newer)
                )
                logger.info(f"Fetching messages with ID greater than {start_id}.")
            else:
                # Fetch latest messages or from the beginning
                messages_iterator = self.client.iter_messages(
                    entity,
                    limit=message_limit,
                    reverse=fetch_reverse # True=oldest first, False=newest first
                )
                logger.info(f"Fetching latest {message_limit} messages (or from beginning if reverse=True).")
            
            async for message in messages_iterator:
                # Basic filtering: Skip messages without text or identifiable sender
                sender_id = self._safe_get_sender_id(message)
                if not message.text or not sender_id or message.id in processed_ids_fetch:
                    continue
                    
                message_dict = {
                    'id': message.id,
                    'date': message.date.isoformat(),
                    'sender_id': sender_id,
                    'text': message.text,
                    'reply_to_msg_id': message.reply_to_msg_id,
                    'original_reply_to': message.reply_to_msg_id, # Store original for reference
                    'inferred_reply_type': None # To track how link was formed
                }
                all_messages_raw.append(message_dict)
                processed_ids_fetch.add(message.id)
                downloaded_count += 1
                if downloaded_count % 100 == 0:
                    logger.info(f"Fetched {downloaded_count} messages...")
                    
            logger.info(f"Finished fetching. Total messages retrieved: {downloaded_count}")
            
            if not all_messages_raw:
                logger.warning("No processable messages found with the given criteria.")
                return [], {}, False
                
            # Sort messages by ID (ascending - oldest first) for processing
            all_messages_raw.sort(key=lambda m: m['id'])
            
        except Exception as e:
            logger.exception(f"An error occurred during message fetching: {e}")
            return [], {}, False
            
        # Create group info
        group_info = {
            'group_name': utils.get_display_name(entity),
            'group_id': entity.id,
            'export_date': datetime.now().isoformat(),
            'fetch_start_id': start_id,
            'fetch_message_limit': message_limit,
            'fetch_reverse': fetch_reverse if start_id is None else True,
            'total_messages_fetched': len(all_messages_raw)
        }
            
        return all_messages_raw, group_info, True
        
    def process_messages_to_chunks(self, 
                               messages: List[Dict],
                               group_info: Dict,
                               time_threshold: timedelta = DEFAULT_TIME_THRESHOLD,
                               id_threshold: int = DEFAULT_ID_THRESHOLD,
                               min_participants: int = DEFAULT_MIN_PARTICIPANTS) -> Dict:
        """
        Process raw messages into conversation chunks.
        
        Args:
            messages: List of raw message dictionaries
            group_info: Dictionary with group information
            time_threshold: Maximum time difference for proximity inference
            id_threshold: Maximum message ID difference for proximity inference
            min_participants: Minimum number of participants per chunk
            
        Returns:
            Dictionary with processing statistics and conversation chunks
        """
        if not messages:
            logger.warning("No messages to process.")
            return {
                'conversation_chunks': [],
                'processing_stats': {
                    'total_messages_fetched': 0,
                    'final_chunks_exported': 0
                }
            }
            
        # --- Create a working copy for modifications ---
        messages_processed = copy.deepcopy(messages)
        message_by_id = {msg['id']: msg for msg in messages_processed}
        available_ids = set(message_by_id.keys())  # Set of IDs present in our fetched batch
        
        # --- [Step 3: Initial Same-User Chaining] ---
        logger.info("Applying initial same-user chaining...")
        same_user_chained_count = 0
        for i in range(len(messages_processed) - 1):
            msg_a = messages_processed[i]
            msg_b = messages_processed[i+1]
            
            # Condition: Same sender AND msg_b has NO original reply_to
            if msg_a['sender_id'] == msg_b['sender_id'] and msg_b['original_reply_to'] is None:
                # Check if msg_b already has an inferred link (shouldn't happen here yet, but safe check)
                if msg_b['reply_to_msg_id'] is None:
                    # Check if the target message (msg_a) exists in our fetched set
                    if msg_a['id'] in available_ids:
                        msg_b['reply_to_msg_id'] = msg_a['id']
                        msg_b['inferred_reply_type'] = 'same_user_consecutive'
                        same_user_chained_count += 1
        
        logger.info(f"Applied same-user chaining to {same_user_chained_count} messages.")
        
        # --- [Step 4: A-B-A Inference] ---
        logger.info("Applying A-B-A inference...")
        aba_inferred_count = 0
        for i in range(len(messages_processed) - 2):
            m1 = messages_processed[i]
            m2 = messages_processed[i+1]
            m3 = messages_processed[i+2]
            
            # Conditions for A-B-A:
            # 1. M1 sender == M3 sender
            # 2. M1 sender != M2 sender
            # 3. M2 explicitly replies to M1 (using original_reply_to for robustness)
            # 4. M3 currently has NO reply_to (neither original nor inferred yet)
            if (m1['sender_id'] == m3['sender_id'] and
                m1['sender_id'] != m2['sender_id'] and
                m2['original_reply_to'] == m1['id'] and  # Check if M2 was *originally* a reply to M1
                m3['reply_to_msg_id'] is None):          # Check if M3 doesn't have a link yet
                
                # Check if the target message (m2) exists in our fetched set
                if m2['id'] in available_ids:
                    m3['reply_to_msg_id'] = m2['id']
                    m3['inferred_reply_type'] = 'aba'
                    aba_inferred_count += 1
        
        logger.info(f"Applied A-B-A inference to {aba_inferred_count} messages.")
        
        # --- [Step 5: Time/Proximity Inference] ---
        logger.info("Applying time/proximity inference...")
        proximity_inferred_count = 0
        for i in range(len(messages_processed) - 1):
            msg_prev = messages_processed[i]
            msg_curr = messages_processed[i+1]
            
            # Conditions for Time/Proximity:
            # 1. Different senders
            # 2. Current message has NO reply_to link yet
            # 3. Time difference is within threshold
            # 4. Message ID difference is within threshold
            if (msg_prev['sender_id'] != msg_curr['sender_id'] and
                msg_curr['reply_to_msg_id'] is None):
                
                try:
                    time_diff = datetime.fromisoformat(msg_curr['date']) - datetime.fromisoformat(msg_prev['date'])
                    is_close_in_time = timedelta(seconds=0) < time_diff <= time_threshold
                except ValueError:
                    is_close_in_time = False
                    
                is_close_in_id = 0 < msg_curr['id'] - msg_prev['id'] <= id_threshold
                
                if is_close_in_time and is_close_in_id:
                    if msg_prev['id'] in available_ids:
                        msg_curr['reply_to_msg_id'] = msg_prev['id']
                        msg_curr['inferred_reply_type'] = 'time_proximity'
                        proximity_inferred_count += 1
        
        logger.info(f"Applied time/proximity inference to {proximity_inferred_count} messages.")
        
        # --- [Step 6: Build Threads] ---
        logger.info("Building thread structures...")
        
        # Build the final reply map based on potentially modified reply_to_msg_id
        replies_to_map = defaultdict(list)
        for msg_id, msg_data in message_by_id.items():
            target_id = msg_data.get('reply_to_msg_id')
            if target_id and target_id in available_ids:
                replies_to_map[target_id].append(msg_id)
        
        # Identify root messages with heuristic validation
        root_message_ids = []
        all_replying_ids = set()
        for replying_ids in replies_to_map.values():
            all_replying_ids.update(replying_ids)
        
        for msg_id in message_by_id:
            msg = message_by_id[msg_id]
            target_id = msg.get('reply_to_msg_id')
            # A root is a message with no reply_to within our set
            if not target_id or target_id not in available_ids:
                # Heuristic: Skip if this message is an interjection (unreferenced and followed by unrelated message)
                msg_index = next((i for i, m in enumerate(messages_processed) if m['id'] == msg_id), None)
                if msg_index is not None and msg_index + 1 < len(messages_processed):
                    next_msg = messages_processed[msg_index + 1]
                    if (next_msg['sender_id'] != msg['sender_id'] and
                        next_msg['reply_to_msg_id'] is None and
                        next_msg['inferred_reply_type'] != 'time_proximity' and
                        msg_id not in replies_to_map):
                        try:
                            time_diff = (datetime.fromisoformat(next_msg['date']) -
                                        datetime.fromisoformat(msg['date']))
                            if time_diff <= timedelta(seconds=30):  # Short gap suggests interjection
                                continue
                        except ValueError:
                            pass
                root_message_ids.append(msg_id)
        
        root_message_ids.sort()  # Process roots in chronological order
        logger.info(f"Identified {len(root_message_ids)} potential root messages.")
        
        # Build threads using BFS with heuristic boundary checks
        threads = []
        processed_in_threads = set()
        MAX_THREAD_MESSAGES = 20  # Cap thread size
        MAX_PARTICIPANTS = 5      # Cap participants
        
        for root_id in root_message_ids:
            if root_id in processed_in_threads:
                continue
            
            current_thread_ids = []
            participants = set()
            queue = [root_id]
            visited_in_this_thread = set()
            
            while queue:
                current_msg_id = queue.pop(0)
                
                if current_msg_id not in available_ids or current_msg_id in visited_in_this_thread:
                    continue
                
                current_msg = message_by_id[current_msg_id]
                participants.add(current_msg['sender_id'])
                
                # Heuristic: Stop if thread grows too large
                if len(current_thread_ids) >= MAX_THREAD_MESSAGES or len(participants) > MAX_PARTICIPANTS:
                    break
                
                # Heuristic: Detect interjections (non-root messages)
                if current_thread_ids and current_msg_id != root_id:
                    prev_msg_id = current_thread_ids[-1]
                    prev_msg = message_by_id[prev_msg_id]
                    # Skip if no reply link, different sender, and not a proximity reply
                    if (current_msg['reply_to_msg_id'] != prev_msg_id and
                        current_msg['sender_id'] != prev_msg['sender_id'] and
                        current_msg['inferred_reply_type'] != 'time_proximity'):
                        try:
                            time_diff = (datetime.fromisoformat(current_msg['date']) -
                                        datetime.fromisoformat(prev_msg['date']))
                            if time_diff <= timedelta(seconds=30):  # Short gap suggests interjection
                                continue
                        except ValueError:
                            pass
                
                visited_in_this_thread.add(current_msg_id)
                processed_in_threads.add(current_msg_id)
                current_thread_ids.append(current_msg_id)
                
                # Add replies to queue
                replies = sorted(replies_to_map.get(current_msg_id, []))
                for reply_id in replies:
                    if reply_id not in visited_in_this_thread:
                        queue.append(reply_id)
            
            # Retrieve full message dicts for the thread and sort by ID
            if current_thread_ids:
                thread_messages = [message_by_id[msg_id] for msg_id in current_thread_ids]
                thread_messages.sort(key=lambda m: m['id'])
                threads.append(thread_messages)
        
        logger.info(f"Constructed {len(threads)} raw threads.")
        
        # --- [Step 7: Process Threads into Chunks (Condense, Anonymize, Format, Filter)] ---
        logger.info("Processing threads into final conversation chunks...")
        conversation_chunks = []
        total_processed_thread_count = 0
        filtered_out_count = 0
        
        for thread in threads:
            total_processed_thread_count += 1
            if not thread: continue
            
            condensed_parts = []
            user_map = {}
            anon_id_counter = 0
            participants = set()
            
            # Condense consecutive messages from the same user within the thread
            i = 0
            while i < len(thread):
                current_msg = thread[i]
                current_sender_id = current_msg['sender_id']
                participants.add(current_sender_id)
                
                # Assign anonymous ID if new
                if current_sender_id not in user_map:
                    user_map[current_sender_id] = f"user{anon_id_counter}"
                    anon_id_counter += 1
                anon_user = user_map[current_sender_id]
                
                # Collect texts from consecutive messages by this user
                block_texts = [current_msg['text']]
                j = i + 1
                while j < len(thread) and thread[j]['sender_id'] == current_sender_id:
                    block_texts.append(thread[j]['text'])
                    participants.add(thread[j]['sender_id'])  # Should be same, but safe
                    j += 1
                
                # Add the condensed block
                condensed_parts.append({
                    'user': anon_user,
                    'text': "\n".join(block_texts)  # Join messages with newline
                })
                
                # Move main index past the processed block
                i = j
            
            # Filter out threads with fewer than MIN_PARTICIPANTS_PER_CHUNK
            if len(participants) < min_participants:
                filtered_out_count += 1
                continue
            
            # Format the final text chunk
            conversation_text = "\n".join([f"{part['user']}: {part['text']}" for part in condensed_parts])
            
            # Add chunk to the final list
            root_message = thread[0]  # First message chronologically
            conversation_chunks.append({
                'id': f"tg_{group_info['group_id']}_{root_message['id']}",
                'source': 'telegram',
                'group_id': group_info['group_id'],
                'group_name': group_info['group_name'],
                'root_message_id': root_message['id'],
                'start_date': root_message['date'],
                'end_date': thread[-1]['date'],  # Last message chronologically
                'participant_count': len(participants),
                'message_count_original': len(thread),  # How many original msgs in thread
                'turn_count_condensed': len(condensed_parts),  # How many turns after condensing
                'content': conversation_text.strip()  # Using 'content' as the standard field name
            })
        
        logger.info(f"Processed {total_processed_thread_count} threads.")
        logger.info(f"Filtered out {filtered_out_count} threads with < {min_participants} participants.")
        logger.info(f"Created {len(conversation_chunks)} final conversation chunks.")
        
        # Prepare final results
        result = {
            'group_info': group_info,
            'processing_parameters': {
                'time_threshold_proximity_minutes': time_threshold.total_seconds() / 60,
                'id_threshold_proximity': id_threshold,
                'min_participants_per_chunk': min_participants
            },
            'processing_stats': {
                'total_messages_fetched': len(messages),
                'same_user_chained_count': same_user_chained_count,
                'aba_inferred_count': aba_inferred_count,
                'proximity_inferred_count': proximity_inferred_count,
                'total_raw_threads_built': len(threads),
                'threads_filtered_out': filtered_out_count,
                'final_chunks_exported': len(conversation_chunks)
            },
            'conversation_chunks': conversation_chunks
        }
        
        return result
        
    async def export_conversations(self,
                             group_username: str,
                             start_id: Optional[int] = None,
                             message_limit: int = DEFAULT_MESSAGE_LIMIT,
                             fetch_reverse: bool = False,
                             time_threshold: timedelta = DEFAULT_TIME_THRESHOLD,
                             id_threshold: int = DEFAULT_ID_THRESHOLD,
                             min_participants: int = DEFAULT_MIN_PARTICIPANTS,
                             save_to_file: bool = True) -> Dict:
        """
        Fetch and process conversations from a Telegram group.
        
        Args:
            group_username: Username or ID of the group
            start_id: Message ID to start from (None for latest)
            message_limit: Maximum number of messages to fetch
            fetch_reverse: Whether to fetch in reverse order (oldest first)
            time_threshold: Maximum time difference for proximity inference
            id_threshold: Maximum message ID difference for proximity inference
            min_participants: Minimum number of participants per chunk
            save_to_file: Whether to save the results to files
            
        Returns:
            Dictionary with processing results and conversation chunks
        """
        # Connect if not already connected
        if not self.client or not self.client.is_connected():
            if not await self.connect():
                return {"error": "Failed to connect to Telegram API"}
                
        # Fetch messages
        messages, group_info, success = await self.fetch_messages(
            group_username=group_username,
            start_id=start_id,
            message_limit=message_limit,
            fetch_reverse=fetch_reverse
        )
        
        if not success:
            await self.disconnect()
            return {"error": "Failed to fetch messages"}
            
        # Process messages to chunks
        results = self.process_messages_to_chunks(
            messages=messages,
            group_info=group_info,
            time_threshold=time_threshold,
            id_threshold=id_threshold,
            min_participants=min_participants
        )
        
        # Save results to files if requested
        if save_to_file:
            original_file = os.path.join(self.output_dir, f"{group_username}_original_messages.json")
            chunks_file = os.path.join(self.output_dir, f"{group_username}_conversation_chunks.json")
            
            # Save original messages
            try:
                with open(original_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        **group_info,
                        'messages': messages
                    }, f, ensure_ascii=False, indent=2)
                logger.info(f"Original raw message data saved to {original_file}")
            except IOError as e:
                logger.error(f"Failed to save original messages: {e}")
                
            # Save processed chunks
            try:
                with open(chunks_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                logger.info(f"Export complete! Conversation chunks saved to {chunks_file}")
                results['output_files'] = {
                    'original_messages': original_file,
                    'conversation_chunks': chunks_file
                }
            except IOError as e:
                logger.error(f"Failed to save final conversation chunks: {e}")
        
        # Return results
        return results


async def fetch_telegram_conversations(group_username: str,
                                  api_id: Optional[str] = None,
                                  api_hash: Optional[str] = None,
                                  phone_number: Optional[str] = None,
                                  message_limit: int = DEFAULT_MESSAGE_LIMIT,
                                  start_id: Optional[int] = None,
                                  output_dir: str = DEFAULT_OUTPUT_DIR) -> Dict:
    """
    Convenience function to fetch and process conversations from a Telegram group.
    
    Args:
        group_username: Username or ID of the group
        api_id: Telegram API ID (from environment if None)
        api_hash: Telegram API Hash (from environment if None)
        phone_number: Phone number for Telegram account (from environment if None)
        message_limit: Maximum number of messages to fetch
        start_id: Message ID to start from (None for latest)
        output_dir: Directory to save output files
        
    Returns:
        Dictionary with processing results and conversation chunks
    """
    fetcher = TelegramFetcher(
        api_id=api_id,
        api_hash=api_hash,
        phone_number=phone_number,
        output_dir=output_dir
    )
    
    try:
        results = await fetcher.export_conversations(
            group_username=group_username,
            start_id=start_id,
            message_limit=message_limit,
            save_to_file=True
        )
        return results
    finally:
        await fetcher.disconnect()


if __name__ == '__main__':
    """
    Command-line interface for exporting Telegram conversations.
    
    Usage:
        python -m app.sources.telegram GROUP_USERNAME [OPTIONS]
        
    Options:
        --start-id ID         Message ID to start from
        --limit LIMIT         Maximum number of messages to fetch
        --reverse             Fetch in reverse order (oldest first)
        --output-dir DIR      Directory to save output files
    """
    import argparse
    import sys
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Export conversations from a Telegram group')
    parser.add_argument('group_username', help='Username or ID of the Telegram group')
    parser.add_argument('--start-id', type=int, help='Message ID to start from')
    parser.add_argument('--limit', type=int, default=DEFAULT_MESSAGE_LIMIT, help='Maximum number of messages to fetch')
    parser.add_argument('--reverse', action='store_true', help='Fetch in reverse order (oldest first)')
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR, help='Directory to save output files')
    
    args = parser.parse_args()
    
    # Run the exporter
    async def run():
        try:
            results = await fetch_telegram_conversations(
                group_username=args.group_username,
                message_limit=args.limit,
                start_id=args.start_id,
                output_dir=args.output_dir
            )
            
            # Show statistics
            if 'error' in results:
                print(f"Error: {results['error']}")
                return 1
                
            stats = results['processing_stats']
            print(f"\nExport Statistics:")
            print(f"  Total messages fetched: {stats['total_messages_fetched']}")
            print(f"  Total conversation chunks: {stats['final_chunks_exported']}")
            print(f"  Output files:")
            if 'output_files' in results:
                for name, path in results['output_files'].items():
                    print(f"    {name}: {path}")
                    
            return 0
            
        except Exception as e:
            print(f"Error: {e}")
            return 1
    
    # Run the async function
    sys.exit(asyncio.run(run()))