"""
Main entry point for the Refugee Information Telegram Bot.

This script provides command-line functionality for setting up the database,
indexing documents, and starting the bot.
"""

import os
import sys
import argparse
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Import modules
from app.indexer import setup_collections, disconnect, index_from_file


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Refugee Information Telegram Bot")
    
    # Add subparsers for different commands
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Set up the database collections")
    setup_parser.add_argument(
        "--recreate", 
        action="store_true", 
        help="Recreate collections if they exist"
    )
    
    # Index command
    index_parser = subparsers.add_parser("index", help="Index documents")
    index_parser.add_argument(
        "--file", 
        type=str, 
        required=True, 
        help="Path to the JSON file containing documents"
    )
    index_parser.add_argument(
        "--text-field", 
        type=str, 
        default="content", 
        help="Field containing the text to index"
    )
    index_parser.add_argument(
        "--chunk-size", 
        type=int, 
        default=1000, 
        help="Maximum chunk size in characters"
    )
    index_parser.add_argument(
        "--model", 
        type=str, 
        default="voyage-3-large", 
        help="Embedding model to use"
    )
    index_parser.add_argument(
        "--id-field", 
        type=str, 
        help="Field to use as document ID"
    )
    
    # Bot command (placeholder for future implementation)
    bot_parser = subparsers.add_parser("bot", help="Start the Telegram bot")
    
    # Admin command (placeholder for future implementation)
    admin_parser = subparsers.add_parser("admin", help="Start the admin interface")
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    if args.command == "setup":
        # Set up database collections
        logger.info("Setting up database collections...")
        success = setup_collections(recreate=args.recreate)
        
        if success:
            logger.info("Database collections set up successfully")
        else:
            logger.error("Failed to set up database collections")
            return 1
            
    elif args.command == "index":
        # Index documents from a file
        logger.info(f"Indexing documents from {args.file}...")
        
        # Ensure the file exists
        if not os.path.isfile(args.file):
            logger.error(f"File not found: {args.file}")
            return 1
        
        # Set up collections if needed
        setup_collections(recreate=False)
        
        # Index the documents
        doc_ids = index_from_file(
            args.file,
            text_field=args.text_field,
            chunk_size=args.chunk_size,
            model=args.model,
            id_field=args.id_field
        )
        
        # Report results
        logger.info(f"Indexed {len(doc_ids)} documents")
        
    elif args.command == "bot":
        # Start the Telegram bot (placeholder)
        logger.info("Starting Telegram bot...")
        logger.warning("Bot functionality not yet implemented")
        
    elif args.command == "admin":
        # Start the admin interface (placeholder)
        logger.info("Starting admin interface...")
        logger.warning("Admin interface not yet implemented")
        
    else:
        # No command specified, show help
        logger.info("No command specified, use --help for usage information")
        return 1
    
    # Clean up
    disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())