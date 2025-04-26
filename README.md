# Refugee Information Telegram Bot

A Telegram bot that helps refugees find relevant information from multiple sources.

## Overview

This project aims to create a system that:

1. Collects information from refugee-related websites and Telegram groups
2. Processes and indexes this information for efficient retrieval
3. Provides a Telegram bot interface for refugees to query this information
4. Offers an admin interface for managing and updating the information

## Architecture

The system uses a functional approach with clear separation of concerns:

- **Storage Layer**: MongoDB for document storage and Milvus for vector embeddings
- **Processing Layer**: Text chunking, embedding generation, and search
- **Interface Layer**: Telegram bot and admin interface

## Setup

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- VoyageAI API key (for embeddings)
- Telegram API credentials (for scraping Telegram groups)

### Infrastructure

The project uses Docker Compose to manage the following services:

- **MongoDB**: Document database for storing content
  - Port: 27017
  - Admin UI: Mongo Express on port 8081

- **Milvus**: Vector database for storing embeddings
  - Port: 19530
  - Admin UI: Attu on port 8000

### Installation

1. Clone the repository
   ```bash
   git clone https://github.com/your-username/ino-refugee-bot.git
   cd ino-refugee-bot
   ```

2. Create a virtual environment and install dependencies
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Set up environment variables (create a `.env` file)
   ```bash
   # Copy the example file
   cp .env.example .env
   
   # Edit the file to add your credentials
   nano .env  # or use any text editor
   ```

4. Start the database services
   ```bash
   docker compose up -d
   ```

5. Set up the database collections
   ```bash
   python main.py setup
   ```

### Connect to Admin Interfaces

- **MongoDB Express**: http://your-server-ip:8081
  - Username and password are configured in .env file

- **Milvus Attu**: http://your-server-ip:8000

## Usage

### Indexing Documents

To index documents from a JSON file:

```bash
python main.py index --file data/documents.json --text-field content
```

### Running the Bot

To start the Telegram bot:

```bash
python main.py bot
```

### Running the Admin Interface

To start the admin interface:

```bash
python main.py admin
```

## Project Structure

```
refugee-bot/
├── app/                      # Application code
│   ├── storage/              # Storage abstractions
│   │   ├── interfaces.py     # Storage interfaces
│   │   ├── mongodb.py        # MongoDB implementation
│   │   └── milvus.py         # Milvus implementation
│   ├── processing/           # Text processing
│   │   ├── chunking.py       # Text chunking functions
│   │   └── embedding.py      # Embedding generation
│   └── indexer.py            # Document indexing
├── data/                     # Data storage
├── tests/                    # Test files
├── docker-compose.yml        # Docker Compose configuration
├── main.py                   # Main entry point
└── requirements.txt          # Python dependencies
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.