# Nexus Analytics System

A comprehensive SQL and Analytics agent system for partner affiliate program analytics, featuring natural language query processing, data analysis, and visualization capabilities.

## Project Structure

```
partner-nexus1/
├── backend/                    # Backend services directory
│   ├── main.py                # Flask API server and route handlers
│   ├── sql_agent.py           # SQL query generation and execution
│   ├── analytics_agent.py     # Data analysis and visualization
│   ├── screener.py            # Partner screener functionality
│   ├── spotlight_dashboard.py # Dashboard data and insights
│   ├── progress_manager.py    # WebSocket progress tracking
│   ├── config.py              # Environment and client configuration
│   ├── logging_config.py      # Logging setup and configuration
│   ├── schema_manager.py      # Database schema operations
│   ├── utils.py               # Shared utility functions
│   ├── vector_store.py        # FAISS vector store initialization
│   ├── logs/                  # Log files directory
│   └── metadata/              # Schema and vector store data
│       ├── schema_metadata.json
│       └── schema_vectorstore/
├── frontend/                  # React frontend application
│   ├── src/                  # React source code
│   ├── public/               # Static assets
│   ├── package.json          # NPM dependencies
│   └── package-lock.json     # Locked NPM versions
├── .env.example              # Example environment variables
└── README.md                 # This file
```

## Quick Start

### 1. Backend Setup

From the project root directory:

```bash
# 1. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up environment variables
cp .env.example .env
# Edit .env with your credentials:
# - Database configuration (host, port, dbname, user, password)
# - OpenAI API configuration (API key, base URL, model name)
# - Supabase configuration (URL, anon key, service role key)

# 4. Run the backend server
cd backend
python main.py
```

The backend will be available at: `http://127.0.0.1:5001`

### 2. Frontend (React UI)

In a new terminal, from the project root:

```bash
cd frontend
npm install --force
npm audit fix --force
npm start
```

The frontend will be available at: `http://localhost:3000`

## Core Components

### SQL Agent (`sql_agent.py`)
- Natural language to SQL query conversion
- Query validation and auto-correction
- Schema-aware query generation using vector search
- Real-time progress tracking via WebSocket
- Key features:
  - Intent verification with LLM
  - Syntax validation using EXPLAIN
  - Auto-correction with retry loops
  - Schema context retrieval using FAISS

### Analytics Agent (`analytics_agent.py`)
- Advanced data analysis and visualization
- Pattern and trend detection
- Business insight generation
- Visualization types:
  - Bar charts (single and grouped)
  - Line charts (single and multi-series)
  - Pie charts and histograms
  - Custom visualizations based on data type

### Partner Screener (`screener.py`)
- Partner performance analysis
- Filtering and sorting capabilities
- Detailed metrics calculation
- Performance trend visualization

### Spotlight Dashboard (`spotlight_dashboard.py`)
- Real-time partner program metrics
- AI-generated insights for each widget
- Comprehensive data aggregation
- Key metrics:
  - Partner acquisition and activation
  - Regional performance analysis
  - VAN trip effectiveness
  - Conversion funnel metrics

## API Endpoints

The API supports both HTTP and WebSocket connections:

### HTTP Endpoints
- `GET /health` - Server health check
- `POST /sql-agent` - Execute SQL queries from natural language
- `POST /analytics-agent` - Run analytics workflow
- `GET /screener` - Get partner screener data
- `GET /spotlight-dashboard` - Get dashboard metrics
- `GET /funnel-metrics` - Get conversion funnel data

### WebSocket Events
- `progress_update` - Real-time task progress
- `query_result` - SQL query results
- `analysis_complete` - Analytics completion
- `error` - Error notifications

## Configuration Files

### `config.py`
- Environment variable management
- Database connection setup
- OpenAI client configuration
- Supabase client initialization

### `logging_config.py`
- Centralized logging setup
- File and console handlers
- Log rotation configuration

### `schema_manager.py`
- Database schema metadata handling
- Table relationship inference
- Schema caching and updates

### `vector_store.py`
- FAISS vector store setup
- Schema embedding management
- Semantic search functionality

## Development Guidelines

### Backend Development
- Python 3.8+ required
- Uses Flask for API server
- LangChain for LLM workflows
- PostgreSQL database
- Key dependencies:
  - `psycopg2` for database access
  - `openai` for LLM integration
  - `langchain` for agent workflows
  - `faiss-cpu` for vector search
  - `plotly` for visualization

### Frontend Development
- React 18+
- Material-UI components
- WebSocket for real-time updates
- Key features:
  - Real-time progress tracking
  - Interactive visualizations
  - Responsive dashboard layout
  - Query history management

### Logging
All operations are logged in `backend/logs/`:
- `sql_agent.log` - Query operations
- `analytics_agent.log` - Analysis operations
- Log rotation: 10MB max size, 5 backup files 
