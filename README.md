# Nexus Analytics System

A comprehensive SQL and Analytics agent system for partner affiliate program analytics.

## Project Structure

```
nexus/
├── README.md                   # This file
├── start_backend.py           # Backend startup script
├── .env                       # Environment variables
├── backend/                   # Backend services directory
│   ├── main.py               # Flask API server (renamed from backend.py)
│   ├── sql_agent.py          # SQL Agent (renamed from sql_agent1.py)
│   ├── analytics_agent.py    # Analytics Agent with visualizations
│   └── logs/                 # Log files directory
├── frontend/                 # React frontend
│   ├── src/
│   ├── public/
│   ├── package.json
│   └── ...
└── venv/                     # Virtual environment
```

## Quick Start

### 1. Backend (API Server)

From the project root directory:

```bash
# Option 1: Use the startup script
python start_backend.py

# Option 2: Run directly from backend directory
cd backend
python main.py
```

The backend will be available at: `http://localhost:5000`

### 2. Frontend (React UI)

```bash
cd frontend
npm start
```

The frontend will be available at: `http://localhost:3000`

## API Endpoints

- `GET /health` - Health check
- `POST /sql-agent` - Test SQL Agent only
- `POST /analytics-agent` - Test SQL + Analytics combined workflow

## Features

### SQL Agent
- Generates SQL queries from natural language
- Query verification and auto-correction
- Handles ambiguous column references
- Database schema-aware

### Analytics Agent
- Statistical analysis of SQL results
- Pattern recognition
- Trend analysis
- Business insights generation
- Multi-series visualizations for comparisons
- Support for bar charts, line charts, pie charts, histograms

### Visualization Types
- **Single Bar Chart**: For simple categorical data
- **Grouped Bar Chart**: For comparing categories across groups
- **Single Line Chart**: For time series data
- **Multi-Line Chart**: For comparing multiple entities over time
- **Pie Chart**: For distribution analysis
- **Histogram**: For data distribution
- **Correlation Heatmap**: For correlation analysis

## Environment Setup

Create a `.env` file with:

```env
# Database Configuration
host=your_host
port=your_port
dbname=your_database
user=your_username
password=your_password

# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key
API_BASE_URL=your_api_base_url
OPENAI_MODEL_NAME=your_model_name
```

## Development

### Backend Development
All backend code is organized in the `backend/` directory:
- `main.py`: Main Flask API server
- `sql_agent.py`: SQL generation and execution logic
- `analytics_agent.py`: Data analysis and visualization logic

### Frontend Development
React frontend is in the `frontend/` directory with standard React project structure.

### Logs
Application logs are stored in `backend/logs/`:
- `sql_agent.log`: SQL agent operations
- `analytics_agent.log`: Analytics operations

## Usage Examples

### Query Examples
- "Top 5 partners by client deposits in March 2025"
- "Compare partner performance across regions with bar graph"
- "Show partner commission trends over time"
- "Middle East partner deposit analysis with visualization"

### Comparison Queries
When asking for comparisons, the system automatically generates multi-series visualizations:
- Multi-line charts for time-based comparisons
- Grouped bar charts for categorical comparisons
- Enhanced trend analysis for performance comparisons 