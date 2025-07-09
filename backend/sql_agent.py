import psycopg2
from dotenv import load_dotenv
import os
import json
import sqlglot
from sqlglot.errors import ParseError
from psycopg2.extras import RealDictCursor
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from tabulate import tabulate
from typing import TypedDict, Annotated, Sequence, Optional, Callable
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import OpenAIEmbeddings
import logging
from logging.handlers import RotatingFileHandler
import datetime
from supabase import create_client, Client

# Progress callback type
ProgressCallback = Callable[[str, int], None]

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs') 

# Set up logging configuration
def setup_logger():
    """Set up a logger with file and console handlers."""
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        'logs/sql_agent.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Create logger
    logger = logging.getLogger('sql_agent')
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Create logger
logger = setup_logger()

# Load environment variables from .env
load_dotenv()

db_params = {
    'host': os.getenv('host'),
    'port': os.getenv('port'),
    'database': os.getenv('dbname'),
    'user': os.getenv('user'),
    'password': os.getenv('password')
}

# OpenAI configuration from environment variables
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
API_BASE_URL = os.getenv('API_BASE_URL')
OPENAI_MODEL_NAME = os.getenv('OPENAI_MODEL_NAME')

conn = psycopg2.connect(**db_params)
cursor = conn.cursor(cursor_factory=RealDictCursor)


# Supabase configuration
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def get_llm(model=OPENAI_MODEL_NAME, temperature=0) -> ChatOpenAI:
    config = {"model_name": model, "temperature": temperature}
    if API_BASE_URL:
        config["base_url"] = API_BASE_URL
    return ChatOpenAI(**config)

def get_multi_schema_metadata(schemas: list[str]):
    """
    Fetches metadata for the given schemas by calling the Supabase RPC function,
    formats it, and saves it to a JSON file.
    """
    logger.info("Fetching metadata for schemas: %s", schemas)
    response = supabase_client.rpc("get_schema_metadata", {"p_schema_names_json": schemas}).execute()

    rows = response.data
    if not rows:
        logger.warning("Metadata fetch returned no data.")
        rows = []

    logger.info("Successfully retrieved metadata.")

    schema_dict = {}
    for table_data in rows:
        schema = table_data.get('table_schema')
        table = table_data.get('table_name')
        if not schema or not table:
            continue
            
        key = f"{schema}.{table}"
        schema_dict[key] = {
            "columns": table_data.get('columns', []),
            "relationships": [],  # Initialize empty relationships list
            "description": table_data.get('table_description')
        }
    
    # Infer and add relationships directly to the schema metadata
    schema_dict_with_relationships = infer_table_relationships(schema_dict)
    
    # Create a directory for storing metadata if it doesn't exist
    if not os.path.exists('metadata'):
        os.makedirs('metadata')
    
    # Write schema data to JSON file
    json_file_path = 'metadata/schema_metadata.json'
    with open(json_file_path, 'w') as f:
        json.dump(schema_dict_with_relationships, f, indent=4)
    
    return json_file_path

def read_schema_metadata():
    """Read schema metadata from JSON file."""
    json_file_path = 'metadata/schema_metadata.json'
    if not os.path.exists(json_file_path):
        schema_file = get_multi_schema_metadata(['partner', 'client'])
    
    with open(json_file_path, 'r') as f:
        schema_dict = json.load(f)
    return schema_dict

def infer_table_relationships(schema_dict: dict):
    """
    Infers table relationships from the schema based on naming conventions.
    Adds the inferred relationships directly to the schema_dict and returns it.
    Also saves the relationships to a separate JSON file for backward compatibility.
    """
    logger.info("Inferring table relationships from schema...")
    
    primary_keys = {}
    for table_name, table_data in schema_dict.items():
        for column in table_data['columns']:
            if column.get('is_primary_key'):
                # Store as {column_name: table_name} for easy lookup
                pk_col_name = column.get('column_name')
                if pk_col_name:
                    primary_keys[pk_col_name] = table_name

    # Create a copy of schema_dict to modify
    updated_schema_dict = schema_dict.copy()
    relationships_dict = {}  # For backward compatibility
    
    for table_name, table_data in schema_dict.items():
        relations = []
        for column in table_data['columns']:
            col_name = column.get('column_name')
            if not col_name or column.get('is_primary_key'):
                continue

            # Heuristic 1: column name is a primary key in another table
            if col_name in primary_keys and primary_keys[col_name] != table_name:
                relation = {
                    'foreign_key_column': col_name,
                    'references_table': primary_keys[col_name],
                    'references_column': col_name,
                    'constraint_name': f"inferred_fk_{table_name.replace('.', '_')}_{col_name}"
                }
                relations.append(relation)
                continue
            
            # Heuristic 2: column name of the form `..._id` or `..._pk`
            if col_name.endswith(('_id', '_pk')):
                # Try to find a table that matches the prefix
                prefix = col_name.rsplit('_', 1)[0]
                # Look for tables named `prefix` or `prefix` + 's' (plural)
                for pk_name, pk_table_name in primary_keys.items():
                    pk_table_base_name = pk_table_name.split('.')[-1]
                    if pk_table_base_name == prefix or pk_table_base_name == prefix + 's':
                        relation = {
                            'foreign_key_column': col_name,
                            'references_table': pk_table_name,
                            'references_column': pk_name,
                            'constraint_name': f"inferred_fk_{table_name.replace('.', '_')}_{col_name}"
                        }
                        relations.append(relation)

        # Add relationships to the schema metadata
        if relations:
            updated_schema_dict[table_name]['relationships'] = relations
            relationships_dict[table_name] = relations  # For backward compatibility

    logger.info("Successfully inferred relationships.")
    
    return updated_schema_dict

# Get schema metadata
schema_dict = read_schema_metadata()

def schema_dict_to_chunks(schema_dict):
    chunks = []
    for table, meta in schema_dict.items():
        columns = meta.get("columns", [])
        relationships = meta.get("relationships", [])
        desc = meta.get("description", "")
        column_text = "\n".join(
            f"- {col['column_name']} ({col['data_type']})"
            for col in columns
        )
        rel_text = "\n".join(
            f"- {r['foreign_key_column']} → {r['references_table']}.{r['references_column']}"
            for r in relationships
        )
        chunk = f"""
        Table: {table}
        Description: {desc}
        Columns:
        {column_text}
        Relationships:
        {rel_text}
        """
        chunks.append(chunk.strip())
    return chunks


embedding = OpenAIEmbeddings(
    openai_api_key=OPENAI_API_KEY,  # not used by LiteLLM but required
    openai_api_base=API_BASE_URL,  # Your LiteLLM proxy
    model="text-embedding-ada-002"
)

schema_chunks = schema_dict_to_chunks(read_schema_metadata())
vectorstore = FAISS.from_texts(schema_chunks, embedding=embedding)
vectorstore.save_local("metadata/schema_vectorstore")

retriever = FAISS.load_local("metadata/schema_vectorstore", embedding, allow_dangerous_deserialization=True)

def retrieve_context(user_query: str) -> str:
    docs = retriever.similarity_search(user_query, k=7)
    return "\n\n".join([doc.page_content for doc in docs])

# Define the state type
class AgentState(TypedDict):
    prompt: str
    sql_query: str
    verification_result: str
    matches_intent: bool
    improved_prompt: str
    error_message: str
    results: str
    error: str
    attempt: int
    table_info: str
    progress: Optional[int]  # New field for progress tracking

def get_table_info(prompt: str) -> str:
    table_info = retrieve_context(prompt)
    return table_info

def clean_sql_query(sql_text: str) -> str:
    """Clean SQL query by removing markdown formatting and extra whitespace."""
    # Remove markdown code blocks
    sql_text = sql_text.strip()
    
    # Remove ```sql and ``` markdown formatting
    if sql_text.startswith('```sql'):
        sql_text = sql_text[6:]  # Remove ```sql
    elif sql_text.startswith('```'):
        sql_text = sql_text[3:]   # Remove ```
    
    if sql_text.endswith('```'):
        sql_text = sql_text[:-3]  # Remove trailing ```
    
    # Clean up extra whitespace
    sql_text = sql_text.strip()
    
    return sql_text

def generate_sql_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None) -> AgentState:
    """Generate SQL query from natural language input."""
    if progress_callback:
        progress_callback("Generating SQL query...", 20)
    
    logger.info("\n=== Generating SQL Query ===")
    logger.info(f"Input prompt: {state['prompt']}")

    table_info = retrieve_context(state["prompt"])
    
    # Configure ChatOpenAI with environment variables
    llm = get_llm()
    
    system_prompt = f"""You are an expert PostgreSQL query generator that creates accurate SQL queries based on natural language questions and database metadata. You will analyze user questions and generate appropriate PostgreSQL queries using the provided database schema information.

Here is the comprehensive database metadata from Supabase: {schema_chunks}

Your task is to:

- Analyze the user's question to understand their intent and requirements
- Examine the metadata to identify the relevant schemas, tables, and columns
- Generate an appropriate PostgreSQL query that answers the question accurately

Important guidelines:

- Use proper PostgreSQL syntax and features
- Include appropriate JOINs when data spans multiple tables
- Use CTEs (Common Table Expressions) for complex queries when helpful
- Apply window functions for analytical queries when appropriate
- Use proper aggregation functions (SUM, COUNT, AVG, etc.) when needed
- Include appropriate WHERE clauses for filtering
- Use LIMIT/OFFSET for pagination when relevant
- Handle NULL values appropriately
- Use proper data types and casting when necessary
- Include ORDER BY clauses when sorting is implied or beneficial
- Use subqueries when they improve readability or performance
- Apply proper grouping with GROUP BY when using aggregation functions

Schema considerations:

- Pay attention to table relationships and foreign keys
- Use the correct schema names if multiple schemas exist
- Ensure column names and table names match exactly with the metadata
- Consider indexes that might affect query performance
- Use appropriate table aliases for readability

MANDATORY EXCLUSION RULES:
- ALWAYS exclude internal partners/clients in ALL queries
- For partner.partner_info table: ALWAYS add "AND is_internal = FALSE" or "WHERE is_internal = FALSE"
- For client tables: ALWAYS add "AND is_internal = FALSE" or "WHERE is_internal = FALSE"
- For trade tables: ALWAYS add "AND is_internal = FALSE" or "WHERE is_internal = FALSE"
- This applies to ALL tables that have an is_internal column - NEVER include internal records
- Internal records skew business metrics and should be excluded from all business analysis

GEOGRAPHIC FIELD SELECTION RULES:
- When user mentions specific COUNTRIES (Nigeria, India, Vietnam, etc.), use "partner_country" field
- When user mentions REGIONS (Africa, Asia, Europe, etc.), use "partner_region" field
- partner_country contains specific country names like 'Nigeria', 'India', 'Vietnam'
- partner_region contains broader regions like 'Africa', 'Asia', 'Europe'
- NEVER confuse country vs region - they are different fields with different purposes

CRITICAL TIME-BASED QUERY RULES:
- When asked about data "in [month/period]", filter for records WITHIN that specific period using BETWEEN
- When asked about data "from [month/year]", use ">=" for the start date only
- NEVER use <= for period-specific queries (this gives cumulative data)
- For "activation rate in Jan 2025": compare partners who joined in Jan 2025 vs those who got activated in Jan 2025
- Use date_joined BETWEEN '2025-01-01' AND '2025-01-31' for January 2025 data
- Partner activation means having first_earning_date, first_client_joined_date, or similar activation indicators

Strictly return ONLY the SQL query, DO NOT include any other text, markdown or other formatting. The query should be complete and executable."""
    
    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=state["prompt"])
    ])
    
    # Clean the SQL query to remove any markdown formatting
    cleaned_sql = clean_sql_query(response.content)
    
    logger.info(f"Generated SQL (raw):\n{response.content}")
    logger.info(f"Generated SQL (cleaned):\n{cleaned_sql}")
    return {"sql_query": cleaned_sql, "progress": 20}

def verify_intent_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None) -> AgentState:
    """Verify if the SQL query matches the original intent."""
    if progress_callback:
        progress_callback("Verifying query intent...", 40)
    
    logger.info("\n=== Verifying SQL Intent ===")
    logger.info(f"SQL to verify:\n{state['sql_query']}")
    
    # Configure ChatOpenAI with environment variables
    llm = get_llm()
    
    system_prompt = f"""You are a PostgreSQL query validator. Your task is to verify that a generated SQL query correctly matches the business intent expressed in the original natural language prompt.
    Here is the original natural language prompt that describes the business intent: {state["prompt"]}
    Your validation process should follow these steps:

    Translate SQL to Natural Language: 
    First, interpret the generated SQL query and translate it back into clear, natural language that describes exactly what the query does.
    Compare Intent: Compare your natural language translation of the SQL query against the original prompt

    Focus entirely on verifying whether the logic, filters, and selected columns used in the SQL query align EXACTLY with the business intent.

    If the query is valid, return "valid". If it is invalid, return "invalid" and provide a brief explanation of why it is invalid along with improved prompt to generate the correct query.

    Output Format:
    Return your response in the following JSON format:
    
    "is_valid": "true/false",
    "explanation": "brief explanation of why the query is valid/invalid",
    "improved_prompt": "improved prompt to generate the correct query"""
    
    user_prompt = f"""Translate the following SQL query into natural language: {state["sql_query"]}"""

    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt)
    ])
    
    parsed = json.loads(response.content)
    # Check for intent match AND make sure no syntax issues are mentioned

    matches_intent = parsed.get("is_valid", False)
    improved_prompt = parsed.get("improved_prompt", state["prompt"])
    
    logger.info(f"Intent match from response: {matches_intent}")

    logger.info(f"Final intent match: {matches_intent}")
    
    return {
        "matches_intent": matches_intent,
        "improved_prompt": improved_prompt,
        "progress": 40
    }

def validate_sql_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None) -> AgentState:
    """Validate the SQL query."""
    if progress_callback:
        progress_callback("Validating SQL syntax...", 60)
    
    logger.info("\n=== Validating SQL Query ===")
    logger.info(f"SQL to validate:\n{state['sql_query']}")

    try:
        explain = supabase_client.rpc("run_raw_sql", {"raw_sql": state["sql_query"]}).execute()
        result_data = getattr(explain, "data", None)

        if result_data:
            logger.info("Validation response received:")
            logger.info(result_data)

            # Detect if result_data contains an error string
            if result_data.find("cost") == -1:
                if progress_callback:
                    progress_callback("SQL validation failed, correcting syntax...", 60)
                return {
                    "syntax_validation_passed": False,
                    "error_message": result_data,
                    "progress": 60
                }

            # Otherwise, assume it's valid EXPLAIN output
            if progress_callback:
                progress_callback("SQL validation passed", 60)
            return {
                "syntax_validation_passed": True,
                "explain_output": result_data,
                "progress": 60
            }

        else:
            logger.warning("Validation returned empty data.")
            if progress_callback:
                progress_callback("SQL validation failed - no output", 60)
            return {
                "syntax_validation_passed": False,
                "error_message": "No output returned from Supabase RPC.",
                "progress": 60
            }

    except Exception as e:
        logger.error(f"Unexpected error during SQL validation: {str(e)}")
        if progress_callback:
            progress_callback(f"SQL validation error: {str(e)}", 60)
        return {
            "syntax_validation_passed": False,
            "error_message": str(e),
            "progress": 60
        }

def correct_sql_node(state: AgentState) -> AgentState:
    """Correct the SQL query based on verification results."""
    logger.info("\n=== Correcting SQL Query ===")
    
    # Increment attempt counter
    current_attempt = state.get("attempt", 0) + 1
    logger.info(f"Correction attempt: {current_attempt}")

    improved_prompt = state["improved_prompt"]

    return {"prompt": improved_prompt}
    
def correct_syntax_node(state: AgentState) -> AgentState:
    """Correct the SQL query based on verification results."""
    logger.info("\n=== Correcting SQL Query ===")
    logger.info(f"SQL to correct:\n{state['sql_query']}")

    current_attempt = state.get("attempt", 0) + 1
    llm = get_llm()

    system_prompt = f"""You are a PostgreSQL query corrector. Your task is to:
    1. Analyze the verification results of the generated SQL query
    2. Identify the issues that need to be fixed
    3. Generate a corrected SQL query that addresses all issues.
    Strictly return ONLY the corrected SQL query, DO NOT include any other text, markdown or other formatting. The query should be complete and executable.

    Error Message: {state["error_message"]}
    Generated SQL Query: {state["sql_query"]}
    Available Schema:
    {read_schema_metadata()}"""
    
    user_prompt = f"""Correct the following SQL query based on the error message: {state["error_message"]}"""

    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt)
    ])

    cleaned_sql = clean_sql_query(response.content)
    return {"sql_query": state["sql_query"]}

def execute_query_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None) -> AgentState:
    """Execute the SQL query and return results."""
    if progress_callback:
        progress_callback("Executing query...", 80)
    
    logger.info("\n=== Executing SQL Query ===")
    logger.info(f"Executing:\n{state['sql_query']}")
    
    try:
        sql_query = state["sql_query"]
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1].strip()
        results = supabase_client.rpc("run_sql", {"query": sql_query}).execute()
        results = results.data

        if not results:
            logger.info("Query executed successfully but returned no results")
            if progress_callback:
                progress_callback("Query executed - no results found", 80)
            return {"results": "✅ Query ran successfully, but no results were found.", "progress": 80}

        columns = list(results[0].keys())
        rows = [list(row.values()) for row in results]

        summary = f"✅ Query successful. Retrieved {len(results)} row(s).\n"
        table = tabulate(rows, headers=columns, tablefmt="pretty")
        
        logger.info(f"Query executed successfully. Retrieved {len(results)} rows")
        if progress_callback:
            progress_callback(f"Query executed - found {len(results)} rows", 80)
        return {"results": summary + "\n" + table, "progress": 80}

    except Exception as e:
        error_msg = f"❌ Query failed:\n{str(e)}"
        logger.error(f"Query execution failed: {str(e)}")
        if progress_callback:
            progress_callback(f"Query execution failed: {str(e)}", 80)
        return {"error": error_msg, "progress": 80}

def format_response_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None) -> AgentState:
    """Format the final response."""
    if progress_callback:
        progress_callback("Formatting results...", 100)
    
    logger.info("\n=== Formatting Final Response ===")
    if state.get("error"):
        logger.info("Formatting error response")
        return {"results": state["error"], "progress": 100}
    logger.info("Formatting successful response")
    state["progress"] = 100
    return state

def should_retry(state: AgentState) -> bool:
    """Determine if we should retry the query generation."""
    current_attempt = state.get("attempt", 0)
    matches_intent = state.get("matches_intent", False) 
    syntax_validation_passed = state.get("syntax_validation_passed", False)
    should_retry = not matches_intent and not syntax_validation_passed and current_attempt < 3
    
    logger.info(f"\n=== Retry Decision ===")
    logger.info(f"Current attempt: {current_attempt}")
    logger.info(f"Intent match: {matches_intent}")
    logger.info(f"Should retry: {should_retry}")
    
    if current_attempt >= 3:
        logger.warning("Maximum retry attempts reached. Proceeding to execution.")
    
    return should_retry

def create_workflow(progress_callback: Optional[ProgressCallback] = None) -> StateGraph:
    """Create and return a workflow with optional progress callback"""
    workflow = StateGraph(AgentState)

    # Add nodes with progress callback
    workflow.add_node("generate_sql", lambda x: generate_sql_node(x, progress_callback))
    workflow.add_node("verify_intent", lambda x: verify_intent_node(x, progress_callback))
    workflow.add_node("validate_sql", lambda x: validate_sql_node(x, progress_callback))
    workflow.add_node("correct_sql", correct_sql_node)
    workflow.add_node("correct_syntax", correct_syntax_node)
    workflow.add_node("execute_query", lambda x: execute_query_node(x, progress_callback))
    workflow.add_node("format_response", lambda x: format_response_node(x, progress_callback))

    # Define the flow
    workflow.add_edge("generate_sql", "verify_intent")
    workflow.add_conditional_edges(
        "verify_intent",
        should_retry,
        {
            True: "correct_sql",
            False: "validate_sql"
        }
    )
    workflow.add_edge("correct_sql", "generate_sql")
    workflow.add_conditional_edges(
        "validate_sql",
        should_retry,
        {
            True: "correct_syntax",
            False: "execute_query"
        }
    )
    workflow.add_edge("correct_syntax", "validate_sql")
    workflow.add_edge("execute_query", "format_response")
    workflow.add_edge("format_response", END)

    # Set the entry point
    workflow.set_entry_point("generate_sql")

    return workflow.compile()

# Create a default compiled workflow without progress callback
app = create_workflow()

# Main execution
if __name__ == "__main__":
    logger.info("\n=== Starting SQL Agent Workflow ===")
    prompt = "What is the top 10 countries by new partner signups for April 2025"
    logger.info(f"Input prompt: {prompt}")
    
    # Initialize state
    initial_state = {
        "prompt": prompt,
        "sql_query": "",
        "verification_result": "",
        "matches_intent": False,
        "results": "",
        "error": "",
        "attempt": 0,
        "syntax_validation_passed": False,
        "explain_output": "",
        "improved_prompt": "",
        "error_message": "",
        "progress": 0
    }
    
    # Run the workflow with recursion limit as safety measure
    logger.info("\n=== Executing Workflow ===")
    result = app.invoke(initial_state, config={"recursion_limit": 50})
    
    # Print and log results
    logger.info("\n=== Final Results ===")
    logger.info(f"Query Results:\n{result['results']}")
    print("\nFinal Results:")
    print("=============")
    print(result["results"])
    