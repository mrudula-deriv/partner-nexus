import psycopg2
from dotenv import load_dotenv
import os
import json
from psycopg2.extras import RealDictCursor
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from tabulate import tabulate
from typing import TypedDict, Annotated, Sequence
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver
import logging
from logging.handlers import RotatingFileHandler
import datetime

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


def get_multi_schema_metadata(schemas: list[str]):
    query = """
    SELECT table_schema, table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = ANY(%s)
    ORDER BY table_schema, table_name, ordinal_position;
    """
    cursor.execute(query, (schemas,))
    rows = cursor.fetchall()

    schema_dict = {}
    for row in rows:
        schema = row['table_schema']
        table = row['table_name']
        key = f"{schema}.{table}"
        if key not in schema_dict:
            schema_dict[key] = []
        schema_dict[key].append((row['column_name'], row['data_type']))
    
    # Create a directory for storing metadata if it doesn't exist
    if not os.path.exists('metadata'):
        os.makedirs('metadata')
    
    # Write schema data to JSON file
    json_file_path = 'metadata/schema_metadata.json'
    with open(json_file_path, 'w') as f:
        json.dump(schema_dict, f, indent=4)
    
    return json_file_path

def read_schema_metadata():
    """Read schema metadata from JSON file."""
    json_file_path = 'metadata/schema_metadata.json'
    if not os.path.exists(json_file_path):
        schema_file = get_multi_schema_metadata(['partner', 'client'])
    
    with open(json_file_path, 'r') as f:
        schema_dict = json.load(f)
    return schema_dict

# Get schema metadata
schema_dict = read_schema_metadata()
table_info = "\n".join(
    [f"{table}: {', '.join([f'{col} ({dtype})' for col, dtype in cols])}" for table, cols in schema_dict.items()]
)

# Define the state type
class AgentState(TypedDict):
    prompt: str
    sql_query: str
    verification_result: str
    matches_intent: bool
    results: str
    error: str
    attempt: int

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

def generate_sql_node(state: AgentState) -> AgentState:
    """Generate SQL query from natural language input."""
    logger.info("\n=== Generating SQL Query ===")
    logger.info(f"Input prompt: {state['prompt']}")
    
    # Configure ChatOpenAI with environment variables
    llm_config = {
        'temperature': 0, 
        'model_name': OPENAI_MODEL_NAME
    }
    if API_BASE_URL:
        llm_config['base_url'] = API_BASE_URL
    
    llm = ChatOpenAI(**llm_config)
    
    system_prompt = f"""You are a master SQL query generator specialized in Business Intelligence and KPI reporting. You are experienced in generating SQL queries for complex business requirements. You understand the business requirements and can generate the correct and constrained SQL query.

 Available Schema:
   {table_info}

Strictly return ONLY the SQL query, DO NOT include any other text, markdown or other formatting. The query should be complete and executable."""
    
    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=state["prompt"])
    ])
    
    # Clean the SQL query to remove any markdown formatting
    cleaned_sql = clean_sql_query(response.content)
    
    logger.info(f"Generated SQL (raw):\n{response.content}")
    logger.info(f"Generated SQL (cleaned):\n{cleaned_sql}")
    return {"sql_query": cleaned_sql}

def verify_intent_node(state: AgentState) -> AgentState:
    """Verify if the SQL query matches the original intent."""
    logger.info("\n=== Verifying SQL Intent ===")
    logger.info(f"SQL to verify:\n{state['sql_query']}")
    
    # Configure ChatOpenAI with environment variables
    llm_config = {
        'temperature': 0, 
        'model_name': OPENAI_MODEL_NAME
    }
    if API_BASE_URL:
        llm_config['base_url'] = API_BASE_URL
    
    llm = ChatOpenAI(**llm_config)
    
    system_prompt = f"""You are a SQL query interpreter and validator. Your task is to:
    1. Translate the SQL query into natural language
    2. Compare it with the original question and make sure the meaning is exactly the same
    3. SHOULD check if the query returns exactly what is asked for in the original question
    4. Verify the query against the available schema
    5. Check for potential issues in:
       - Table and column selection
       - Data type compatibility
       - Join conditions
       - Aggregation methods
       - Time period handling
       - Business logic interpretation
       - SQL syntax issues like ambiguous column references
    
    Available Schema:
    {table_info}
    
    CRITICAL: If there are ANY SQL syntax issues (like ambiguous column references, missing table aliases, etc.), 
    you MUST mark INTENT MATCH as False so the query gets corrected.
    
    Format your response as:
    SQL MEANING: [natural language translation of the SQL]
    INTENT MATCH: [True/False - False if there are ANY syntax issues or schema problems]
    SCHEMA VERIFICATION: [List any schema-related issues including syntax problems]
    BUSINESS LOGIC: [Check if the query correctly implements the business requirements]
    SUGGESTIONS: [Specific improvements if needed]"""
    
    user_prompt = f"""Original Question: {state["prompt"]}

SQL Query: {state["sql_query"]}

Please analyze if the SQL query:
1. Correctly captures the intent of the original question
2. Uses the appropriate tables and columns from the schema
3. Implements the business logic correctly
4. Handles time periods and aggregations appropriately
5. Has NO SQL syntax issues (like ambiguous column references)"""

    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt)
    ])
    
    # Check for intent match AND make sure no syntax issues are mentioned
    response_content = response.content.lower()
    intent_match_true = "intent match: true" in response_content
    has_ambiguity_issues = any(keyword in response_content for keyword in [
        "ambiguous", "ambiguity", "syntax", "alias", "clarify", "specify which"
    ])
    
    # If there are ambiguity issues mentioned, override the intent match
    matches_intent = intent_match_true and not has_ambiguity_issues
    
    logger.info(f"Verification result:\n{response.content}")
    logger.info(f"Intent match from response: {intent_match_true}")
    logger.info(f"Has ambiguity issues: {has_ambiguity_issues}")
    logger.info(f"Final intent match: {matches_intent}")
    
    return {
        "verification_result": response.content,
        "matches_intent": matches_intent
    }

def correct_sql_node(state: AgentState) -> AgentState:
    """Correct the SQL query based on verification results."""
    logger.info("\n=== Correcting SQL Query ===")
    logger.info(f"Current SQL:\n{state['sql_query']}")
    logger.info(f"Verification feedback:\n{state['verification_result']}")
    
    # Increment attempt counter
    current_attempt = state.get("attempt", 0) + 1
    logger.info(f"Correction attempt: {current_attempt}")
    
    # Configure ChatOpenAI with environment variables
    llm_config = {
        'temperature': 0, 
        'model_name': OPENAI_MODEL_NAME
    }
    if API_BASE_URL:
        llm_config['base_url'] = API_BASE_URL
    
    llm = ChatOpenAI(**llm_config)
    
    system_prompt = f"""You are a SQL query corrector. Your task is to:
    1. Analyze the verification results
    2. Identify the issues that need to be fixed
    3. Generate a corrected SQL query that addresses all issues
    
    Available Schema:
    {table_info}
    
    CRITICAL SQL SYNTAX FIXES:
    - For ambiguous column references, use explicit table aliases (e.g., conv.partner_id instead of partner_id)
    - Ensure all column references are properly qualified with table aliases
    - Fix any JOIN conditions that might be unclear
    - Ensure GROUP BY and ORDER BY clauses use the same aliases as SELECT
    
    IMPORTANT: Return ONLY the corrected SQL query. Do not include any explanations, labels, or additional text.
    The response should be a single SQL query that can be executed directly."""
    
    user_prompt = f"""Original Question: {state["prompt"]}

Original SQL Query: {state["sql_query"]}

Verification Results:
{state["verification_result"]}

Please provide ONLY the corrected SQL query that fixes all identified issues, especially any ambiguous column references by using proper table aliases."""

    response = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content=user_prompt)
    ])
    
    # Clean the corrected SQL query to remove any markdown formatting
    cleaned_sql = clean_sql_query(response.content)
    
    logger.info(f"Corrected SQL (raw):\n{response.content}")
    logger.info(f"Corrected SQL (cleaned):\n{cleaned_sql}")
    return {
        "sql_query": cleaned_sql,
        "attempt": current_attempt
    }

def execute_query_node(state: AgentState) -> AgentState:
    """Execute the SQL query and return results."""
    logger.info("\n=== Executing SQL Query ===")
    logger.info(f"Executing:\n{state['sql_query']}")
    
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(state["sql_query"])
                results = cursor.fetchall()

                if not results:
                    logger.info("Query executed successfully but returned no results")
                    return {"results": "✅ Query ran successfully, but no results were found."}

                columns = list(results[0].keys())
                rows = [list(row.values()) for row in results]

                summary = f"✅ Query successful. Retrieved {len(results)} row(s).\n"
                table = tabulate(rows, headers=columns, tablefmt="pretty")
                
                logger.info(f"Query executed successfully. Retrieved {len(results)} rows")
                return {"results": summary + "\n" + table}

    except Exception as e:
        error_msg = f"❌ Query failed:\n{str(e)}"
        logger.error(f"Query execution failed: {str(e)}")
        return {"error": error_msg}

def format_response_node(state: AgentState) -> AgentState:
    """Format the final response."""
    logger.info("\n=== Formatting Final Response ===")
    if state.get("error"):
        logger.info("Formatting error response")
        return {"results": state["error"]}
    logger.info("Formatting successful response")
    return state

def should_retry(state: AgentState) -> bool:
    """Determine if we should retry the query generation."""
    current_attempt = state.get("attempt", 0)
    matches_intent = state.get("matches_intent", False)
    should_retry = not matches_intent and current_attempt < 3
    
    logger.info(f"\n=== Retry Decision ===")
    logger.info(f"Current attempt: {current_attempt}")
    logger.info(f"Intent match: {matches_intent}")
    logger.info(f"Should retry: {should_retry}")
    
    if current_attempt >= 3:
        logger.warning("Maximum retry attempts reached. Proceeding to execution.")
    
    return should_retry

# Create the graph
workflow = StateGraph(AgentState)

# Add nodes
workflow.add_node("generate_sql", generate_sql_node)
workflow.add_node("verify_intent", verify_intent_node)
workflow.add_node("correct_sql", correct_sql_node)
workflow.add_node("execute_query", execute_query_node)
workflow.add_node("format_response", format_response_node)

# Define the flow
workflow.add_edge("generate_sql", "verify_intent")
workflow.add_conditional_edges(
    "verify_intent",
    should_retry,
    {
        True: "correct_sql",
        False: "execute_query"
    }
)
workflow.add_edge("correct_sql", "verify_intent")
workflow.add_edge("execute_query", "format_response")
workflow.add_edge("format_response", END)

# Set the entry point
workflow.set_entry_point("generate_sql")

# Compile the graph
app = workflow.compile()

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
        "attempt": 0
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
