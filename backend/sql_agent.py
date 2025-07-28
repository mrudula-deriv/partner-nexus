from utils import get_openai_client, get_supabase_client
import json, datetime, os
import sqlglot
from sqlglot.errors import ParseError

from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver
from tabulate import tabulate
from typing import TypedDict, Annotated, Sequence, Optional, Callable

from supabase import create_client, Client
from logging_config import LoggingConfig
from schema_manager import read_schema_metadata, schema_dict_to_chunks
from vector_store import retrieve_context, initialize_vector_store
from progress_manager import ProgressManager, SQLProgressStages, ProgressCallback

# Create logger
logger = LoggingConfig('sql_agent').setup_logger()

# Initialize clients
supabase_client: Client = get_supabase_client()
llm = get_openai_client()

# Initialize vector store
try:
    vectorstore = initialize_vector_store(supabase_client)
    logger.info("Vector store initialized successfully")
except Exception as e:
    logger.warning(f"Failed to initialize vector store: {str(e)}")
    logger.warning("SQL Agent will work without vector store features")
    vectorstore = None

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
    """Get relevant table information for the prompt."""
    return retrieve_context(prompt)

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

def generate_sql_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None, progress_manager: Optional[ProgressManager] = None, llm: ChatOpenAI = llm) -> AgentState:
    """Generate SQL query from natural language input."""
    if progress_manager:
        progress_manager.update_progress("Generating SQL query...", SQLProgressStages.GENERATE_SQL, progress_callback)
    
    logger.info("\n=== Generating SQL Query ===")
    logger.info(f"Input prompt: {state['prompt']}")

    schema_chunks = schema_dict_to_chunks(read_schema_metadata(supabase_client))
    
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
    
    logger.info(f"Generated SQL (cleaned):\n{cleaned_sql}")
    return {"sql_query": cleaned_sql, "progress": SQLProgressStages.GENERATE_SQL}

def verify_intent_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None, progress_manager: Optional[ProgressManager] = None, llm: ChatOpenAI = llm) -> AgentState:
    """Verify if the SQL query matches the original intent."""
    if progress_manager:
        progress_manager.update_progress("Verifying query intent...", SQLProgressStages.VERIFY_INTENT, progress_callback)
    
    logger.info("\n=== Verifying SQL Intent ===")
    logger.info(f"SQL to verify:\n{state['sql_query']}")
    
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
        "progress": SQLProgressStages.VERIFY_INTENT
    }

def validate_sql_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None, progress_manager: Optional[ProgressManager] = None, llm: ChatOpenAI = llm) -> AgentState:
    """Validate the SQL query."""
    if progress_manager:
        progress_manager.update_progress("Validating SQL syntax...", SQLProgressStages.VALIDATE_SQL, progress_callback)
    
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
                    progress_callback("SQL validation failed, correcting syntax...", SQLProgressStages.VALIDATE_SQL)
                return {
                    "syntax_validation_passed": False,
                    "error_message": result_data,
                    "progress": SQLProgressStages.VALIDATE_SQL
                }

            # Otherwise, assume it's valid EXPLAIN output
            if progress_callback:
                progress_callback("SQL validation passed", SQLProgressStages.VALIDATE_SQL)
                #validate_values = supabase_client.rpc("validate_query_categorical_values", {"query_text": state["sql_query"]}).execute()
            return {
                "syntax_validation_passed": True,
                "explain_output": result_data,
                "progress": SQLProgressStages.VALIDATE_SQL
            }

        else:
            logger.warning("Validation returned empty data.")
            if progress_callback:
                progress_callback("SQL validation failed - no output", SQLProgressStages.VALIDATE_SQL)
            return {
                "syntax_validation_passed": False,
                "error_message": "No output returned from Supabase RPC.",
                "progress": SQLProgressStages.VALIDATE_SQL
            }

    except Exception as e:
        logger.error(f"Unexpected error during SQL validation: {str(e)}")
        if progress_callback:
            progress_callback(f"SQL validation error: {str(e)}", SQLProgressStages.VALIDATE_SQL)
        return {
            "syntax_validation_passed": False,
            "error_message": str(e),
            "progress": SQLProgressStages.VALIDATE_SQL
        }

def correct_sql_node(state: AgentState) -> AgentState:
    """Correct the SQL query based on verification results."""
    logger.info("\n=== Correcting SQL Query ===")
    
    # Increment attempt counter
    current_attempt = state.get("attempt", 0) + 1
    logger.info(f"Correction attempt: {current_attempt}")

    improved_prompt = state["improved_prompt"]

    return {"prompt": improved_prompt}
    
def correct_syntax_node(state: AgentState, llm: ChatOpenAI = llm) -> AgentState:
    """Correct the SQL query based on verification results."""
    logger.info("\n=== Correcting SQL Query ===")
    logger.info(f"SQL to correct:\n{state['sql_query']}")

    current_attempt = state.get("attempt", 0) + 1

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
    return {"sql_query": cleaned_sql}

def execute_query_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None, progress_manager: Optional[ProgressManager] = None, supabase_client: Client = supabase_client) -> AgentState:
    """Execute the SQL query and return results."""
    if progress_manager:
        progress_manager.update_progress("Executing query...", SQLProgressStages.EXECUTE_QUERY, progress_callback)
    
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
                progress_callback("Query executed - no results found", SQLProgressStages.EXECUTE_QUERY)
            return {"results": "✅ Query ran successfully, but no results were found.", "progress": SQLProgressStages.EXECUTE_QUERY}

        columns = list(results[0].keys())
        rows = [list(row.values()) for row in results]

        summary = f"✅ Query successful. Retrieved {len(results)} row(s).\n"
        table = tabulate(rows, headers=columns, tablefmt="pretty")
        
        logger.info(f"Query executed successfully. Retrieved {len(results)} rows")
        if progress_callback:
            progress_callback(f"Query executed - found {len(results)} rows", SQLProgressStages.EXECUTE_QUERY)
        return {"results": summary + "\n" + table, "progress": SQLProgressStages.EXECUTE_QUERY}

    except Exception as e:
        error_msg = f"❌ Query failed:\n{str(e)}"
        logger.error(f"Query execution failed: {str(e)}")
        if progress_callback:
            progress_callback(f"Query execution failed: {str(e)}", SQLProgressStages.EXECUTE_QUERY)
        return {"error": error_msg, "progress": SQLProgressStages.EXECUTE_QUERY}

def format_response_node(state: AgentState, progress_callback: Optional[ProgressCallback] = None, progress_manager: Optional[ProgressManager] = None) -> AgentState:
    """Format the final response."""
    if progress_manager:
        progress_manager.update_progress("Formatting results...", SQLProgressStages.FORMAT_RESPONSE, progress_callback)
    
    logger.info("\n=== Formatting Final Response ===")
    if state.get("error"):
        logger.info("Formatting error response")
        return {"results": state["error"], "progress": SQLProgressStages.FORMAT_RESPONSE}
    logger.info("Formatting successful response")
    state["progress"] = SQLProgressStages.FORMAT_RESPONSE
    return state

def should_retry(state: AgentState) -> bool:
    """Determine if we should retry the query generation."""
    current_attempt = state.get("attempt", 0)
    matches_intent = state.get("matches_intent", False) 
    syntax_validation_passed = state.get("syntax_validation_passed", False)
    should_retry = not matches_intent and not syntax_validation_passed and current_attempt < 3
    
    logger.info(f"\n=== Retry Decision ===")
    logger.info(f"Intent match: {matches_intent}")
    logger.info(f"Should retry: {should_retry}")
    
    if current_attempt >= 3:
        logger.warning("Maximum retry attempts reached. Proceeding to execution.")
    
    return should_retry

def create_workflow(progress_callback: Optional[ProgressCallback] = None, is_analytics_workflow: bool = False) -> StateGraph:
    """Create and return a workflow with optional progress callback"""
    workflow = StateGraph(AgentState)

    # Initialize progress manager
    progress_manager = ProgressManager(is_sub_workflow=is_analytics_workflow)

    # Add nodes with progress manager
    workflow.add_node("generate_sql", lambda x: generate_sql_node(x, progress_callback, progress_manager))
    workflow.add_node("verify_intent", lambda x: verify_intent_node(x, progress_callback, progress_manager))
    workflow.add_node("validate_sql", lambda x: validate_sql_node(x, progress_callback, progress_manager))
    workflow.add_node("correct_sql", correct_sql_node)
    workflow.add_node("correct_syntax", correct_syntax_node)
    workflow.add_node("execute_query", lambda x: execute_query_node(x, progress_callback, progress_manager))
    workflow.add_node("format_response", lambda x: format_response_node(x, progress_callback, progress_manager))

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
    