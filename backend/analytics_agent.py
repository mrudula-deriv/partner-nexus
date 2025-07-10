import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import json
import matplotlib
matplotlib.use('Agg')  # Use non-GUI backend for headless operation
import matplotlib.pyplot as plt
import seaborn as sns
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from typing import TypedDict, Annotated, Sequence, Dict, Any, List, Optional, Callable
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langgraph.checkpoint.memory import MemorySaver
import logging
from logging.handlers import RotatingFileHandler
import datetime
from scipy import stats as scipy_stats  # Renamed to avoid conflict
import warnings
import base64
from io import BytesIO
warnings.filterwarnings('ignore')

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
        'logs/analytics_agent.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Create logger
    logger = logging.getLogger('analytics_agent')
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Create logger
logger = setup_logger()

# Load environment variables from .env
load_dotenv()

# OpenAI configuration from environment variables
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
API_BASE_URL = os.getenv('API_BASE_URL')
OPENAI_MODEL_NAME = os.getenv('OPENAI_MODEL_NAME')

# Define the state type
class AnalyticsState(TypedDict):
    original_query: str
    sql_results: str
    parsed_data: Dict[str, Any]
    statistical_analysis: Dict[str, Any]
    trends_analysis: Dict[str, Any]
    insights: List[str]
    visualizations: List[str]
    visualization_images: List[Dict[str, str]]  # New field for base64 images
    formatted_response: str
    error: str

def parse_data_node(state: AnalyticsState, progress_callback: Optional[ProgressCallback] = None) -> AnalyticsState:
    """Parse SQL results into structured data for analysis."""
    if progress_callback:
        progress_callback("Parsing SQL results...", 60)
    
    logger.info("\n=== Parsing Data ===")
    logger.info(f"Original query: {state['original_query']}")
    logger.info(f"SQL results preview: {state['sql_results'][:200]}...")
    
    try:
        # Extract tabular data from SQL results
        lines = state['sql_results'].split('\n')
        logger.info(f"Total lines in SQL results: {len(lines)}")
        
        # Debug: Print all lines to see the actual format
        for i, line in enumerate(lines):
            logger.info(f"Line {i}: '{line}'")
        
        # Find the start of the table (usually after "Query successful" message)
        table_start = 0
        for i, line in enumerate(lines):
            if '✅ Query successful' in line:
                table_start = i + 1
                break
        
        logger.info(f"Table starts at line: {table_start}")
        
        # Extract table content (everything after the success message)
        table_lines = []
        for line in lines[table_start:]:
            stripped_line = line.strip()
            if stripped_line and not stripped_line.startswith('✅') and not stripped_line.startswith('❌'):
                table_lines.append(stripped_line)
        
        logger.info(f"Table lines found: {len(table_lines)}")
        for i, line in enumerate(table_lines):
            logger.info(f"Table line {i}: '{line}'")
        
        if not table_lines:
            logger.warning("No table data found in SQL results")
            return {"parsed_data": {"error": "No tabular data found"}}
        
        # More robust table parsing
        # Look for lines with "|" (table rows) and lines with "+" and "-" (separators)
        header_line = None
        data_rows = []
        headers = []
        
        # Strategy 1: Find first line with "|" that looks like headers
        for i, line in enumerate(table_lines):
            if '|' in line and '+' not in line and '-' not in line:
                # This could be headers or data
                cells = [cell.strip() for cell in line.split('|') if cell.strip()]
                
                if not header_line and cells:  # First data-like line is probably headers
                    header_line = line
                    headers = cells
                    logger.info(f"Found headers: {headers}")
                elif headers and len(cells) == len(headers):  # Subsequent lines with same column count are data
                    data_rows.append(cells)
                    logger.info(f"Found data row: {cells}")
            elif '|' in line and headers:
                # This line has '|' and we already have headers, so it might be data
                # even if it has '+' or '-' (like timezone data)
                # Check if it's actually data by seeing if it has substantial content
                content_check = line.replace('+', '').replace('-', '').replace('|', '').strip()
                if content_check and len(content_check) >= 10:  # Has substantial content = data
                    cells = [cell.strip() for cell in line.split('|') if cell.strip()]
                    if len(cells) == len(headers):
                        data_rows.append(cells)
                        logger.info(f"Found data row (with special chars): {cells}")
        
        # Strategy 2: If Strategy 1 didn't work, try alternative parsing
        if not headers or not data_rows:
            logger.info("Strategy 1 failed, trying alternative parsing...")
            
            # Look for typical ASCII table patterns
            # Separator lines have '+' and '-' but are mostly made of these characters
            # Data lines have '|' and actual data
            separator_indices = []
            for i, line in enumerate(table_lines):
                # A separator line should be mostly '+' and '-' characters
                # and not contain alphanumeric content between pipes
                if '+' in line and '-' in line:
                    # Check if this is actually a separator (not data with timezone +00:00)
                    # Remove the '+' and '-' and '|' characters and see what's left
                    content_check = line.replace('+', '').replace('-', '').replace('|', '').strip()
                    if not content_check or len(content_check) < 5:  # Very little content = separator
                        separator_indices.append(i)
            
            logger.info(f"Separator indices: {separator_indices}")
            
            if len(separator_indices) >= 2:
                # Extract headers (line between first two separators)
                header_line_idx = separator_indices[0] + 1
                if header_line_idx < len(table_lines):
                    header_line = table_lines[header_line_idx]
                    headers = [col.strip() for col in header_line.split('|') if col.strip()]
                    logger.info(f"Alt strategy headers: {headers}")
                
                # Extract data rows (between second separator and last separator or end)
                data_start = separator_indices[1] + 1 if len(separator_indices) > 1 else separator_indices[0] + 2
                data_end = separator_indices[-1] if len(separator_indices) > 2 else len(table_lines)
                
                for i in range(data_start, data_end):
                    if i < len(table_lines):
                        line = table_lines[i]
                        # Only process lines that have '|' but are not separators
                        if '|' in line:
                            # Double-check this isn't a separator
                            content_check = line.replace('+', '').replace('-', '').replace('|', '').strip()
                            if content_check and len(content_check) >= 5:  # Has actual content
                                row = [cell.strip() for cell in line.split('|') if cell.strip()]
                                if headers and len(row) == len(headers):
                                    data_rows.append(row)
                                    logger.info(f"Alt strategy data row: {row}")
        
        # Strategy 3: If still no luck, try simple "|" split on all non-separator lines
        if not headers or not data_rows:
            logger.info("Both strategies failed, trying simple split...")
            
            all_data_lines = []
            for line in table_lines:
                if '|' in line and not ('+' in line and '-' in line):
                    cells = [cell.strip() for cell in line.split('|') if cell.strip()]
                    if cells:
                        all_data_lines.append(cells)
            
            if all_data_lines:
                # First line is headers, rest are data
                headers = all_data_lines[0]
                data_rows = all_data_lines[1:] if len(all_data_lines) > 1 else []
                logger.info(f"Simple strategy headers: {headers}")
                logger.info(f"Simple strategy data rows: {len(data_rows)}")
        
        logger.info(f"Final result - Headers: {headers}, Data rows: {len(data_rows)}")
        
        if not headers:
            logger.warning("No headers found")
            return {"parsed_data": {"error": "No headers found"}}
        
        if len(data_rows) == 0:
            logger.warning("No data rows found")
            return {"parsed_data": {"error": "No data rows found"}}
        
        parsed_data = {
            "headers": headers,
            "rows": data_rows,
            "row_count": len(data_rows),
            "column_count": len(headers)
        }
        
        logger.info(f"Successfully parsed {len(data_rows)} rows with {len(headers)} columns")
        return {"parsed_data": parsed_data}
        
    except Exception as e:
        error_msg = f"Error parsing data: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg, "parsed_data": {"error": error_msg}}

def statistical_analysis_node(state: AnalyticsState, progress_callback: Optional[ProgressCallback] = None) -> AnalyticsState:
    """Perform statistical analysis on the parsed data."""
    if progress_callback:
        progress_callback("Performing statistical analysis...", 70)
    
    logger.info("\n=== Statistical Analysis ===")
    
    try:
        if "error" in state["parsed_data"]:
            return {"statistical_analysis": {"error": "Cannot perform analysis on invalid data"}}
        
        data = state["parsed_data"]
        headers = data["headers"]
        rows = data["rows"]
        
        if not rows:
            return {"statistical_analysis": {"error": "No data rows to analyze"}}
        
        # Convert to DataFrame for easier analysis
        df = pd.DataFrame(rows, columns=headers)
        
        # Identify ID/categorical columns first (before numeric classification)
        id_columns = []
        for col in df.columns:
            col_lower = col.lower()
            # Detect ID columns by name patterns
            if any(pattern in col_lower for pattern in ['_id', 'id_', 'partner_id', 'client_id', 'account_id', 'user_id']):
                id_columns.append(col)
            # Also check if values look like IDs (all integers with high uniqueness)
            elif col_lower.endswith('id') or col_lower.startswith('id'):
                id_columns.append(col)
        
        # Identify numeric columns
        numeric_columns = []
        for col in df.columns:
            if col not in id_columns:  # Skip ID columns
                try:
                    # Try to convert to numeric
                    numeric_data = pd.to_numeric(df[col], errors='coerce')
                    # If more than 50% of values are numeric, consider it a numeric column
                    if numeric_data.notna().sum() > len(df) * 0.5:
                        numeric_columns.append(col)
                        df[col] = numeric_data
                except:
                    continue
        
        # Identify date columns
        date_columns = []
        for col in df.columns:
            if col not in numeric_columns and col not in id_columns:
                try:
                    # Try to parse as dates
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    if df[col].notna().sum() > len(df) * 0.5:
                        date_columns.append(col)
                except:
                    continue
        
        # Categorical columns include ID columns and any remaining non-numeric, non-date columns
        categorical_columns = id_columns + [col for col in df.columns if col not in numeric_columns and col not in date_columns and col not in id_columns]
        
        # Perform statistical analysis
        stats_analysis = {
            "total_records": len(df),
            "numeric_columns": numeric_columns,
            "date_columns": date_columns,
            "categorical_columns": categorical_columns
        }
        
        # Numeric statistics
        if numeric_columns:
            numeric_stats = {}
            for col in numeric_columns:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    numeric_stats[col] = {
                        "mean": float(col_data.mean()),
                        "median": float(col_data.median()),
                        "std": float(col_data.std()) if len(col_data) > 1 else 0,
                        "min": float(col_data.min()),
                        "max": float(col_data.max()),
                        "sum": float(col_data.sum()),
                        "count": int(col_data.count()),
                        "null_count": int(df[col].isna().sum())
                    }
            stats_analysis["numeric_statistics"] = numeric_stats
        
        # Categorical statistics
        if categorical_columns:
            categorical_stats = {}
            for col in categorical_columns:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    value_counts = col_data.value_counts()
                    categorical_stats[col] = {
                        "unique_values": int(col_data.nunique()),
                        "most_frequent": str(value_counts.index[0]) if len(value_counts) > 0 else None,
                        "most_frequent_count": int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                        "value_distribution": {str(k): int(v) for k, v in value_counts.head(10).items()}
                    }
            stats_analysis["categorical_statistics"] = categorical_stats
        
        logger.info(f"Statistical analysis completed for {len(headers)} columns")
        logger.info(f"Column classification - Categorical: {categorical_columns}, Numeric: {numeric_columns}, Date: {date_columns}")
        return {"statistical_analysis": stats_analysis}
        
    except Exception as e:
        error_msg = f"Error in statistical analysis: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg, "statistical_analysis": {"error": error_msg}}

def trends_analysis_node(state: AnalyticsState, progress_callback: Optional[ProgressCallback] = None) -> AnalyticsState:
    """Analyze trends in the data."""
    if progress_callback:
        progress_callback("Analyzing trends...", 80)
    
    logger.info("\n=== Trends Analysis ===")
    
    try:
        if "error" in state["parsed_data"] or "error" in state["statistical_analysis"]:
            return {"trends_analysis": {"error": "Cannot analyze trends on invalid data"}}
        
        data = state["parsed_data"]
        stats = state["statistical_analysis"]
        
        trends_analysis = {
            "temporal_trends": [],
            "volume_trends": [],
            "performance_trends": [],
            "comparative_trends": []
        }
        
        # Create DataFrame
        df = pd.DataFrame(data["rows"], columns=data["headers"])
        
        # Convert numeric columns
        numeric_columns = stats.get("numeric_columns", [])
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert date columns
        date_columns = stats.get("date_columns", [])
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Temporal trends analysis
        if date_columns and numeric_columns:
            for date_col in date_columns:
                for num_col in numeric_columns:
                    try:
                        temporal_df = df[[date_col, num_col]].dropna()
                        if len(temporal_df) > 2:
                            temporal_df = temporal_df.sort_values(date_col)
                            
                            # Calculate period-over-period changes
                            temporal_df['change'] = temporal_df[num_col].pct_change()
                            avg_change = temporal_df['change'].mean()
                            
                            # Volatility analysis
                            volatility = temporal_df['change'].std()
                            
                            # Recent vs historical comparison
                            if len(temporal_df) >= 4:
                                mid_point = len(temporal_df) // 2
                                recent_avg = temporal_df[num_col].iloc[mid_point:].mean()
                                historical_avg = temporal_df[num_col].iloc[:mid_point].mean()
                                change_pct = ((recent_avg - historical_avg) / historical_avg) * 100 if historical_avg != 0 else 0
                                
                                trends_analysis["temporal_trends"].append({
                                    "metric": num_col,
                                    "time_column": date_col,
                                    "average_period_change": float(avg_change) if not np.isnan(avg_change) else 0,
                                    "volatility": float(volatility) if not np.isnan(volatility) else 0,
                                    "recent_vs_historical_change": float(change_pct),
                                    "trend_strength": "high" if abs(change_pct) > 20 else "medium" if abs(change_pct) > 10 else "low"
                                })
                    except Exception as e:
                        logger.warning(f"Could not analyze temporal trend for {date_col} vs {num_col}: {str(e)}")
        
        # Volume trends (identify highest/lowest performers)
        if numeric_columns:
            for col in numeric_columns:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    # Identify top and bottom performers
                    sorted_data = col_data.sort_values(ascending=False)
                    top_20_pct = sorted_data.head(max(1, len(sorted_data) // 5))
                    bottom_20_pct = sorted_data.tail(max(1, len(sorted_data) // 5))
                    
                    trends_analysis["volume_trends"].append({
                        "metric": col,
                        "top_20_percent_avg": float(top_20_pct.mean()),
                        "bottom_20_percent_avg": float(bottom_20_pct.mean()),
                        "performance_gap": float(top_20_pct.mean() - bottom_20_pct.mean()),
                        "concentration_ratio": float(top_20_pct.sum() / col_data.sum() * 100) if col_data.sum() != 0 else 0
                    })
        
        # Comparative trends (between categories if categorical data exists)
        categorical_columns = stats.get("categorical_columns", [])
        if categorical_columns and numeric_columns:
            for cat_col in categorical_columns:
                for num_col in numeric_columns:
                    try:
                        grouped = df.groupby(cat_col)[num_col].agg(['mean', 'sum', 'count']).reset_index()
                        grouped = grouped.dropna()
                        
                        if len(grouped) > 1:
                            # Find best and worst performing categories
                            best_performer = grouped.loc[grouped['mean'].idxmax()]
                            worst_performer = grouped.loc[grouped['mean'].idxmin()]
                            
                            trends_analysis["comparative_trends"].append({
                                "category_column": cat_col,
                                "metric_column": num_col,
                                "best_performer": {
                                    "category": str(best_performer[cat_col]),
                                    "average": float(best_performer['mean']),
                                    "total": float(best_performer['sum']),
                                    "count": int(best_performer['count'])
                                },
                                "worst_performer": {
                                    "category": str(worst_performer[cat_col]),
                                    "average": float(worst_performer['mean']),
                                    "total": float(worst_performer['sum']),
                                    "count": int(worst_performer['count'])
                                },
                                "performance_ratio": float(best_performer['mean'] / worst_performer['mean']) if worst_performer['mean'] != 0 else float('inf')
                            })
                    except Exception as e:
                        logger.warning(f"Could not analyze comparative trend for {cat_col} vs {num_col}: {str(e)}")
        
        logger.info("Trends analysis completed")
        return {"trends_analysis": trends_analysis}
        
    except Exception as e:
        error_msg = f"Error in trends analysis: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg, "trends_analysis": {"error": error_msg}}

def generate_insights_node(state: AnalyticsState, progress_callback: Optional[ProgressCallback] = None) -> AnalyticsState:
    """Generate business insights using LLM based on the analysis."""
    if progress_callback:
        progress_callback("Generating business insights...", 90)
    
    logger.info("\n=== Generating Insights ===")
    
    try:
        # Configure ChatOpenAI with environment variables
        llm_config = {
            'temperature': 0.3, 
            'model_name': OPENAI_MODEL_NAME
        }
        if API_BASE_URL:
            llm_config['base_url'] = API_BASE_URL
        
        llm = ChatOpenAI(**llm_config)
        
        # Prepare analysis summary for LLM
        analysis_summary = {
            "original_query": state["original_query"],
            "data_overview": {
                "total_records": state["statistical_analysis"].get("total_records", 0),
                "columns": state["parsed_data"].get("headers", [])
            },
            "statistical_findings": state["statistical_analysis"],
            "trends_analysis": state["trends_analysis"]
        }
        
        system_prompt = """You are a senior business analyst specializing in partner affiliate programs, trading platforms, and financial services. 

Your task is to analyze data and provide actionable business insights for stakeholders in the affiliate marketing and trading industry.

Focus on:
1. **Business Impact**: What do these numbers mean for business performance?
2. **Actionable Insights**: Specific recommendations based on the data
3. **Risk Identification**: Potential issues or opportunities revealed
4. **Performance Indicators**: Key metrics that stand out
5. **Strategic Implications**: How this affects business strategy

Format your response as a list of clear, concise insights. Each insight should:
- Start with a clear statement
- Include supporting data/evidence
- Suggest actionable next steps when relevant

Be specific about partner performance, activation rates, geographic trends, commission patterns, and client acquisition if relevant to the data."""

        user_prompt = f"""Analyze the following data and provide business insights:

Original Query: {state["original_query"]}

Data Analysis Results:
{json.dumps(analysis_summary, indent=2, default=str)}

Provide 5-10 actionable business insights based on this analysis."""

        response = llm.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ])
        
        # Parse insights from response
        insights_text = response.content
        insights = [insight.strip() for insight in insights_text.split('\n') if insight.strip() and not insight.strip().startswith('#')]
        
        # Filter out empty insights and headers
        filtered_insights = []
        for insight in insights:
            if len(insight) > 20 and not insight.startswith('**') and not insight.startswith('##'):
                # Clean up bullet points and numbering
                cleaned_insight = insight.lstrip('- ').lstrip('• ').lstrip('* ')
                if cleaned_insight and len(cleaned_insight) > 10:
                    filtered_insights.append(cleaned_insight)
        
        logger.info(f"Generated {len(filtered_insights)} business insights")
        return {"insights": filtered_insights}
        
    except Exception as e:
        error_msg = f"Error generating insights: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg, "insights": [f"Error generating insights: {error_msg}"]}

def create_visualizations_node(state: AnalyticsState, progress_callback: Optional[ProgressCallback] = None) -> AnalyticsState:
    """Create visualizations based on the data and analysis."""
    if progress_callback:
        progress_callback("Creating visualizations...", 95)
    
    logger.info("\n=== Creating Visualizations ===")
    
    try:
        if "error" in state["parsed_data"]:
            return {
                "visualizations": ["Error: Cannot create visualizations with invalid data"],
                "visualization_images": []
            }
        
        data = state["parsed_data"]
        stats = state["statistical_analysis"]
        query = state["original_query"].lower()
        
        # Create DataFrame
        df = pd.DataFrame(data["rows"], columns=data["headers"])
        
        # Convert numeric columns
        numeric_columns = stats.get("numeric_columns", [])
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert date columns
        date_columns = stats.get("date_columns", [])
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        visualizations = []
        visualization_images = []
        
        # Set style for better looking plots
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        def plot_to_base64(fig):
            """Convert matplotlib figure to base64 string"""
            buffer = BytesIO()
            fig.savefig(buffer, format='png', dpi=300, bbox_inches='tight')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            buffer.close()
            plt.close(fig)
            return image_base64
        
        # Determine what type of visualization to create based on query
        categorical_columns = stats.get("categorical_columns", [])
        
        # Detect comparison queries
        is_comparison = any(keyword in query for keyword in [
            "compare", "comparison", "vs", "versus", "top 5", "top 10", "rank", "between"
        ])
        
        # Detect distribution queries (should be pie chart for categorical distribution)
        is_distribution = any(keyword in query for keyword in [
            "distribution by", "breakdown by", "split by", "share by"
        ])
        
        # Use LLM to intelligently choose chart type unless explicitly specified
        explicit_chart_type = None
        if "bar chart" in query or "bar graph" in query:
            explicit_chart_type = "bar"
        elif "pie chart" in query or "pie graph" in query:
            explicit_chart_type = "pie"
        elif "line chart" in query or "line graph" in query:
            explicit_chart_type = "line"
        elif any(keyword in query for keyword in [
            "as a chart", "as chart", "give chart", "show chart", "create a chart", 
            "make a chart", "generate a chart", "visualize this", "visualize it",
            "plot this", "plot it", "graph this", "graph it", "chart it", 
            "create visualization", "show visualization", "display chart",
            "draw a chart", "create graph", "make graph"
        ]):
            # User explicitly wants some kind of chart but didn't specify type
            explicit_chart_type = "auto"
        elif is_comparison and (date_columns or len(categorical_columns) > 0):
            # Comparison queries with time/categories usually need visualization
            explicit_chart_type = "auto"
        elif is_distribution and categorical_columns:
            explicit_chart_type = "pie"  # Distribution by category = pie chart
        
        # Prepare data structure summary for LLM calls
        data_summary = {
            "total_rows": len(df),
            "categorical_columns": categorical_columns,
            "numeric_columns": numeric_columns,
            "date_columns": date_columns,
            "sample_data": df.head(3).to_dict('records') if len(df) > 0 else []
        }
        
        # If user explicitly requested a chart, skip the LLM decision about necessity
        should_create_visualization = True
        if explicit_chart_type:
            logger.info(f"User explicitly requested chart type: {explicit_chart_type}")
            should_create_visualization = True
        else:
            # Only ask LLM if visualization is needed when user didn't explicitly request one
            logger.info("No explicit chart request - using LLM to determine if visualization is necessary")
            
            # Configure ChatOpenAI
            llm_config = {
                'temperature': 0.1, 
                'model_name': OPENAI_MODEL_NAME
            }
            if API_BASE_URL:
                llm_config['base_url'] = API_BASE_URL
            
            llm = ChatOpenAI(**llm_config)
            
            system_prompt = """You are a data visualization expert. Your task is to determine if a chart/visualization would add meaningful value to the user's analysis.

Consider:
1. Would a chart make the data clearer or more actionable?
2. Is the data suitable for visualization (not too complex, reasonable number of data points)?
3. Does the question benefit from visual representation?
4. Are there clear patterns/trends that visualization would highlight?

Some cases where visualization is NOT needed:
- Simple ranking/listing queries where the data is already clear
- Very few data points where a table is sufficient  
- Complex multi-metric data with vastly different scales
- Statistical summary questions where insights are the main value

Respond with only "YES" if visualization would be valuable, or "NO" if insights alone are sufficient."""

            user_prompt = f"""Should a visualization be created for this analysis?

User Query: "{state['original_query']}"

Data Structure:
- Categorical columns: {categorical_columns}
- Numeric columns: {numeric_columns}  
- Date columns: {date_columns}
- Total rows: {len(df)}

Sample data: {data_summary['sample_data']}

Would a chart add meaningful value to this analysis?"""

            try:
                response = llm.invoke([
                    SystemMessage(content=system_prompt),
                    HumanMessage(content=user_prompt)
                ])
                
                should_visualize = response.content.strip().upper()
                logger.info(f"LLM decision on visualization: {should_visualize}")
                
                should_create_visualization = (should_visualize == "YES")
                    
            except Exception as e:
                logger.error(f"Error getting visualization decision from LLM: {str(e)}")
                # Default to not creating visualization on error
                should_create_visualization = False
        
        # Check if we should proceed with visualization
        if not should_create_visualization:
            logger.info("Skipping visualization based on decision logic")
            return {
                "visualizations": ["No visualization needed - insights provide sufficient analysis"],
                "visualization_images": []
            }
        
        # Determine chart type if user requested "auto" or didn't specify a type but wants a chart
        if explicit_chart_type == "auto" or not explicit_chart_type:
            # Let LLM intelligently choose the best chart type
            logger.info("Using LLM to choose appropriate chart type")
            
            llm_config = {
                'temperature': 0.1, 
                'model_name': OPENAI_MODEL_NAME
            }
            if API_BASE_URL:
                llm_config['base_url'] = API_BASE_URL
            
            llm = ChatOpenAI(**llm_config)
            
            chart_system_prompt = """You are a data visualization expert. Your task is to choose the most appropriate chart type based on the user's query and data structure.

Available chart types:
- "bar": Simple bar chart for categorical vs numeric comparisons
- "grouped_bar": Multi-metric comparisons or grouped categorical data
- "pie": Part-to-whole relationships, distribution across categories
- "line": Time series data, trends over time
- "multi_line": Comparing multiple entities over time

Consider:
1. The question being asked (comparison, distribution, trend, etc.)
2. Data structure (categorical, numeric, date columns)
3. Best practices for data visualization
4. Distribution BY category = pie chart (not histogram)

Respond with only the chart type name."""

            chart_user_prompt = f"""Choose the best chart type for this analysis:

User Query: "{state['original_query']}"

Data Structure:
- Categorical columns: {categorical_columns}
- Numeric columns: {numeric_columns}  
- Date columns: {date_columns}
- Total rows: {len(df)}
- Is comparison query: {is_comparison}
- Is distribution query: {is_distribution}

Sample data: {data_summary['sample_data']}

What is the most appropriate chart type?"""

            try:
                response = llm.invoke([
                    SystemMessage(content=chart_system_prompt),
                    HumanMessage(content=chart_user_prompt)
                ])
                
                chart_type = response.content.strip().lower()
                logger.info(f"LLM selected chart type: {chart_type}")
                
                # Validate LLM response
                valid_types = ["bar", "grouped_bar", "pie", "line", "multi_line"]
                if chart_type not in valid_types:
                    logger.warning(f"Invalid chart type from LLM: {chart_type}, falling back to auto-detect")
                    chart_type = "pie" if is_distribution and categorical_columns else "bar"  # Better fallback
                    
            except Exception as e:
                logger.error(f"Error getting chart type from LLM: {str(e)}")
                # Fallback to simple logic
                if is_distribution and categorical_columns:
                    chart_type = "pie"
                elif date_columns and numeric_columns:
                    chart_type = "multi_line" if is_comparison and categorical_columns else "line"
                elif categorical_columns and len(numeric_columns) >= 2:
                    chart_type = "grouped_bar"
                elif categorical_columns and numeric_columns:
                    chart_type = "bar"
                else:
                    chart_type = "bar"
        else:
            chart_type = explicit_chart_type
        
        if not chart_type:
            return {
                "visualizations": ["No specific chart type requested"],
                "visualization_images": []
            }
        
        # Create the requested visualization
        if chart_type in ["bar", "grouped_bar"] and categorical_columns and numeric_columns:
            # Check for multi-metric comparison (1 categorical, multiple numeric columns)
            if chart_type == "grouped_bar" or (is_comparison and len(numeric_columns) >= 2):
                cat_col = categorical_columns[0]  # e.g., country
                
                # Create grouped bar chart for multiple metrics per category
                if len(numeric_columns) >= 2:
                    logger.info(f"Creating multi-metric grouped bar chart for {cat_col} with metrics: {numeric_columns}")
                    
                    # Check if metrics have vastly different scales (STRICTER: 10x instead of 100x)
                    scale_check_failed = False
                    metric_ranges = {}
                    
                    for col in numeric_columns:
                        col_data = pd.to_numeric(df[col], errors='coerce').dropna()
                        if len(col_data) > 0:
                            metric_ranges[col] = {
                                'min': col_data.min(),
                                'max': col_data.max(),
                                'mean': col_data.mean()
                            }
                    
                    # Check if any two metrics have different scales (>10x difference - MUCH STRICTER)
                    if len(metric_ranges) >= 2:
                        scale_ratios = []
                        metric_names = list(metric_ranges.keys())
                        for i in range(len(metric_names)):
                            for j in range(i+1, len(metric_names)):
                                metric1 = metric_names[i]
                                metric2 = metric_names[j]
                                if metric_ranges[metric2]['mean'] != 0:
                                    ratio = metric_ranges[metric1]['mean'] / metric_ranges[metric2]['mean']
                                    scale_ratios.append(abs(ratio))
                        
                        max_ratio = max(scale_ratios) if scale_ratios else 1
                        if max_ratio > 10:  # STRICTER: 10x instead of 100x
                            scale_check_failed = True
                            logger.info(f"Scale difference too large ({max_ratio:.1f}x) - skipping grouped bar chart")
                    
                    if not scale_check_failed:
                        # Prepare data for grouped bar chart
                        chart_data = df[[cat_col] + numeric_columns].set_index(cat_col)
                        
                        if len(chart_data) > 0 and len(chart_data) <= 15:  # Reasonable number of categories
                            fig, ax = plt.subplots(figsize=(14, 8))
                            
                            # Create grouped bar chart
                            chart_data.plot(kind='bar', ax=ax, width=0.8, rot=45)
                            
                            # Improve formatting
                            ax.set_title(f'Comparison: {" vs ".join([col.replace("_", " ").title() for col in numeric_columns])} by {cat_col.replace("_", " ").title()}')
                            ax.set_xlabel(cat_col.replace("_", " ").title())
                            ax.set_ylabel('Values')
                            ax.legend(title='Metrics', labels=[col.replace("_", " ").title() for col in numeric_columns], 
                                    bbox_to_anchor=(1.05, 1), loc='upper left')
                            ax.grid(True, alpha=0.3)
                            plt.xticks(rotation=45, ha='right')
                            plt.tight_layout()
                            
                            image_base64 = plot_to_base64(fig)
                            chart_title = f"{' vs '.join([col.replace('_', ' ').title() for col in numeric_columns])} by {cat_col.replace('_', ' ').title()}"
                            visualizations.append(f"Grouped bar chart: {chart_title}")
                            visualization_images.append({
                                "title": chart_title,
                                "type": "grouped_bar_chart", 
                                "data": image_base64
                            })
                            
                            logger.info(f"Successfully created multi-metric grouped bar chart")
                        else:
                            logger.warning(f"Too many categories ({len(chart_data)}) for grouped bar chart, falling back to simple bar")
                            chart_type = "bar"
                    else:
                        # Scales too different - create simple bar chart for primary metric
                        logger.info("Scale difference too large - creating simple bar chart for primary metric instead")
                        chart_type = "bar"
            
        # Simple bar chart (handle both original "bar" requests and fallbacks from grouped_bar)
        if chart_type == "bar" and categorical_columns and numeric_columns:
            cat_col = categorical_columns[0]
            num_col = numeric_columns[0]
            
            cat_df = df[[cat_col, num_col]].dropna()
            if len(cat_df) > 1:
                grouped = cat_df.groupby(cat_col)[num_col].sum().sort_values(ascending=False)
                
                if len(grouped) > 0 and len(grouped) <= 20:
                    fig, ax = plt.subplots(figsize=(12, 6))
                    bars = ax.bar(range(len(grouped)), grouped.values, color=plt.cm.Set3(np.linspace(0, 1, len(grouped))))
                    
                    # Create better title for comparison queries
                    if is_comparison:
                        ax.set_title(f'Top {len(grouped)} {cat_col.replace("_", " ").title()} by {num_col.replace("_", " ").title()}')
                    else:
                        ax.set_title(f'{num_col} by {cat_col}')
                    
                    ax.set_xlabel(cat_col.replace("_", " ").title())
                    ax.set_ylabel(num_col.replace("_", " ").title())
                    ax.set_xticks(range(len(grouped)))
                    ax.set_xticklabels(grouped.index, rotation=45, ha='right')
                    
                    # Add value labels on bars
                    for i, bar in enumerate(bars):
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width()/2., height,
                               f'{height:.0f}', ha='center', va='bottom')
                    
                    ax.grid(True, alpha=0.3)
                    plt.tight_layout()
                    
                    image_base64 = plot_to_base64(fig)
                    chart_title = f"Top {len(grouped)} {cat_col.replace('_', ' ').title()} by {num_col.replace('_', ' ').title()}" if is_comparison else f"{num_col} by {cat_col}"
                    visualizations.append(f"Bar chart: {chart_title}")
                    visualization_images.append({
                        "title": chart_title,
                        "type": "bar_chart",
                        "data": image_base64
                    })
        
        elif chart_type in ["line", "multi_line"] and numeric_columns:
            # Check if we have comparison data (multiple series)
            if chart_type == "multi_line" or (is_comparison and categorical_columns and date_columns):
                # Multi-line chart for comparison over time
                cat_col = categorical_columns[0]  # e.g., partner_country
                date_col = date_columns[0]        # e.g., signup_month
                num_col = numeric_columns[0]      # e.g., partner_signups
                
                time_df = df[[cat_col, date_col, num_col]].dropna()
                logger.info(f"Multi-line chart data: {len(time_df)} rows for {cat_col} vs {date_col} vs {num_col}")
                
                if len(time_df) > 1:
                    fig, ax = plt.subplots(figsize=(14, 8))
                    
                    # Plot a line for each category (country)
                    categories = sorted(time_df[cat_col].unique())
                    colors = plt.cm.Set1(np.linspace(0, 1, len(categories)))
                    
                    logger.info(f"Creating lines for categories: {categories}")
                    
                    for i, category in enumerate(categories):
                        cat_data = time_df[time_df[cat_col] == category].sort_values(date_col)
                        logger.info(f"Category {category}: {len(cat_data)} data points")
                        
                        if len(cat_data) > 0:
                            ax.plot(cat_data[date_col], cat_data[num_col], 
                                   marker='o', linewidth=3, markersize=8, 
                                   label=f'{category}', color=colors[i])
                    
                    ax.set_title(f'{num_col.replace("_", " ").title()} Comparison by {cat_col.replace("_", " ").title()} Over Time')
                    ax.set_xlabel(date_col.replace("_", " ").title())
                    ax.set_ylabel(num_col.replace("_", " ").title())
                    
                    # Force Y-axis to start at 0 for proper context
                    ax.set_ylim(bottom=0)
                    
                    ax.legend(title=cat_col.replace("_", " ").title(), bbox_to_anchor=(1.05, 1), loc='upper left')
                    plt.xticks(rotation=45)
                    ax.grid(True, alpha=0.3)
                    plt.tight_layout()
                    
                    image_base64 = plot_to_base64(fig)
                    chart_title = f"{num_col.replace('_', ' ').title()} Comparison by {cat_col.replace('_', ' ').title()} Over Time"
                    visualizations.append(f"Multi-line chart: {chart_title}")
                    visualization_images.append({
                        "title": chart_title,
                        "type": "multi_line_chart",
                        "data": image_base64
                    })
                    
                    # Don't fall back to simple line chart if multi-line was successful
                    logger.info(f"Successfully created multi-line chart with {len(categories)} lines")
                    
            # Simple line chart (only if multi-line wasn't created)
            elif chart_type == "line" and date_columns and len(visualization_images) == 0:
                date_col = date_columns[0]
                num_col = numeric_columns[0]
                
                time_df = df[[date_col, num_col]].dropna()
                if len(time_df) > 1:
                    time_df = time_df.sort_values(date_col)
                    
                    fig, ax = plt.subplots(figsize=(12, 6))
                    ax.plot(time_df[date_col], time_df[num_col], marker='o', linewidth=2, markersize=6)
                    ax.set_title(f'{num_col} over Time')
                    ax.set_xlabel(date_col)
                    ax.set_ylabel(num_col)
                    
                    # Force Y-axis to start at 0 for proper context
                    ax.set_ylim(bottom=0)
                    
                    plt.xticks(rotation=45)
                    ax.grid(True, alpha=0.3)
                    plt.tight_layout()
                    
                    image_base64 = plot_to_base64(fig)
                    visualizations.append(f"Line graph: {num_col} over time")
                    visualization_images.append({
                        "title": f"{num_col} over Time",
                        "type": "line_chart",
                        "data": image_base64
                    })
        
        elif chart_type == "pie" and categorical_columns and numeric_columns:
            # Pie chart for categorical data
            cat_col = categorical_columns[0]
            num_col = numeric_columns[0]
            
            cat_df = df[[cat_col, num_col]].dropna()
            if len(cat_df) > 1:
                grouped = cat_df.groupby(cat_col)[num_col].sum().sort_values(ascending=False)
                
                if len(grouped) > 0:
                    # Handle large number of categories by grouping smaller ones as "Others"
                    if len(grouped) > 15:
                        # Take top 10 and group rest as "Others"
                        top_categories = grouped.head(10)
                        others_total = grouped.tail(len(grouped) - 10).sum()
                        
                        # Create new series with "Others" category
                        final_data = top_categories.copy()
                        if others_total > 0:
                            final_data['Others'] = others_total
                    else:
                        # Use all categories if 15 or fewer
                        final_data = grouped
                    
                    fig, ax = plt.subplots(figsize=(12, 10))
                    colors = plt.cm.Set3(np.linspace(0, 1, len(final_data)))
                    
                    # Create pie chart with improved formatting
                    wedges, texts, autotexts = ax.pie(final_data.values, labels=final_data.index, 
                                                    autopct='%1.1f%%', startangle=90, colors=colors,
                                                    textprops={'fontsize': 10})
                    
                    # Improve label formatting for better readability
                    for text in texts:
                        text.set_fontsize(9)
                    for autotext in autotexts:
                        autotext.set_color('white')
                        autotext.set_fontweight('bold')
                        autotext.set_fontsize(9)
                    
                    ax.set_title(f'{num_col.replace("_", " ").title()} Distribution by {cat_col.replace("_", " ").title()}', 
                               fontsize=14, fontweight='bold', pad=20)
                    plt.tight_layout()
                    
                    image_base64 = plot_to_base64(fig)
                    chart_title = f"{num_col.replace('_', ' ').title()} Distribution by {cat_col.replace('_', ' ').title()}"
                    visualizations.append(f"Pie chart: {chart_title}")
                    visualization_images.append({
                        "title": chart_title,
                        "type": "pie_chart",
                        "data": image_base64
                    })
                    
                    logger.info(f"Successfully created pie chart with {len(final_data)} categories")
                else:
                    logger.warning("No data available for pie chart")
            else:
                logger.warning("Insufficient data for pie chart (need more than 1 row)")
        
        if not visualizations:
            visualizations = [f"Could not create {chart_type} chart with available data"]
            visualization_images = []
        
        logger.info(f"Created {len(visualization_images)} visualization(s) of type: {chart_type}")
        return {
            "visualizations": visualizations,
            "visualization_images": visualization_images
        }
        
    except Exception as e:
        error_msg = f"Error creating visualizations: {str(e)}"
        logger.error(error_msg)
        return {
            "error": error_msg, 
            "visualizations": [error_msg],
            "visualization_images": []
        }

def format_response_node(state: AnalyticsState, progress_callback: Optional[ProgressCallback] = None) -> AnalyticsState:
    """Format the final analytics response."""
    if progress_callback:
        progress_callback("Formatting analytics report...", 100)
    
    logger.info("\n=== Formatting Final Response ===")
    
    try:
        if state.get("error"):
            return {"formatted_response": f"❌ Analytics Error: {state['error']}"}
        
        # Build simplified response with only requested sections
        response_parts = []
        
        # Header with Query
        response_parts.append("📊 ANALYTICS REPORT")
        response_parts.append("=" * 50)
        response_parts.append(f"Query: {state['original_query']}\n")
        
        # Trends Analysis (enhanced)
        if state.get("trends_analysis") and "error" not in state["trends_analysis"]:
            trends = state["trends_analysis"]
            response_parts.append("📊 TRENDS ANALYSIS")
            response_parts.append("-" * 20)
            
            # Temporal trends
            if trends.get("temporal_trends"):
                response_parts.append("Time-based Trends:")
                for trend in trends["temporal_trends"]:
                    change_direction = "increase" if trend['recent_vs_historical_change'] > 0 else "decrease"
                    response_parts.append(f"  • {trend['metric']}: {trend['trend_strength']} {change_direction} ({trend['recent_vs_historical_change']:+.1f}% change from historical average)")
                    if trend.get('volatility', 0) > 0.2:
                        response_parts.append(f"    - High volatility detected ({trend['volatility']:.2f})")
                response_parts.append("")
            
            # Volume trends
            if trends.get("volume_trends"):
                response_parts.append("Performance Distribution:")
                for trend in trends["volume_trends"]:
                    response_parts.append(f"  • {trend['metric']}:")
                    response_parts.append(f"    - Top 20% average: {trend['top_20_percent_avg']:.2f}")
                    response_parts.append(f"    - Bottom 20% average: {trend['bottom_20_percent_avg']:.2f}")
                    response_parts.append(f"    - Performance gap: {trend['performance_gap']:.2f}")
                    response_parts.append(f"    - Top 20% concentration: {trend['concentration_ratio']:.1f}% of total")
                response_parts.append("")
            
            # Comparative trends (enhanced)
            if trends.get("comparative_trends"):
                response_parts.append("Comparative Performance by Category:")
                for trend in trends["comparative_trends"]:
                    best = trend["best_performer"]
                    worst = trend["worst_performer"]
                    ratio = trend.get("performance_ratio", 1)
                    
                    response_parts.append(f"  • {trend['metric_column']} by {trend['category_column']}:")
                    response_parts.append(f"    - Best performer: {best['category']} (avg: {best['average']:.2f}, total: {best['total']:.2f})")
                    response_parts.append(f"    - Worst performer: {worst['category']} (avg: {worst['average']:.2f}, total: {worst['total']:.2f})")
                    
                    if ratio != float('inf'):
                        response_parts.append(f"    - Performance ratio: {ratio:.2f}x difference")
                    else:
                        response_parts.append(f"    - Performance ratio: Significantly higher")
                response_parts.append("")
        
        # Business Insights (cleaned up formatting)
        if state.get("insights") and state["insights"]:
            response_parts.append("💡 BUSINESS INSIGHTS")
            response_parts.append("-" * 22)
            
            for i, insight in enumerate(state["insights"][:8], 1):  # Limit to top 8
                # Clean up markdown formatting
                cleaned_insight = insight.replace('**', '').replace('*', '')
                # Remove numbering if it exists at the start
                cleaned_insight = cleaned_insight.strip()
                if cleaned_insight.startswith(f'{i}.'):
                    cleaned_insight = cleaned_insight[2:].strip()
                elif cleaned_insight[0].isdigit() and cleaned_insight[1] == '.':
                    cleaned_insight = cleaned_insight[2:].strip()
                
                response_parts.append(f"{i}. {cleaned_insight}")
            response_parts.append("")
        
        # Footer
        response_parts.append("=" * 50)
        response_parts.append(f"Report generated at: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        formatted_response = "\n".join(response_parts)
        
        logger.info("Analytics response formatted successfully")
        return {"formatted_response": formatted_response}
        
    except Exception as e:
        error_msg = f"Error formatting response: {str(e)}"
        logger.error(error_msg)
        return {"error": error_msg, "formatted_response": f"❌ Error formatting analytics response: {error_msg}"}

# Create the graph
workflow = StateGraph(AnalyticsState)

# Add nodes with different names to avoid state key conflicts
workflow.add_node("parse_data_node", parse_data_node)
workflow.add_node("statistical_analysis_node", statistical_analysis_node)
workflow.add_node("trends_analysis_node", trends_analysis_node)
workflow.add_node("generate_insights_node", generate_insights_node)
workflow.add_node("create_visualizations_node", create_visualizations_node)
workflow.add_node("format_response_node", format_response_node)

# Define the flow
workflow.add_edge("parse_data_node", "statistical_analysis_node")
workflow.add_edge("statistical_analysis_node", "trends_analysis_node")
workflow.add_edge("trends_analysis_node", "generate_insights_node")
workflow.add_edge("generate_insights_node", "create_visualizations_node")
workflow.add_edge("create_visualizations_node", "format_response_node")
workflow.add_edge("format_response_node", END)

# Set the entry point
workflow.set_entry_point("parse_data_node")

# Compile the graph
app = workflow.compile()

# Helper function to run analytics on SQL results
def analyze_sql_results(original_query: str, sql_results: str, progress_callback: Optional[ProgressCallback] = None) -> str:
    """
    Main function to run analytics on SQL results.
    
    Args:
        original_query: The original user question
        sql_results: The formatted results from SQL agent
        progress_callback: Optional callback for progress updates
    
    Returns:
        Formatted analytics report
    """
    logger.info(f"\n=== Starting Analytics for Query: {original_query} ===")
    
    # Initialize state
    initial_state = {
        "original_query": original_query,
        "sql_results": sql_results,
        "parsed_data": {},
        "statistical_analysis": {},
        "trends_analysis": {},
        "insights": [],
        "visualizations": [],
        "visualization_images": [],
        "formatted_response": "",
        "error": ""
    }
    
    # Run the workflow
    try:
        # Create workflow with progress callback
        workflow = StateGraph(AnalyticsState)

        # Add nodes with different names to avoid state key conflicts
        workflow.add_node("parse_data_node", lambda state: parse_data_node(state, progress_callback))
        workflow.add_node("statistical_analysis_node", lambda state: statistical_analysis_node(state, progress_callback))
        workflow.add_node("trends_analysis_node", lambda state: trends_analysis_node(state, progress_callback))
        workflow.add_node("generate_insights_node", lambda state: generate_insights_node(state, progress_callback))
        workflow.add_node("create_visualizations_node", lambda state: create_visualizations_node(state, progress_callback))
        workflow.add_node("format_response_node", lambda state: format_response_node(state, progress_callback))

        # Define the flow
        workflow.add_edge("parse_data_node", "statistical_analysis_node")
        workflow.add_edge("statistical_analysis_node", "trends_analysis_node")
        workflow.add_edge("trends_analysis_node", "generate_insights_node")
        workflow.add_edge("generate_insights_node", "create_visualizations_node")
        workflow.add_edge("create_visualizations_node", "format_response_node")
        workflow.add_edge("format_response_node", END)

        # Set the entry point
        workflow.set_entry_point("parse_data_node")

        # Compile and run
        app = workflow.compile()
        result = app.invoke(initial_state, config={"recursion_limit": 20})
        
        logger.info("Analytics workflow completed successfully")
        return result["formatted_response"]
    except Exception as e:
        error_msg = f"Analytics workflow failed: {str(e)}"
        logger.error(error_msg)
        return f"❌ Analytics Error: {error_msg}" 