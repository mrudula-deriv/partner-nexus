"""
Flask Backend API for SQL and Analytics Agents

This API provides endpoints to test:
1. SQL Agent only
2. SQL + Analytics combined workflow
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from sql_agent import app as sql_app
from analytics_agent import analyze_sql_results
import logging
import traceback
import json
from psycopg2.extras import RealDictCursor
from collections import OrderedDict
import datetime

# Import screener functions
from screener import (
    get_available_metrics, 
    get_filter_options, 
    fetch_metrics_data, 
    create_filter_query,
    get_db_connection
)

# Set up Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "message": "API is running"})

@app.route('/sql-agent', methods=['POST'])
def test_sql_agent():
    """Test SQL Agent only"""
    try:
        data = request.get_json()
        
        if not data or 'query' not in data:
            return jsonify({
                "error": "Missing 'query' field in request body"
            }), 400
        
        user_query = data['query'].strip()
        
        if not user_query:
            return jsonify({
                "error": "Query cannot be empty"
            }), 400
        
        logger.info(f"Processing SQL query: {user_query}")
        
        # Initialize SQL agent state
        sql_initial_state = {
            "prompt": user_query,
            "sql_query": "",
            "verification_result": "",
            "matches_intent": False,
            "results": "",
            "error": "",
            "attempt": 0
        }
        
        # Run SQL agent
        sql_result = sql_app.invoke(sql_initial_state, config={"recursion_limit": 50})
        
        response = {
            "success": True,
            "query": user_query,
            "sql_query": sql_result.get("sql_query", ""),
            "results": sql_result.get("results", ""),
            "error": sql_result.get("error", ""),
            "verification_result": sql_result.get("verification_result", ""),
            "attempts": sql_result.get("attempt", 0)
        }
        
        logger.info("SQL agent completed successfully")
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in SQL agent: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

@app.route('/analytics-agent', methods=['POST'])
def test_analytics_agent():
    """Test SQL + Analytics combined workflow"""
    try:
        data = request.get_json()
        
        if not data or 'query' not in data:
            return jsonify({
                "error": "Missing 'query' field in request body"
            }), 400
        
        user_query = data['query'].strip()
        
        if not user_query:
            return jsonify({
                "error": "Query cannot be empty"
            }), 400
        
        logger.info(f"Processing combined SQL + Analytics query: {user_query}")
        
        # Step 1: Run SQL Agent
        logger.info("Step 1: Running SQL Agent...")
        sql_initial_state = {
            "prompt": user_query,
            "sql_query": "",
            "verification_result": "",
            "matches_intent": False,
            "results": "",
            "error": "",
            "attempt": 0
        }
        
        sql_result = sql_app.invoke(sql_initial_state, config={"recursion_limit": 50})
        sql_output = sql_result["results"]
        
        # Check if SQL agent failed
        if sql_result.get("error"):
            return jsonify({
                "success": False,
                "error": f"SQL Agent failed: {sql_result['error']}",
                "sql_query": sql_result.get("sql_query", ""),
                "sql_results": sql_output
            }), 500
        
        # Step 2: Run Analytics Agent
        logger.info("Step 2: Running Analytics Agent...")
        analytics_result = analyze_sql_results(user_query, sql_output)
        
        # Extract visualization images from analytics result if available
        visualization_images = []
        try:
            # The analytics agent returns a formatted string, but we need to get the raw result
            # Let's modify this to get the full analytics state
            from analytics_agent import app as analytics_app
            
            analytics_initial_state = {
                "original_query": user_query,
                "sql_results": sql_output,
                "parsed_data": {},
                "statistical_analysis": {},
                "patterns_identified": [],
                "trends_analysis": {},
                "insights": [],
                "visualizations": [],
                "visualization_images": [],
                "formatted_response": "",
                "error": ""
            }
            
            full_analytics_result = analytics_app.invoke(analytics_initial_state, config={"recursion_limit": 20})
            visualization_images = full_analytics_result.get("visualization_images", [])
            
        except Exception as e:
            logger.warning(f"Could not extract visualization images: {str(e)}")
        
        response = {
            "success": True,
            "query": user_query,
            "sql_query": sql_result.get("sql_query", ""),
            "sql_results": sql_output,
            "analytics_report": analytics_result,
            "visualization_images": visualization_images,
            "sql_attempts": sql_result.get("attempt", 0),
            "verification_result": sql_result.get("verification_result", "")
        }
        
        logger.info("Combined workflow completed successfully")
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error in combined workflow: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": f"Internal server error: {str(e)}"
        }), 500

@app.route('/screener/metrics', methods=['GET'])
def get_metrics():
    """Get available metrics for the screener"""
    try:
        metrics = get_available_metrics()
        return jsonify({
            "success": True,
            "metrics": metrics
        })
    except Exception as e:
        logger.error(f"Error getting metrics: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Failed to get metrics: {str(e)}"
        }), 500

@app.route('/screener/filters', methods=['GET'])
def get_filters():
    """Get available filter options for the screener"""
    try:
        filters = get_filter_options()
        return jsonify({
            "success": True,
            "filters": filters
        })
    except Exception as e:
        logger.error(f"Error getting filters: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Failed to get filters: {str(e)}"
        }), 500

@app.route('/screener/data', methods=['POST'])
def get_screener_data():
    """Get screener data based on selected metrics and filters"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "Request body is required"
            }), 400
        
        selected_metrics = data.get('metrics', [])
        filters = data.get('filters', {})
        
        if not selected_metrics:
            return jsonify({
                "error": "At least one metric must be selected"
            }), 400
        
        logger.info(f"Processing screener request with {len(selected_metrics)} metrics and {len(filters)} filter groups")
        
        # Create filter query
        where_clause, params = create_filter_query(filters)
        
        # Fetch data
        df = fetch_metrics_data(
            selected_metrics,
            where_clause,
            params,
            active_filters=filters
        )
        
        # Convert DataFrame to JSON-friendly format
        if not df.empty:
            # Convert DataFrame to dictionary format
            data_dict = {
                "columns": df.columns.tolist(),
                "data": df.to_dict('records')
            }
        else:
            data_dict = {
                "columns": [],
                "data": []
            }
        
        return jsonify({
            "success": True,
            "data": data_dict,
            "row_count": len(df) if not df.empty else 0
        })
        
    except Exception as e:
        logger.error(f"Error getting screener data: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": f"Failed to get screener data: {str(e)}"
        }), 500

@app.route('/live-screeners/screener1', methods=['POST'])
def get_screener1_data():
    """Get data for Live Screener 1 - Performance Overview"""
    try:
        data = request.get_json() or {}
        filters = data.get('filters', {})
        
        # Create filter query
        where_clause, params = create_filter_query(filters)
        
        # First table - Region/Plan overview with proper column ordering
        table1_metrics = [
            'Application Count',
            'First Activated Count - Signup',
            'First Activated Count - Deposit', 
            'First Activated Count - Traded',
            'First Activated Count - Earning'
        ]
        
        table1_data = fetch_metrics_data(
            selected_metrics=table1_metrics,
            where_clause=where_clause,
            params=params,
            group_by=['partner_region', 'partner_country', 'aff_type']  # Fixed order: Region → Country → Plan
        )
        
        # Second table - Platform/Event overview with proper column ordering
        table2_metrics = [
            'Application Count',
            'First Activated Count - Signup',
            'First Activated Count - Deposit',
            'First Activated Count - Traded', 
            'First Activated Count - Earning'
        ]
        
        table2_data = fetch_metrics_data(
            selected_metrics=table2_metrics,
            where_clause=where_clause,
            params=params,
            group_by=['partner_region', 'partner_country', 'partner_platform', 'attended_onboarding_event', 'aff_type']  # Fixed order
        )
        
        # Simple approach: Create ordered dictionaries correctly
        def reorder_dataframe_columns(df, desired_order):
            if df.empty:
                return []
            
            # Create a list of records with proper column order
            records = []
            for _, row in df.iterrows():
                record = {}
                # Add columns in desired order first
                for col in desired_order:
                    if col in df.columns:
                        record[col] = row[col]
                
                # Add any remaining columns
                for col in df.columns:
                    if col not in desired_order:
                        record[col] = row[col]
                
                records.append(record)
            
            return records
        
        # Define exact column order: Region → Country → Plan → [metrics]
        table1_order = ['Region', 'Country', 'Plan'] + table1_metrics
        table1_records = reorder_dataframe_columns(table1_data, table1_order)
        
        # Define exact column order for table2
        table2_order = ['Region', 'Country', 'Platform', 'Attended Event', 'Plan'] + table2_metrics
        table2_records = reorder_dataframe_columns(table2_data, table2_order)
        
        # Use json.dumps with ensure_ascii=False to maintain order, then parse back
        response_data = {
            "success": True,
            "table1": table1_records,
            "table2": table2_records
        }
        
        # Force proper JSON serialization to maintain key order
        json_str = json.dumps(response_data, ensure_ascii=False)
        return json_str, 200, {'Content-Type': 'application/json'}
        
    except Exception as e:
        logger.error(f"Error in screener1: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Failed to get screener1 data: {str(e)}"
        }), 500

@app.route('/live-screeners/screener2', methods=['POST'])
def get_screener2_data():
    """Get data for Live Screener 2 - Trend Analysis"""
    try:
        data = request.get_json() or {}
        filters = data.get('filters', {})
        date_range = data.get('date_range', 6)  # Default to 6 months
        date_filter_type = data.get('date_filter_type', 'rolling')
        specific_month = data.get('specific_month')
        specific_year = data.get('specific_year')
        start_month = data.get('start_month')
        end_month = data.get('end_month')
        
        logger.info(f"Screener2 request - filters: {filters}, date_range: {date_range}, date_filter_type: {date_filter_type}")
        
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Build the query to get monthly data
                base_query = """
                WITH monthly_data AS (
                    SELECT 
                        COALESCE(partner_region, 'Unknown') as region,
                        COALESCE(partner_country, 'Unknown') as country,
                        COALESCE(aff_type, 'Unknown') as plan,
                        TO_CHAR(DATE_TRUNC('month', date_joined), 'Mon YYYY') as month_label,
                        DATE_TRUNC('month', date_joined) as month_date,
                        COUNT(DISTINCT partner_id) as app_count,
                        COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL 
                              AND DATE_TRUNC('month', first_client_joined_date) = DATE_TRUNC('month', date_joined) 
                              THEN partner_id END) as activated_same_month
                    FROM partner.partner_info
                    WHERE is_internal = FALSE
                """
                
                # Initialize params list
                params = []
                
                # Add date condition based on filter type
                if date_filter_type == 'rolling':
                    base_query += " AND date_joined >= CURRENT_DATE - INTERVAL %s"
                    params.append(f"{date_range} months")
                elif date_filter_type == 'specific' and specific_month and specific_year:
                    base_query += " AND DATE_TRUNC('month', date_joined) = %s"
                    params.append(f"{specific_year}-{specific_month:02d}-01")
                elif date_filter_type == 'range' and start_month and end_month:
                    base_query += " AND DATE_TRUNC('month', date_joined) >= %s AND DATE_TRUNC('month', date_joined) <= %s"
                    params.extend([f"{start_month}-01", f"{end_month}-01"])
                else:
                    base_query += " AND date_joined >= CURRENT_DATE - INTERVAL %s"
                    params.append("6 months")
                
                # Add filter conditions
                where_clause, filter_params = create_filter_query(filters)
                if where_clause:
                    base_query += f" AND {where_clause}"
                    params.extend(filter_params)
                
                base_query += """
                    GROUP BY partner_region, partner_country, aff_type, DATE_TRUNC('month', date_joined)
                )
                SELECT 
                    region,
                    country,
                    plan,
                    month_label,
                    month_date,
                    app_count,
                    ROUND(
                        CAST(activated_same_month AS NUMERIC) / 
                        NULLIF(CAST(app_count AS NUMERIC), 0) * 100,
                        2
                    ) as activation_rate
                FROM monthly_data
                ORDER BY region, country, plan, month_date
                """
                
                logger.info(f"Executing screener2 query with params: {params}")
                cursor.execute(base_query, params)
                results = cursor.fetchall()
                
                # Process results to create pivot table structure
                pivot_data = {}
                months_set = set()
                
                # First pass: collect all months and organize data
                for row in results:
                    region_country_plan_key = f"{row['region']}|{row['country']}|{row['plan']}"
                    month_label = row['month_label']
                    months_set.add((row['month_date'], month_label))
                    
                    if region_country_plan_key not in pivot_data:
                        pivot_data[region_country_plan_key] = {
                            'Region': row['region'],
                            'Country': row['country'],
                            'Plan': row['plan']
                        }
                    
                    # Store both Application Count and Activation Rate for this month
                    pivot_data[region_country_plan_key][f"{month_label}_app_count"] = row['app_count']
                    pivot_data[region_country_plan_key][f"{month_label}_activation_rate"] = row['activation_rate']
                
                # Sort months chronologically
                sorted_months = sorted(months_set, key=lambda x: x[0])
                month_labels = [m[1] for m in sorted_months]
                
                # Create the final structured response
                formatted_data = []
                for key, row_data in sorted(pivot_data.items()):
                    formatted_row = {
                        'Region': row_data['Region'],
                        'Country': row_data['Country'],
                        'Plan': row_data['Plan']
                    }
                    
                    # Add Application Count columns for each month
                    for month_label in month_labels:
                        app_count_key = f"{month_label}_app_count"
                        formatted_row[f"App Count - {month_label}"] = row_data.get(app_count_key, 0)
                    
                    # Add Activation Rate columns for each month
                    for month_label in month_labels:
                        rate_key = f"{month_label}_activation_rate"
                        rate_value = row_data.get(rate_key, 0)
                        formatted_row[f"Act Rate - {month_label}"] = f"{rate_value:.2f}%" if rate_value else "0.00%"
                    
                    formatted_data.append(formatted_row)
                
                # Create column definitions for frontend
                columns = ['Region', 'Country', 'Plan']
                
                # Add Application Count columns
                for month_label in month_labels:
                    columns.append(f"App Count - {month_label}")
                
                # Add Activation Rate columns
                for month_label in month_labels:
                    columns.append(f"Act Rate - {month_label}")
                
                response_data = {
                    "success": True,
                    "trend_data": formatted_data,
                    "columns": columns,
                    "months": month_labels,
                    "total_records": len(formatted_data)
                }
                
                # Return with proper JSON serialization
                json_str = json.dumps(response_data, ensure_ascii=False, default=str)
                return json_str, 200, {'Content-Type': 'application/json'}
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in screener2: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": f"Failed to get screener2 data: {str(e)}"
        }), 500

@app.route('/live-screeners/screener3', methods=['POST'])
def get_screener3_data():
    """Get data for Live Screener 3 - Individual Partner
    Returns two tables:
    - Table 1: Partner overview by region/country/plan
    - Table 2: Individual partner details with activation metrics
    """
    try:
        data = request.get_json() or {}
        filters = data.get('filters', {})
        date_filters = data.get('date_filters', {})
        
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Table 1: Partner overview aggregated data
                query1 = """
                SELECT 
                    COALESCE(partner_region, 'Unknown') as "Region",
                    COALESCE(partner_country, 'Unknown') as "Country",
                    COALESCE(aff_type, 'Unknown') as "Plan",
                    COALESCE(partner_platform, 'Unknown') as "Partner Platform",
                    COALESCE(aff_type, 'Unknown') as "Affiliate Type",
                    COALESCE(partner_level::text, 'Unknown') as "Partner Level",
                    COALESCE(attended_onboarding_event::text, 'Unknown') as "Attended Onboarding Event",
                    COALESCE(earning_acquisition, 'Unknown') as "Earning Acquisition",
                    COUNT(DISTINCT partner_id) as "Partner Count"
                FROM partner.partner_info
                WHERE is_internal = FALSE
                """
                
                # Add date filters
                params1 = []
                
                # Single date filters - filter for records on or after the selected date
                if date_filters.get('date_joined'):
                    query1 += " AND date_joined >= %s"
                    params1.append(date_filters['date_joined'])
                
                if date_filters.get('first_client_joined_date'):
                    query1 += " AND first_client_joined_date >= %s"
                    params1.append(date_filters['first_client_joined_date'])
                
                if date_filters.get('first_client_deposit_date'):
                    query1 += " AND first_client_deposit_date >= %s"
                    params1.append(date_filters['first_client_deposit_date'])
                
                if date_filters.get('first_client_trade_date'):
                    query1 += " AND first_client_trade_date >= %s"
                    params1.append(date_filters['first_client_trade_date'])
                
                if date_filters.get('first_earning_date'):
                    query1 += " AND first_earning_date >= %s"
                    params1.append(date_filters['first_earning_date'])
                
                # Add other filters
                where_clause, filter_params = create_filter_query(filters)
                if where_clause:
                    query1 += f" AND {where_clause}"
                    params1.extend(filter_params)
                
                query1 += """
                GROUP BY partner_region, partner_country, aff_type, partner_platform, 
                         partner_level, attended_onboarding_event, earning_acquisition
                ORDER BY "Region", "Country", "Plan"
                """
                
                # Table 2: Individual partner details
                query2 = """
                SELECT 
                    partner_id as "Partner ID",
                    COALESCE(partner_country, 'Unknown') as "Country",
                    COALESCE(aff_type, 'Unknown') as "Aff Type",
                    CASE WHEN first_client_joined_date IS NOT NULL THEN 1 ELSE 0 END as "First Activated Count - Signup",
                    ROUND(
                        CAST(
                            EXTRACT(EPOCH FROM (first_client_joined_date::timestamp - date_joined::timestamp))::numeric / 86400
                        AS NUMERIC),
                        1
                    ) as "Median Time to Activation - Signup",
                    first_client_joined_date as "First Client Joined Date"
                FROM partner.partner_info
                WHERE is_internal = FALSE
                """
                
                # Add date filters for table 2
                params2 = []
                
                # Single date filters - filter for records on or after the selected date
                if date_filters.get('date_joined'):
                    query2 += " AND date_joined >= %s"
                    params2.append(date_filters['date_joined'])
                
                if date_filters.get('first_client_joined_date'):
                    query2 += " AND first_client_joined_date >= %s"
                    params2.append(date_filters['first_client_joined_date'])
                
                if date_filters.get('first_client_deposit_date'):
                    query2 += " AND first_client_deposit_date >= %s"
                    params2.append(date_filters['first_client_deposit_date'])
                
                if date_filters.get('first_client_trade_date'):
                    query2 += " AND first_client_trade_date >= %s"
                    params2.append(date_filters['first_client_trade_date'])
                
                if date_filters.get('first_earning_date'):
                    query2 += " AND first_earning_date >= %s"
                    params2.append(date_filters['first_earning_date'])
                
                # Add other filters
                if where_clause:
                    query2 += f" AND {where_clause}"
                    params2.extend(filter_params)
                
                query2 += " ORDER BY partner_id LIMIT 100"
                
                # Execute both queries
                cursor.execute(query1, params1)
                table1_results = cursor.fetchall()
                
                cursor.execute(query2, params2)
                table2_results = cursor.fetchall()
                
                # Format results
                def format_records(records):
                    formatted = []
                    for record in records:
                        formatted_record = {}
                        for key, value in record.items():
                            if isinstance(value, (datetime.date, datetime.datetime)):
                                formatted_record[key] = value.strftime('%Y-%m-%d') if value else None
                            elif key == "Median Time to Activation - Signup" and value is not None:
                                formatted_record[key] = f"{value:.1f} days" if value >= 0 else "-"
                            else:
                                formatted_record[key] = value
                        formatted.append(formatted_record)
                    return formatted
                
                response_data = {
                    "success": True,
                    "table1": format_records(table1_results),
                    "table2": format_records(table2_results),
                    "total_records": {
                        "table1": len(table1_results),
                        "table2": len(table2_results)
                    }
                }
                
                # Return with proper JSON serialization
                json_str = json.dumps(response_data, ensure_ascii=False, default=str)
                return json_str, 200, {'Content-Type': 'application/json'}
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in screener3: {str(e)}")
        return jsonify({
            "success": False,
            "error": f"Failed to get screener3 data: {str(e)}"
        }), 500

@app.route('/live-screeners/screener4', methods=['POST'])
def get_screener4_data():
    """Get data for Live Screener 4 - Cohort Analysis"""
    try:
        data = request.get_json() or {}
        filters = data.get('filters', {})
        breakdown_filter = data.get('breakdown_filter', 'partner_region')
        result_filter = data.get('result_filter', 'absolute')
        milestone_type = data.get('milestone_type', 'first_client_joined_date')
        date_range = data.get('date_range', 12)  # Default to 12 months
        date_filter_type = data.get('date_filter_type', 'rolling')
        specific_month = data.get('specific_month')
        specific_year = data.get('specific_year')
        start_month = data.get('start_month')
        end_month = data.get('end_month')
        cohort_type = data.get('cohort_type', 'forward')  # New parameter: 'forward' or 'reverse'
        
        logger.info(f"Screener4 request - breakdown: {breakdown_filter}, result: {result_filter}, milestone: {milestone_type}, date_filter_type: {date_filter_type}, cohort_type: {cohort_type}")
        
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Build cohort analysis query with proper column ordering
                breakdown_alias_map = {
                    'partner_region': 'Region',
                    'partner_country': 'Country',
                    'aff_type': 'Plan',
                    'partner_platform': 'Platform',
                    'attended_onboarding_event': 'Attended Event',
                    'partner_level': 'Partner Level'
                }
                
                breakdown_alias = breakdown_alias_map.get(breakdown_filter, breakdown_filter)
                
                # Define milestone column mapping
                milestone_map = {
                    'signup': 'first_client_joined_date',
                    'deposit': 'first_client_deposit_date', 
                    'trade': 'first_client_trade_date',
                    'earning': 'first_earning_date',
                    'first_client_joined_date': 'first_client_joined_date',
                    'first_client_deposit_date': 'first_client_deposit_date', 
                    'first_client_trade_date': 'first_client_trade_date',
                    'first_earning_date': 'first_earning_date'
                }
                
                milestone_col = milestone_map.get(milestone_type, 'first_client_joined_date')
                
                # Build date filter condition based on type
                date_condition = ""
                params = []
                
                if cohort_type == 'reverse':
                    # For reverse cohort, filter by activation date
                    if date_filter_type == 'rolling':
                        date_condition = f"{milestone_col} >= CURRENT_DATE - INTERVAL %s AND {milestone_col} IS NOT NULL"
                        params.append(f"{date_range} months")
                    elif date_filter_type == 'specific' and specific_month and specific_year:
                        date_condition = f"DATE_TRUNC('month', {milestone_col}) = %s AND {milestone_col} IS NOT NULL"
                        params.append(f"{specific_year}-{specific_month:02d}-01")
                    elif date_filter_type == 'range' and start_month and end_month:
                        date_condition = f"DATE_TRUNC('month', {milestone_col}) >= %s AND DATE_TRUNC('month', {milestone_col}) <= %s AND {milestone_col} IS NOT NULL"
                        params.extend([f"{start_month}-01", f"{end_month}-01"])
                    else:
                        date_condition = f"{milestone_col} >= CURRENT_DATE - INTERVAL %s AND {milestone_col} IS NOT NULL"
                        params.append("12 months")
                else:
                    # Original forward cohort logic
                    if date_filter_type == 'rolling':
                        date_condition = "date_joined >= CURRENT_DATE - INTERVAL %s"
                        params.append(f"{date_range} months")
                    elif date_filter_type == 'specific' and specific_month and specific_year:
                        date_condition = "DATE_TRUNC('month', date_joined) = %s"
                        params.append(f"{specific_year}-{specific_month:02d}-01")
                    elif date_filter_type == 'range' and start_month and end_month:
                        date_condition = "DATE_TRUNC('month', date_joined) >= %s AND DATE_TRUNC('month', date_joined) <= %s"
                        params.extend([f"{start_month}-01", f"{end_month}-01"])
                    else:
                        date_condition = "date_joined >= CURRENT_DATE - INTERVAL %s"
                        params.append("12 months")
                
                # Build the query based on cohort type
                if cohort_type == 'reverse':
                    # Reverse cohort: Group by activation month, show when they joined
                    base_query = f"""
                    WITH cohort_data AS (
                        SELECT 
                            DATE_TRUNC('month', {milestone_col}) as cohort_month,
                            COALESCE(partner_region, 'Unknown') as region,
                            COALESCE(partner_country, 'Unknown') as country,
                            COALESCE({breakdown_filter}, 'Unknown') as breakdown_value,
                            COUNT(DISTINCT partner_id) as total_partners,
                            COUNT(DISTINCT CASE WHEN date_joined >= {milestone_col} - INTERVAL '30 days' 
                                  THEN partner_id END) as m1_count,
                            COUNT(DISTINCT CASE WHEN date_joined >= {milestone_col} - INTERVAL '60 days' 
                                  AND date_joined < {milestone_col} - INTERVAL '30 days'
                                  THEN partner_id END) as m2_count,
                            COUNT(DISTINCT CASE WHEN date_joined >= {milestone_col} - INTERVAL '90 days' 
                                  AND date_joined < {milestone_col} - INTERVAL '60 days'
                                  THEN partner_id END) as m3_count
                        FROM partner.partner_info
                        WHERE is_internal = FALSE
                        AND {date_condition}
                    """
                else:
                    # Original forward cohort logic
                    base_query = f"""
                    WITH cohort_data AS (
                        SELECT 
                            DATE_TRUNC('month', date_joined) as cohort_month,
                            COALESCE(partner_region, 'Unknown') as region,
                            COALESCE(partner_country, 'Unknown') as country,
                            COALESCE({breakdown_filter}, 'Unknown') as breakdown_value,
                            COUNT(DISTINCT partner_id) as total_partners,
                            COUNT(DISTINCT CASE WHEN {milestone_col} IS NOT NULL 
                                  AND {milestone_col} <= date_joined + INTERVAL '30 days' 
                                  THEN partner_id END) as m1_count,
                            COUNT(DISTINCT CASE WHEN {milestone_col} IS NOT NULL 
                                  AND {milestone_col} <= date_joined + INTERVAL '60 days' 
                                  THEN partner_id END) as m2_count,
                            COUNT(DISTINCT CASE WHEN {milestone_col} IS NOT NULL 
                                  AND {milestone_col} <= date_joined + INTERVAL '90 days' 
                                  THEN partner_id END) as m3_count
                        FROM partner.partner_info
                        WHERE is_internal = FALSE
                        AND {date_condition}
                    """
                
                # Add filters
                where_clause, filter_params = create_filter_query(filters)
                if where_clause:
                    base_query += f" AND {where_clause}"
                    params.extend(filter_params)
                
                base_query += f"""
                    GROUP BY DATE_TRUNC('month', {milestone_col if cohort_type == 'reverse' else 'date_joined'}), partner_region, partner_country, {breakdown_filter}
                    HAVING COUNT(DISTINCT partner_id) >= 5
                )
                SELECT 
                    TO_CHAR(cohort_month, 'Mon YYYY') as "Cohort Month",
                    region as "Region",
                    country as "Country",
                    total_partners as "Total Partners",
                """
                
                # Add M1, M2, M3 columns based on result filter
                if result_filter == 'percentage':
                    base_query += """
                    ROUND(CAST(m1_count AS NUMERIC) / NULLIF(total_partners, 0) * 100, 2) as "M1 (%%)",
                    ROUND(CAST(m2_count AS NUMERIC) / NULLIF(total_partners, 0) * 100, 2) as "M2 (%%)",
                    ROUND(CAST(m3_count AS NUMERIC) / NULLIF(total_partners, 0) * 100, 2) as "M3 (%%)"
                    """
                else:
                    base_query += """
                    m1_count as "M1",
                    m2_count as "M2", 
                    m3_count as "M3"
                    """
                
                base_query += f' FROM cohort_data ORDER BY cohort_month DESC, "Region", "Country"'
                
                logger.info(f"Executing screener4 query with {len(params)} parameters")
                logger.info(f"Parameters: {params}")
                logger.info(f"Query: {base_query}")
                
                # Only pass params if we have filter parameters, otherwise execute without params
                if params:
                    cursor.execute(base_query, params)
                else:
                    cursor.execute(base_query)
                
                results = cursor.fetchall()
                
                # Define the desired column order: Cohort Month → Region → Country → Total Partners → M1 → M2 → M3
                if result_filter == 'percentage':
                    desired_order = ['Cohort Month', 'Region', 'Country', 'Total Partners', 'M1 (%)', 'M2 (%)', 'M3 (%)']
                else:
                    desired_order = ['Cohort Month', 'Region', 'Country', 'Total Partners', 'M1', 'M2', 'M3']
                
                ordered_results = []
                for record in results:
                    ordered_record = {}
                    # Add columns in desired order
                    for col in desired_order:
                        if col in record:
                            value = record[col]
                            if isinstance(value, (int, float)) and '(%)' in col:
                                ordered_record[col] = f"{value:.2f}%" if value else "0.00%"
                            elif isinstance(value, (int, float)):
                                ordered_record[col] = f"{value:,.0f}" if value else "0"
                            else:
                                ordered_record[col] = str(value) if value is not None else "Unknown"
                    # Add any remaining columns
                    for col in record:
                        if col not in desired_order:
                            ordered_record[col] = str(record[col]) if record[col] is not None else "Unknown"
                    ordered_results.append(ordered_record)
                
                logger.info(f"Screener4 returning {len(ordered_results)} cohort records")
                
                # Also prepare heatmap data for visualization - use raw results before formatting
                heatmap_data = []
                if results and result_filter == 'percentage':
                    for record in results:
                        # Use raw values from database results
                        total_partners_raw = record.get('Total Partners', 0)
                        region_value = record.get('Region', 'Unknown')
                        
                        # M1 data point
                        if 'M1 (%)' in record and record['M1 (%)'] is not None:
                            heatmap_data.append({
                                'x': 'M1 (30 days)',
                                'y': region_value,
                                'value': float(record['M1 (%)']),
                                'count': int(total_partners_raw)
                            })
                        # M2 data point
                        if 'M2 (%)' in record and record['M2 (%)'] is not None:
                            heatmap_data.append({
                                'x': 'M2 (60 days)',
                                'y': region_value,
                                'value': float(record['M2 (%)']),
                                'count': int(total_partners_raw)
                            })
                        # M3 data point
                        if 'M3 (%)' in record and record['M3 (%)'] is not None:
                            heatmap_data.append({
                                'x': 'M3 (90 days)',
                                'y': region_value,
                                'value': float(record['M3 (%)']),
                                'count': int(total_partners_raw)
                            })
                
                response_data = {
                    "success": True,
                    "cohort_data": ordered_results,
                    "heatmap_data": heatmap_data,
                    "breakdown_filter": breakdown_filter,
                    "result_filter": result_filter,
                    "milestone_type": milestone_type,
                    "cohort_type": cohort_type,
                    "total_cohorts": len(ordered_results)
                }
                
                # Return with proper JSON serialization
                json_str = json.dumps(response_data, ensure_ascii=False, default=str)
                return json_str, 200, {'Content-Type': 'application/json'}
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error in screener4: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({
            "success": False,
            "error": f"Failed to get screener4 data: {str(e)}"
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    print("🚀 Starting Flask API Server...")
    print("📊 Available endpoints:")
    print("  • GET  /health - Health check")
    print("  • POST /sql-agent - Test SQL Agent only")
    print("  • POST /analytics-agent - Test SQL + Analytics")
    print("  • GET  /screener/metrics - Get available metrics")
    print("  • GET  /screener/filters - Get filter options")
    print("  • POST /screener/data - Get screener data")
    print("  • POST /live-screeners/screener1 - Get data for Live Screener 1")
    print("  • POST /live-screeners/screener2 - Get data for Live Screener 2")
    print("  • POST /live-screeners/screener3 - Get data for Live Screener 3")
    print("  • POST /live-screeners/screener4 - Get data for Live Screener 4")
    print("\n🌐 API will be available at: http://localhost:5000")
    print("📝 Make sure to start the React frontend on http://localhost:3000")
    
    app.run(debug=True, host='0.0.0.0', port=5000) 