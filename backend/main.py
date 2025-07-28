"""
Flask Backend API for SQL and Analytics Agents

This API provides endpoints to test:
1. SQL Agent only
2. SQL + Analytics combined workflow
"""

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from sql_agent import create_workflow
from analytics_agent import analyze_sql_results
from logging_config import LoggingConfig
import traceback
import json
from psycopg2.extras import RealDictCursor
from collections import OrderedDict
import datetime
import os
import queue
import threading

# Import screener functions
from screener import (
    get_available_metrics, 
    get_filter_options, 
    fetch_metrics_data, 
    create_filter_query,
    get_db_connection
)

# Import spotlight dashboard functions
from spotlight_dashboard import (
    get_spotlight_dashboard_data,
    get_funnel_metrics
)

# Import country dashboard functions
from country_dashboard import (
    get_country_performance_overview,
    get_country_growth_trends,
    get_country_detailed_metrics,
    get_country_comparison_data,
    get_available_countries,
    get_partner_application_chart_data,
    get_partner_application_chart_data_supabase,
    get_partner_application_chart_data_client_only,
    get_partner_funnel_data,
    get_partner_activation_chart_data,
    get_events_data,
    get_country_performance_contribution,
    get_active_partners_chart_data,
    get_performance_stats_data,
    get_earning_partners_chart_data,
    get_top_partners_data,
    get_inactive_partners_data,
    get_new_partner_support_data,
    generate_country_dashboard_insights
)

# Set up Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Set up logging
logger = LoggingConfig('flask_app').setup_logger()

# Create a thread-safe queue for progress updates
progress_queues = {}

def generate_progress_id():
    """Generate a unique progress ID"""
    import uuid
    return str(uuid.uuid4())

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "message": "API is running"})

@app.route('/sql-agent/progress/<progress_id>', methods=['GET'])
def get_progress(progress_id):
    """SSE endpoint for progress updates"""
    def generate():
        if progress_id not in progress_queues:
            progress_queues[progress_id] = queue.Queue()
        
        q = progress_queues[progress_id]
        
        while True:
            try:
                progress_data = q.get(timeout=30)  # 30 second timeout
                if progress_data == "DONE":
                    return
                yield f"data: {json.dumps(progress_data)}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'progress': -1, 'message': 'Timeout'})}\n\n"
                return
    
    return Response(generate(), mimetype='text/event-stream')

@app.route('/sql-agent', methods=['POST'])
def test_sql_agent():
    """Test SQL Agent endpoint"""
    try:
        data = request.json
        user_query = data.get('query', '')
        
        if not user_query:
            return jsonify({
                "success": False,
                "error": "No query provided"
            }), 400

        # Generate a progress ID for this request
        progress_id = generate_progress_id()
        progress_queues[progress_id] = queue.Queue()

        def progress_callback(message: str, progress: int):
            progress_queues[progress_id].put({
                "message": message,
                "progress": progress
            })

        # Create workflow with progress callback - independent mode
        workflow = create_workflow(progress_callback, is_analytics_workflow=False)
        
        # Initialize state
        initial_state = {
            "prompt": user_query,
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

        # Run workflow in a background thread
        def run_workflow():
            try:
                result = workflow.invoke(initial_state, config={"recursion_limit": 50})
                # Send final result
                progress_queues[progress_id].put({
                    "message": "Completed",
                    "progress": 100,
                    "result": {
                        "success": True,
                        "results": result["results"],
                        "sql_query": result.get("sql_query", ""),
                        "attempt": result.get("attempt", 0),
                        "verification_result": result.get("verification_result", "")
                    }
                })
            except Exception as e:
                # Send error
                progress_queues[progress_id].put({
                    "message": f"Error: {str(e)}",
                    "progress": 100,
                    "error": str(e)
                })
            finally:
                # Signal completion
                progress_queues[progress_id].put("DONE")

        # Start workflow in background thread
        thread = threading.Thread(target=run_workflow)
        thread.start()
        
        return jsonify({
            "success": True,
            "message": "Processing started",
            "progress_id": progress_id
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/sql-analytics', methods=['POST'])
def test_sql_analytics():
    """Test SQL + Analytics combined workflow"""
    try:
        data = request.json
        user_query = data.get('query', '')
        
        if not user_query:
            return jsonify({
                "success": False,
                "error": "No query provided"
            }), 400

        # Generate a progress ID for this request
        progress_id = generate_progress_id()
        progress_queues[progress_id] = queue.Queue()

        def sql_progress_callback(message: str, progress: int):
            progress_queues[progress_id].put({
                "message": message,
                "progress": progress
            })

        def analytics_progress_callback(message: str, progress: int):
            progress_queues[progress_id].put({
                "message": message,
                "progress": progress
            })

        def run_workflow():
            try:
                # Step 1: Run SQL Agent (0-50% progress)
                logger.info("Step 1: Running SQL Agent...")
                workflow = create_workflow(sql_progress_callback, is_analytics_workflow=True)
                
                sql_initial_state = {
                    "prompt": user_query,
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
                
                sql_result = workflow.invoke(sql_initial_state, config={"recursion_limit": 50})
                sql_output = sql_result["results"]
                
                # Check if SQL agent failed
                if sql_result.get("error"):
                    progress_queues[progress_id].put({
                        "message": f"SQL Agent failed: {sql_result['error']}",
                        "progress": 50,
                        "error": sql_result['error']
                    })
                    progress_queues[progress_id].put("DONE")
                    return
                
                # Step 2: Run Analytics Agent (50-100% progress)
                logger.info("Step 2: Running Analytics Agent...")
                analytics_result = analyze_sql_results(user_query, sql_output, analytics_progress_callback)
                
                # Extract visualization images from analytics result if available
                visualization_images = []
                if isinstance(analytics_result, dict):
                    visualization_images = analytics_result.pop("visualization_images", [])
                
                # Send final result
                progress_queues[progress_id].put({
                    "message": "Completed",
                    "progress": 100,
                    "result": {
                        "success": True,
                        "query": user_query,
                        "sql_query": sql_result.get("sql_query", ""),
                        "sql_results": sql_output,
                        "analytics_report": analytics_result,
                        "visualization_images": visualization_images,
                        "sql_attempts": sql_result.get("attempt", 0),
                        "verification_result": sql_result.get("verification_result", "")
                    }
                })
            except Exception as e:
                # Send error
                progress_queues[progress_id].put({
                    "message": f"Error: {str(e)}",
                    "progress": 100,
                    "error": str(e)
                })
            finally:
                # Signal completion
                progress_queues[progress_id].put("DONE")

        # Start workflow in background thread
        thread = threading.Thread(target=run_workflow)
        thread.start()
        
        return jsonify({
            "success": True,
            "message": "Processing started",
            "progress_id": progress_id
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
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

@app.route('/spotlight/dashboard', methods=['GET'])
def get_spotlight_dashboard():
    """Get complete spotlight dashboard data"""
    try:
        logger.info("Fetching spotlight dashboard data...")
        
        # Get date range from query parameter, default to 90 days
        date_range = request.args.get('date_range', 90, type=int)
        
        dashboard_data = get_spotlight_dashboard_data(date_range)
        
        return jsonify({
            'success': True,
            'data': dashboard_data
        })
    except Exception as e:
        logger.error(f"Error in spotlight dashboard: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/spotlight/funnel-metrics', methods=['GET'])
def get_funnel_metrics_endpoint():
    """Get conversion funnel metrics with optional country filter"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        country = request.args.get('country', None)
        
        logger.info(f"Fetching funnel metrics for date_range={date_range}, country={country}")
        
        # Get funnel data
        funnel_data = get_funnel_metrics(date_range=date_range, country=country)
        
        return jsonify({
            'success': True,
            'data': funnel_data
        })
        
    except Exception as e:
        logger.error(f"Error in funnel metrics endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/ai-insight', methods=['POST'])
def get_ai_insight():
    """Generate AI insights for widget data"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                "error": "Request body is required"
            }), 400
        
        widget_type = data.get('widget_type', '')
        widget_data = data.get('data', [])
        title = data.get('title', '')
        
        logger.info(f"Generating AI insight for widget: {title}")
        
        # Generate contextual insights based on widget type and data
        insight = generate_widget_insight(widget_type, widget_data, title)
        
        return jsonify({
            'success': True,
            'insight': insight
        })
        
    except Exception as e:
        logger.error(f"Error generating AI insight: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'insight': 'Unable to generate insights at this time. Please try again later.'
        }), 500

@app.route('/country-dashboard/overview', methods=['GET'])
def get_country_dashboard_overview():
    """Get country performance overview"""
    try:
        date_range = request.args.get('date_range', 90, type=int)
        
        logger.info(f"Fetching country overview for date_range={date_range}")
        
        overview_data = get_country_performance_overview(date_range)
        
        return jsonify({
            'success': True,
            'data': overview_data
        })
        
    except Exception as e:
        logger.error(f"Error in country overview endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/growth-trends', methods=['GET'])
def get_country_dashboard_growth_trends():
    """Get country growth trends"""
    try:
        date_range = request.args.get('date_range', 180, type=int)
        
        logger.info(f"Fetching country growth trends for date_range={date_range}")
        
        trends_data = get_country_growth_trends(date_range)
        
        return jsonify({
            'success': True,
            'data': trends_data
        })
        
    except Exception as e:
        logger.error(f"Error in country growth trends endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/country-details', methods=['GET'])
def get_country_dashboard_details():
    """Get detailed metrics for a specific country"""
    try:
        country = request.args.get('country')
        date_range = request.args.get('date_range', 90, type=int)
        
        if not country:
            return jsonify({
                'success': False,
                'error': 'Country parameter is required'
            }), 400
        
        logger.info(f"Fetching country details for {country}, date_range={date_range}")
        
        details_data = get_country_detailed_metrics(country, date_range)
        
        return jsonify({
            'success': True,
            'data': details_data
        })
        
    except Exception as e:
        logger.error(f"Error in country details endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/compare', methods=['POST'])
def get_country_dashboard_comparison():
    """Compare metrics across multiple countries"""
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'Request body is required'
            }), 400
        
        countries = data.get('countries', [])
        date_range = data.get('date_range', 90)
        
        if not countries:
            return jsonify({
                'success': False,
                'error': 'Countries list is required'
            }), 400
        
        logger.info(f"Comparing countries: {countries}, date_range={date_range}")
        
        comparison_data = get_country_comparison_data(countries, date_range)
        
        return jsonify({
            'success': True,
            'data': comparison_data
        })
        
    except Exception as e:
        logger.error(f"Error in country comparison endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/countries', methods=['GET'])
def get_country_dashboard_countries():
    """Get list of available countries"""
    try:
        logger.info("Fetching available countries")
        
        countries_data = get_available_countries()
        
        return jsonify({
            'success': True,
            'data': countries_data
        })
        
    except Exception as e:
        logger.error(f"Error in countries list endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/application-chart', methods=['GET'])
def get_country_dashboard_application_chart():
    """Get partner application chart data with event overlays"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        period_type = request.args.get('period_type', 'monthly', type=str)
        start_date = request.args.get('start_date', None, type=str)
        end_date = request.args.get('end_date', None, type=str)
        partner_country = request.args.get('partner_country', None, type=str)
        
        logger.info(f"Fetching application chart data - range: {date_range} days, period: {period_type}, start: {start_date}, end: {end_date}, country: {partner_country}")
        
        # Use start_date and end_date if provided, otherwise fall back to date_range
        chart_data = get_partner_application_chart_data(date_range, period_type, start_date, end_date, partner_country)
        
        return jsonify({
            'success': True,
            'data': chart_data
        })
        
    except Exception as e:
        logger.error(f"Error in application chart endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/partner-funnel', methods=['GET'])
def get_country_dashboard_partner_funnel():
    """Get partner funnel conversion data"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        partner_country = request.args.get('partner_country', None, type=str)
        
        logger.info(f"Fetching partner funnel data - range: {date_range} days, country: {partner_country}")
        
        # Get funnel data
        funnel_data = get_partner_funnel_data(date_range, partner_country)
        
        return jsonify({
            'success': True,
            'data': funnel_data
        })
        
    except Exception as e:
        logger.error(f"Error in partner funnel endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/activation-chart', methods=['GET'])
def get_country_dashboard_activation_chart():
    """Get partner activation chart data"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        period_type = request.args.get('period_type', 'monthly', type=str)
        partner_country = request.args.get('partner_country', None, type=str)
        
        logger.info(f"Fetching activation chart data - range: {date_range} days, period: {period_type}, country: {partner_country}")
        
        # Get activation data
        activation_data = get_partner_activation_chart_data(date_range, period_type, partner_country)
        
        return jsonify({
            'success': True,
            'data': activation_data
        })
        
    except Exception as e:
        logger.error(f"Error in activation chart endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/events', methods=['GET'])
def get_country_dashboard_events():
    """Get past and upcoming events data"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        partner_country = request.args.get('partner_country', None, type=str)
        
        logger.info(f"Fetching events data - range: {date_range} days, country: {partner_country}")
        
        # Get events data
        events_data = get_events_data(date_range, partner_country)
        
        return jsonify({
            'success': True,
            'data': events_data
        })
        
    except Exception as e:
        logger.error(f"Error in events endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/performance-contribution', methods=['GET'])
def get_country_dashboard_performance_contribution():
    """Get country performance contribution to regions"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        partner_country = request.args.get('partner_country', None, type=str)
        
        logger.info(f"Fetching performance contribution data - range: {date_range} days, country: {partner_country}")
        
        # Get performance contribution data
        contribution_data = get_country_performance_contribution(date_range, partner_country)
        
        return jsonify({
            'success': True,
            'data': contribution_data
        })
        
    except Exception as e:
        logger.error(f"Error in performance contribution endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/active-partners-chart', methods=['GET'])
def get_country_dashboard_active_partners_chart():
    """Get active partners chart data"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        period_type = request.args.get('period_type', 'monthly', type=str)
        start_date = request.args.get('start_date', None, type=str)
        end_date = request.args.get('end_date', None, type=str)
        partner_country = request.args.get('partner_country', None, type=str)
        
        logger.info(f"Fetching active partners chart data - range: {date_range} days, period: {period_type}, country: {partner_country}")
        
        # Get active partners chart data
        chart_data = get_active_partners_chart_data(date_range, period_type, start_date, end_date, partner_country)
        
        return jsonify({
            'success': True,
            'data': chart_data
        })
        
    except Exception as e:
        logger.error(f"Error in active partners chart endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/performance-stats', methods=['GET'])
def get_country_dashboard_performance_stats():
    """Get performance stats data"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        period_type = request.args.get('period_type', 'monthly', type=str)
        start_date = request.args.get('start_date', None, type=str)
        end_date = request.args.get('end_date', None, type=str)
        partner_country = request.args.get('partner_country', None, type=str)
        
        logger.info(f"Fetching performance stats data - range: {date_range} days, period: {period_type}, country: {partner_country}")
        
        # Get performance stats data
        stats_data = get_performance_stats_data(date_range, period_type, start_date, end_date, partner_country)
        
        return jsonify({
            'success': True,
            'data': stats_data
        })
        
    except Exception as e:
        logger.error(f"Error in performance stats endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/earning-partners-chart', methods=['GET'])
def get_country_dashboard_earning_partners_chart():
    """Get earning partners chart data"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        period_type = request.args.get('period_type', 'monthly', type=str)
        start_date = request.args.get('start_date', None, type=str)
        end_date = request.args.get('end_date', None, type=str)
        partner_country = request.args.get('partner_country', None, type=str)
        
        logger.info(f"Fetching earning partners chart data - range: {date_range} days, period: {period_type}, country: {partner_country}")
        
        # Get earning partners chart data
        chart_data = get_earning_partners_chart_data(date_range, period_type, start_date, end_date, partner_country)
        
        return jsonify({
            'success': True,
            'data': chart_data
        })
        
    except Exception as e:
        logger.error(f"Error in earning partners chart endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/top-partners', methods=['GET'])
def get_country_dashboard_top_partners():
    """Get top 20 partners data"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        partner_country = request.args.get('partner_country', None, type=str)
        limit = request.args.get('limit', 20, type=int)
        
        logger.info(f"Fetching top partners data - range: {date_range} days, country: {partner_country}, limit: {limit}")
        
        # Get top partners data
        partners_data = get_top_partners_data(date_range, partner_country, limit)
        
        return jsonify({
            'success': True,
            'data': partners_data
        })
        
    except Exception as e:
        logger.error(f"Error in top partners endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/inactive-partners', methods=['GET'])
def get_country_dashboard_inactive_partners():
    """Get inactive partners data by tiers"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        partner_country = request.args.get('partner_country', None, type=str)
        limit = request.args.get('limit', 50, type=int)
        
        logger.info(f"Fetching inactive partners data - range: {date_range} days, country: {partner_country}, limit: {limit}")
        
        # Get inactive partners data
        inactive_data = get_inactive_partners_data(date_range, partner_country, limit)
        
        return jsonify({
            'success': True,
            'data': inactive_data
        })
        
    except Exception as e:
        logger.error(f"Error in inactive partners endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/new-partner-support', methods=['GET'])
def get_country_dashboard_new_partner_support():
    """Get new partner support data"""
    try:
        # Get query parameters
        date_range = request.args.get('date_range', 90, type=int)
        partner_country = request.args.get('partner_country', None, type=str)
        limit = request.args.get('limit', 100, type=int)
        
        logger.info(f"Fetching new partner support data - range: {date_range} days, country: {partner_country}, limit: {limit}")
        
        # Get new partner support data
        support_data = get_new_partner_support_data(date_range, partner_country, limit)
        
        return jsonify({
            'success': True,
            'data': support_data
        })
        
    except Exception as e:
        logger.error(f"Error in new partner support endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/country-dashboard/ai-insights', methods=['GET'])
def get_country_dashboard_ai_insights():
    """Generate AI insights for country dashboard by fetching data directly from backend"""
    try:
        # Get parameters from query string
        date_range = request.args.get('date_range', 90, type=int)
        partner_country = request.args.get('partner_country', 'All')
        report_type = request.args.get('report_type', 'monthly')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        logger.info(f"Generating AI insights for country dashboard - range: {date_range} days, country: {partner_country}, type: {report_type}")
        
        # Fetch all necessary data directly from backend functions
        overview_data = get_country_performance_overview(date_range)
        
        # Prepare dashboard data by calling backend functions directly
        dashboard_data = {
            'date_range': date_range,
            'partner_country': partner_country,
            'report_type': report_type,
            'start_date': start_date,
            'end_date': end_date,
            'overview': overview_data,
            'funnel': get_partner_funnel_data(date_range, partner_country),
            'top_partners': get_top_partners_data(date_range, partner_country, limit=20),
            'inactive_partners': get_inactive_partners_data(date_range, partner_country, limit=50),
            'performance_contribution': get_country_performance_contribution(date_range, partner_country)
        }
        
        # Generate insights
        insights_result = generate_country_dashboard_insights(dashboard_data)
        
        return jsonify({
            'success': True,
            'data': insights_result
        })
        
    except Exception as e:
        logger.error(f"Error generating country dashboard AI insights: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def generate_widget_insight(widget_type, data, title):
    """Generate contextual insights based on widget data using LLM"""
    
    if not data or len(data) == 0:
        return "• No data available for analysis. Consider expanding your date range or checking data filters."
    
    # Try to use LLM for insights if available
    try:
        import os
        from langchain_openai import ChatOpenAI
        from langchain_core.messages import SystemMessage, HumanMessage
        
        OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        API_BASE_URL = os.getenv('API_BASE_URL')
        OPENAI_MODEL_NAME = os.getenv('OPENAI_MODEL_NAME')
        
        if OPENAI_API_KEY and API_BASE_URL and OPENAI_MODEL_NAME:
            llm = ChatOpenAI(
                api_key=OPENAI_API_KEY,
                base_url=API_BASE_URL,
                model=OPENAI_MODEL_NAME,
                temperature=0.7
            )
            
            # Prepare data summary based on data structure
            if isinstance(data, dict):
                # Handle dictionary data structure (like conversion funnel)
                data_summary = f"""
                Widget: {title}
                Data Structure: Dictionary with {len(data)} keys
                Keys: {list(data.keys())}
                
                Key Metrics:
                """
                
                # Add funnel overview metrics if available
                if 'funnel_overview' in data:
                    funnel_overview = data['funnel_overview']
                    for key, value in funnel_overview.items():
                        data_summary += f"- {key}: {value}\n"
                
                # Add country performance data if available
                if 'country_performance' in data:
                    country_data = data['country_performance']
                    data_summary += f"- Country performance data: {len(country_data)} countries\n"
                    if country_data:
                        data_summary += f"- Sample country data: {country_data[:2]}\n"
                
                # Add legacy support for overview_metrics
                if 'overview_metrics' in data:
                    overview = data['overview_metrics']
                    for key, value in overview.items():
                        data_summary += f"- {key}: {value}\n"
                
                # Add legacy support for conversion_funnel
                if 'conversion_funnel' in data:
                    funnel_data = data['conversion_funnel']
                    data_summary += f"- Conversion funnel countries: {len(funnel_data)}\n"
                    if funnel_data:
                        data_summary += f"- Sample funnel data: {funnel_data[:2]}\n"
            else:
                # Handle list/array data structure
                data_summary = f"""
                Widget: {title}
                Data Points: {len(data) if data else 0}
                Sample Data: {data[:3] if data and len(data) > 3 else data}
                
                Key Metrics:
                """
                
                # Add specific metrics based on data structure
                if data and isinstance(data[0], dict):
                    for key in data[0].keys():
                        if any(metric in key.lower() for metric in ['rate', 'count', 'total', 'growth', 'days']):
                            values = [item.get(key, 0) for item in data[:5]]
                            data_summary += f"- {key}: {values}\n"
            
            messages = [
                SystemMessage(content="""You are a partner analytics expert. Analyze the widget data and provide exactly 5-6 bullet points (1-2 sentences each).
                
                Format your response as bullet points starting with •
                Focus on:
                - Key insights from the data
                - Performance patterns
                - Actionable recommendations
                - Strategic implications
                
                Be specific and data-driven. Keep each bullet concise."""),
                HumanMessage(content=f"Analyze this widget data and provide insights:\n\n{data_summary}")
            ]
            
            response = llm.invoke(messages)
            return response.content
            
    except Exception as e:
        logger.error(f"Error generating LLM insights: {str(e)}")
        # Provide informative error message for authentication issues
        if "401" in str(e) or "Authentication" in str(e):
            return "• API authentication error - please check your LiteLLM proxy configuration and API key.\n• Widget insights are temporarily using fallback analysis.\n• Contact your system administrator to resolve API access issues.\n• Refresh the page after fixing authentication to get AI-powered insights.\n• The dashboard will continue to work with standard analytics."
        # Fall back to rule-based insights if LLM fails
        pass
    
    # Activation Speed Leaders insights
    if "Activation Speed Leaders" in title:
        top_country = data[0] if data else None
        if top_country:
            avg_days = top_country.get('avg_days_to_activate', 0)
            rate = top_country.get('activation_rate', 0)
            country = top_country.get('country', 'Unknown')
            
            insights = [
                f"• {country} leads with {avg_days} days average activation time and {rate}% conversion rate.",
                f"• {len(data)} countries show strong activation potential for focused investment.",
                "• Fast activation indicates effective onboarding processes and partner readiness.",
                "• Consider replicating top performers' strategies across other markets.",
                "• Automated follow-ups could further reduce activation timeframes.",
                "• Focus resources on proven markets for maximum ROI potential."
            ]
            
            return "\n".join(insights)
    
    # Partner Comeback Success insights
    elif "Partner Comeback Success" in title or "Comeback" in title:
        total_reactivated = sum(item.get('reactivated_30d', 0) for item in data)
        top_country = data[0] if data else None
        
        insights = [
            f"• {total_reactivated} partners successfully returned to earning status across {len(data)} countries.",
            f"• {top_country.get('country', 'Unknown')} leads with {top_country.get('reactivated_30d', 0)} reactivated partners." if top_country else "• No clear leader in partner reactivation.",
            "• High reactivation numbers indicate good market conditions and partner satisfaction.",
            "• Analyze what triggered these comebacks to create systematic re-engagement campaigns.",
            "• Focus on preventing churn rather than just reactivating dormant partners.",
            "• Implement automated win-back sequences for better retention results."
        ]
        
        return "\n".join(insights)
    
    # High-Volume Opportunities insights
    elif "High-Volume Opportunities" in title or "Volume" in title:
        total_apps = sum(item.get('total_applications', 0) for item in data)
        avg_rate = sum(float(item.get('activation_rate', 0)) for item in data) / len(data) if data else 0
        top_opportunity = data[0] if data else None
        
        insights = [
            f"• {total_apps:,} total applications with {avg_rate:.1f}% average activation rate across high-volume markets.",
            f"• {top_opportunity.get('country', 'Unknown')} shows biggest opportunity with {top_opportunity.get('total_applications', 0):,} applications." if top_opportunity else "• No clear high-volume opportunity identified.",
            "• Low activation rates indicate systemic onboarding issues that need addressing.",
            "• Audit onboarding flows for friction points and implement progressive training.",
            "• Add dedicated support resources for high-volume markets.",
            "• A/B test improved onboarding flows to push rates above 15% target."
        ]
        
        return "\n".join(insights)
    
    # Growth Momentum Leaders insights
    elif "Growth Momentum" in title or "Growth" in title:
        avg_growth = sum(float(item.get('growth_rate', 0)) for item in data) / len(data) if data else 0
        top_grower = data[0] if data else None
        
        insights = [
            f"• Average {avg_growth:.0f}% growth across {len(data)} momentum markets shows strong expansion.",
            f"• {top_grower.get('country', 'Unknown')} leads with {top_grower.get('growth_rate', 0)}% growth and {top_grower.get('current_signups', 0)} recent signups." if top_grower else "• No clear growth leader identified.",
            "• Exceptional growth requires increased resource allocation and support infrastructure.",
            "• Monitor quality metrics to ensure growth doesn't compromise partner experience.",
            "• Study growth drivers and replicate successful strategies in other markets.",
            "• Growth markets need different support than mature markets - prioritize scalability."
        ]
        
        return "\n".join(insights)
    
    # Event Effectiveness insights
    elif "Event Effectiveness" in title or "Event" in title:
        total_attended = sum(item.get('attended_count', 0) for item in data)
        avg_rate = sum(float(item.get('activation_rate', 0)) for item in data) / len(data) if data else 0
        
        insights = [
            f"• {total_attended:,} partners attended onboarding events with {avg_rate:.1f}% average activation rate.",
            f"• {data[0].get('country', 'Unknown')} shows best event performance with {data[0].get('activation_rate', 0)}% rate." if data else "• No clear event leader identified.",
            "• High event attendance correlates with better partner activation and retention.",
            "• Consider expanding successful event formats to underperforming markets.",
            "• Track long-term retention rates for event attendees vs non-attendees.",
            "• Implement virtual event options to reach more partners cost-effectively."
        ]
        return "\n".join(insights)
    
    # Conversion Funnel insights
    elif "Conversion" in title and "Funnel" in title:
        # Handle funnel data structure from /spotlight/funnel-metrics endpoint
        if isinstance(data, dict) and 'funnel_overview' in data:
            funnel_overview = data.get('funnel_overview', {})
            country_performance = data.get('country_performance', [])
            
            activation_rate = funnel_overview.get('activation_rate', 0)
            total_apps = funnel_overview.get('total_applications', 0)
            signup_activations = funnel_overview.get('signup_activations', 0)
            avg_days = funnel_overview.get('avg_days_to_activation', 0)
            active_partners_rate = funnel_overview.get('active_partners_rate', 0)
            application_growth = funnel_overview.get('application_growth_rate', 0)
            
            insights = [
                f"• Overall activation rate of {activation_rate:.1f}% with {signup_activations:,} partners activated from {total_apps:,} total applications.",
                f"• Average time to first activation is {avg_days:.1f} days, indicating partner onboarding efficiency.",
                f"• {active_partners_rate:.1f}% of partners are currently active in the last 30 days, showing ongoing engagement.",
                f"• Application growth rate of {application_growth:+.1f}% vs previous period shows market momentum.",
                f"• {len(country_performance)} countries analyzed for stage-by-stage conversion performance optimization.",
                "• Focus on improving conversion rates at each funnel stage to maximize overall activation performance."
            ]
            
            return "\n".join(insights)
        else:
            # Fallback for simple data structure
            insights = [
                f"• {len(data) if isinstance(data, list) else 'Multiple'} conversion stages analyzed showing partner journey progression.",
                "• Identify biggest drop-off points to focus optimization efforts effectively.",
                "• Each stage improvement compounds to significantly boost overall conversion.",
                "• Implement targeted interventions at the weakest conversion points.",
                "• Monitor stage-specific metrics to track improvement over time.",
                "• Consider A/B testing different approaches at each conversion stage."
            ]
            return "\n".join(insights)
    
    # Top Performing Countries insights
    elif "Top Performing" in title or "Country" in title:
        insights = [
            f"• {len(data)} countries analyzed for performance benchmarking and optimization.",
            f"• {data[0].get('country', 'Unknown')} leads performance metrics in this analysis." if data else "• No clear country leader identified.",
            "• Top performers can serve as models for scaling successful strategies.",
            "• Analyze what makes leaders successful and replicate across other markets.",
            "• Consider increased investment in consistently high-performing markets.",
            "• Use underperforming markets as test grounds for new improvement strategies."
        ]
        return "\n".join(insights)
    
    # Top Activation Opportunities insights
    elif "Activation Opportunities" in title or "Opportunities" in title:
        insights = [
            f"• {len(data)} activation opportunities identified across partner portfolio.",
            "• Focus on high-potential, low-activation markets for maximum impact.",
            "• Quick wins available through improved onboarding and support processes.",
            "• Systematic approach to opportunity capture will drive significant growth.",
            "• Monitor conversion improvements to validate optimization efforts.",
            "• Scale successful activation strategies across similar market segments."
        ]
        return "\n".join(insights)
    
    # Country ROI Analysis insights
    elif "Country ROI" in title or "ROI Analysis" in title:
        insights = [
            f"• {len(data)} countries analyzed for return on investment performance.",
            "• High ROI markets deserve increased resource allocation and support.",
            "• Low ROI markets need strategic intervention or resource reallocation.",
            "• Track ROI trends over time to identify improving or declining markets.",
            "• Consider market maturity when evaluating ROI performance.",
            "• Balance short-term ROI with long-term market development potential."
        ]
        return "\n".join(insights)
    
    # Partner Retention Cohorts insights
    elif "Partner Retention" in title or "Cohorts" in title or "Retention" in title:
        insights = [
            f"• {len(data)} partner cohorts analyzed for retention patterns and trends.",
            "• Early retention indicators predict long-term partner success and value.",
            "• Identify at-risk cohorts early to implement targeted retention strategies.",
            "• Strong retention cohorts indicate effective onboarding and support processes.",
            "• Monitor cohort performance over time to validate retention improvements.",
            "• Use cohort insights to optimize partner lifecycle management strategies."
        ]
        return "\n".join(insights)
    
    # Generic insights for other widgets
    else:
        insights = [
            f"• {len(data)} data points analyzed for {title} performance insights.",
            "• Focus on top-performing entries to identify success factors for replication.",
            "• Look for patterns and trends that indicate optimization opportunities.",
            "• Consider market context when interpreting performance variations.",
            "• Implement systematic tracking to monitor improvement over time.",
            "• Use insights to guide strategic resource allocation and planning decisions."
        ]
        return "\n".join(insights)

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
    print("  • POST /sql-analytics - Test SQL + Analytics")
    print("  • GET  /screener/metrics - Get available metrics")
    print("  • GET  /screener/filters - Get filter options")
    print("  • POST /screener/data - Get screener data")
    print("  • POST /live-screeners/screener1 - Get data for Live Screener 1")
    print("  • POST /live-screeners/screener2 - Get data for Live Screener 2")
    print("  • POST /live-screeners/screener3 - Get data for Live Screener 3")
    print("  • POST /live-screeners/screener4 - Get data for Live Screener 4")
    print("  • GET  /spotlight/dashboard - Get complete spotlight dashboard")
    print("  • GET  /spotlight/funnel-metrics - Get conversion funnel metrics")
    print("  • POST /ai-insight - Generate AI insights for widget data")
    print("  • GET  /country-dashboard/overview - Get country performance overview")
    print("  • GET  /country-dashboard/growth-trends - Get country growth trends")
    print("  • GET  /country-dashboard/country-details - Get detailed country metrics")
    print("  • POST /country-dashboard/compare - Compare multiple countries")
    print("  • GET  /country-dashboard/countries - Get available countries list")
    print("  • GET  /country-dashboard/application-chart - Get partner application chart with events")
    print("\n🌐 API will be available at: http://localhost:5001")
    print("📝 Make sure to start the React frontend on http://localhost:3000")
    
    app.run(debug=True, host='0.0.0.0', port=5001) 