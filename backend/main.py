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
    print("\n🌐 API will be available at: http://localhost:5000")
    print("📝 Make sure to start the React frontend on http://localhost:3000")
    
    app.run(debug=True, host='0.0.0.0', port=5000) 