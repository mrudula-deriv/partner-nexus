import React, { useState } from 'react';
import axios from 'axios';

const API_BASE_URL = 'http://localhost:5000';

function App() {
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [sqlResult, setSqlResult] = useState(null);
  const [analyticsResult, setAnalyticsResult] = useState(null);
  const [activeTab, setActiveTab] = useState('sql');
  const [error, setError] = useState(null);

  const handleSqlTest = async () => {
    if (!query.trim()) {
      setError('Please enter a query');
      return;
    }

    setLoading(true);
    setError(null);
    setSqlResult(null);
    setAnalyticsResult(null);

    try {
      const response = await axios.post(`${API_BASE_URL}/sql-agent`, {
        query: query
      });

      setSqlResult(response.data);
      setActiveTab('sql');
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to process SQL query');
    } finally {
      setLoading(false);
    }
  };

  const handleAnalyticsTest = async () => {
    if (!query.trim()) {
      setError('Please enter a query');
      return;
    }

    setLoading(true);
    setError(null);
    setSqlResult(null);
    setAnalyticsResult(null);

    try {
      const response = await axios.post(`${API_BASE_URL}/analytics-agent`, {
        query: query
      });

      setAnalyticsResult(response.data);
      setActiveTab('analytics');
    } catch (err) {
      setError(err.response?.data?.error || 'Failed to process analytics query');
    } finally {
      setLoading(false);
    }
  };

  const clearResults = () => {
    setSqlResult(null);
    setAnalyticsResult(null);
    setError(null);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center">
              <h1 className="text-3xl font-bold text-gray-900">
                🧠 Nexus Agent UI
              </h1>
              <span className="ml-4 px-3 py-1 bg-blue-100 text-blue-800 text-sm font-medium rounded-full">
                SQL + Analytics Testing
              </span>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Query Input Section */}
        <div className="bg-white rounded-lg shadow p-6 mb-8">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            🔍 Enter Your Business Query
          </h2>
          
          <div className="space-y-4">
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Enter your business query here (e.g., 'What is the number of new partner signups per month, broken down by country?')"
              className="w-full h-32 px-4 py-3 border border-gray-300 rounded-md focus:ring-blue-500 focus:border-blue-500 resize-none"
            />

            <div className="flex flex-col sm:flex-row gap-4">
              <button
                onClick={handleSqlTest}
                disabled={loading}
                className="flex-1 bg-blue-600 text-white px-6 py-3 rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {loading && activeTab === 'sql' ? (
                  <span className="flex items-center justify-center">
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                    Testing SQL Agent...
                  </span>
                ) : (
                  '🗄️ Test SQL Agent Only'
                )}
              </button>

              <button
                onClick={handleAnalyticsTest}
                disabled={loading}
                className="flex-1 bg-purple-600 text-white px-6 py-3 rounded-md hover:bg-purple-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {loading && activeTab === 'analytics' ? (
                  <span className="flex items-center justify-center">
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2"></div>
                    Testing SQL + Analytics...
                  </span>
                ) : (
                  '📊 Test SQL + Analytics'
                )}
              </button>

              <button
                onClick={clearResults}
                className="px-6 py-3 border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50 transition-colors"
              >
                Clear Results
              </button>
            </div>
          </div>

          {error && (
            <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-md">
              <p className="text-red-800">❌ {error}</p>
            </div>
          )}
        </div>

        {/* Results Section */}
        {(sqlResult || analyticsResult) && (
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-xl font-semibold text-gray-900 mb-4">
              📋 Results
            </h3>

            {/* SQL Results */}
            {sqlResult && (
              <div className="mb-8">
                <h4 className="text-lg font-medium text-blue-600 mb-3">
                  🗄️ SQL Agent Results
                </h4>
                
                <div className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Original Query:
                    </label>
                    <div className="bg-gray-50 p-3 rounded border text-sm">
                      {sqlResult.query}
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Generated SQL:
                    </label>
                    <pre className="bg-gray-900 text-green-400 p-4 rounded overflow-x-auto text-sm">
                      {sqlResult.sql_query}
                    </pre>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Query Results:
                    </label>
                    <pre className="bg-gray-50 p-4 rounded border overflow-x-auto text-sm whitespace-pre-wrap">
                      {sqlResult.results}
                    </pre>
                  </div>

                  {sqlResult.verification_result && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Verification Details:
                      </label>
                      <div className="bg-yellow-50 p-3 rounded border text-sm">
                        {sqlResult.verification_result}
                      </div>
                    </div>
                  )}

                  <div className="flex gap-4 text-sm text-gray-600">
                    <span>Attempts: {sqlResult.attempts}</span>
                    <span>Status: {sqlResult.success ? '✅ Success' : '❌ Failed'}</span>
                  </div>
                </div>
              </div>
            )}

            {/* Analytics Results */}
            {analyticsResult && (
              <div>
                <h4 className="text-lg font-medium text-purple-600 mb-3">
                  📊 SQL + Analytics Results
                </h4>
                
                <div className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Original Query:
                    </label>
                    <div className="bg-gray-50 p-3 rounded border text-sm">
                      {analyticsResult.query}
                    </div>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      Generated SQL:
                    </label>
                    <pre className="bg-gray-900 text-green-400 p-4 rounded overflow-x-auto text-sm">
                      {analyticsResult.sql_query}
                    </pre>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      SQL Results:
                    </label>
                    <pre className="bg-gray-50 p-4 rounded border overflow-x-auto text-sm whitespace-pre-wrap">
                      {analyticsResult.sql_results}
                    </pre>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      📈 Analytics Report:
                    </label>
                    <div className="bg-blue-50 p-4 rounded border">
                      <pre className="whitespace-pre-wrap text-sm text-gray-800">
                        {analyticsResult.analytics_report}
                      </pre>
                    </div>
                  </div>

                  {/* Visualization Images */}
                  {analyticsResult.visualization_images && analyticsResult.visualization_images.length > 0 && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        📊 Generated Visualizations:
                      </label>
                      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                        {analyticsResult.visualization_images.map((viz, index) => (
                          <div key={index} className="bg-white p-4 rounded border shadow">
                            <h4 className="text-sm font-medium text-gray-800 mb-2">{viz.title}</h4>
                            <img 
                              src={`data:image/png;base64,${viz.data}`}
                              alt={viz.title}
                              className="w-full h-auto rounded border"
                            />
                            <p className="text-xs text-gray-500 mt-2">Type: {viz.type}</p>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  <div className="flex gap-4 text-sm text-gray-600">
                    <span>SQL Attempts: {analyticsResult.sql_attempts}</span>
                    <span>Status: {analyticsResult.success ? '✅ Success' : '❌ Failed'}</span>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Instructions */}
        <div className="mt-8 bg-blue-50 border border-blue-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-blue-900 mb-3">
            📚 How to Use
          </h3>
          <div className="space-y-2 text-blue-800 text-sm">
            <p><strong>🗄️ SQL Agent Only:</strong> Tests the SQL generation and execution workflow. Returns raw SQL results.</p>
            <p><strong>📊 SQL + Analytics:</strong> Runs the full pipeline including statistical analysis, pattern recognition, trend analysis, and business insights with visualizations.</p>
            <p><strong>🔄 Backend:</strong> Make sure the Flask API is running on http://localhost:5000</p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App; 