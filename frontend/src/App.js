import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

const API_BASE_URL = 'http://localhost:5000';

// Icon components with consistent styling
const DatabaseIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <ellipse cx="12" cy="5" rx="9" ry="3"></ellipse>
    <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"></path>
    <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"></path>
  </svg>
);

const ChartIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="18" y1="20" x2="18" y2="10"></line>
    <line x1="12" y1="20" x2="12" y2="4"></line>
    <line x1="6" y1="20" x2="6" y2="14"></line>
  </svg>
);

const TrendingIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline>
    <polyline points="17 6 23 6 23 12"></polyline>
  </svg>
);

const RocketIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"></path>
  </svg>
);

const DownloadIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"></path>
    <polyline points="7 10 12 15 17 10"></polyline>
    <line x1="12" y1="15" x2="12" y2="3"></line>
  </svg>
);

const FilterIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"></polygon>
  </svg>
);

const CloseIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="18" y1="6" x2="6" y2="18"></line>
    <line x1="6" y1="6" x2="18" y2="18"></line>
  </svg>
);

const ArrowUpIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="18 15 12 9 6 15"></polyline>
  </svg>
);

const ArrowDownIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="6 9 12 15 18 9"></polyline>
  </svg>
);

function App() {
  // All existing state variables remain the same
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [sqlResult, setSqlResult] = useState(null);
  const [analyticsResult, setAnalyticsResult] = useState(null);
  const [activeTab, setActiveTab] = useState('live-screeners');
  const [error, setError] = useState(null);

  // Screener states
  const [screenerLoading, setScreenerLoading] = useState(false);
  const [availableMetrics, setAvailableMetrics] = useState({});
  const [availableFilters, setAvailableFilters] = useState({});
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [activeFilters, setActiveFilters] = useState({});
  const [screenerData, setScreenerData] = useState(null);
  const [screenerError, setScreenerError] = useState(null);

  // Live Screeners states
  const [liveScreenerLoading, setLiveScreenerLoading] = useState(false);
  const [liveScreenerError, setLiveScreenerError] = useState(null);
  const [activeScreener, setActiveScreener] = useState(1);
  const [screener1Data, setScreener1Data] = useState({ table1: [], table2: [] });
  const [screener2Data, setScreener2Data] = useState([]);
  const [screener3Data, setScreener3Data] = useState({ table1: [], table2: [] });
  const [screener4Data, setScreener4Data] = useState({ cohort_data: [], heatmap_data: [] });
  const [liveFilters, setLiveFilters] = useState({});
  const [dateFilters, setDateFilters] = useState({});
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
  const [selectedDateRange, setSelectedDateRange] = useState(null);

  // Enhanced screener states
  const [trendDateRange, setTrendDateRange] = useState(6);
  const [trendSettings, setTrendSettings] = useState({
    date_range: 6,
    date_filter_type: 'range',
    specific_month: new Date().getMonth() + 1,
    specific_year: new Date().getFullYear(),
    start_month: `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`,
    end_month: `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`
  });
  const [cohortSettings, setCohortSettings] = useState({
    breakdown_filter: 'partner_region',
    result_filter: 'percentage',
    milestone_type: 'first_client_joined_date',
    date_range: 12,
    date_filter_type: 'rolling',
    specific_month: new Date().getMonth() + 1,
    specific_year: new Date().getFullYear(),
    start_month: `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`,
    end_month: `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`,
    cohort_type: 'forward'  // Add cohort type: 'forward' or 'reverse'
  });

  // Add this state variable near the other state declarations
  const [filterColumnOrder, setFilterColumnOrder] = useState([]);
  const [filterOptions, setFilterOptions] = useState({});
  const [screenerResults, setScreenerResults] = useState(null);

  // All existing functions remain the same
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

  // Screener functions
  const loadScreenerData = async () => {
    setScreenerLoading(true);
    setScreenerError(null);
    
    try {
      const [metricsResponse, filtersResponse] = await Promise.all([
        axios.get(`${API_BASE_URL}/screener/metrics`),
        axios.get(`${API_BASE_URL}/screener/filters`)
      ]);
      
      setAvailableMetrics(metricsResponse.data.metrics);
      setAvailableFilters(filtersResponse.data.filters);
      
      // Set default metrics to match the required columns
      const defaultMetrics = [
        'Application Count',
        'First Activated Count - Signup',
        'First Activated Count - Deposit',
        'First Activated Count - Traded'
      ];
      setSelectedMetrics(defaultMetrics);
      
    } catch (err) {
      setScreenerError(err.response?.data?.error || 'Failed to load screener data');
    } finally {
      setScreenerLoading(false);
    }
  };

  const handleMetricToggle = (metric) => {
    const newMetrics = selectedMetrics.includes(metric)
      ? selectedMetrics.filter(m => m !== metric)
      : [...selectedMetrics, metric];
    setSelectedMetrics(newMetrics);
  };

  const handleFilterChange = (filterType, values) => {
    const newFilters = { ...activeFilters };
    
    if (filterType === 'date_joined_from' || filterType === 'date_joined_to') {
      if (!newFilters.date_joined) {
        newFilters.date_joined = {
          values: [],
          showAsColumn: true,
          start_date: filterType === 'date_joined_from' ? values : '',
          end_date: filterType === 'date_joined_to' ? values : ''
        };
      } else {
        if (filterType === 'date_joined_from') {
          newFilters.date_joined.start_date = values;
        } else {
          newFilters.date_joined.end_date = values;
        }
      }
      
      // Remove the filter if both dates are empty
      if (!newFilters.date_joined.start_date && !newFilters.date_joined.end_date) {
        delete newFilters.date_joined;
      }
    } else {
      if (!values || (Array.isArray(values) && values.length === 0)) {
        delete newFilters[filterType];
      } else {
        newFilters[filterType] = {
          values: Array.isArray(values) ? values : [values],
          showAsColumn: true
        };
      }
    }
    
    setActiveFilters(newFilters);
  };

  const removeFilter = (filterType) => {
    setActiveFilters(prev => {
      const newFilters = { ...prev };
      delete newFilters[filterType];
      return newFilters;
    });
  };

  const fetchScreenerResults = async (metrics = selectedMetrics, filters = activeFilters) => {
    if (metrics.length === 0) {
      setScreenerData(null);
      return;
    }

    setScreenerLoading(true);
    setScreenerError(null);

    try {
      const response = await axios.post(`${API_BASE_URL}/screener/data`, {
        metrics: metrics,
        filters: filters
      });

      if (response.data && response.data.data) {
        setScreenerData({
          data: response.data.data.data,
          columns: response.data.data.columns
        });
      }
    } catch (err) {
      setScreenerError(err.response?.data?.error || 'Failed to fetch screener data');
    } finally {
      setScreenerLoading(false);
    }
  };

  // Auto-fetch data when metrics or filters change
  useEffect(() => {
    if (activeTab === 'screener' && selectedMetrics.length > 0) {
      const delayedFetch = setTimeout(() => {
        fetchScreenerResults();
      }, 500);
      
      return () => clearTimeout(delayedFetch);
    }
  }, [selectedMetrics, activeFilters, activeTab]);

  const clearScreenerResults = () => {
    setScreenerData(null);
    setScreenerError(null);
    setSelectedMetrics([]);
    setActiveFilters({});
  };

  // Live Screeners functions
  const loadLiveScreenerData = async (screenerNum) => {
    setLiveScreenerLoading(true);
    setLiveScreenerError(null);

    try {
      const endpoint = `${API_BASE_URL}/live-screeners/screener${screenerNum}`;
      const payload = {
        filters: liveFilters,
        date_filters: dateFilters
      };

      if (screenerNum === 2) {
        payload.date_range = trendSettings.date_range || 6;
        payload.date_filter_type = trendSettings.date_filter_type || 'rolling';
        payload.specific_month = trendSettings.specific_month;
        payload.specific_year = trendSettings.specific_year;
        payload.start_month = trendSettings.start_month;
        payload.end_month = trendSettings.end_month;
      } else if (screenerNum === 4) {
        payload.breakdown_filter = cohortSettings.breakdown_filter;
        payload.result_filter = cohortSettings.result_filter;
        payload.milestone_type = cohortSettings.milestone_type;
        payload.date_range = cohortSettings.date_range || 12;
        payload.date_filter_type = cohortSettings.date_filter_type || 'rolling';
        payload.specific_month = cohortSettings.specific_month;
        payload.specific_year = cohortSettings.specific_year;
        payload.start_month = cohortSettings.start_month;
        payload.end_month = cohortSettings.end_month;
        payload.cohort_type = cohortSettings.cohort_type || 'forward';  // Add cohort type
      }

      const response = await axios.post(endpoint, payload);

      switch (screenerNum) {
        case 1:
          setScreener1Data(response.data);
          break;
        case 2:
          setScreener2Data(response.data.trend_data || []);
          // Store the months array in the data object for the table to access
          if (response.data.months) {
            setScreener2Data(prevData => {
              const dataWithMonths = response.data.trend_data || [];
              dataWithMonths.months = response.data.months;
              return dataWithMonths;
            });
          }
          break;
        case 3:
          setScreener3Data(response.data);
          break;
        case 4:
          setScreener4Data(response.data);
          break;
      }
    } catch (err) {
      setLiveScreenerError(err.response?.data?.error || `Failed to load screener ${screenerNum} data`);
    } finally {
      setLiveScreenerLoading(false);
    }
  };

  const handleLiveFilterChange = (filterType, values) => {
    setLiveFilters(prev => ({
      ...prev,
      [filterType]: values
    }));
  };

  const handleDateFilterChange = (dateType, value) => {
    setDateFilters(prev => ({
      ...prev,
      [dateType]: value
    }));
  };

  const handleSort = (key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  const sortData = (data) => {
    if (!data || data.length === 0) {
      return data;
    }

    // Create a copy of the data
    let sortedData = [...data];

    // If we have a specific sort configuration, use that
    if (sortConfig.key) {
      return sortedData.sort((a, b) => {
        const aValue = a[sortConfig.key];
        const bValue = b[sortConfig.key];

        if (aValue === null || aValue === undefined || aValue === '-') return 1;
        if (bValue === null || bValue === undefined || bValue === '-') return -1;

        const parseNumericValue = (value) => {
          if (typeof value === 'number') return value;
          if (typeof value === 'string') {
            const cleaned = value.replace(/,/g, '').replace(/%/g, '').replace(/\+/g, '');
            const parsed = parseFloat(cleaned);
            return isNaN(parsed) ? value : parsed;
          }
          return value;
        };

        const aParsed = parseNumericValue(aValue);
        const bParsed = parseNumericValue(bValue);

        let comparison = 0;
        if (typeof aParsed === 'number' && typeof bParsed === 'number') {
          comparison = aParsed - bParsed;
        } else {
          comparison = String(aParsed).localeCompare(String(bParsed));
        }

        return sortConfig.direction === 'asc' ? comparison : -comparison;
      });
    }

    // Otherwise, use hierarchical sorting based on filter columns
    const colMap = {
      'partner_regions': 'Partner Region',
      'partner_countries': 'Partner Country',
      'partner_platforms': 'Platform',
      'aff_types': 'Plan Type',
      'partner_levels': 'Partner Level',
      'event_statuses': 'Event Status',
      'acquisition_types': 'Acquisition Type',
      'plan_types': 'Plan Types'
    };

    // Get the active filter columns in their display order
    const sortColumns = filterColumnOrder
      .filter(filterType => activeFilters[filterType]?.showAsColumn)
      .map(filterType => colMap[filterType]);

    return sortedData.sort((a, b) => {
      // Compare each column in order until we find a difference
      for (const column of sortColumns) {
        const aValue = (a[column] || '').toString();
        const bValue = (b[column] || '').toString();
        
        const comparison = aValue.localeCompare(bValue);
        if (comparison !== 0) {
          return comparison;
        }
      }
      return 0;
    });
  };

  useEffect(() => {
    if (activeTab === 'live-screeners') {
      const delayedFetch = setTimeout(() => {
        loadLiveScreenerData(activeScreener);
      }, 500);
      
      return () => clearTimeout(delayedFetch);
    }
  }, [activeScreener, liveFilters, dateFilters, activeTab, trendSettings, cohortSettings]);

  const exportToCSV = (data, filename, screenerType = 'standard') => {
    let csv = '';
    
    if (screenerType === 'trend' && Array.isArray(data)) {
      if (data.length > 0) {
        const headers = Object.keys(data[0]);
        csv = headers.join(',') + '\n';
        csv += data.map(row => headers.map(header => `"${row[header] || ''}"`).join(',')).join('\n');
      }
    } else if (screenerType === 'cohort' && data.cohort_data) {
      if (data.cohort_data.length > 0) {
        const headers = Object.keys(data.cohort_data[0]);
        csv = headers.join(',') + '\n';
        csv += data.cohort_data.map(row => headers.map(header => `"${row[header] || ''}"`).join(',')).join('\n');
      }
    } else if (Array.isArray(data) && data.length > 0) {
      const headers = Object.keys(data[0]);
      csv = headers.join(',') + '\n';
      csv += data.map(row => headers.map(header => `"${row[header] || ''}"`).join(',')).join('\n');
    }
    
    if (csv) {
      const blob = new Blob([csv], { type: 'text/csv' });
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      a.click();
      window.URL.revokeObjectURL(url);
    }
  };

  const handleFilterCheckboxChange = (filterType) => {
    const newFilters = { ...activeFilters };
    
    if (newFilters[filterType]) {
      // If unchecking, remove all values and the filter entirely
      delete newFilters[filterType];
      // Remove from filter column order
      setFilterColumnOrder(prev => prev.filter(f => f !== filterType));
    } else {
      // If checking, initialize with empty values
      newFilters[filterType] = {
        values: [],
        showAsColumn: true
      };
      // Add to filter column order if not already present
      if (!filterColumnOrder.includes(filterType)) {
        setFilterColumnOrder(prev => [...prev, filterType]);
      }
    }
    
    setActiveFilters(newFilters);
  };

  return (
    <div className="app-container">
      {/* Header */}
      <header className="header">
        <div className="header-content">
          <div className="header-top">
            <div className="logo-section">
              <div className="logo-container">
                <img 
                  src="/Deriv.png" 
                  alt="Deriv Logo" 
                  className="deriv-logo"
                />
                <div className="logo">
                  <div className="logo-icon">N</div>
                  <span>Nexus Partner Analytics</span>
                </div>
              </div>
            </div>
          </div>
          
          {/* Navigation */}
          <nav className="nav-tabs">
            <button
              onClick={() => {
                setActiveTab('live-screeners');
                if (Object.keys(availableFilters).length === 0) {
                  loadScreenerData();
                }
              }}
              className={`nav-tab ${activeTab === 'live-screeners' ? 'active' : ''}`}
            >
              Live Screeners
            </button>
            <button
              onClick={() => {
                setActiveTab('screener');
                if (Object.keys(availableMetrics).length === 0) {
                  loadScreenerData();
                }
              }}
              className={`nav-tab ${activeTab === 'screener' ? 'active' : ''}`}
            >
              Metrics Test
            </button>
            <button
              onClick={() => setActiveTab('agent-test')}
              className={`nav-tab ${activeTab === 'agent-test' || activeTab === 'sql' || activeTab === 'analytics' ? 'active' : ''}`}
            >
              Agents Test
            </button>
          </nav>
        </div>
      </header>

      <main className="main-content">
        {/* Agent Test Tab Content */}
        {(activeTab === 'agent-test' || activeTab === 'sql' || activeTab === 'analytics') && (
          <div className="fade-in">
            {/* Query Input Section */}
            <div className="card">
              <div className="card-header">
                <h2 className="heading-md">Enter Your Business Query</h2>
                <p className="text-sm" style={{ color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
                  Ask questions about your partner data in natural language
                </p>
              </div>
              
              <div className="card-body">
                <div className="form-group">
                  <textarea
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Example: What is the number of new partner signups per month, broken down by country?"
                    className="form-textarea"
                    style={{ fontFamily: 'IBM Plex Sans' }}
                  />
                </div>

                <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap' }}>
                  <button
                    onClick={handleSqlTest}
                    disabled={loading}
                    className="btn btn-primary"
                    style={{ opacity: loading ? 0.7 : 1 }}
                  >
                    {loading && !analyticsResult ? (
                      <>
                        <div className="loading-spinner" style={{ width: '16px', height: '16px' }}></div>
                        Testing SQL Agent...
                      </>
                    ) : (
                      <>
                        <DatabaseIcon />
                        Test SQL Agent Only
                      </>
                    )}
                  </button>

                  <button
                    onClick={handleAnalyticsTest}
                    disabled={loading}
                    className="btn btn-primary"
                    style={{ opacity: loading ? 0.7 : 1 }}
                  >
                    {loading && !sqlResult ? (
                      <>
                        <div className="loading-spinner" style={{ width: '16px', height: '16px' }}></div>
                        Testing SQL + Analytics...
                      </>
                    ) : (
                      <>
                        <ChartIcon />
                        Test SQL + Analytics
                      </>
                    )}
                  </button>

                  <button
                    onClick={clearResults}
                    className="btn btn-secondary"
                  >
                    Clear Results
                  </button>
                </div>

                {error && (
                  <div style={{
                    marginTop: '1rem',
                    padding: '1rem',
                    backgroundColor: 'var(--primary-red-light)',
                    color: 'var(--primary-red)',
                    borderRadius: 'var(--radius-md)',
                    fontSize: '0.875rem'
                  }}>
                    {error}
                  </div>
                )}
              </div>
            </div>

            {/* Results Section */}
            {(sqlResult || analyticsResult) && (
              <div className="card" style={{ marginTop: '1.5rem' }}>
                <div className="card-header">
                  <h3 className="heading-md">Results</h3>
                </div>

                <div className="card-body">
                  {/* SQL Results */}
                  {sqlResult && (
                    <div style={{ marginBottom: '2rem' }}>
                      <h4 className="heading-sm" style={{ color: 'var(--primary-red)', marginBottom: '1rem' }}>
                        SQL Agent Results
                      </h4>
                      
                      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                        <div>
                          <label className="form-label">Original Query:</label>
                          <div style={{
                            backgroundColor: 'var(--bg-tertiary)',
                            padding: '1rem',
                            borderRadius: 'var(--radius-md)',
                            fontSize: '0.875rem'
                          }}>
                            {sqlResult.query}
                          </div>
                        </div>

                        <div>
                          <label className="form-label">Generated SQL:</label>
                          <pre style={{
                            backgroundColor: '#1f2937',
                            color: '#10b981',
                            padding: '1rem',
                            borderRadius: 'var(--radius-md)',
                            overflowX: 'auto',
                            fontSize: '0.8125rem',
                            fontFamily: 'monospace'
                          }}>
                            {sqlResult.sql_query}
                          </pre>
                        </div>

                        <div>
                          <label className="form-label">Query Results:</label>
                          <pre style={{
                            backgroundColor: 'var(--bg-tertiary)',
                            padding: '1rem',
                            borderRadius: 'var(--radius-md)',
                            overflowX: 'auto',
                            fontSize: '0.8125rem',
                            whiteSpace: 'pre-wrap'
                          }}>
                            {sqlResult.results}
                          </pre>
                        </div>

                        {sqlResult.verification_result && (
                          <div>
                            <label className="form-label">Verification Details:</label>
                            <div style={{
                              backgroundColor: '#fef3c7',
                              color: '#92400e',
                              padding: '1rem',
                              borderRadius: 'var(--radius-md)',
                              fontSize: '0.875rem'
                            }}>
                              {sqlResult.verification_result}
                            </div>
                          </div>
                        )}

                        <div style={{
                          display: 'flex',
                          gap: '1rem',
                          fontSize: '0.8125rem',
                          color: 'var(--text-secondary)'
                        }}>
                          <span>Attempts: <strong>{sqlResult.attempts}</strong></span>
                          <span>Status: <span style={{
                            color: sqlResult.success ? '#059669' : 'var(--primary-red)',
                            fontWeight: '600'
                          }}>
                            {sqlResult.success ? '✓ Success' : '✗ Failed'}
                          </span></span>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Analytics Results */}
                  {analyticsResult && (
                    <div>
                      <h4 className="heading-sm" style={{ color: 'var(--primary-red)', marginBottom: '1rem' }}>
                        SQL + Analytics Results
                      </h4>
                      
                      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                        <div>
                          <label className="form-label">Original Query:</label>
                          <div style={{
                            backgroundColor: 'var(--bg-tertiary)',
                            padding: '1rem',
                            borderRadius: 'var(--radius-md)',
                            fontSize: '0.875rem'
                          }}>
                            {analyticsResult.query}
                          </div>
                        </div>

                        <div>
                          <label className="form-label">Generated SQL:</label>
                          <pre style={{
                            backgroundColor: '#1f2937',
                            color: '#10b981',
                            padding: '1rem',
                            borderRadius: 'var(--radius-md)',
                            overflowX: 'auto',
                            fontSize: '0.8125rem',
                            fontFamily: 'monospace'
                          }}>
                            {analyticsResult.sql_query}
                          </pre>
                        </div>

                        <div>
                          <label className="form-label">SQL Results:</label>
                          <pre style={{
                            backgroundColor: 'var(--bg-tertiary)',
                            padding: '1rem',
                            borderRadius: 'var(--radius-md)',
                            overflowX: 'auto',
                            fontSize: '0.8125rem',
                            whiteSpace: 'pre-wrap'
                          }}>
                            {analyticsResult.sql_results}
                          </pre>
                        </div>

                        <div>
                          <label className="form-label">Analytics Report:</label>
                          <div style={{
                            backgroundColor: 'var(--primary-red-lighter)',
                            padding: '1rem',
                            borderRadius: 'var(--radius-md)',
                            border: '1px solid var(--primary-red-light)'
                          }}>
                            <pre style={{
                              whiteSpace: 'pre-wrap',
                              fontSize: '0.875rem',
                              color: 'var(--text-primary)',
                              fontFamily: 'IBM Plex Sans'
                            }}>
                              {analyticsResult.analytics_report}
                            </pre>
                          </div>
                        </div>

                        {/* Visualization Images */}
                        {analyticsResult.visualization_images && analyticsResult.visualization_images.length > 0 && (
                          <div>
                            <label className="form-label">Generated Visualizations:</label>
                            <div style={{
                              display: 'grid',
                              gridTemplateColumns: 'repeat(auto-fit, minmax(400px, 1fr))',
                              gap: '1rem'
                            }}>
                              {analyticsResult.visualization_images.map((viz, index) => (
                                <div key={index} className="card">
                                  <div className="card-header" style={{
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    alignItems: 'center'
                                  }}>
                                    <h5 className="heading-sm">{viz.title}</h5>
                                    <span className="badge badge-gray">{viz.type}</span>
                                  </div>
                                  <div className="card-body">
                                    <img 
                                      src={`data:image/png;base64,${viz.data}`}
                                      alt={viz.title}
                                      style={{
                                        width: '100%',
                                        height: 'auto',
                                        borderRadius: 'var(--radius-md)'
                                      }}
                                    />
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}

                        <div style={{
                          display: 'flex',
                          gap: '1rem',
                          fontSize: '0.8125rem',
                          color: 'var(--text-secondary)'
                        }}>
                          <span>SQL Attempts: <strong>{analyticsResult.sql_attempts}</strong></span>
                          <span>Status: <span style={{
                            color: analyticsResult.success ? '#059669' : 'var(--primary-red)',
                            fontWeight: '600'
                          }}>
                            {analyticsResult.success ? '✓ Success' : '✗ Failed'}
                          </span></span>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Screener Tab Content */}
        {activeTab === 'screener' && (
          <div className="fade-in">
            {/* Screener Controls */}
            <div className="card">
              <div className="card-header" style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center'
              }}>
                <div>
                  <h2 className="heading-md">Metrics Screener</h2>
                  <p className="text-sm" style={{ color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
                    Select metrics and apply filters to analyze partner performance
                  </p>
                </div>
                {screenerLoading && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', color: 'var(--primary-red)' }}>
                    <div className="loading-spinner" style={{ width: '16px', height: '16px' }}></div>
                    <span className="text-sm">Updating...</span>
                  </div>
                )}
              </div>
              
              <div className="card-body">
                {screenerLoading && Object.keys(availableMetrics).length === 0 ? (
                  <div style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    padding: '3rem'
                  }}>
                    <div className="loading-spinner" style={{ marginRight: '1rem' }}></div>
                    <span>Loading metrics and filters...</span>
                  </div>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
                    {/* Metrics Selection */}
                    <div>
                      <h3 className="heading-sm" style={{ marginBottom: '1rem' }}>Select Metrics</h3>
                      <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
                        gap: '1rem'
                      }}>
                        {Object.entries(availableMetrics).map(([category, metrics]) => (
                          <div key={category} className="card" style={{ padding: '1rem' }}>
                            <h4 style={{
                              fontSize: '0.75rem',
                              fontWeight: '600',
                              textTransform: 'uppercase',
                              letterSpacing: '0.05em',
                              color: 'var(--text-secondary)',
                              marginBottom: '0.75rem'
                            }}>
                              {category.replace('_', ' ')}
                            </h4>
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                              {Object.entries(metrics).map(([key, displayName]) => (
                                <label key={key} style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  padding: '0.5rem',
                                  borderRadius: 'var(--radius-sm)',
                                  cursor: 'pointer',
                                  transition: 'background-color 0.2s',
                                  backgroundColor: selectedMetrics.includes(displayName) ? 'var(--primary-red-lighter)' : 'transparent'
                                }}>
                                  <input
                                    type="checkbox"
                                    checked={selectedMetrics.includes(displayName)}
                                    onChange={() => handleMetricToggle(displayName)}
                                    style={{
                                      marginRight: '0.75rem',
                                      width: '16px',
                                      height: '16px',
                                      accentColor: 'var(--primary-red)'
                                    }}
                                  />
                                  <span className="text-sm">{displayName}</span>
                                </label>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* Filters */}
                    <div>
                      <div style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                        marginBottom: '1rem'
                      }}>
                        <h3 className="heading-sm">Filters</h3>
                        {Object.keys(activeFilters).length > 0 && (
                          <button
                            onClick={() => setActiveFilters({})}
                            className="btn btn-sm btn-ghost"
                            style={{ color: 'var(--primary-red)' }}
                          >
                            Clear All Filters
                          </button>
                        )}
                      </div>
                      
                      {/* Filters section */}
                      <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
                        gap: '1rem'
                      }}>
                        {/* Existing filters */}
                        {Object.entries(availableFilters).map(([filterType, options]) => (
                          <div key={filterType} className="form-group">
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                              <label className="form-label" style={{ margin: 0, flex: 1 }}>
                                {filterType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                              </label>
                              <input
                                type="checkbox"
                                checked={!!activeFilters[filterType]?.showAsColumn}
                                onChange={() => {
                                  const newFilters = { ...activeFilters };
                                  if (newFilters[filterType]) {
                                    delete newFilters[filterType];
                                    setFilterColumnOrder(prev => prev.filter(f => f !== filterType));
                                  } else {
                                    newFilters[filterType] = {
                                      values: [],
                                      showAsColumn: true
                                    };
                                    if (!filterColumnOrder.includes(filterType)) {
                                      setFilterColumnOrder(prev => [...prev, filterType]);
                                    }
                                  }
                                  setActiveFilters(newFilters);
                                }}
                                style={{
                                  width: '16px',
                                  height: '16px',
                                  accentColor: 'var(--primary-red)',
                                  border: '1px solid var(--border-color)',
                                  borderRadius: '3px'
                                }}
                              />
                            </div>
                            <select
                              value=""
                              onChange={(e) => {
                                if (e.target.value) {
                                  const currentValues = activeFilters[filterType]?.values || [];
                                  if (!currentValues.includes(e.target.value)) {
                                    const newFilters = { ...activeFilters };
                                    if (!newFilters[filterType]) {
                                      newFilters[filterType] = { 
                                        values: [e.target.value], 
                                        showAsColumn: true  // Automatically show as column when values are selected
                                      };
                                      // Add to filter column order if not already present
                                      if (!filterColumnOrder.includes(filterType)) {
                                        setFilterColumnOrder(prev => [...prev, filterType]);
                                      }
                                    } else {
                                      newFilters[filterType] = { 
                                        ...newFilters[filterType], 
                                        values: [...currentValues, e.target.value],
                                        showAsColumn: true  // Ensure column is shown when values are selected
                                      };
                                      // Add to filter column order if not already present
                                      if (!filterColumnOrder.includes(filterType)) {
                                        setFilterColumnOrder(prev => [...prev, filterType]);
                                      }
                                    }
                                    setActiveFilters(newFilters);
                                  }
                                }
                              }}
                              className="form-select"
                              style={{
                                backgroundColor: 'var(--bg-primary)',
                                color: 'var(--text-primary)',
                                opacity: activeFilters[filterType]?.showAsColumn ? 1 : 0.5,
                                cursor: activeFilters[filterType]?.showAsColumn ? 'pointer' : 'not-allowed'
                              }}
                              disabled={!activeFilters[filterType]?.showAsColumn}
                            >
                              <option value="">Select {filterType.replace('_', ' ')}...</option>
                              {(options || []).map(option => (
                                <option 
                                  key={option} 
                                  value={option}
                                  disabled={activeFilters[filterType]?.values?.includes(option)}
                                >
                                  {option}
                                </option>
                              ))}
                            </select>
                            {activeFilters[filterType]?.values && activeFilters[filterType].values.length > 0 && (
                              <div style={{ 
                                display: 'flex', 
                                flexWrap: 'wrap', 
                                gap: '0.5rem', 
                                marginTop: '0.5rem',
                                minHeight: '28px'
                              }}>
                                {activeFilters[filterType].values.map(value => (
                                  <span 
                                    key={value} 
                                    className="badge badge-red"
                                    style={{
                                      display: 'flex',
                                      alignItems: 'center',
                                      gap: '0.25rem',
                                      padding: '0.25rem 0.5rem',
                                      fontSize: '0.75rem'
                                    }}
                                  >
                                    {value}
                                    <button
                                      onClick={() => {
                                        const newFilters = { ...activeFilters };
                                        const newValues = newFilters[filterType].values.filter(v => v !== value);
                                        if (newValues.length === 0) {
                                          // If no values left, remove the filter completely
                                          delete newFilters[filterType];
                                          // Remove from filter column order
                                          setFilterColumnOrder(prev => prev.filter(f => f !== filterType));
                                        } else {
                                          newFilters[filterType] = {
                                            ...newFilters[filterType],
                                            values: newValues
                                          };
                                        }
                                        setActiveFilters(newFilters);
                                      }}
                                      style={{
                                        background: 'none',
                                        border: 'none',
                                        cursor: 'pointer',
                                        color: 'inherit'
                                      }}
                                    >
                                      ×
                                    </button>
                                  </span>
                                ))}
                              </div>
                            )}
                          </div>
                        ))}

                        {/* Date Joined Filter */}
                        <div className="form-group">
                          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                            <label className="form-label" style={{ margin: 0, flex: 1 }}>
                              Date Joined Filter
                            </label>
                            <input
                              type="checkbox"
                              checked={!!activeFilters['date_joined']?.showAsColumn}
                              onChange={() => {
                                const newFilters = { ...activeFilters };
                                if (newFilters['date_joined']) {
                                  delete newFilters['date_joined'];
                                  setFilterColumnOrder(prev => prev.filter(f => f !== 'date_joined'));
                                } else {
                                  newFilters['date_joined'] = {
                                    start_date: `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`,
                                    end_date: `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`,
                                    showAsColumn: true
                                  };
                                  if (!filterColumnOrder.includes('date_joined')) {
                                    setFilterColumnOrder(prev => [...prev, 'date_joined']);
                                  }
                                }
                                setActiveFilters(newFilters);
                              }}
                              style={{
                                width: '16px',
                                height: '16px',
                                accentColor: 'var(--primary-red)',
                                border: '1px solid var(--border-color)',
                                borderRadius: '3px'
                              }}
                            />
                          </div>
                          <div style={{ display: 'flex', gap: '0.5rem' }}>
                            <input
                              type="month"
                              value={activeFilters.date_joined?.start_date || `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`}
                              onChange={(e) => {
                                const newFilters = { ...activeFilters };
                                if (!newFilters['date_joined']) {
                                  newFilters['date_joined'] = {
                                    start_date: e.target.value,
                                    end_date: e.target.value,
                                    showAsColumn: true
                                  };
                                } else {
                                  newFilters['date_joined'] = {
                                    ...newFilters['date_joined'],
                                    start_date: e.target.value,
                                    end_date: e.target.value
                                  };
                                }
                                setActiveFilters(newFilters);
                              }}
                              className="form-input"
                              style={{
                                opacity: activeFilters['date_joined']?.showAsColumn ? 1 : 0.5,
                                cursor: activeFilters['date_joined']?.showAsColumn ? 'pointer' : 'not-allowed'
                              }}
                              disabled={!activeFilters['date_joined']?.showAsColumn}
                            />
                          </div>
                          {activeFilters['date_joined']?.showAsColumn && (activeFilters.date_joined?.start_date || activeFilters.date_joined?.end_date) && (
                            <div style={{ 
                              display: 'flex', 
                              flexWrap: 'wrap', 
                              gap: '0.5rem', 
                              marginTop: '0.5rem',
                              minHeight: '28px'
                            }}>
                              <span 
                                className="badge badge-red"
                                style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '0.25rem',
                                  padding: '0.25rem 0.5rem',
                                  fontSize: '0.75rem'
                                }}
                              >
                                {new Date(activeFilters.date_joined.start_date).toLocaleDateString('default', { month: 'short', year: 'numeric' })}
                                <button
                                  onClick={() => {
                                    const newFilters = { ...activeFilters };
                                    delete newFilters['date_joined'];
                                    setFilterColumnOrder(prev => prev.filter(f => f !== 'date_joined'));
                                    setActiveFilters(newFilters);
                                  }}
                                  style={{
                                    background: 'none',
                                    border: 'none',
                                    cursor: 'pointer',
                                    color: 'inherit',
                                    padding: '0 0 0 0.25rem',
                                    fontSize: '1rem',
                                    lineHeight: '1',
                                    display: 'flex',
                                    alignItems: 'center'
                                  }}
                                >
                                  ×
                                </button>
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>

                    {/* Selected Metrics Summary */}
                    {selectedMetrics.length > 0 && (
                      <div style={{
                        padding: '1rem',
                        backgroundColor: 'var(--primary-red-lighter)',
                        border: '1px solid var(--primary-red-light)',
                        borderRadius: 'var(--radius-md)'
                      }}>
                        <div style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center'
                        }}>
                          <div>
                            <p className="text-sm" style={{
                              fontWeight: '600',
                              color: 'var(--primary-red)'
                            }}>
                              {selectedMetrics.length} Metric{selectedMetrics.length !== 1 ? 's' : ''} Selected
                            </p>
                            <p className="text-xs" style={{
                              color: 'var(--primary-red)',
                              marginTop: '0.25rem'
                            }}>
                              {selectedMetrics.join(' • ')}
                            </p>
                          </div>
                          <button
                            onClick={clearScreenerResults}
                            className="btn btn-sm btn-secondary"
                          >
                            Reset All
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                )}

                {screenerError && (
                  <div style={{
                    marginTop: '1rem',
                    padding: '1rem',
                    backgroundColor: 'var(--primary-red-light)',
                    color: 'var(--primary-red)',
                    borderRadius: 'var(--radius-md)',
                    fontSize: '0.875rem'
                  }}>
                    {screenerError}
                  </div>
                )}
              </div>
            </div>

            {/* Results Section */}
            {screenerData && (
              <div className="card" style={{ marginTop: '1.5rem' }}>
                <div className="card-header" style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center'
                }}>
                  <div>
                    <h3 className="heading-md">Results</h3>
                    <p className="text-sm" style={{
                      color: 'var(--text-secondary)',
                      marginTop: '0.25rem'
                    }}>
                      {screenerData.data.length} row{screenerData.data.length !== 1 ? 's' : ''} • {screenerData.columns.length} column{screenerData.columns.length !== 1 ? 's' : ''}
                    </p>
                  </div>
                  <button
                    onClick={() => exportToCSV(screenerData.data, 'screener_results.csv')}
                    className="btn btn-sm btn-primary"
                  >
                    <DownloadIcon />
                    Export CSV
                  </button>
                </div>

                {screenerData.data.length > 0 ? (
                  <div className="table-container">
                    <table className="grid-table">
                      <thead>
                        <tr>
                          {/* Show filter columns in order they were checked */}
                          {filterColumnOrder
                            .filter(filterType => activeFilters[filterType]?.showAsColumn)
                            .map(filterType => (
                              <th
                                key={filterType}
                                className="sortable"
                                onClick={() => handleSort(filterType)}
                                style={{
                                  position: 'sticky',
                                  top: 0,
                                  backgroundColor: 'var(--bg-tertiary)',
                                  zIndex: 1
                                }}
                              >
                                <div style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '0.25rem'
                                }}>
                                  {filterType.replace('_', ' ').replace(/\b\w/g, l => l.toUpperCase())}
                                  {sortConfig.key === filterType && (
                                    sortConfig.direction === 'asc' ? <ArrowUpIcon /> : <ArrowDownIcon />
                                  )}
                                </div>
                              </th>
                            ))}
                          {/* Then show metric columns */}
                          {selectedMetrics.map(metric => (
                            <th
                              key={metric}
                              className="sortable"
                              onClick={() => handleSort(metric)}
                              style={{
                                position: 'sticky',
                                top: 0,
                                backgroundColor: 'var(--bg-primary)',
                                zIndex: 1
                              }}
                            >
                              <div style={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: '0.25rem'
                              }}>
                                {metric}
                                {sortConfig.key === metric && (
                                  sortConfig.direction === 'asc' ? <ArrowUpIcon /> : <ArrowDownIcon />
                                )}
                              </div>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sortData(screenerData.data).map((row, idx) => (
                          <tr key={idx}>
                            {/* Show filter values in same order as headers */}
                            {filterColumnOrder
                              .filter(filterType => activeFilters[filterType]?.showAsColumn)
                              .map(filterType => {
                                const colMap = {
                                  'partner_regions': 'Partner Region',
                                  'partner_countries': 'Partner Country',
                                  'partner_platforms': 'Platform',
                                  'aff_types': 'Plan Type',
                                  'partner_levels': 'Partner Level',
                                  'event_statuses': 'Event Status',
                                  'acquisition_types': 'Acquisition Type',
                                  'plan_types': 'Plan Types',
                                  'date_joined': 'Date Joined'  // Add date_joined mapping
                                };
                                return (
                                  <td key={filterType} style={{
                                    backgroundColor: 'var(--bg-tertiary)'
                                  }}>
                                    {filterType === 'date_joined' && row[colMap[filterType]] 
                                      ? new Date(row[colMap[filterType]]).toLocaleDateString('default', { month: 'short', year: 'numeric' })
                                      : row[colMap[filterType]] || '-'}
                                  </td>
                                );
                              })}
                            {/* Then show all metric values */}
                            {selectedMetrics.map(metric => (
                              <td key={metric}>
                                {row[metric] || '-'}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="empty-state">
                    <div className="empty-state-icon">
                      <ChartIcon />
                    </div>
                    <h4 className="empty-state-title">No data available</h4>
                    <p className="empty-state-text">
                      Try adjusting your filters or selecting different metrics.
                    </p>
                  </div>
                )}
              </div>
            )}

            {/* Quick Start Guide */}
            {selectedMetrics.length === 0 && Object.keys(availableMetrics).length > 0 && (
              <div className="card" style={{
                marginTop: '1.5rem',
                backgroundColor: 'var(--primary-red-lighter)',
                border: '1px solid var(--primary-red-light)'
              }}>
                <div className="card-body">
                  <h3 className="heading-sm" style={{ color: 'var(--primary-red)', marginBottom: '0.75rem' }}>
                    Quick Start Guide
                  </h3>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                    <p className="text-sm"><strong>1.</strong> Select one or more metrics from the categories above</p>
                    <p className="text-sm"><strong>2.</strong> Add filters to narrow down your analysis (optional)</p>
                    <p className="text-sm"><strong>3.</strong> Results will appear automatically as you make selections</p>
                    <p className="text-sm"><strong>4.</strong> Export your results to CSV when ready</p>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Live Screeners Tab Content */}
        {activeTab === 'live-screeners' && (
          <div className="fade-in">
            {/* Screener Selection */}
            <div className="card">
              <div className="card-header" style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center'
              }}>
                <div>
                  <h2 className="heading-md">Live Screeners</h2>
                  <p className="text-sm" style={{ color: 'var(--text-secondary)', marginTop: '0.25rem' }}>
                    Pre-built dashboards for comprehensive partner analysis
                  </p>
                </div>
                {liveScreenerLoading && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', color: 'var(--primary-red)' }}>
                    <div className="loading-spinner" style={{ width: '16px', height: '16px' }}></div>
                    <span className="text-sm">Loading...</span>
                  </div>
                )}
              </div>

              <div className="card-body">
                {/* Screener Tabs */}
                <div style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  borderBottom: '1px solid var(--border-color)',
                  marginBottom: '1.5rem',
                  paddingBottom: '0'
                }}>
                  {[
                    { id: 1, name: 'Performance Overview' },
                    { id: 2, name: 'Trend Analysis' },
                    { id: 3, name: 'Individual Partner' },
                    { id: 4, name: 'Cohort Analysis' }
                  ].map(screener => (
                    <button
                      key={screener.id}
                      onClick={() => setActiveScreener(screener.id)}
                      className={`nav-tab ${activeScreener === screener.id ? 'active' : ''}`}
                      style={{ 
                        flex: '1',
                        padding: '0.75rem 1rem',
                        borderBottom: activeScreener === screener.id ? '3px solid var(--primary-red)' : 'none',
                        marginBottom: activeScreener === screener.id ? '-1px' : '0'
                      }}
                    >
                      {screener.name}
                    </button>
                  ))}
                </div>

                {/* Common Filters */}
                <div style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
                  gap: '1rem',
                  marginBottom: '1.5rem'
                }}>
                  <div className="form-group">
                    <label className="form-label">Filter by Region</label>
                    <select
                      value=""
                      onChange={(e) => {
                        if (e.target.value) {
                          const currentValues = liveFilters.partner_regions || [];
                          if (!currentValues.includes(e.target.value)) {
                            handleLiveFilterChange('partner_regions', [...currentValues, e.target.value]);
                          }
                        }
                      }}
                      className="form-select"
                    >
                      <option value="">Select a region...</option>
                      {(availableFilters.partner_regions || []).map(region => (
                        <option key={region} value={region}>{region}</option>
                      ))}
                    </select>
                    {liveFilters.partner_regions && liveFilters.partner_regions.length > 0 && (
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem', marginTop: '0.5rem' }}>
                        {liveFilters.partner_regions.map(region => (
                          <span key={region} className="badge badge-red">
                            {region}
                            <button
                              onClick={() => {
                                const newValues = liveFilters.partner_regions.filter(r => r !== region);
                                handleLiveFilterChange('partner_regions', newValues);
                              }}
                              style={{
                                marginLeft: '0.5rem',
                                background: 'none',
                                border: 'none',
                                cursor: 'pointer',
                                color: 'inherit'
                              }}
                            >
                              ×
                            </button>
                          </span>
                        ))}
                      </div>
                    )}
                  </div>

                  <div className="form-group">
                    <label className="form-label">Filter by Country</label>
                    <select
                      value=""
                      onChange={(e) => {
                        if (e.target.value) {
                          const currentValues = liveFilters.partner_countries || [];
                          if (!currentValues.includes(e.target.value)) {
                            handleLiveFilterChange('partner_countries', [...currentValues, e.target.value]);
                          }
                        }
                      }}
                      className="form-select"
                    >
                      <option value="">Select a country...</option>
                      {(availableFilters.partner_countries || []).map(country => (
                        <option key={country} value={country}>{country}</option>
                      ))}
                    </select>
                    {liveFilters.partner_countries && liveFilters.partner_countries.length > 0 && (
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem', marginTop: '0.5rem' }}>
                        {liveFilters.partner_countries.map(country => (
                          <span key={country} className="badge badge-red">
                            {country}
                            <button
                              onClick={() => {
                                const newValues = liveFilters.partner_countries.filter(c => c !== country);
                                handleLiveFilterChange('partner_countries', newValues);
                              }}
                              style={{
                                marginLeft: '0.5rem',
                                background: 'none',
                                border: 'none',
                                cursor: 'pointer',
                                color: 'inherit'
                              }}
                            >
                              ×
                            </button>
                          </span>
                        ))}
                      </div>
                    )}
                  </div>

                  <div className="form-group">
                    <label className="form-label">Filter by Plan Type</label>
                    <select
                      value=""
                      onChange={(e) => {
                        if (e.target.value) {
                          const currentValues = liveFilters.aff_types || [];
                          if (!currentValues.includes(e.target.value)) {
                            handleLiveFilterChange('aff_types', [...currentValues, e.target.value]);
                          }
                        }
                      }}
                      className="form-select"
                    >
                      <option value="">Select a plan type...</option>
                      {(availableFilters.aff_types || []).map(type => (
                        <option key={type} value={type}>{type}</option>
                      ))}
                    </select>
                    {liveFilters.aff_types && liveFilters.aff_types.length > 0 && (
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem', marginTop: '0.5rem' }}>
                        {liveFilters.aff_types.map(type => (
                          <span key={type} className="badge badge-red">
                            {type}
                            <button
                              onClick={() => {
                                const newValues = liveFilters.aff_types.filter(t => t !== type);
                                handleLiveFilterChange('aff_types', newValues);
                              }}
                              style={{
                                marginLeft: '0.5rem',
                                background: 'none',
                                border: 'none',
                                cursor: 'pointer',
                                color: 'inherit'
                              }}
                            >
                              ×
                            </button>
                          </span>
                        ))}
                      </div>
                    )}
                  </div>

                  {/* Screener 2 - Trend Analysis Controls */}
                  {activeScreener === 2 && (
                    <>
                      <div className="form-group">
                        <label className="form-label">From Month-Year</label>
                        <input
                          type="month"
                          value={trendSettings.start_month || `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`}
                          onChange={(e) => setTrendSettings(prev => ({...prev, start_month: e.target.value, date_filter_type: 'range'}))}
                          className="form-input"
                        />
                      </div>
                      <div className="form-group">
                        <label className="form-label">To Month-Year</label>
                        <input
                          type="month"
                          value={trendSettings.end_month || `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`}
                          onChange={(e) => setTrendSettings(prev => ({...prev, end_month: e.target.value, date_filter_type: 'range'}))}
                          className="form-input"
                        />
                      </div>
                    </>
                  )}

                  {/* Screener 3 - Date Filters */}
                  {activeScreener === 3 && (
                    <>
                      <div className="form-group">
                        <label className="form-label">Date Joined</label>
                        <input
                          type="date"
                          value={dateFilters.date_joined || ''}
                          onChange={(e) => handleDateFilterChange('date_joined', e.target.value)}
                          className="form-input"
                        />
                      </div>
                      <div className="form-group">
                        <label className="form-label">First Client Joined Date</label>
                        <input
                          type="date"
                          value={dateFilters.first_client_joined_date || ''}
                          onChange={(e) => handleDateFilterChange('first_client_joined_date', e.target.value)}
                          className="form-input"
                        />
                      </div>
                      <div className="form-group">
                        <label className="form-label">First Client Deposit Date</label>
                        <input
                          type="date"
                          value={dateFilters.first_client_deposit_date || ''}
                          onChange={(e) => handleDateFilterChange('first_client_deposit_date', e.target.value)}
                          className="form-input"
                        />
                      </div>
                      <div className="form-group">
                        <label className="form-label">First Client Trade Date</label>
                        <input
                          type="date"
                          value={dateFilters.first_client_trade_date || ''}
                          onChange={(e) => handleDateFilterChange('first_client_trade_date', e.target.value)}
                          className="form-input"
                        />
                      </div>
                      <div className="form-group">
                        <label className="form-label">First Earning Date</label>
                        <input
                          type="date"
                          value={dateFilters.first_earning_date || ''}
                          onChange={(e) => handleDateFilterChange('first_earning_date', e.target.value)}
                          className="form-input"
                        />
                      </div>
                    </>
                  )}

                  {/* Screener 4 - Cohort Analysis Controls */}
                  {activeScreener === 4 && (
                    <>
                      <div className="form-group">
                        <label className="form-label">Breakdown By</label>
                        <select
                          value={cohortSettings.breakdown_filter}
                          onChange={(e) => setCohortSettings(prev => ({...prev, breakdown_filter: e.target.value}))}
                          className="form-select"
                        >
                          <option value="partner_region">Region</option>
                          <option value="partner_country">Country</option>
                          <option value="aff_type">Affiliate Type</option>
                          <option value="partner_platform">Platform</option>
                        </select>
                      </div>
                      <div className="form-group">
                        <label className="form-label">Result Format</label>
                        <select
                          value={cohortSettings.result_filter}
                          onChange={(e) => setCohortSettings(prev => ({...prev, result_filter: e.target.value}))}
                          className="form-select"
                        >
                          <option value="percentage">Percentage (%)</option>
                          <option value="absolute">Absolute Numbers</option>
                        </select>
                      </div>
                      <div className="form-group">
                        <label className="form-label">Milestone Type</label>
                        <select
                          value={cohortSettings.milestone_type}
                          onChange={(e) => setCohortSettings(prev => ({...prev, milestone_type: e.target.value}))}
                          className="form-select"
                        >
                          <option value="first_client_joined_date">First Signup</option>
                          <option value="first_client_deposit_date">First Deposit</option>
                          <option value="first_client_trade_date">First Trade</option>
                          <option value="first_earning_date">First Earning</option>
                        </select>
                      </div>
                      <div className="form-group">
                        <label className="form-label">Filter Type</label>
                        <select
                          value={cohortSettings.date_filter_type || 'rolling'}
                          onChange={(e) => setCohortSettings(prev => ({...prev, date_filter_type: e.target.value}))}
                          className="form-select"
                        >
                          <option value="rolling">Rolling Range (Last X Months)</option>
                          <option value="specific">Specific Month/Year</option>
                          <option value="range">Custom Range</option>
                        </select>
                      </div>
                      
                      {cohortSettings.date_filter_type === 'rolling' && (
                        <div className="form-group">
                          <label className="form-label">Rolling Period</label>
                          <select
                            value={cohortSettings.date_range || 12}
                            onChange={(e) => setCohortSettings(prev => ({...prev, date_range: parseInt(e.target.value)}))}
                            className="form-select"
                          >
                            <option value={3}>Last 3 Months</option>
                            <option value={6}>Last 6 Months</option>
                            <option value={12}>Last 12 Months</option>
                            <option value={18}>Last 18 Months</option>
                            <option value={24}>Last 24 Months</option>
                          </select>
                        </div>
                      )}
                      
                      {/* Cohort View Type - moved to after Rolling Period */}
                      <div className="form-group">
                        <label className="form-label">Cohort View Type</label>
                        <select
                          value={cohortSettings.cohort_type || 'forward'}
                          onChange={(e) => setCohortSettings(prev => ({...prev, cohort_type: e.target.value}))}
                          className="form-select"
                        >
                          <option value="forward">Forward Cohort (Join Date → Activation)</option>
                          <option value="reverse">Reverse Cohort (Activation → Join Date)</option>
                        </select>
                      </div>
                      
                      {cohortSettings.date_filter_type === 'specific' && (
                        <>
                          <div className="form-group">
                            <label className="form-label">Month</label>
                            <select
                              value={cohortSettings.specific_month || new Date().getMonth() + 1}
                              onChange={(e) => setCohortSettings(prev => ({...prev, specific_month: parseInt(e.target.value)}))}
                              className="form-select"
                            >
                              {[
                                { value: 1, label: 'January' },
                                { value: 2, label: 'February' },
                                { value: 3, label: 'March' },
                                { value: 4, label: 'April' },
                                { value: 5, label: 'May' },
                                { value: 6, label: 'June' },
                                { value: 7, label: 'July' },
                                { value: 8, label: 'August' },
                                { value: 9, label: 'September' },
                                { value: 10, label: 'October' },
                                { value: 11, label: 'November' },
                                { value: 12, label: 'December' }
                              ].map(month => (
                                <option key={month.value} value={month.value}>
                                  {month.label}
                                </option>
                              ))}
                            </select>
                          </div>
                          <div className="form-group">
                            <label className="form-label">Year</label>
                            <select
                              value={cohortSettings.specific_year || new Date().getFullYear()}
                              onChange={(e) => setCohortSettings(prev => ({...prev, specific_year: parseInt(e.target.value)}))}
                              className="form-select"
                            >
                              {Array.from({ length: 5 }, (_, i) => new Date().getFullYear() - i).map(year => (
                                <option key={year} value={year}>
                                  {year}
                                </option>
                              ))}
                            </select>
                          </div>
                        </>
                      )}
                      
                      {cohortSettings.date_filter_type === 'range' && (
                        <>
                          <div className="form-group">
                            <label className="form-label">From Month-Year</label>
                            <input
                              type="month"
                              value={cohortSettings.start_month || `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`}
                              onChange={(e) => setCohortSettings(prev => ({...prev, start_month: e.target.value}))}
                              className="form-input"
                            />
                          </div>
                          <div className="form-group">
                            <label className="form-label">To Month-Year</label>
                            <input
                              type="month"
                              value={cohortSettings.end_month || `${new Date().getFullYear()}-${String(new Date().getMonth() + 1).padStart(2, '0')}`}
                              onChange={(e) => setCohortSettings(prev => ({...prev, end_month: e.target.value}))}
                              className="form-input"
                            />
                          </div>
                        </>
                      )}
                    </>
                  )}
                </div>

                {/* Quick Date Range Buttons for Screener 3 */}
                {activeScreener === 3 && (
                  <div style={{ marginBottom: '1rem' }}>
                    <label className="form-label">Quick Date Ranges:</label>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
                      {[
                        { label: 'Last 7 Days', days: 7 },
                        { label: 'Last 30 Days', days: 30 },
                        { label: 'Last 90 Days', days: 90 },
                        { label: 'Last 6 Months', days: 180 },
                        { label: 'Last Year', days: 365 }
                      ].map(range => (
                        <button
                          key={range.days}
                          onClick={() => {
                            const startDate = new Date(Date.now() - range.days * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
                            handleDateFilterChange('date_joined', startDate);
                            setSelectedDateRange(range.days);
                          }}
                          className={`btn btn-sm ${selectedDateRange === range.days ? 'btn-primary' : 'btn-secondary'}`}
                        >
                          {range.label}
                        </button>
                      ))}
                      <button
                        onClick={() => {
                          setDateFilters({});
                          setSelectedDateRange(null);
                        }}
                        className="btn btn-sm btn-ghost"
                        style={{ color: 'var(--primary-red)' }}
                      >
                        Clear All
                      </button>
                    </div>
                  </div>
                )}

                {/* Clear All Filters Button */}
                {(Object.values(liveFilters).some(filter => filter && filter.length > 0) || 
                  (activeScreener === 3 && Object.keys(dateFilters).length > 0)) && (
                  <div style={{ marginBottom: '1rem' }}>
                    <button
                      onClick={() => {
                        setLiveFilters({});
                        setDateFilters({});
                        setSelectedDateRange(null);
                        if (activeScreener) {
                          loadLiveScreenerData(activeScreener);
                        }
                      }}
                      className="btn btn-secondary"
                    >
                      Clear All Filters
                    </button>
                  </div>
                )}

                {liveScreenerError && (
                  <div style={{
                    padding: '1rem',
                    backgroundColor: 'var(--primary-red-light)',
                    color: 'var(--primary-red)',
                    borderRadius: 'var(--radius-md)',
                    fontSize: '0.875rem',
                    marginBottom: '1rem'
                  }}>
                    {liveScreenerError}
                  </div>
                )}
              </div>
            </div>

            {/* Screener 1 - Performance Overview */}
            {activeScreener === 1 && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem', marginTop: '1.5rem' }}>
                {/* Table 1 - Region/Plan Overview */}
                <div className="card">
                  <div className="card-header" style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center'
                  }}>
                    <h3 className="heading-md">Performance by Region & Plan</h3>
                    {screener1Data.table1 && screener1Data.table1.length > 0 && (
                      <button
                        onClick={() => exportToCSV(screener1Data.table1, 'performance_by_region_plan.csv')}
                        className="btn btn-sm btn-primary"
                      >
                        <DownloadIcon />
                        Export CSV
                      </button>
                    )}
                  </div>
                  {screener1Data.table1 && screener1Data.table1.length > 0 ? (
                    <div className="table-container">
                      <table className="grid-table">
                        <thead>
                          <tr>
                            {Object.keys(screener1Data.table1[0] || {}).map(column => (
                              <th
                                key={column}
                                className="sortable"
                                onClick={() => handleSort(column)}
                              >
                                <div style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '0.25rem'
                                }}>
                                  {column}
                                  {sortConfig.key === column && (
                                    sortConfig.direction === 'asc' ? <ArrowUpIcon /> : <ArrowDownIcon />
                                  )}
                                </div>
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sortData(screener1Data.table1).map((row, idx) => (
                            <tr key={idx}>
                              {Object.values(row).map((value, colIdx) => (
                                <td key={colIdx}>
                                  {value}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty-state">
                      <p>No data available</p>
                    </div>
                  )}
                </div>

                {/* Table 2 - Platform/Event Overview */}
                <div className="card">
                  <div className="card-header" style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center'
                  }}>
                    <h3 className="heading-md">Performance by Platform & Events</h3>
                    {screener1Data.table2 && screener1Data.table2.length > 0 && (
                      <button
                        onClick={() => exportToCSV(screener1Data.table2, 'performance_by_platform_events.csv')}
                        className="btn btn-sm btn-primary"
                      >
                        <DownloadIcon />
                        Export CSV
                      </button>
                    )}
                  </div>
                  {screener1Data.table2 && screener1Data.table2.length > 0 ? (
                    <div className="table-container">
                      <table className="grid-table">
                        <thead>
                          <tr>
                            {Object.keys(screener1Data.table2[0] || {}).map(column => (
                              <th
                                key={column}
                                className="sortable"
                                onClick={() => handleSort(column)}
                              >
                                <div style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '0.25rem'
                                }}>
                                  {column}
                                  {sortConfig.key === column && (
                                    sortConfig.direction === 'asc' ? <ArrowUpIcon /> : <ArrowDownIcon />
                                  )}
                                </div>
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sortData(screener1Data.table2).map((row, idx) => (
                            <tr key={idx}>
                              {Object.values(row).map((value, colIdx) => (
                                <td key={colIdx}>
                                  {value}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty-state">
                      <p>No data available</p>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Screener 2 - Trend Analysis */}
            {activeScreener === 2 && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem', marginTop: '1.5rem' }}>
                {/* Trend Data Table */}
                <div className="card">
                  <div className="card-header" style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center'
                  }}>
                    <h3 className="heading-md">Screener 2 - Trend Analysis</h3>
                    {screener2Data.length > 0 && (
                      <button
                        onClick={() => exportToCSV(screener2Data, 'trend_analysis.csv', 'trend')}
                        className="btn btn-sm btn-primary"
                      >
                        <DownloadIcon />
                        Export CSV
                      </button>
                    )}
                  </div>
                  
                  {screener2Data.length > 0 ? (
                    <div className="table-container">
                      <table className="data-table trend-analysis-table">
                        <thead>
                          <tr>
                            {/* Fixed columns */}
                            <th rowSpan="2">Region</th>
                            <th rowSpan="2">Country</th>
                            <th rowSpan="2">Plan</th>
                            
                            {/* Application Count header */}
                            <th 
                              colSpan={screener2Data.months ? screener2Data.months.length : 0}
                              style={{ 
                                textAlign: 'center',
                                backgroundColor: 'var(--bg-tertiary)',
                                fontWeight: '600'
                              }}
                            >
                              Application Count
                            </th>
                            
                            {/* Activation Rate header */}
                            <th 
                              colSpan={screener2Data.months ? screener2Data.months.length : 0}
                              style={{ 
                                textAlign: 'center',
                                backgroundColor: 'var(--bg-tertiary)',
                                fontWeight: '600'
                              }}
                            >
                              Activation Rate - Signup
                            </th>
                          </tr>
                          <tr>
                            {/* Month headers for Application Count */}
                            {screener2Data.months && screener2Data.months.map(month => (
                              <th key={`app-${month}`} style={{ 
                                backgroundColor: 'var(--bg-tertiary)',
                                textAlign: 'center',
                                fontWeight: '500'
                              }}>
                                {month}
                              </th>
                            ))}
                            
                            {/* Month headers for Activation Rate */}
                            {screener2Data.months && screener2Data.months.map(month => (
                              <th key={`rate-${month}`} style={{ 
                                backgroundColor: 'var(--bg-tertiary)',
                                textAlign: 'center',
                                fontWeight: '500'
                              }}>
                                {month}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {screener2Data.map((row, idx) => (
                            <tr key={idx}>
                              <td style={{ fontWeight: '500' }}>{row.Region}</td>
                              <td style={{ fontWeight: '500' }}>{row.Country}</td>
                              <td style={{ fontWeight: '500' }}>{row.Plan}</td>
                              
                              {/* Application Count values */}
                              {screener2Data.months && screener2Data.months.map(month => (
                                <td key={`app-${month}-${idx}`} style={{ textAlign: 'right', fontFamily: 'monospace', fontSize: '0.8125rem' }}>
                                  {row[`App Count - ${month}`] || 0}
                                </td>
                              ))}
                              
                              {/* Activation Rate values */}
                              {screener2Data.months && screener2Data.months.map(month => (
                                <td key={`rate-${month}-${idx}`} style={{ textAlign: 'right', fontFamily: 'monospace', fontSize: '0.8125rem' }}>
                                  {row[`Act Rate - ${month}`] || '0.00%'}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty-state">
                      <div className="empty-state-icon">
                        <TrendingIcon />
                      </div>
                      <h4 className="empty-state-title">No trend data available</h4>
                      <p className="empty-state-text">
                        Try adjusting the analysis period or filters.
                      </p>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Screener 3 - Individual Partner */}
            {activeScreener === 3 && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem', marginTop: '1.5rem' }}>
                {/* Table 1 - Partner Overview */}
                <div className="card">
                  <div className="card-header" style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center'
                  }}>
                    <h3 className="heading-md">Partner Overview by Region</h3>
                    {screener3Data.table1 && screener3Data.table1.length > 0 && (
                      <button
                        onClick={() => exportToCSV(screener3Data.table1, 'partner_overview.csv')}
                        className="btn btn-sm btn-primary"
                      >
                        <DownloadIcon />
                        Export CSV
                      </button>
                    )}
                  </div>
                  {screener3Data.table1 && screener3Data.table1.length > 0 ? (
                    <div className="table-container">
                      <table className="grid-table">
                        <thead>
                          <tr>
                            {Object.keys(screener3Data.table1[0] || {}).map(column => (
                              <th
                                key={column}
                                className="sortable"
                                onClick={() => handleSort(column)}
                              >
                                <div style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '0.25rem'
                                }}>
                                  {column}
                                  {sortConfig.key === column && (
                                    sortConfig.direction === 'asc' ? <ArrowUpIcon /> : <ArrowDownIcon />
                                  )}
                                </div>
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sortData(screener3Data.table1).map((row, idx) => (
                            <tr key={idx}>
                              {Object.values(row).map((value, colIdx) => (
                                <td key={colIdx}>
                                  {value}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty-state">
                      <p>No data available</p>
                    </div>
                  )}
                </div>

                {/* Table 2 - Individual Partner Details */}
                <div className="card">
                  <div className="card-header" style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center'
                  }}>
                    <div>
                      <h3 className="heading-md">Individual Partner Details</h3>
                      <p className="text-sm" style={{
                        color: 'var(--text-secondary)',
                        marginTop: '0.25rem'
                      }}>
                        Showing first 100 partners
                      </p>
                    </div>
                    {screener3Data.table2 && screener3Data.table2.length > 0 && (
                      <button
                        onClick={() => exportToCSV(screener3Data.table2, 'individual_partner_details.csv')}
                        className="btn btn-sm btn-primary"
                      >
                        <DownloadIcon />
                        Export CSV
                      </button>
                    )}
                  </div>
                  {screener3Data.table2 && screener3Data.table2.length > 0 ? (
                    <div className="table-container">
                      <table className="grid-table">
                        <thead>
                          <tr>
                            {Object.keys(screener3Data.table2[0] || {}).map(column => (
                              <th
                                key={column}
                                className="sortable"
                                onClick={() => handleSort(column)}
                              >
                                <div style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '0.25rem'
                                }}>
                                  {column}
                                  {sortConfig.key === column && (
                                    sortConfig.direction === 'asc' ? <ArrowUpIcon /> : <ArrowDownIcon />
                                  )}
                                </div>
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sortData(screener3Data.table2).map((row, idx) => (
                            <tr key={idx}>
                              {Object.values(row).map((value, colIdx) => (
                                <td key={colIdx}>
                                  {value}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty-state">
                      <p>No data available</p>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Screener 4 - Cohort Analysis */}
            {activeScreener === 4 && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem', marginTop: '1.5rem' }}>
                {/* Cohort Data Table */}
                <div className="card">
                  <div className="card-header" style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center'
                  }}>
                    <h3 className="heading-md">
                      {cohortSettings.cohort_type === 'reverse' 
                        ? 'Reverse Cohort Analysis (When did activated partners join?)'
                        : 'Forward Cohort Analysis (How quickly do partners activate after joining?)'}
                    </h3>
                    {screener4Data.cohort_data && screener4Data.cohort_data.length > 0 && (
                      <button
                        onClick={() => exportToCSV(screener4Data, 'cohort_analysis.csv', 'cohort')}
                        className="btn btn-sm btn-primary"
                      >
                        <DownloadIcon />
                        Export CSV
                      </button>
                    )}
                  </div>
                  
                  {/* Add description for clarity */}
                  <div style={{ 
                    padding: '0.75rem 1rem', 
                    backgroundColor: 'var(--bg-tertiary)',
                    borderBottom: '1px solid var(--border-color)'
                  }}>
                    <p className="text-sm" style={{ color: 'var(--text-secondary)' }}>
                      {cohortSettings.cohort_type === 'reverse' 
                        ? 'M1: Joined 0-30 days before activation | M2: Joined 31-60 days before activation | M3: Joined 61-90 days before activation'
                        : 'M1: Activated within 30 days | M2: Activated within 60 days | M3: Activated within 90 days'}
                    </p>
                  </div>
                  
                  {screener4Data.cohort_data && screener4Data.cohort_data.length > 0 ? (
                    <div className="table-container">
                      <table className="grid-table">
                        <thead>
                          <tr>
                            {Object.keys(screener4Data.cohort_data[0] || {}).map(column => (
                              <th
                                key={column}
                                className="sortable"
                                onClick={() => handleSort(column)}
                              >
                                <div style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '0.25rem'
                                }}>
                                  {column.replace('_', ' ')}
                                  {sortConfig.key === column && (
                                    sortConfig.direction === 'asc' ? <ArrowUpIcon /> : <ArrowDownIcon />
                                  )}
                                </div>
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sortData(screener4Data.cohort_data).map((row, idx) => (
                            <tr key={idx}>
                              {Object.entries(row).map(([key, value], colIdx) => (
                                <td 
                                  key={colIdx}
                                >
                                  {value}
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="empty-state">
                      <div className="empty-state-icon">
                        <RocketIcon />
                      </div>
                      <h4 className="empty-state-title">No cohort data available</h4>
                      <p className="empty-state-text">
                        Try adjusting the filters or date range.
                      </p>
                    </div>
                  )}
                </div>

                {/* Heatmap Visualization */}
                {screener4Data.heatmap_data && screener4Data.heatmap_data.length > 0 && (
                  <div className="card">
                    <div className="card-header">
                      <h3 className="heading-md">Performance Heatmap</h3>
                    </div>
                    <div className="card-body">
                      <div style={{ overflowX: 'auto' }}>
                        <div style={{
                          display: 'inline-block',
                          minWidth: '100%'
                        }}>
                          <div style={{
                            display: 'grid',
                            gap: '1px',
                            gridTemplateColumns: `repeat(${Array.from(new Set(screener4Data.heatmap_data.map(d => d.x))).length + 1}, minmax(120px, 1fr))`
                          }}>
                            {/* Header */}
                            <div style={{ padding: '0.5rem', fontWeight: '500', fontSize: '0.75rem' }}></div>
                            {Array.from(new Set(screener4Data.heatmap_data.map(d => d.x))).map(milestone => (
                              <div key={milestone} style={{
                                padding: '0.5rem',
                                fontWeight: '500',
                                fontSize: '0.75rem',
                                textAlign: 'center',
                                borderBottom: '1px solid var(--border-color)'
                              }}>
                                {milestone}
                              </div>
                            ))}
                            
                            {/* Data Rows */}
                            {Array.from(new Set(screener4Data.heatmap_data.map(d => d.y))).map(breakdown => (
                              <React.Fragment key={breakdown}>
                                <div style={{
                                  padding: '0.5rem',
                                  fontWeight: '500',
                                  fontSize: '0.75rem',
                                  borderRight: '1px solid var(--border-color)',
                                  backgroundColor: 'var(--bg-tertiary)'
                                }}>
                                  {breakdown}
                                </div>
                                {Array.from(new Set(screener4Data.heatmap_data.map(d => d.x))).map(milestone => {
                                  const dataPoint = screener4Data.heatmap_data.find(d => d.x === milestone && d.y === breakdown);
                                  const value = dataPoint ? dataPoint.value : 0;
                                  const count = dataPoint ? dataPoint.count : 0;
                                  const intensity = Math.min(value / 50, 1);
                                  
                                  return (
                                    <div 
                                      key={`${breakdown}-${milestone}`}
                                      style={{
                                        padding: '0.5rem',
                                        fontSize: '0.75rem',
                                        textAlign: 'center',
                                        border: '1px solid var(--border-light)',
                                        position: 'relative',
                                        cursor: 'help',
                                        backgroundColor: `rgba(220, 38, 38, ${intensity})`,
                                        color: intensity > 0.5 ? 'white' : 'var(--text-primary)'
                                      }}
                                      title={`${breakdown} - ${milestone}: ${value}% (${count} partners)`}
                                    >
                                      {value.toFixed(1)}%
                                      <div style={{
                                        position: 'absolute',
                                        bottom: '100%',
                                        left: '50%',
                                        transform: 'translateX(-50%)',
                                        marginBottom: '0.5rem',
                                        padding: '0.25rem 0.5rem',
                                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                                        color: 'white',
                                        fontSize: '0.75rem',
                                        borderRadius: 'var(--radius-sm)',
                                        whiteSpace: 'nowrap',
                                        opacity: 0,
                                        transition: 'opacity 0.2s',
                                        zIndex: 10,
                                        pointerEvents: 'none'
                                      }}
                                      className="heatmap-tooltip"
                                      >
                                        {count} partners
                                      </div>
                                    </div>
                                  );
                                })}
                              </React.Fragment>
                            ))}
                          </div>
                        </div>
                      </div>
                      
                      {/* Heatmap Legend */}
                      <div style={{
                        marginTop: '1rem',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        gap: '1rem'
                      }}>
                        <span className="text-sm" style={{ color: 'var(--text-secondary)' }}>Performance Rate:</span>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                          <span className="text-xs" style={{ color: 'var(--text-tertiary)' }}>0%</span>
                          <div style={{ display: 'flex' }}>
                            {[0, 0.2, 0.4, 0.6, 0.8, 1].map(intensity => (
                              <div 
                                key={intensity}
                                style={{
                                  width: '16px',
                                  height: '16px',
                                  backgroundColor: `rgba(220, 38, 38, ${intensity})`
                                }}
                              ></div>
                            ))}
                          </div>
                          <span className="text-xs" style={{ color: 'var(--text-tertiary)' }}>50%+</span>
                        </div>
                      </div>
                    </div>
                  </div>
                )}

                {/* Cohort Summary */}
                {screener4Data.cohort_data && screener4Data.cohort_data.length > 0 && (
                  <div className="card" style={{
                    backgroundColor: 'var(--bg-tertiary)',
                    border: '1px solid var(--border-color)'
                  }}>
                    <div className="card-body">
                      <h4 className="heading-sm" style={{ color: 'var(--text-primary)', marginBottom: '0.75rem' }}>
                        Cohort Analysis Summary
                      </h4>
                      <div style={{
                        display: 'grid',
                        gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                        gap: '1rem'
                      }}>
                        <div>
                          <span className="text-sm" style={{ fontWeight: '600' }}>View Type:</span>
                          <span className="text-sm" style={{ marginLeft: '0.5rem' }}>
                            {cohortSettings.cohort_type === 'reverse' ? 'Reverse Cohort' : 'Forward Cohort'}
                          </span>
                        </div>
                        <div>
                          <span className="text-sm" style={{ fontWeight: '600' }}>Cohorts Analyzed:</span>
                          <span className="text-sm" style={{ marginLeft: '0.5rem' }}>{screener4Data.cohort_data.length}</span>
                        </div>
                        <div>
                          <span className="text-sm" style={{ fontWeight: '600' }}>Breakdown:</span>
                          <span className="text-sm" style={{ marginLeft: '0.5rem' }}>{cohortSettings.breakdown_filter.replace('_', ' ')}</span>
                        </div>
                        <div>
                          <span className="text-sm" style={{ fontWeight: '600' }}>Milestone:</span>
                          <span className="text-sm" style={{ marginLeft: '0.5rem' }}>{cohortSettings.milestone_type.replace('_', ' ').replace('date', '')}</span>
                        </div>
                        <div>
                          <span className="text-sm" style={{ fontWeight: '600' }}>Date Filter:</span>
                          <span className="text-sm" style={{ marginLeft: '0.5rem' }}>
                            {
                              cohortSettings.date_filter_type === 'rolling' 
                                ? `Last ${cohortSettings.date_range} months`
                                : cohortSettings.date_filter_type === 'specific'
                                ? `${['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][cohortSettings.specific_month - 1]} ${cohortSettings.specific_year}`
                                : cohortSettings.date_filter_type === 'range'
                                ? `${cohortSettings.start_month} to ${cohortSettings.end_month}`
                                : 'Rolling 12 months'
                            }
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Instructions */}
        <div className="card" style={{
          marginTop: '2rem',
          backgroundColor: 'var(--bg-tertiary)',
          border: '1px solid var(--border-color)'
        }}>
          <div className="card-body">
            <h3 className="heading-sm" style={{ color: 'var(--text-primary)', marginBottom: '0.75rem' }}>
              How to Use
            </h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              <p className="text-sm"><strong>Agent Test:</strong> Test natural language queries against your partner database. Choose between SQL Agent (raw SQL results) or SQL + Analytics (includes visualizations and insights).</p>
              <p className="text-sm"><strong>Metrics Screener:</strong> Interactive dashboard for analyzing partner KPI metrics with customizable filters and exportable results.</p>
              <p className="text-sm"><strong>Live Screeners:</strong> Specialized dashboards for performance overview, trend analysis, individual partner tracking, and cohort analysis.</p>
              <p className="text-sm"><strong>Backend:</strong> Make sure the Flask API is running on http://localhost:5000</p>
            </div>
          </div>
        </div>
      </main>

      <style jsx>{`
        .heatmap-tooltip:hover {
          opacity: 1 !important;
        }
        
        .flex {
          display: flex;
        }
        
        .gap-3 {
          gap: 0.75rem;
        }
        
        .mt-4 {
          margin-top: 1rem;
        }
        
        .mt-6 {
          margin-top: 1.5rem;
        }
        
        .mb-4 {
          margin-bottom: 1rem;
        }
        
        .mb-8 {
          margin-bottom: 2rem;
        }
        
        .space-y-4 > * + * {
          margin-top: 1rem;
        }
        
        .bg-dark {
          background-color: #1f2937;
        }
        
        .text-white {
          color: white;
        }
        
        .bg-tertiary {
          background-color: var(--bg-tertiary);
        }
        
        .bg-warning-light {
          background-color: #fef3c7;
        }
        
        .text-success {
          color: #059669;
        }
        
        .text-error {
          color: var(--primary-red);
        }
        
        .text-secondary {
          color: var(--text-secondary);
        }
        
        .bg-primary-light {
          background-color: var(--primary-red-light);
        }
        
        .text-primary-red {
          color: var(--primary-red);
        }
        
        .grid {
          display: grid;
        }
        
        .grid-cols-1 {
          grid-template-columns: repeat(1, minmax(0, 1fr));
        }
        
        @media (min-width: 1024px) {
          .lg\\:grid-cols-2 {
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }
        }
      `}</style>
    </div>
  );
}

export default App; 