import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import './App.css';
import ProgressBar from './components/ProgressBar/ProgressBar.tsx';
import SampleQuestions from './components/SampleQuestions/SampleQuestions.tsx';

const API_BASE_URL = 'http://localhost:5001';

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

const SpotlightIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="11" cy="11" r="8"></circle>
    <path d="m21 21-4.35-4.35"></path>
    <path d="M11 8v6"></path>
    <path d="M8 11h6"></path>
  </svg>
);

const AlertIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="12" cy="12" r="10"></circle>
    <line x1="12" y1="8" x2="12" y2="12"></line>
    <line x1="12" y1="16" x2="12.01" y2="16"></line>
  </svg>
);

const GrowthIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M22 12h-4l-3 9L9 3l-3 9H2"></path>
  </svg>
);

const UsersIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path>
    <circle cx="9" cy="7" r="4"></circle>
    <path d="M23 21v-2a4 4 0 0 0-3-3.87"></path>
    <path d="M16 3.13a4 4 0 0 1 0 7.75"></path>
  </svg>
);

const AIInsightIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z"></path>
  </svg>
);

function App() {
  // All existing state variables remain the same
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);
  const [sqlLoading, setSqlLoading] = useState(false);
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [sqlResult, setSqlResult] = useState(null);
  const [analyticsResult, setAnalyticsResult] = useState(null);
  const [activeTab, setActiveTab] = useState('spotlight'); // Changed default to spotlight
  const [error, setError] = useState(null);
  const [results, setResults] = useState(null);
  const [progress, setProgress] = useState(0);
  const [progressMessage, setProgressMessage] = useState('');
  const eventSourceRef = useRef(null);

  // Screener states
  const [screenerLoading, setScreenerLoading] = useState(false);
  const [availableMetrics, setAvailableMetrics] = useState({});
  const [availableFilters, setAvailableFilters] = useState({});
  const [selectedMetrics, setSelectedMetrics] = useState([]);
  const [activeFilters, setActiveFilters] = useState({});
  const [screenerData, setScreenerData] = useState(null);
  const [screenerError, setScreenerError] = useState(null);

  // Live Screeners states
  const [liveScreenerData, setLiveScreenerData] = useState(null);
  const [liveScreenerLoading, setLiveScreenerLoading] = useState(false);
  const [liveScreenerError, setLiveScreenerError] = useState(null);
  const [activeScreener, setActiveScreener] = useState(1);
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'ascending' });
  const [selectedFilters, setSelectedFilters] = useState([]);
  
  // Add missing state variables for live screeners
  const [screener1Data, setScreener1Data] = useState({ table1: [], table2: [] });
  const [screener2Data, setScreener2Data] = useState([]);
  const [screener3Data, setScreener3Data] = useState({ table1: [], table2: [] });
  const [screener4Data, setScreener4Data] = useState({ cohort_data: [], heatmap_data: [] });
  const [liveFilters, setLiveFilters] = useState({});
  const [dateFilters, setDateFilters] = useState({});
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

  // Add cleanup function for SSE
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    };
  }, []);

  const handleSampleQuestionSelect = (selectedQuestion) => {
    setQuery(selectedQuestion);
  };

  const handleSqlTest = async (selectedQuery = null) => {
    const queryToUse = selectedQuery || query;
    if (!queryToUse.trim()) {
      setError('Please enter a query');
      return;
    }

    setSqlLoading(true);
    setError(null);
    setSqlResult(null);
    setProgress(0);
    setProgressMessage('');

    try {
      // Close any existing SSE connection
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      const response = await axios.post(`${API_BASE_URL}/sql-agent`, {
        query: queryToUse
      });

      if (response.data.progress_id) {
        // Set up SSE for progress updates
        const eventSource = new EventSource(`${API_BASE_URL}/sql-agent/progress/${response.data.progress_id}`);
        eventSourceRef.current = eventSource;

        eventSource.onmessage = (event) => {
          const data = JSON.parse(event.data);
          if (data.progress === -1) {
            eventSource.close();
            setSqlLoading(false);
            setError('Operation timed out');
          } else if (data.progress === 100 && data.result) {
            eventSource.close();
            setSqlLoading(false);
            setSqlResult(data.result);
          } else if (data.error) {
            eventSource.close();
            setSqlLoading(false);
            setError(data.error);
          } else {
            setProgress(data.progress);
            setProgressMessage(data.message);
          }
        };

        eventSource.onerror = () => {
          eventSource.close();
          setSqlLoading(false);
          setError('Lost connection to server');
        };
      }
    } catch (err) {
      setSqlLoading(false);
      setError(err.response?.data?.error || err.message);
    }
  };

  const handleAnalyticsTest = async () => {
    if (!query.trim()) {
      setError('Please enter a query');
      return;
    }

    setAnalyticsLoading(true);
    setError(null);
    setSqlResult(null);
    setAnalyticsResult(null);
    setProgress(0);
    setProgressMessage('');

    try {
      // Close any existing SSE connection
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }

      const response = await axios.post(`${API_BASE_URL}/sql-analytics`, {
        query: query
      });

      if (response.data.progress_id) {
        // Set up SSE for progress updates
        const eventSource = new EventSource(`${API_BASE_URL}/sql-agent/progress/${response.data.progress_id}`);
        eventSourceRef.current = eventSource;

        eventSource.onmessage = (event) => {
          const data = JSON.parse(event.data);
          if (data.progress === -1) {
            eventSource.close();
            setAnalyticsLoading(false);
            setError('Operation timed out');
          } else if (data.progress === 100 && data.result) {
            eventSource.close();
            setAnalyticsLoading(false);
            setAnalyticsResult(data.result);
          } else if (data.error) {
            eventSource.close();
            setAnalyticsLoading(false);
            setError(data.error);
          } else {
            setProgress(data.progress);
            setProgressMessage(data.message);
          }
        };

        eventSource.onerror = () => {
          eventSource.close();
          setAnalyticsLoading(false);
          setError('Lost connection to server');
        };
      }
    } catch (err) {
      setAnalyticsLoading(false);
      setError(err.response?.data?.error || 'Failed to process analytics query');
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

  const formatNumber = (num) => {
    if (num === null || num === undefined) return '0';
    return new Intl.NumberFormat('en-US').format(num);
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
              onClick={() => setActiveTab('spotlight')}
              className={`nav-tab ${activeTab === 'spotlight' ? 'active' : ''}`}
            >
              <SpotlightIcon />
              Spotlight
            </button>
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
        {/* Spotlight Dashboard Tab Content */}
        {activeTab === 'spotlight' && (
          <div className="fade-in">
            <SpotlightDashboard />
          </div>
        )}

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
                {/* Sample Questions */}
                <SampleQuestions onQuestionSelect={handleSampleQuestionSelect} />
                
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
                    onClick={() => handleSqlTest()}
                    disabled={sqlLoading || analyticsLoading}
                    className="btn btn-primary"
                    style={{ opacity: sqlLoading ? 0.7 : 1 }}
                  >
                    {sqlLoading ? (
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
                    disabled={sqlLoading || analyticsLoading}
                    className="btn btn-primary"
                    style={{ opacity: loading ? 0.7 : 1 }}
                  >
                    {analyticsLoading ? (
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

                {(sqlLoading || analyticsLoading) && progress > 0 && (
                  <ProgressBar progress={progress} message={progressMessage} />
                )}

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
                </div>

                {/* Quick Date Range Buttons for Screener 3 */}
                {activeScreener === 3 && activeTab === 'live-screeners' && (
                  <div style={{ marginBottom: '1.5rem' }}>
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

                {/* Screener 4 - Cohort Analysis */}
                {activeScreener === 4 && (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem', marginTop: '1.5rem' }}>
                    {/* Cohort Analysis Settings */}
                    <div className="card">
                      <div className="card-header">
                        <h3 className="heading-md">Cohort Analysis Settings</h3>
                      </div>
                      <div className="card-body">
                        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem', marginBottom: '1rem' }}>
                          {/* Cohort Type Selection */}
                          <div className="form-group">
                            <label className="form-label">Cohort Type</label>
                            <select
                              value={cohortSettings.cohort_type}
                              onChange={(e) => setCohortSettings(prev => ({ ...prev, cohort_type: e.target.value }))}
                              className="form-select"
                            >
                              <option value="forward">Forward Cohort (Time to Activation)</option>
                              <option value="reverse">Reverse Cohort (When did they join?)</option>
                            </select>
                          </div>

                          {/* Milestone Type Selection */}
                          <div className="form-group">
                            <label className="form-label">Milestone Type</label>
                            <select
                              value={cohortSettings.milestone_type}
                              onChange={(e) => setCohortSettings(prev => ({ ...prev, milestone_type: e.target.value }))}
                              className="form-select"
                            >
                              <option value="first_client_joined_date">First Client Signup</option>
                              <option value="first_client_deposit_date">First Client Deposit</option>
                              <option value="first_client_trade_date">First Client Trade</option>
                              <option value="first_client_earning_date">First Client Earning</option>
                            </select>
                          </div>

                          {/* Breakdown Filter Selection */}
                          <div className="form-group">
                            <label className="form-label">Breakdown By</label>
                            <select
                              value={cohortSettings.breakdown_filter}
                              onChange={(e) => setCohortSettings(prev => ({ ...prev, breakdown_filter: e.target.value }))}
                              className="form-select"
                            >
                              <option value="partner_region">Region</option>
                              <option value="partner_country">Country</option>
                              <option value="platform">Platform</option>
                              <option value="plan_type">Plan Type</option>
                              <option value="partner_level">Partner Level</option>
                              <option value="event_status">Event Status</option>
                            </select>
                          </div>

                          {/* Result Type Selection */}
                          <div className="form-group">
                            <label className="form-label">Result Type</label>
                            <select
                              value={cohortSettings.result_filter}
                              onChange={(e) => setCohortSettings(prev => ({ ...prev, result_filter: e.target.value }))}
                              className="form-select"
                            >
                              <option value="percentage">Percentage</option>
                              <option value="absolute">Absolute Numbers</option>
                            </select>
                          </div>
                        </div>

                        {/* Date Filter Section */}
                        <div style={{ 
                          borderTop: '1px solid var(--border-color)', 
                          paddingTop: '1rem',
                          display: 'grid', 
                          gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', 
                          gap: '1rem' 
                        }}>
                          {/* Date Filter Type Selection */}
                          <div className="form-group">
                            <label className="form-label">Date Filter Type</label>
                            <select
                              value={cohortSettings.date_filter_type}
                              onChange={(e) => setCohortSettings(prev => ({ ...prev, date_filter_type: e.target.value }))}
                              className="form-select"
                            >
                              <option value="rolling">Rolling Months</option>
                              <option value="specific">Specific Month</option>
                              <option value="range">Date Range</option>
                            </select>
                          </div>

                          {/* Conditional Date Filter Controls */}
                          {cohortSettings.date_filter_type === 'rolling' && (
                            <div className="form-group">
                              <label className="form-label">Number of Months</label>
                              <select
                                value={cohortSettings.date_range}
                                onChange={(e) => setCohortSettings(prev => ({ ...prev, date_range: parseInt(e.target.value) }))}
                                className="form-select"
                              >
                                {[3, 6, 12, 24, 36].map(months => (
                                  <option key={months} value={months}>Last {months} Months</option>
                                ))}
                              </select>
                            </div>
                          )}

                          {cohortSettings.date_filter_type === 'specific' && (
                            <>
                              <div className="form-group">
                                <label className="form-label">Month</label>
                                <select
                                  value={cohortSettings.specific_month}
                                  onChange={(e) => setCohortSettings(prev => ({ ...prev, specific_month: parseInt(e.target.value) }))}
                                  className="form-select"
                                >
                                  {Array.from({ length: 12 }, (_, i) => i + 1).map(month => (
                                    <option key={month} value={month}>
                                      {new Date(2000, month - 1).toLocaleString('default', { month: 'long' })}
                                    </option>
                                  ))}
                                </select>
                              </div>
                              <div className="form-group">
                                <label className="form-label">Year</label>
                                <select
                                  value={cohortSettings.specific_year}
                                  onChange={(e) => setCohortSettings(prev => ({ ...prev, specific_year: parseInt(e.target.value) }))}
                                  className="form-select"
                                >
                                  {Array.from({ length: 5 }, (_, i) => new Date().getFullYear() - i).map(year => (
                                    <option key={year} value={year}>{year}</option>
                                  ))}
                                </select>
                              </div>
                            </>
                          )}

                          {cohortSettings.date_filter_type === 'range' && (
                            <>
                              <div className="form-group">
                                <label className="form-label">From Month</label>
                                <input
                                  type="month"
                                  value={cohortSettings.start_month}
                                  onChange={(e) => setCohortSettings(prev => ({ ...prev, start_month: e.target.value }))}
                                  className="form-input"
                                />
                              </div>
                              <div className="form-group">
                                <label className="form-label">To Month</label>
                                <input
                                  type="month"
                                  value={cohortSettings.end_month}
                                  onChange={(e) => setCohortSettings(prev => ({ ...prev, end_month: e.target.value }))}
                                  className="form-input"
                                />
                              </div>
                            </>
                          )}
                        </div>
                      </div>
                    </div>

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
            </div>
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
              <p className="text-sm"><strong>Spotlight:</strong> Live dashboard with key partner analytics insights, including event effectiveness, country performance, and retention metrics.</p>
              <p className="text-sm"><strong>Agent Test:</strong> Test natural language queries against your partner database. Choose between SQL Agent (raw SQL results) or SQL + Analytics (includes visualizations and insights).</p>
              <p className="text-sm"><strong>Metrics Screener:</strong> Interactive dashboard for analyzing partner KPI metrics with customizable filters and exportable results.</p>
              <p className="text-sm"><strong>Live Screeners:</strong> Specialized dashboards for performance overview, trend analysis, individual partner tracking, and cohort analysis.</p>
              <p className="text-sm"><strong>Backend:</strong> Make sure the Flask API is running on http://localhost:5001</p>
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

        /* AI Insight Tooltip Styles */
        .ai-insight-container {
          position: relative;
        }

        .ai-insight-tooltip {
          position: fixed;
          width: 450px;
          background: white;
          border-radius: 12px;
          box-shadow: 0 20px 40px -8px rgba(0, 0, 0, 0.25);
          z-index: 9999;
          animation: tooltipSlideIn 0.3s ease-out;
          border: 1px solid #e5e7eb;
        }

        @keyframes tooltipSlideIn {
          from {
            opacity: 0;
            transform: translateY(10px) scale(0.95);
          }
          to {
            opacity: 1;
            transform: translateY(0) scale(1);
          }
        }

        .ai-insight-tooltip-content {
          padding: 1.5rem;
          max-height: 400px;
          overflow-y: auto;
          background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
          border-radius: 12px 12px 8px 8px;
        }

        .ai-insight-tooltip-arrow {
          position: absolute;
          top: 100%;
          left: 50%;
          transform: translateX(-50%);
          width: 0;
          height: 0;
          border-left: 10px solid transparent;
          border-right: 10px solid transparent;
          border-top: 10px solid white;
        }

        .ai-insight-tooltip-arrow::before {
          content: '';
          position: absolute;
          top: -11px;
          left: -10px;
          width: 0;
          height: 0;
          border-left: 10px solid transparent;
          border-right: 10px solid transparent;
          border-top: 10px solid #e5e7eb;
        }

        .ai-insight-tooltip.below .ai-insight-tooltip-arrow {
          top: -10px;
          border-top: none;
          border-bottom: 10px solid white;
        }

        .ai-insight-tooltip.below .ai-insight-tooltip-arrow::before {
          top: -9px;
          border-top: none;
          border-bottom: 10px solid #e5e7eb;
        }

        .ai-insight-loading-small {
          display: flex;
          align-items: center;
          gap: 0.75rem;
          color: #6b7280;
          font-size: 0.9rem;
        }

        .loading-spinner-small {
          width: 16px;
          height: 16px;
          border: 2px solid #e5e7eb;
          border-top: 2px solid #667eea;
          border-radius: 50%;
          animation: spin 1s linear infinite;
        }

        .ai-insight-text-formatted {
          color: #374151;
          font-size: 0.9rem;
          line-height: 1.6;
        }

        .insight-section {
          margin-bottom: 0.75rem;
          padding: 0.75rem;
          border-radius: 8px;
          background: rgba(255, 255, 255, 0.6);
          border-left: 3px solid #667eea;
        }

        .insight-section.warning {
          background: rgba(254, 243, 199, 0.7);
          border-left-color: #f59e0b;
        }

        .insight-section.success {
          background: rgba(220, 252, 231, 0.7);
          border-left-color: #10b981;
        }

        .insight-section.action {
          background: rgba(239, 246, 255, 0.7);
          border-left-color: #3b82f6;
        }

        .insight-section.strategy {
          background: rgba(245, 243, 255, 0.7);
          border-left-color: #8b5cf6;
        }

        .insight-icon {
          margin-right: 0.5rem;
          font-size: 1rem;
        }

        .ai-insight-icon {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 20px;
          height: 20px;
          border-radius: 50%;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white;
          cursor: pointer;
          transition: all 0.2s;
          margin-left: 8px;
          opacity: 0.7;
        }

        .ai-insight-icon:hover {
          opacity: 1;
          transform: scale(1.1);
          box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
        }
      `}</style>


    </div>
  );
}

// Add this component before the SpotlightDashboard component
const FunnelChart = ({ funnelData, selectedCountry, dateRange, onFunnelDataUpdate }) => {
  const [countryFilter, setCountryFilter] = useState(selectedCountry || null);
  const [funnelMetrics, setFunnelMetrics] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchFunnelMetrics();
  }, [countryFilter, dateRange]);

  const fetchFunnelMetrics = async () => {
    try {
      setLoading(true);
      const params = new URLSearchParams();
      params.append('date_range', dateRange || 90);
      if (countryFilter && countryFilter !== 'All Countries') {
        params.append('country', countryFilter);
      }
      
              const response = await fetch(`${API_BASE_URL}/spotlight/funnel-metrics?${params}`);
      const result = await response.json();
      
      if (result.success) {
        setFunnelMetrics(result.data);
        // Pass the funnel data to parent component for AI analysis
        if (onFunnelDataUpdate) {
          onFunnelDataUpdate(result.data);
        }
      }
    } catch (err) {
      console.error('Error fetching funnel metrics:', err);
    } finally {
      setLoading(false);
    }
  };

  if (loading || !funnelMetrics) {
    return <div className="loading-spinner"></div>;
  }

  const { funnel_overview, country_performance, available_countries } = funnelMetrics;

  const stages = [
    { name: "New Partners Signed Up", value: funnel_overview.total_applications, color: "#3b82f6" },
    { name: "Activate First Sign Up", value: funnel_overview.signup_activations, color: "#8b5cf6" },
    { name: "Activate First Deposit", value: funnel_overview.deposit_activations, color: "#10b981" },
    { name: "Activate First Trade", value: funnel_overview.trade_activations, color: "#06b6d4" },
    { name: "Activate First Earning", value: funnel_overview.earning_activations, color: "#f97316" }
  ];

  const conversionRates = [
    { stage: "Applications → First Client", rate: funnel_overview.apps_to_signup_rate || 0 },
    { stage: "First Client → First Deposit", rate: funnel_overview.signup_to_deposit_rate || 0 },
    { stage: "First Deposit → First Trade", rate: funnel_overview.deposit_to_trade_rate || 0 },
    { stage: "First Trade → First Earnings", rate: funnel_overview.trade_to_earning_rate || 0 }
  ];

  const formatNumber = (num) => {
    if (num === null || num === undefined) return '0';
    return new Intl.NumberFormat('en-US').format(Math.round(num));
  };

  return (
    <div className="funnel-section">
      {/* Funnel Controls */}
      <div className="funnel-controls">
        <select
          value={countryFilter || 'All Countries'}
          onChange={(e) => setCountryFilter(e.target.value === 'All Countries' ? null : e.target.value)}
          className="country-filter-select"
        >
          <option value="All Countries">All Countries</option>
          {available_countries.map(country => (
            <option key={country} value={country}>{country}</option>
          ))}
        </select>
      </div>

      {/* KPI Cards */}
      <div className="funnel-kpi-grid">
        <div className="kpi-card">
          <div className="kpi-label">Overall Activation Rate</div>
          <div className="kpi-value">{funnel_overview.activation_rate || 0}%</div>
          <div className="kpi-subtext">Partners with active clients</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Time to First Activation</div>
          <div className="kpi-value">{Math.round(funnel_overview.avg_days_to_activation || 0)} days</div>
          <div className="kpi-subtext">Average days to first client</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Active Partners</div>
          <div className="kpi-value">{funnel_overview.active_partners_rate || 0}%</div>
          <div className="kpi-subtext">Active in last 30 days</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Application Growth</div>
          <div className="kpi-value">
            {funnel_overview.application_growth_rate > 0 ? '+' : ''}{funnel_overview.application_growth_rate || 0}%
          </div>
          <div className="kpi-subtext">vs previous period</div>
        </div>
      </div>

      {/* Funnel Visualization */}
      <div className="funnel-chart">
        <h4>Partner Activation Funnel {countryFilter ? `- ${countryFilter}` : ''}</h4>
        <div className="funnel-container">
          <div className="funnel-stages-list">
            {stages.map((stage, idx) => {
              const percentage = idx === 0 ? 100 : Math.round((stage.value / stages[0].value) * 100);
              const barWidth = (stage.value / stages[0].value) * 100;
              
              // Create trapezoid shape using CSS
              const clipPath = idx === 0 
                ? 'polygon(0 0, 100% 0, 98% 100%, 2% 100%)' // First stage - widest
                : idx === stages.length - 1
                ? 'polygon(10% 0, 90% 0, 80% 100%, 20% 100%)' // Last stage - narrowest
                : `polygon(${2 + idx * 2}% 0, ${98 - idx * 2}% 0, ${96 - idx * 2}% 100%, ${4 + idx * 2}% 100%)`; // Middle stages
              
              return (
                <div key={idx} className="funnel-stage-row">
                  <div className="stage-label-left">
                    <span className="stage-name-label">{stage.name}</span>
                  </div>
                  <div className="funnel-bar-container">
                    <div 
                      className="funnel-bar"
                      style={{
                        width: `${barWidth}%`,
                        backgroundColor: stage.color,
                        clipPath: clipPath,
                      }}
                    >
                      <div className="funnel-bar-text">
                        <span className="percentage-text">{percentage}%</span>
                      </div>
                    </div>
                  </div>
                  <div className="stage-value-right">
                    <span className="stage-count">{formatNumber(stage.value)}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Conversion Rates */}
      <div className="conversion-rates">
        <h4>Stage-by-Stage Conversion Rates</h4>
        <div className="conversion-grid">
          {conversionRates.map((conv, idx) => (
            <div key={idx} className="conversion-card">
              <div className="conversion-stage">{conv.stage}</div>
              <div className={`conversion-rate ${conv.rate > 50 ? 'high' : conv.rate > 25 ? 'medium' : 'low'}`}>
                {conv.rate}%
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Country Performance Tables - Renamed */}
      <div className="country-performance-section">
        <div className="performance-columns">
          <div className="performance-column">
            <h4>Top Performing Countries</h4>
            <table className="performance-table">
              <thead>
                <tr>
                  <th>Country</th>
                  <th className="text-right">Applications</th>
                  <th className="text-right">Activated</th>
                  <th className="text-right">Avg Days</th>
                  <th className="text-right">Rate</th>
                </tr>
              </thead>
              <tbody>
                {country_performance
                  .sort((a, b) => (b.activation_rate || 0) - (a.activation_rate || 0))
                  .slice(0, 10)
                  .map((country, idx) => (
                    <tr key={idx}>
                      <td className="country-name">{country.partner_country}</td>
                      <td className="text-right">{formatNumber(country.total_applications)}</td>
                      <td className="text-right">{formatNumber(country.activated_partners)}</td>
                      <td className="text-right">{Math.round(country.avg_days_to_activation || 0)}</td>
                      <td className="text-right rate-cell high-rate">
                        <strong>{country.activation_rate || 0}%</strong>
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
          
          <div className="performance-column">
            <h4>🎯 Top Activation Opportunities</h4>
            <table className="performance-table">
              <thead>
                <tr>
                  <th>Country</th>
                  <th className="text-right">Applications</th>
                  <th className="text-right">Retention</th>
                  <th className="text-right">Activation Rate</th>
                </tr>
              </thead>
              <tbody>
                {country_performance
                  .filter(country => country.total_applications >= 50 && country.activated_partners > 0)  // Only countries with volume and some activation
                  .filter(country => country.activation_rate < 8.0 || (country.retention_rate && country.retention_rate < 50.0))  // Below 8% activation or 50% retention
                  .sort((a, b) => b.total_applications - a.total_applications)  // Sort by volume (highest first)
                  .slice(0, 10)
                  .map((country, idx) => (
                    <tr key={idx}>
                      <td className="country-name">{country.partner_country}</td>
                      <td className="text-right">{formatNumber(country.total_applications)}</td>
                      <td className="text-right">{country.retention_rate || 0}%</td>
                      <td className="text-right rate-cell low-rate">
                        <strong>{country.activation_rate || 0}%</strong>
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};

// Function to format insight text from markdown to HTML with enhanced styling
const formatInsightText = (text) => {
  if (!text) return '';
  
  // Convert bullet points to HTML list items with enhanced formatting
  const bulletPoints = text.split('•').filter(point => point.trim());
  
  if (bulletPoints.length <= 1) {
    return `<div class="insight-section">${text}</div>`;
  }
  
  const formattedPoints = bulletPoints.map(point => {
    const trimmed = point.trim();
    if (!trimmed) return '';
    
    // Simple formatting - just bold important terms
    let formatted = trimmed
      // Bold key action words
      .replace(/\b(Focus|Implement|Consider|Analyze|Monitor|Track|Scale|Optimize|Improve|Increase|Reduce|Prioritize|Develop|Create|Build|Launch|Test|Measure|Review|Audit|Study|Replicate|Expand|Add|Remove|Update|Enhance|Strengthen|Accelerate|Automate|Standardize|Deploy|Execute|Establish|Introduce|Leverage|Maximize|Minimize|Streamline)\b/gi, '<strong>$1</strong>')
      
      // Bold performance terms
      .replace(/\b(ROI|return on investment|lifetime value|LTV|conversion rate|activation rate|retention rate|churn rate|growth rate|performance|efficiency|effectiveness|productivity)\b/gi, '<strong>$1</strong>')
      
      // Bold business terms
      .replace(/\b(onboarding|activation|reactivation|engagement|acquisition|retention|monetization|scalability|optimization|automation|personalization)\b/gi, '<strong>$1</strong>');
    
    return `<li>${formatted}</li>`;
  }).filter(point => point);
  
  return `<ul>${formattedPoints.join('')}</ul>`;
};

// Function to format insights for the bottom summary section (paragraph format)
const formatSummaryInsights = (text) => {
  if (!text) return '';
  
  // Convert bullet points to sentences and create paragraph format
  const bulletPoints = text.split('•').filter(point => point.trim());
  
  if (bulletPoints.length <= 1) {
    return text;
  }
  
  // Join bullet points into flowing sentences without any bold formatting
  const sentences = bulletPoints.map(point => {
    const trimmed = point.trim();
    if (!trimmed) return '';
    
    // No formatting - just plain text
    let formatted = trimmed;
    
    // Ensure sentence ends with period
    if (!formatted.endsWith('.') && !formatted.endsWith('!') && !formatted.endsWith('?')) {
      formatted += '.';
    }
    
    return formatted;
  }).filter(sentence => sentence);
  
  // Join sentences with spaces to create flowing paragraphs
  return sentences.join(' ');
};

// AI Insight Hook for reusable tooltip functionality
const useAIInsight = () => {
  const [showInsight, setShowInsight] = useState(false);
  const [insightContent, setInsightContent] = useState('');
  const [insightLoading, setInsightLoading] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ left: 0, top: 0, toRight: true });
  const tooltipRef = useRef(null);
  const buttonRef = useRef(null);

  // Close tooltip when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (tooltipRef.current && !tooltipRef.current.contains(event.target)) {
        setShowInsight(false);
      }
    };

    if (showInsight) {
      document.addEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [showInsight]);

  // Calculate tooltip position
  const calculateTooltipPosition = () => {
    if (!buttonRef.current) return { left: 0, top: 0, toRight: true };
    
    const buttonRect = buttonRef.current.getBoundingClientRect();
    const tooltipWidth = 350;
    const tooltipHeight = 250;
    
    // Position to the right of the button using fixed positioning
    let left = buttonRect.right + 10;
    let top = buttonRect.top - 10;
    let toRight = true;
    
    // If tooltip would go off screen, position to the left
    if (left + tooltipWidth > window.innerWidth - 20) {
      left = buttonRect.left - tooltipWidth - 10;
      toRight = false;
    }
    
    // Adjust vertical position if needed
    if (top < 20) {
      top = 20;
    } else if (top + tooltipHeight > window.innerHeight - 20) {
      top = window.innerHeight - tooltipHeight - 20;
    }
    
    return { left, top, toRight };
  };

  const handleInsightClick = async (title, data, widgetType) => {
    if (showInsight) {
      setShowInsight(false);
      return;
    }
    
    // Calculate position before showing tooltip
    const position = calculateTooltipPosition();
    setTooltipPosition(position);
    
    setInsightLoading(true);
    setShowInsight(true);
    
    try {
      const response = await fetch(`${API_BASE_URL}/ai-insight`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          widget_type: widgetType || title,
          data: data,
          title: title
        })
      });
      
      const result = await response.json();
      setInsightContent(result.insight || 'No insights available at this time.');
    } catch (error) {
      setInsightContent('Unable to generate AI insights at this time. Please try again later.');
    } finally {
      setInsightLoading(false);
    }
  };

  return {
    showInsight,
    insightContent,
    insightLoading,
    tooltipPosition,
    tooltipRef,
    buttonRef,
    handleInsightClick
  };
};

// Country Analysis Tabs Component
// Country Analysis Widget Component
const CountryAnalysisWidget = ({ title, description, data, columns, loading, formatNumber, onAIInsight }) => {
  const [showInsight, setShowInsight] = useState(false);
  const [insightContent, setInsightContent] = useState('');
  const [insightLoading, setInsightLoading] = useState(false);
  const [tooltipPosition, setTooltipPosition] = useState({ left: 0, top: 0, below: false });
  const tooltipRef = useRef(null);
  const buttonRef = useRef(null);

  // Close tooltip when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (tooltipRef.current && !tooltipRef.current.contains(event.target)) {
        setShowInsight(false);
      }
    };

    if (showInsight) {
      document.addEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [showInsight]);

  // Calculate tooltip position
  const calculateTooltipPosition = () => {
    if (!buttonRef.current) return { left: 0, top: 0, toRight: true };
    
    const buttonRect = buttonRef.current.getBoundingClientRect();
    const tooltipWidth = 350;
    const tooltipHeight = 250;
    
    // Position to the right of the button using fixed positioning
    let left = buttonRect.right + 10;
    let top = buttonRect.top - 10;
    let toRight = true;
    
    // If tooltip would go off screen, position to the left
    if (left + tooltipWidth > window.innerWidth - 20) {
      left = buttonRect.left - tooltipWidth - 10;
      toRight = false;
    }
    
    // Adjust vertical position if needed
    if (top < 20) {
      top = 20;
    } else if (top + tooltipHeight > window.innerHeight - 20) {
      top = window.innerHeight - tooltipHeight - 20;
    }
    
    return { left, top, toRight };
  };
  const formatValue = (value, format, highlight) => {
    if (value === null || value === undefined) return '-';
    
    switch (format) {
      case 'number':
        return typeof value === 'number' ? formatNumber(value) : value;
      case 'percentage':
        const numValue = typeof value === 'string' ? parseFloat(value) : value;
        return `${numValue?.toFixed(1) || 0}%`;
      case 'currency':
        const currValue = typeof value === 'string' ? parseFloat(value) : value;
        return `$${currValue?.toLocaleString() || 0}`;
      case 'decimal':
        const decValue = typeof value === 'string' ? parseFloat(value) : value;
        return decValue?.toFixed(1) || '0.0';
      default:
        return value;
    }
  };

  const getCellClass = (value, highlight) => {
    if (!highlight) return '';
    
    const numValue = typeof value === 'string' ? parseFloat(value) : value;
    if (highlight === 'low') {
      return numValue < 30 ? 'rate-cell high-rate' : numValue < 60 ? 'rate-cell medium-rate' : 'rate-cell';
    } else if (highlight === 'high') {
      return numValue > 100 ? 'rate-cell high-rate' : numValue > 50 ? 'rate-cell medium-rate' : 'rate-cell';
    } else if (highlight === 'opportunity') {
      return numValue < 5 ? 'rate-cell opportunity-rate' : numValue < 10 ? 'rate-cell medium-rate' : 'rate-cell';
    }
    return '';
  };

  return (
    <div className="widget table-widget underperforming">
      <div className="widget-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', position: 'relative' }}>
          <h3>{title}</h3>
          <div className="ai-insight-container" ref={tooltipRef}>
              <button 
                ref={buttonRef}
                className="ai-insight-icon"
                onClick={async () => {
                  if (showInsight) {
                    setShowInsight(false);
                    return;
                  }
                  
                  // Calculate position before showing tooltip
                  const position = calculateTooltipPosition();
                  setTooltipPosition(position);
                  
                  setInsightLoading(true);
                  setShowInsight(true);
                  
                  try {
                    const response = await fetch(`${API_BASE_URL}/ai-insight`, {
                      method: 'POST',
                      headers: {
                        'Content-Type': 'application/json',
                      },
                      body: JSON.stringify({
                        widget_type: title,
                        data: data,
                        title: title
                      })
                    });
                    
                    const result = await response.json();
                    setInsightContent(result.insight || 'No insights available at this time.');
                  } catch (error) {
                    setInsightContent('Unable to generate AI insights at this time. Please try again later.');
                  } finally {
                    setInsightLoading(false);
                  }
                }}
                title="Get AI insights for this widget"
              >
                💡
              </button>
              
              {showInsight && (
                <div 
                  className={`ai-insight-tooltip ${tooltipPosition.toRight ? 'to-right' : 'to-left'}`}
                  style={{
                    left: `${tooltipPosition.left}px`,
                    top: `${tooltipPosition.top}px`
                  }}
                >
                  <div className="ai-insight-tooltip-content">
                    {insightLoading ? (
                      <div className="ai-insight-loading-small">
                        <div className="loading-spinner-small"></div>
                        <span>Generating insights...</span>
                      </div>
                    ) : (
                      <div 
                        className="ai-insight-text-formatted"
                        dangerouslySetInnerHTML={{ 
                          __html: formatInsightText(insightContent) 
                        }}
                      />
                    )}
                  </div>
                  <div className="ai-insight-tooltip-arrow"></div>
                </div>
              )}
            </div>
        </div>
        <p className="widget-description">{description}</p>
      </div>

      {loading ? (
        <div className="widget-loading">
          <div className="shimmer-table"></div>
        </div>
      ) : (
        <table className="performance-table">
          <thead>
            <tr>
              {columns.map((col) => (
                <th key={col.key} className={col.align === 'right' ? 'text-right' : ''}>
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.length > 0 ? (
              data.map((item, idx) => (
                <tr key={idx}>
                  {columns.map((col) => (
                    <td 
                      key={col.key} 
                      className={`${col.align === 'right' ? 'text-right' : ''} ${getCellClass(item[col.key], col.highlight)}`}
                    >
                      {col.key === 'country' ? (
                        <span className="country-name">{item[col.key]}</span>
                      ) : (
                        <strong>{formatValue(item[col.key], col.format, col.highlight)}</strong>
                      )}
                    </td>
                  ))}
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={columns.length} className="no-data">
                  No data available
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </div>
  );
};

// Add this component before the export default App line
const SpotlightDashboard = () => {
  const [dashboardData, setDashboardData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [dateRange, setDateRange] = useState(90);
  const [funnelData, setFunnelData] = useState(null);

  // AI Insight hooks for each widget that needs them
  const eventEffectivenessAI = useAIInsight();
  const conversionFunnelAI = useAIInsight();
  const countryROIAI = useAIInsight();
  const retentionCohortsAI = useAIInsight();

  useEffect(() => {
    fetchDashboardData();
  }, [dateRange]);

  const fetchDashboardData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fetch(`${API_BASE_URL}/spotlight/dashboard?date_range=${dateRange}`);
      const result = await response.json();
      
      if (result.success) {
        setDashboardData(result.data);
      } else {
        setError(result.error || 'Failed to fetch dashboard data');
      }
    } catch (err) {
      setError('Failed to connect to server');
      console.error('Error fetching data:', err);
    } finally {
      setLoading(false);
    }
  };

  const formatNumber = (num) => {
    if (num === null || num === undefined) return '0';
    return new Intl.NumberFormat('en-US').format(num);
  };

  const formatCurrency = (num) => {
    if (num === null || num === undefined) return '$0';
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(num);
  };

  // AI Insights function


  // Show skeleton loader for initial load only
  if (loading && !dashboardData) {
    return (
      <div className="spotlight-dashboard">
        <div className="loading-state">
          <div className="spinner"></div>
          <p>Loading partner analytics...</p>
        </div>
      </div>
    );
  }

  if (error && !dashboardData) {
    return (
      <div className="spotlight-dashboard">
        <div className="error-state">
          <p>Error: {error}</p>
          <button onClick={fetchDashboardData}>Retry</button>
        </div>
      </div>
    );
  }

  return (
    <div className="spotlight-dashboard">
      {/* Top Controls Bar */}
      <div className="dashboard-controls">
        <h1 className="dashboard-title">Partner Spotlight Dashboard</h1>
        <div className="controls-right">
          <select
            value={dateRange} 
            onChange={(e) => setDateRange(Number(e.target.value))}
            className="date-filter-select"
          >
            <option value={30}>Last 30 Days</option>
            <option value={60}>Last 60 Days</option>
            <option value={90}>Last 90 Days</option>
            <option value={180}>Last 180 Days</option>
            <option value={365}>Last Year</option>
            <option value={0}>All Time</option>
          </select>
          <button onClick={fetchDashboardData} className="refresh-btn-small">
            <TrendingIcon /> Refresh
          </button>
        </div>
      </div>

      <div className="dashboard-grid">
        {/* Top Row - Key Metrics */}
        <div className="widget metric-widget">
          <h3>Total Applications</h3>
          {loading ? (
            <div className="metric-loading">
              <div className="shimmer"></div>
            </div>
          ) : (
            <>
              <div className="metric-big">
                {formatNumber(dashboardData?.overview_metrics?.total_applications || 0)}
              </div>
              <div className="metric-trend">
                {dateRange === 0 ? 'All time' : `Last ${dateRange} days`}
              </div>
            </>
          )}
        </div>
        
        <div className="widget metric-widget">
          <h3>Activation Rate</h3>
          {loading ? (
            <div className="metric-loading">
              <div className="shimmer"></div>
            </div>
          ) : (
            <>
              <div className="metric-big">
                {dashboardData?.overview_metrics?.overall_activation_rate || 0}%
              </div>
              <div className="metric-trend">Overall performance</div>
            </>
          )}
        </div>
        
        <div className="widget metric-widget">
          <h3>VAN Trip ROI</h3>
          {loading ? (
            <div className="metric-loading">
              <div className="shimmer"></div>
            </div>
          ) : (
            <>
              <div className="metric-big">
                {formatCurrency(dashboardData?.van_roi_data?.total_van_earnings || 0)}
              </div>
              <div className="metric-trend">
                {formatNumber(dashboardData?.van_roi_data?.total_van_partners || 0)} VAN partners
              </div>
            </>
          )}
        </div>
        
        <div className="widget metric-widget">
          <h3>Network Retention</h3>
          {loading ? (
            <div className="metric-loading">
              <div className="shimmer"></div>
            </div>
          ) : (
            <>
              <div className="metric-big">
                {dashboardData?.network_retention?.network_retention_rate || 0}%
              </div>
              <div className="metric-trend">Active in last 30 days</div>
            </>
          )}
        </div>

        {/* Event Effectiveness by Activation Rate - full width row */}
        <div className="widget chart-widget event-impact" style={{ gridColumn: 'span 12' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', position: 'relative' }}>
            <h3>Event Effectiveness by Activation Rate</h3>
            <div className="ai-insight-container" ref={eventEffectivenessAI.tooltipRef}>
              <button 
                ref={eventEffectivenessAI.buttonRef}
                className="ai-insight-icon"
                onClick={() => eventEffectivenessAI.handleInsightClick(
                  'Event Effectiveness by Activation Rate',
                  dashboardData?.event_impact || [],
                  'Event Effectiveness'
                )}
                title="Get AI insights for this widget"
              >
                💡
              </button>
              
              {eventEffectivenessAI.showInsight && (
                <div 
                  className={`ai-insight-tooltip ${eventEffectivenessAI.tooltipPosition.toRight ? 'to-right' : 'to-left'}`}
                  style={{
                    left: `${eventEffectivenessAI.tooltipPosition.left}px`,
                    top: `${eventEffectivenessAI.tooltipPosition.top}px`
                  }}
                >
                  <div className="ai-insight-tooltip-content">
                    {eventEffectivenessAI.insightLoading ? (
                      <div className="ai-insight-loading-small">
                        <div className="loading-spinner-small"></div>
                        <span>Generating insights...</span>
                      </div>
                    ) : (
                      <div 
                        className="ai-insight-text-formatted"
                        dangerouslySetInnerHTML={{ 
                          __html: formatInsightText(eventEffectivenessAI.insightContent) 
                        }}
                      />
                    )}
                  </div>
                  <div className="ai-insight-tooltip-arrow"></div>
                </div>
              )}
            </div>
          </div>
          {loading ? (
            <div className="widget-loading">
              <div className="shimmer-bars"></div>
            </div>
          ) : (
            <div className="event-effectiveness-chart">
              {dashboardData?.event_impact?.filter(event => event.event_type !== 'No Event').map((event, idx) => {
                const barWidth = Math.max(event.activation_rate, 2);
                const isSmallBar = barWidth < 12;
                return (
                  <div key={idx} className="event-bar-row">
                    <div className="event-info">
                      <span className="event-name">{event.event_type}</span>
                      <span className="event-count">{formatNumber(event.partner_count)} partners</span>
                    </div>
                    <div className="event-bar-container">
                      <div 
                        className={`event-bar ${isSmallBar ? 'small-bar' : ''}`}
                        style={{
                          width: `${barWidth}%`,
                          backgroundColor: event.event_type === 'VAN Trip' ? '#dc2626' : 
                                         event.event_type === 'Webinar' ? '#059669' :
                                         event.event_type === 'Conference' ? '#3b82f6' :
                                         event.event_type === 'Seminar' ? '#8b5cf6' : '#6b7280'
                        }}
                      >
                        <span className="event-rate">{event.activation_rate}%</span>
                      </div>
                    </div>
                  </div>
                );
              })}
              {dashboardData?.event_impact?.find(e => e.event_type === 'No Event') && (
                <div className="no-event-comparison">
                  <span>Baseline (No Event): {dashboardData.event_impact.find(e => e.event_type === 'No Event').activation_rate}%</span>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Applications by Platform - 6 columns */}
        <div className="widget chart-widget trends" style={{ gridColumn: 'span 6' }}>
          <h3>Applications by Platform</h3>
          {loading ? (
            <div className="widget-loading">
              <div className="shimmer-chart"></div>
            </div>
          ) : (
            <div className="area-chart">
              <div className="chart-area stacked-bar-area">
                {(() => {
                  if (!dashboardData?.monthly_trends) return null;
                  const months = Array.from(new Set(dashboardData.monthly_trends.map(m => m.month)));
                  // For scaling
                  const maxApplications = Math.max(...months.map(month => {
                    const dw = dashboardData.monthly_trends.find(m => m.month === month && m.platform === 'DynamicWorks') || { applications: 0 };
                    const ma = dashboardData.monthly_trends.find(m => m.month === month && m.platform === 'MyAffiliate') || { applications: 0 };
                    return dw.applications + ma.applications;
                  }));
                  return months.map((month, idx) => {
                    const dw = dashboardData.monthly_trends.find(m => m.month === month && m.platform === 'DynamicWorks') || { applications: 0 };
                    const ma = dashboardData.monthly_trends.find(m => m.month === month && m.platform === 'MyAffiliate') || { applications: 0 };
                    const dwHeight = (dw.applications / maxApplications) * 100;
                    const maHeight = (ma.applications / maxApplications) * 100;
                    return (
                      <div key={month} className="chart-month stacked">
                        <div className="chart-bar-stack">
                          <div
                            className="chart-bar apps dw"
                            style={{ height: `${dwHeight}%`, bottom: `${maHeight}%` }}
                            title={`DynamicWorks: ${formatNumber(dw.applications)} applications`}
                          />
                          <div
                            className="chart-bar apps ma"
                            style={{ height: `${maHeight}%`, bottom: 0 }}
                            title={`MyAffiliate: ${formatNumber(ma.applications)} applications`}
                          />
                        </div>
                        <div className="chart-label">{month}</div>
                      </div>
                    );
                  });
                })()}
              </div>
              <div className="chart-legend grouped">
                <span><i className="dot apps dw"></i>DynamicWorks</span>
                <span><i className="dot apps ma"></i>MyAffiliate</span>
              </div>
            </div>
          )}
        </div>

        {/* Activations by Platform - 6 columns */}
        <div className="widget chart-widget trends" style={{ gridColumn: 'span 6' }}>
          <h3>Activations by Platform</h3>
          {loading ? (
            <div className="widget-loading">
              <div className="shimmer-chart"></div>
            </div>
          ) : (
            <div className="area-chart">
              <div className="chart-area stacked-bar-area">
                {(() => {
                  if (!dashboardData?.monthly_trends) return null;
                  const months = Array.from(new Set(dashboardData.monthly_trends.map(m => m.month)));
                  // For scaling
                  const maxActivations = Math.max(...months.map(month => {
                    const dw = dashboardData.monthly_trends.find(m => m.month === month && m.platform === 'DynamicWorks') || { activations: 0 };
                    const ma = dashboardData.monthly_trends.find(m => m.month === month && m.platform === 'MyAffiliate') || { activations: 0 };
                    return dw.activations + ma.activations;
                  }));
                  return months.map((month, idx) => {
                    const dw = dashboardData.monthly_trends.find(m => m.month === month && m.platform === 'DynamicWorks') || { activations: 0 };
                    const ma = dashboardData.monthly_trends.find(m => m.month === month && m.platform === 'MyAffiliate') || { activations: 0 };
                    const dwHeight = (dw.activations / maxActivations) * 100;
                    const maHeight = (ma.activations / maxActivations) * 100;
                    return (
                      <div key={month} className="chart-month stacked">
                        <div className="chart-bar-stack">
                          <div
                            className="chart-bar active dw"
                            style={{ height: `${dwHeight}%`, bottom: `${maHeight}%` }}
                            title={`DynamicWorks: ${formatNumber(dw.activations)} activations`}
                          />
                          <div
                            className="chart-bar active ma"
                            style={{ height: `${maHeight}%`, bottom: 0 }}
                            title={`MyAffiliate: ${formatNumber(ma.activations)} activations`}
                          />
                        </div>
                        <div className="chart-label">{month}</div>
                      </div>
                    );
                  });
                })()}
              </div>
              <div className="chart-legend grouped">
                <span><i className="dot active dw"></i>DynamicWorks</span>
                <span><i className="dot active ma"></i>MyAffiliate</span>
              </div>
            </div>
          )}
        </div>

        {/* Add Conversion Funnel Section */}
        <div className="widget funnel-widget">
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', position: 'relative' }}>
            <h3>🔄 Conversion & Activation Funnel</h3>
            <div className="ai-insight-container" ref={conversionFunnelAI.tooltipRef}>
              <button 
                ref={conversionFunnelAI.buttonRef}
                className="ai-insight-icon"
                onClick={() => conversionFunnelAI.handleInsightClick(
                  'Conversion & Activation Funnel',
                  funnelData || {
                    funnel_overview: {},
                    country_performance: [],
                    title: 'Conversion & Activation Funnel Analysis'
                  },
                  'Conversion Funnel'
                )}
                title="Get AI insights for this widget"
              >
                💡
              </button>
              
              {conversionFunnelAI.showInsight && (
                <div 
                  className={`ai-insight-tooltip ${conversionFunnelAI.tooltipPosition.toRight ? 'to-right' : 'to-left'}`}
                  style={{
                    left: `${conversionFunnelAI.tooltipPosition.left}px`,
                    top: `${conversionFunnelAI.tooltipPosition.top}px`
                  }}
                >
                  <div className="ai-insight-tooltip-content">
                    {conversionFunnelAI.insightLoading ? (
                      <div className="ai-insight-loading-small">
                        <div className="loading-spinner-small"></div>
                        <span>Generating insights...</span>
                      </div>
                    ) : (
                      <div 
                        className="ai-insight-text-formatted"
                        dangerouslySetInnerHTML={{ 
                          __html: formatInsightText(conversionFunnelAI.insightContent) 
                        }}
                      />
                    )}
                  </div>
                  <div className="ai-insight-tooltip-arrow"></div>
                </div>
              )}
            </div>
          </div>
          <FunnelChart 
            funnelData={dashboardData} 
            selectedCountry={null}
            dateRange={dateRange}
            onFunnelDataUpdate={setFunnelData}
          />
        </div>
        
        {/* Platform Comparison - Changed to Pie Chart */}
        <div className="widget comparison-widget">
          <h3>Platform Performance</h3>
          {loading ? (
            <div className="widget-loading">
              <div className="shimmer-pie"></div>
            </div>
          ) : (
            <div className="platform-comparison">
              <div className="platform-pie-chart">
                {dashboardData?.platform_comparison?.map((platform, idx) => {
                  const total = dashboardData.platform_comparison.reduce((sum, p) => sum + p.total_partners, 0);
                  const percentage = ((platform.total_partners / total) * 100).toFixed(1);
                  const rotation = idx === 0 ? 0 : 
                    dashboardData.platform_comparison.slice(0, idx).reduce((sum, p) => sum + (p.total_partners / total) * 360, 0);
                  
                  return (
                    <div key={idx} className="pie-segment-wrapper">
                      <div 
                        className="pie-segment"
                        style={{ 
                          '--percentage': percentage,
                          '--rotation': rotation,
                          '--color': idx === 0 ? '#dc2626' : '#3b82f6'
                        }}
                      />
                    </div>
                  );
                })}
                <div className="pie-center">
                  <div className="pie-total">{formatNumber(dashboardData?.platform_comparison?.reduce((sum, p) => sum + p.total_partners, 0))}</div>
                  <div className="pie-label">Total Partners</div>
                </div>
              </div>
              <div className="platform-legend">
                {dashboardData?.platform_comparison?.map((platform, idx) => (
                  <div key={idx} className="legend-item">
                    <div className="legend-color" style={{backgroundColor: idx === 0 ? '#dc2626' : '#3b82f6'}}></div>
                    <div className="legend-info">
                      <div className="legend-title">{platform.partner_platform}</div>
                      <div className="legend-stats">
                        <span>{formatNumber(platform.total_partners)} partners</span>
                        <span className="separator">•</span>
                        <span>{platform.retention_rate}% retention</span>
                        <span className="separator">•</span>
                        <span>{formatCurrency(platform.avg_lifetime_value)} avg LTV</span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* VAN Trip Countries */}
        <div className="widget table-widget van-trips">
          <h3>VAN Trip Performance by Country</h3>
          {loading ? (
            <div className="widget-loading">
              <div className="shimmer-table"></div>
            </div>
          ) : (
            <table className="performance-table">
              <thead>
                <tr>
                  <th>Country</th>
                  <th className="text-right">Partners</th>
                  <th className="text-right">Rate</th>
                  <th className="text-right">Earnings</th>
                </tr>
              </thead>
              <tbody>
                {dashboardData?.van_trip_effectiveness?.slice(0, 6).map((country, idx) => (
                  <tr key={idx}>
                    <td className="country-name">{country.country}</td>
                    <td className="text-right">{formatNumber(country.van_trip_partners)}</td>
                    <td className={`text-right rate-cell ${country.van_activation_rate > 20 ? 'high-rate' : country.van_activation_rate > 10 ? 'medium-rate' : 'low-rate'}`}>
                      <strong>{country.van_activation_rate}%</strong>
                    </td>
                    <td className="text-right">{formatCurrency(country.van_earnings)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Country Analysis Widgets - All visible at once */}
        <CountryAnalysisWidget 
          title="⚡ Activation Speed Leaders"
          description="Countries with fastest partner activation times and highest conversion rates"
          data={dashboardData?.conversion_funnel
            ?.filter(country => country.applications >= 20 && parseFloat(country.activation_rate) >= 5)
            .sort((a, b) => parseFloat(a.avg_days_to_activate || 999) - parseFloat(b.avg_days_to_activate || 999))
            .slice(0, 6) || []}
          columns={[
            { key: 'country', label: 'Country', align: 'left' },
            { key: 'applications', label: 'Apps', align: 'right', format: 'number' },
            { key: 'avg_days_to_activate', label: 'Avg Days', align: 'right', format: 'decimal', highlight: 'low' },
            { key: 'activation_rate', label: 'Rate', align: 'right', format: 'percentage', highlight: 'high' }
          ]}
          loading={loading}
          formatNumber={formatNumber}
        />

        <CountryAnalysisWidget 
          title="🔄 Partner Comeback Success"
          description="Partners who were dormant (stopped earning) but returned to active earning in last 30 days"
          data={dashboardData?.country_roi
            ?.filter(country => country.reactivated_30d > 0)
            .sort((a, b) => parseFloat(b.reactivated_30d) - parseFloat(a.reactivated_30d))
            .slice(0, 6) || []}
          columns={[
            { key: 'country', label: 'Country', align: 'left' },
            { key: 'reactivated_30d', label: 'Came Back', align: 'right', format: 'number', highlight: 'high' },
            { key: 'currently_active', label: 'Total Earning Now', align: 'right', format: 'number' },
            { key: 'new_partners_30d', label: 'Brand New', align: 'right', format: 'number' }
          ]}
          loading={loading}
          formatNumber={formatNumber}
        />

        <CountryAnalysisWidget 
          title="🚀 High-Volume Opportunities"
          description="Countries with significant application volume but untapped activation potential"
          data={dashboardData?.underperforming_countries
            ?.filter(country => country.total_applications >= 100)
            .sort((a, b) => (b.total_applications * (20 - parseFloat(b.activation_rate))) - (a.total_applications * (20 - parseFloat(a.activation_rate))))
            .slice(0, 6) || []}
          columns={[
            { key: 'country', label: 'Country', align: 'left' },
            { key: 'total_applications', label: 'Apps', align: 'right', format: 'number' },
            { key: 'activated_partners', label: 'Activated', align: 'right', format: 'number' },
            { key: 'activation_rate', label: 'Rate', align: 'right', format: 'percentage', highlight: 'opportunity' }
          ]}
          loading={loading}
          formatNumber={formatNumber}
        />

        <CountryAnalysisWidget 
          title="📈 Growth Momentum Leaders"
          description="Countries showing strongest month-over-month partner acquisition growth"
          data={dashboardData?.top_growing_countries
            ?.filter(country => parseFloat(country.growth_rate) > 100 && country.current_signups >= 10)
            .sort((a, b) => parseFloat(b.growth_rate) - parseFloat(a.growth_rate))
            .slice(0, 6) || []}
          columns={[
            { key: 'country', label: 'Country', align: 'left' },
            { key: 'previous_signups', label: '31-60d ago', align: 'right', format: 'number' },
            { key: 'current_signups', label: 'Last 30d', align: 'right', format: 'number' },
            { key: 'growth_rate', label: 'Growth', align: 'right', format: 'percentage', highlight: 'high' }
          ]}
          loading={loading}
          formatNumber={formatNumber}
        />

        {/* Country ROI Heatmap - Improved for better readability */}
        <div className="widget heatmap-widget">
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', position: 'relative' }}>
            <h3>Country ROI Analysis</h3>
            <div className="ai-insight-container" ref={countryROIAI.tooltipRef}>
              <button 
                ref={countryROIAI.buttonRef}
                className="ai-insight-icon"
                onClick={() => countryROIAI.handleInsightClick(
                  'Country ROI Analysis',
                  dashboardData?.country_roi || [],
                  'Country ROI Analysis'
                )}
                title="Get AI insights for this widget"
              >
                💡
              </button>
              
              {countryROIAI.showInsight && (
                <div 
                  className={`ai-insight-tooltip ${countryROIAI.tooltipPosition.toRight ? 'to-right' : 'to-left'}`}
                  style={{
                    left: `${countryROIAI.tooltipPosition.left}px`,
                    top: `${countryROIAI.tooltipPosition.top}px`
                  }}
                >
                  <div className="ai-insight-tooltip-content">
                    {countryROIAI.insightLoading ? (
                      <div className="ai-insight-loading-small">
                        <div className="loading-spinner-small"></div>
                        <span>Generating insights...</span>
                      </div>
                    ) : (
                      <div 
                        className="ai-insight-text-formatted"
                        dangerouslySetInnerHTML={{ 
                          __html: formatInsightText(countryROIAI.insightContent) 
                        }}
                      />
                    )}
                  </div>
                  <div className="ai-insight-tooltip-arrow"></div>
                </div>
              )}
            </div>
          </div>
          {loading ? (
            <div className="widget-loading">
              <div className="shimmer-table"></div>
            </div>
          ) : (
            <div className="roi-table-view">
              <table className="roi-table">
                <thead>
                  <tr>
                    <th>Country</th>
                    <th className="text-right">Total Earnings</th>
                    <th className="text-right">Per Partner</th>
                    <th className="text-right">Retention</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboardData?.country_roi?.slice(0, 12).map((country, idx) => {
                    const maxEarnings = Math.max(...dashboardData.country_roi.slice(0, 12).map(c => c.total_earnings));
                    const intensity = Math.min(country.total_earnings / maxEarnings, 1);
                    
                    return (
                      <tr key={idx} className="roi-row">
                        <td className="country-name">
                          <div className="roi-indicator" style={{
                            width: `${intensity * 100}%`,
                            backgroundColor: `rgba(220, 38, 38, ${0.2 + intensity * 0.6})`
                          }}></div>
                          <span>{country.country}</span>
                        </td>
                        <td className="text-right earnings-cell">
                          <strong>{formatCurrency(country.total_earnings)}</strong>
                        </td>
                        <td className="text-right">{formatCurrency(country.earnings_per_partner)}</td>
                        <td className={`text-right ${country.retention_rate > 20 ? 'high-retention' : country.retention_rate > 10 ? 'medium-retention' : 'low-retention'}`}>
                          <strong>{country.retention_rate}%</strong>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Retention Cohorts */}
        <div className="widget cohort-widget">
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', position: 'relative' }}>
            <h3>Partner Retention Cohorts</h3>
            <div className="ai-insight-container" ref={retentionCohortsAI.tooltipRef}>
              <button 
                ref={retentionCohortsAI.buttonRef}
                className="ai-insight-icon"
                onClick={() => retentionCohortsAI.handleInsightClick(
                  'Partner Retention Cohorts',
                  dashboardData?.retention_cohorts || [],
                  'Partner Retention Cohorts'
                )}
                title="Get AI insights for this widget"
              >
                💡
              </button>
              
              {retentionCohortsAI.showInsight && (
                <div 
                  className={`ai-insight-tooltip ${retentionCohortsAI.tooltipPosition.toRight ? 'to-right' : 'to-left'}`}
                  style={{
                    left: `${retentionCohortsAI.tooltipPosition.left}px`,
                    top: `${retentionCohortsAI.tooltipPosition.top}px`
                  }}
                >
                  <div className="ai-insight-tooltip-content">
                    {retentionCohortsAI.insightLoading ? (
                      <div className="ai-insight-loading-small">
                        <div className="loading-spinner-small"></div>
                        <span>Generating insights...</span>
                      </div>
                    ) : (
                      <div 
                        className="ai-insight-text-formatted"
                        dangerouslySetInnerHTML={{ 
                          __html: formatInsightText(retentionCohortsAI.insightContent) 
                        }}
                      />
                    )}
                  </div>
                  <div className="ai-insight-tooltip-arrow"></div>
                </div>
              )}
            </div>
          </div>
          {loading ? (
            <div className="widget-loading">
              <div className="shimmer-grid"></div>
            </div>
          ) : (
            <div className="cohort-grid">
              <div className="cohort-header">
                <span>Cohort</span>
                <span>Size</span>
                <span>Current</span>
                <span>M+1</span>
                <span>M+3</span>
                <span>M+6</span>
              </div>
              {dashboardData?.retention_cohorts?.slice(0, 6).map((cohort, idx) => (
                <div key={idx} className="cohort-row">
                  <span>{cohort.cohort_month}</span>
                  <span>{formatNumber(cohort.cohort_size)}</span>
                  <span className={`retention-cell ${cohort.current_retention > 70 ? 'high' : cohort.current_retention > 50 ? 'medium' : 'low'}`}>
                    {cohort.current_retention}%
                  </span>
                  <span className={`retention-cell ${cohort.m1_retention > 70 ? 'high' : cohort.m1_retention > 50 ? 'medium' : 'low'}`}>
                    {cohort.m1_retention}%
                  </span>
                  <span className={`retention-cell ${cohort.m3_retention > 70 ? 'high' : cohort.m3_retention > 50 ? 'medium' : 'low'}`}>
                    {cohort.m3_retention}%
                  </span>
                  <span className={`retention-cell ${cohort.m6_retention > 70 ? 'high' : cohort.m6_retention > 50 ? 'medium' : 'low'}`}>
                    {cohort.m6_retention}%
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
        
        {/* AI Insights - Always visible */}
        <div className="widget insights-widget">
          <h3>🤖 Key Insights & Recommendations</h3>
          {loading ? (
            <div className="widget-loading">
              <div className="shimmer-text"></div>
            </div>
          ) : (
            <div className="ai-insights-grid">
              {Array.isArray(dashboardData?.ai_insights) ? (
                dashboardData.ai_insights.map((insight, idx) => (
                  <div key={idx} className="insight-card">
                    <h4>{insight.title}</h4>
                    <div 
                      className="ai-summary-text"
                      dangerouslySetInnerHTML={{ 
                        __html: formatSummaryInsights(insight.insight) 
                      }}
                    />
                    <div className="recommendation">
                      <strong>Action:</strong> {insight.recommendation}
                    </div>
                  </div>
                ))
              ) : dashboardData?.ai_insights ? (
                <div className="ai-content">
                  <div 
                    className="ai-summary-text"
                    dangerouslySetInnerHTML={{ 
                      __html: formatSummaryInsights(dashboardData.ai_insights) 
                    }}
                  />
                </div>
              ) : (
                <div className="ai-insights-grid">
                  <div className="insight-card">
                    <h4>🤖 AI Insights Status</h4>
                    <p className="insight-text">
                      {dashboardData?.ai_insights && typeof dashboardData.ai_insights === 'string' && dashboardData.ai_insights.includes('authentication') ? (
                        <>
                          • <span style={{color: '#dc2626'}}>API authentication error detected</span><br/>
                          • Please check your LiteLLM proxy configuration and API key<br/>
                          • Contact your system administrator to resolve API access issues<br/>
                          • The dashboard will continue to work with standard analytics<br/>
                          • Refresh the page after fixing authentication to get AI-powered insights
                        </>
                      ) : (
                        <>
                          • AI insights are being generated based on your current dashboard data<br/>
                          • This analysis will provide personalized recommendations for your partner portfolio<br/>
                          • Insights will cover event effectiveness, conversion optimization, and growth opportunities<br/>
                          • Please check your API configuration if insights don't appear<br/>
                          • Refresh the dashboard to retry AI analysis generation
                        </>
                      )}
                    </p>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>


    </div>
  );
};

export default App; 