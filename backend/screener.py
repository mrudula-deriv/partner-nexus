import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Database connection parameters
db_params = {
    'host': os.getenv('host'),
    'port': os.getenv('port'),
    'database': os.getenv('dbname'),
    'user': os.getenv('user'),
    'password': os.getenv('password')
}

def get_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(**db_params)

def format_metric_value(value):
    """Format metric values for display"""
    if pd.isna(value):
        return "-"
    if isinstance(value, (int, float)):
        return f"{value:,.0f}"
    return str(value)

def get_available_columns():
    """Get list of available columns from the partner_info table"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'partner' 
                AND table_name = 'partner_info';
            """)
            columns = [row[0] for row in cursor.fetchall()]
            print("Available columns:", columns)  # Debug print
            return columns
    finally:
        conn.close()

def get_available_metrics():
    """Get list of available metrics"""
    metrics = {
        'basic_metrics': {
            'application_count': 'Application Count',
            'activated_count_signup': 'First Activated Count - Signup',
            'activated_count_deposit': 'First Activated Count - Deposit',
            'activated_count_traded': 'First Activated Count - Traded',
            'activated_count_earning': 'First Activated Count - Earning'
        },
        'rate_metrics': {
            'activation_rate_signup': 'Activation Rate - Signup',
            'activation_rate_deposit': 'Activation Rate - Deposit',
            'activation_rate_traded': 'Activation Rate - Traded',
            'activation_rate_earning': 'Activation Rate - Earning'
        },
        'time_metrics': {
            'median_time_signup': 'Median Time to Activation - Signup',
            'median_time_deposit': 'Median Time to Activation - Deposit',
            'median_time_traded': 'Median Time to Activation - Traded',
            'median_time_earning': 'Median Time to Activation - Earning'
        },
        'comparison_metrics': {
            'change_3d': '% Change Past 3 Days',
            'change_1w': '% Change Past 1 Week',
            'change_1m': '% Change Past 1 Month',
            'change_3m': '% Change Past 3 Months',
            'change_6m': '% Change Past 6 Months'
        }
    }
    return metrics

def fetch_metrics_data(selected_metrics, where_clause="", params=None, active_filters=None, group_by=None):
    """Fetch metrics data based on available columns
    
    Args:
        selected_metrics (list): List of metrics to fetch
        where_clause (str): SQL WHERE clause for filtering
        params (list): Parameters for the WHERE clause
        active_filters (dict): Active filters for column display
        group_by (list): Optional list of columns to group by. If provided, will use these columns instead of active_filters
    """
    metrics = get_available_metrics()
    
    # Start with empty select parts
    select_parts = []
    group_by_cols = []
    
    # Build the base query with plan type assignment
    base_query = """
    WITH partner_plans AS (
        SELECT 
            partner_id,
            CASE
                WHEN is_master_plan THEN 'Master'
                WHEN is_revshare_plan THEN 'Revenue Share'
                WHEN is_turnover_plan THEN 'Turnover'
                WHEN is_cpa_plan THEN 'CPA'
                WHEN is_ib_plan THEN 'IB'
                ELSE 'Unknown'
            END as plan_type,
            partner_region,
            partner_country,
            partner_platform,
            aff_type,
            partner_level,
            attended_onboarding_event,
            earning_acquisition,
            first_client_joined_date,
            first_client_deposit_date,
            first_client_trade_date,
            first_earning_date,
            date_joined
        FROM partner.partner_info
        WHERE is_internal = FALSE
    )
    """
    
    # Handle grouping columns
    if group_by:
        # Use provided group_by columns for Live Screener
        col_map = {
            'partner_region': 'Region',
            'partner_country': 'Country',
            'aff_type': 'Plan',
            'partner_platform': 'Platform',
            'attended_onboarding_event': 'Event Status',
            'partner_level': 'Partner Level',
            'plan_type': 'Plan Types'
        }
        
        for col in group_by:
            if col in col_map:
                if col == 'attended_onboarding_event':
                    select_parts.append(f"""
                        CASE 
                            WHEN {col} = TRUE THEN 'Attended'
                            WHEN {col} = FALSE THEN 'Not Attended'
                            ELSE 'Unknown'
                        END as "{col_map[col]}"
                    """)
                elif col == 'partner_level':
                    select_parts.append(f'COALESCE({col}::text, \'Unknown\') as "{col_map[col]}"')
                else:
                    select_parts.append(f'COALESCE({col}, \'Unknown\') as "{col_map[col]}"')
                group_by_cols.append(col)
    elif active_filters:
        # Use active_filters for Metrics Test
        col_map = {
            'partner_regions': ('partner_region', 'Partner Region'),
            'partner_countries': ('partner_country', 'Partner Country'),
            'partner_platforms': ('partner_platform', 'Platform'),
            'aff_types': ('aff_type', 'Plan Type'),
            'partner_levels': ('partner_level', 'Partner Level'),
            'event_statuses': ('attended_onboarding_event', 'Event Status'),
            'acquisition_types': ('earning_acquisition', 'Acquisition Type'),
            'plan_types': ('plan_type', 'Plan Types'),
            'date_joined': ('date_joined', 'Date Joined')
        }
        
        for filter_name, filter_data in active_filters.items():
            if filter_data.get('showAsColumn') and filter_name in col_map:
                col_name, display_name = col_map[filter_name]
                if filter_name == 'event_statuses':
                    select_parts.append(f"""
                        CASE 
                            WHEN {col_name} = TRUE THEN 'Attended'
                            WHEN {col_name} = FALSE THEN 'Not Attended'
                            ELSE 'Unknown'
                        END as "{display_name}"
                    """)
                    group_by_cols.append(col_name)  # Add the actual column to GROUP BY
                elif filter_name == 'partner_levels':
                    select_parts.append(f'COALESCE({col_name}::text, \'Unknown\') as "{display_name}"')
                    group_by_cols.append(col_name)
                elif filter_name == 'date_joined':
                    select_parts.append(f'TO_CHAR(date_joined, \'Mon YYYY\') as "{display_name}"')
                    group_by_cols.append(f'TO_CHAR(date_joined, \'Mon YYYY\')')
                else:
                    select_parts.append(f'COALESCE({col_name}, \'Unknown\') as "{display_name}"')
                    group_by_cols.append(col_name)

    # Add selected metrics to select_parts
    for metric in selected_metrics:
        if metric in metrics['basic_metrics'].values():
            # Add basic count metrics
            if metric == 'Application Count':
                select_parts.append(
                    "COUNT(DISTINCT partner_id) as \"Application Count\""
                )
            elif metric == 'First Activated Count - Signup':
                select_parts.append(
                    "COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) as \"First Activated Count - Signup\""
                )
            elif metric == 'First Activated Count - Deposit':
                select_parts.append(
                    "COUNT(DISTINCT CASE WHEN first_client_deposit_date IS NOT NULL THEN partner_id END) as \"First Activated Count - Deposit\""
                )
            elif metric == 'First Activated Count - Traded':
                select_parts.append(
                    "COUNT(DISTINCT CASE WHEN first_client_trade_date IS NOT NULL THEN partner_id END) as \"First Activated Count - Traded\""
                )
            elif metric == 'First Activated Count - Earning':
                select_parts.append(
                    "COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as \"First Activated Count - Earning\""
                )
        elif metric in metrics['rate_metrics'].values():
            # Add rate metrics
            if metric == 'Activation Rate - Signup':
                select_parts.append(
                    """
                    ROUND(
                        CAST(
                            CAST(COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) AS NUMERIC) /
                            NULLIF(CAST(COUNT(DISTINCT partner_id) AS NUMERIC), 0) * 100
                        AS NUMERIC),
                        2
                    ) as "Activation Rate - Signup"
                    """
                )
            elif metric == 'Activation Rate - Deposit':
                select_parts.append(
                    """
                    ROUND(
                        CAST(
                            CAST(COUNT(DISTINCT CASE WHEN first_client_deposit_date IS NOT NULL THEN partner_id END) AS NUMERIC) /
                            NULLIF(CAST(COUNT(DISTINCT partner_id) AS NUMERIC), 0) * 100
                        AS NUMERIC),
                        2
                    ) as "Activation Rate - Deposit"
                    """
                )
            elif metric == 'Activation Rate - Traded':
                select_parts.append(
                    """
                    ROUND(
                        CAST(
                            CAST(COUNT(DISTINCT CASE WHEN first_client_trade_date IS NOT NULL THEN partner_id END) AS NUMERIC) /
                            NULLIF(CAST(COUNT(DISTINCT partner_id) AS NUMERIC), 0) * 100
                        AS NUMERIC),
                        2
                    ) as "Activation Rate - Traded"
                    """
                )
            elif metric == 'Activation Rate - Earning':
                select_parts.append(
                    """
                    ROUND(
                        CAST(
                            CAST(COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) AS NUMERIC) /
                            NULLIF(CAST(COUNT(DISTINCT partner_id) AS NUMERIC), 0) * 100
                        AS NUMERIC),
                        2
                    ) as "Activation Rate - Earning"
                    """
                )
        elif metric in metrics['time_metrics'].values():
            # Add time metrics
            if metric == 'Median Time to Activation - Signup':
                select_parts.append(
                    """
                    ROUND(
                        CAST(
                            PERCENTILE_CONT(0.5) WITHIN GROUP (
                                ORDER BY EXTRACT(EPOCH FROM (first_client_joined_date::timestamp - date_joined::timestamp))::numeric / 86400
                            ) FILTER (WHERE first_client_joined_date IS NOT NULL)
                        AS NUMERIC),
                        1
                    ) as "Median Time to Activation - Signup"
                    """
                )
            elif metric == 'Median Time to Activation - Deposit':
                select_parts.append(
                    """
                    ROUND(
                        CAST(
                            PERCENTILE_CONT(0.5) WITHIN GROUP (
                                ORDER BY EXTRACT(EPOCH FROM (first_client_deposit_date::timestamp - date_joined::timestamp))::numeric / 86400
                            ) FILTER (WHERE first_client_deposit_date IS NOT NULL)
                        AS NUMERIC),
                        1
                    ) as "Median Time to Activation - Deposit"
                    """
                )
            elif metric == 'Median Time to Activation - Traded':
                select_parts.append(
                    """
                    ROUND(
                        CAST(
                            PERCENTILE_CONT(0.5) WITHIN GROUP (
                                ORDER BY EXTRACT(EPOCH FROM (first_client_trade_date::timestamp - date_joined::timestamp))::numeric / 86400
                            ) FILTER (WHERE first_client_trade_date IS NOT NULL)
                        AS NUMERIC),
                        1
                    ) as "Median Time to Activation - Traded"
                    """
                )
            elif metric == 'Median Time to Activation - Earning':
                select_parts.append(
                    """
                    ROUND(
                        CAST(
                            PERCENTILE_CONT(0.5) WITHIN GROUP (
                                ORDER BY EXTRACT(EPOCH FROM (first_earning_date::timestamp - date_joined::timestamp))::numeric / 86400
                            ) FILTER (WHERE first_client_joined_date IS NOT NULL)
                        AS NUMERIC),
                        1
                    ) as "Median Time to Activation - Earning"
                    """
                )

    # Ensure we have at least one column to select
    if not select_parts:
        select_parts.append("COUNT(*) as \"Total\"")

    # Build the final query
    query = f"""
    {base_query}
    SELECT {', '.join(select_parts)}
    FROM partner_plans
    """
    
    # Add where_clause if provided
    if where_clause and params:
        query += f" WHERE {where_clause}"
    
    # Add GROUP BY if we have grouping columns
    if group_by_cols:
        query += f"\nGROUP BY {', '.join(group_by_cols)}"
        # Add ORDER BY to ensure consistent ordering
        query += f"\nORDER BY {', '.join(group_by_cols)}"
    
    # Execute the query
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Debug print
            if params:
                print("Query parameters:", params)
                try:
                    mogrified = cursor.mogrify(query, params)
                    if isinstance(mogrified, bytes):
                        print("Final query:", mogrified.decode())
                    else:
                        print("Final query:", mogrified)
                except Exception as e:
                    print(f"Could not mogrify query: {e}")
                    print("Query template:", query)
            else:
                print("Final query:", query)
            
            cursor.execute(query, params if params else None)
            results = cursor.fetchall()
            df = pd.DataFrame(results)
            
            # Format the metrics
            if not df.empty:
                for col in df.columns:
                    if 'Rate' in col and df[col].dtype in [float, 'float64']:
                        df[col] = df[col].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else '-')
                    elif 'Time' in col and df[col].dtype in [float, 'float64']:
                        df[col] = df[col].apply(lambda x: f"{x:.1f} days" if pd.notnull(x) else '-')
                    elif '% Change' in col and df[col].dtype in [float, 'float64']:
                        df[col] = df[col].apply(lambda x: f"{x:+.2f}%" if pd.notnull(x) else '-')
                    elif df[col].dtype in [float, 'float64', int, 'int64']:
                        df[col] = df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else '-')
            
            return df
    finally:
        conn.close()

def get_filter_options():
    """Get all filter options from the database"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Regions
            cursor.execute("""
                SELECT DISTINCT partner_region 
                FROM partner.partner_info
                WHERE partner_region IS NOT NULL 
                AND is_internal = FALSE
                ORDER BY partner_region;
            """)
            regions = [row[0] for row in cursor.fetchall()]

            # Countries
            cursor.execute("""
                SELECT DISTINCT partner_country 
                FROM partner.partner_info
                WHERE partner_country IS NOT NULL 
                AND is_internal = FALSE
                ORDER BY partner_country;
            """)
            countries = [row[0] for row in cursor.fetchall()]

            # Platform Partners
            cursor.execute("""
                SELECT DISTINCT partner_platform 
                FROM partner.partner_info
                WHERE partner_platform IS NOT NULL 
                AND is_internal = FALSE
                ORDER BY partner_platform;
            """)
            platforms = [row[0] for row in cursor.fetchall()]

            # Affiliate Types
            cursor.execute("""
                SELECT DISTINCT aff_type 
                FROM partner.partner_info
                WHERE aff_type IS NOT NULL 
                AND is_internal = FALSE
                ORDER BY aff_type;
            """)
            aff_types = [row[0] for row in cursor.fetchall()]

            # Partner Levels
            cursor.execute("""
                SELECT DISTINCT partner_level 
                FROM partner.partner_info
                WHERE partner_level IS NOT NULL 
                AND is_internal = FALSE
                ORDER BY partner_level;
            """)
            partner_levels = [row[0] for row in cursor.fetchall()]

            # Onboarding Event Status
            cursor.execute("""
                SELECT DISTINCT 
                    CASE 
                        WHEN attended_onboarding_event = TRUE THEN 'Attended'
                        WHEN attended_onboarding_event = FALSE THEN 'Not Attended'
                        ELSE 'Unknown'
                    END as event_status
                FROM partner.partner_info
                WHERE attended_onboarding_event IS NOT NULL 
                AND is_internal = FALSE
                ORDER BY event_status;
            """)
            event_statuses = [row[0] for row in cursor.fetchall()]

            # Earning Acquisition Types
            cursor.execute("""
                SELECT DISTINCT earning_acquisition 
                FROM partner.partner_info
                WHERE earning_acquisition IS NOT NULL 
                AND is_internal = FALSE
                ORDER BY earning_acquisition;
            """)
            acquisition_types = [row[0] for row in cursor.fetchall()]

            return {
                'partner_regions': regions,
                'partner_countries': countries,
                'partner_platforms': platforms,
                'aff_types': aff_types,
                'partner_levels': partner_levels,
                'event_statuses': event_statuses,
                'acquisition_types': acquisition_types,
                'plan_types': ["Revenue Share", "Turnover", "CPA", "IB", "Master"]
            }
    finally:
        conn.close()

def create_filter_query(filters):
    """Create SQL WHERE clause from filters
    
    Args:
        filters (dict): Can be either:
            1. {filter_type: {values: [...], showAsColumn: bool}} (Metrics Test format)
            2. {filter_type: [...]} (Live Screener format)
    """
    if not filters:
        return "", []
    
    conditions = []
    params = []
    
    for filter_name, filter_data in filters.items():
        # Skip if no filter data
        if not filter_data:
            continue
            
        # Handle both filter formats
        values = filter_data.get('values', []) if isinstance(filter_data, dict) else filter_data
        
        # Handle date range filter for date_joined
        if filter_name == 'date_joined':
            if isinstance(filter_data, dict):
                start_date = filter_data.get('start_date')
                end_date = filter_data.get('end_date')
                
                if start_date and end_date:
                    # Convert YYYY-MM to YYYY-MM-DD format for first and last day
                    start_year, start_month = start_date.split('-')
                    end_year, end_month = end_date.split('-')
                    
                    # Calculate first day of start month and last day of end month
                    first_day = f"{start_year}-{start_month}-01"
                    
                    # For end date, use calendar to get last day of month
                    import calendar
                    last_day = f"{end_year}-{end_month}-{calendar.monthrange(int(end_year), int(end_month))[1]}"
                    
                    conditions.append("date_joined >= %s AND date_joined <= %s")
                    params.extend([first_day, last_day])
                elif start_date:
                    # Handle single month for start date
                    start_year, start_month = start_date.split('-')
                    first_day = f"{start_year}-{start_month}-01"
                    import calendar
                    last_day = f"{start_year}-{start_month}-{calendar.monthrange(int(start_year), int(start_month))[1]}"
                    
                    conditions.append("date_joined >= %s AND date_joined <= %s")
                    params.extend([first_day, last_day])
                elif end_date:
                    # Handle single month for end date
                    end_year, end_month = end_date.split('-')
                    first_day = f"{end_year}-{end_month}-01"
                    import calendar
                    last_day = f"{end_year}-{end_month}-{calendar.monthrange(int(end_year), int(end_month))[1]}"
                    
                    conditions.append("date_joined >= %s AND date_joined <= %s")
                    params.extend([first_day, last_day])
            continue
            
        # Skip if values is empty or only contains "All"
        if not values or values == ["All"]:
            continue
            
        col_map = {
            'partner_regions': 'partner_region',
            'partner_countries': 'partner_country',
            'partner_platforms': 'partner_platform',
            'aff_types': 'aff_type',
            'partner_levels': 'partner_level',
            'event_statuses': 'attended_onboarding_event',
            'acquisition_types': 'earning_acquisition',
            'plan_types': 'plan_type'
        }
        
        if filter_name in col_map:
            # Filter out None and "All" values
            valid_values = [v for v in values if v is not None and v != "All"]
            if valid_values:
                if filter_name == 'event_statuses':
                    # Handle event status string values
                    status_conditions = []
                    for value in valid_values:
                        if value == 'Attended':
                            status_conditions.append(f"{col_map[filter_name]} = %s")
                            params.append(True)
                        elif value == 'Not Attended':
                            status_conditions.append(f"{col_map[filter_name]} = %s")
                            params.append(False)
                    if status_conditions:
                        if len(status_conditions) > 1:
                            # If multiple status conditions, use OR
                            conditions.append(f"({' OR '.join(status_conditions)})")
                        else:
                            conditions.extend(status_conditions)
                else:
                    # Use a single IN clause with the correct number of placeholder
                    placeholders = ','.join(['%s'] * len(valid_values))
                    conditions.append(f"{col_map[filter_name]} IN ({placeholders})")
                    params.extend(valid_values)
    
    where_clause = " AND ".join(conditions) if conditions else ""
    return where_clause, params