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
    """Fetch metrics data based on available columns"""
    metrics = get_available_metrics()
    
    # Start with group by columns first in the correct order
    select_parts = []
    
    # Define the correct order for group_by columns
    group_by_order = ['partner_region', 'partner_country', 'aff_type', 'partner_platform', 'attended_onboarding_event', 'partner_level']
    
    # Add group by columns first in proper order (only those that are in the group_by list)
    if group_by:
        for col in group_by_order:
            if col in group_by:
                # Add proper aliases for display
                if col == 'partner_region':
                    select_parts.append(f"COALESCE({col}, 'Unknown') as \"Region\"")
                elif col == 'partner_country':
                    select_parts.append(f"COALESCE({col}, 'Unknown') as \"Country\"")
                elif col == 'aff_type':
                    select_parts.append(f"COALESCE({col}, 'Unknown') as \"Plan\"")
                elif col == 'partner_platform':
                    select_parts.append(f"COALESCE({col}, 'Unknown') as \"Platform\"")
                elif col == 'attended_onboarding_event':
                    select_parts.append(f"COALESCE({col}::text, 'Unknown') as \"Attended Event\"")
                elif col == 'partner_level':
                    select_parts.append(f"COALESCE({col}, 'Unknown') as \"Partner Level\"")
                else:
                    select_parts.append(f"COALESCE({col}, 'Unknown') as \"{col.replace('_', ' ').title()}\"")

    # Add selected metrics to select_parts (these come after grouping columns)
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
    SELECT {', '.join(select_parts)}
    FROM partner.partner_info
    WHERE is_internal = FALSE
    """
    
    # Only add where_clause if it's not empty and we have parameters
    if where_clause and params:
        query += f" AND {where_clause}"
    
    # Handle GROUP BY - use explicit group_by parameter if provided
    if group_by:
        query += f" GROUP BY {', '.join(group_by)}"
        # Add ORDER BY to ensure consistent ordering using the same order as group_by_order
        order_cols = [col for col in group_by_order if col in group_by]
        query += f" ORDER BY {', '.join(order_cols)}"
    else:
        # Only add GROUP BY if we have non-aggregated columns (fallback behavior)
        group_by_cols = [col for col in select_parts if ' as ' not in col.lower() and 'count(' not in col.lower()]
        if group_by_cols:
            query += f" GROUP BY {', '.join(group_by_cols)}"
            query += f" ORDER BY {', '.join(group_by_cols)}"
    
    # Execute the query
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Debug print with safe parameter handling
            if params:
                try:
                    print("Query parameters:", params)
                    print("Final query:", cursor.mogrify(query, params).decode())
                except Exception as e:
                    print("Query:", query)
                    print("Parameters:", params)
                    print("Mogrify error:", str(e))
            else:
                print("Final query:", query)
            
            # Execute the query with parameters
            cursor.execute(query, params if params else None)
            results = cursor.fetchall()
            df = pd.DataFrame(results)
            
            # CRITICAL FIX: Reorder DataFrame columns to ensure proper order
            if not df.empty and group_by:
                # Define the desired column order
                desired_order = []
                
                # Add grouping columns first in the correct order
                column_aliases = {
                    'partner_region': 'Region',
                    'partner_country': 'Country', 
                    'aff_type': 'Plan',
                    'partner_platform': 'Platform',
                    'attended_onboarding_event': 'Attended Event',
                    'partner_level': 'Partner Level'
                }
                
                for col in group_by_order:
                    if col in group_by and column_aliases.get(col) in df.columns:
                        desired_order.append(column_aliases[col])
                
                # Add all other columns (metrics) that are not grouping columns
                for col in df.columns:
                    if col not in desired_order:
                        desired_order.append(col)
                
                # Reorder the DataFrame columns
                df = df[desired_order]
            
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
    """Create SQL WHERE clause from filters"""
    if not filters:
        return "", []
    
    conditions = []
    params = []
    
    for key, values in filters.items():
        # Skip if values is empty or only contains "All"
        if not values or values == ["All"] or not isinstance(values, list):
            continue
            
        if key == 'plan_types':
            # Handle plan types differently as they're boolean columns
            plan_conditions = []
            plan_column_map = {
                'Revenue Share': 'is_revshare_plan',
                'Turnover': 'is_turnover_plan',
                'CPA': 'is_cpa_plan',
                'IB': 'is_ib_plan',
                'Master': 'is_master_plan'
            }
            for plan in values:
                if plan in plan_column_map:
                    plan_conditions.append(f"{plan_column_map[plan]} = TRUE")
            if plan_conditions:
                conditions.append(f"({' OR '.join(plan_conditions)})")
        else:
            col_map = {
                'partner_regions': 'partner_region',
                'partner_countries': 'partner_country',
                'partner_platforms': 'partner_platform',
                'aff_types': 'aff_type',
                'partner_levels': 'partner_level',
                'event_statuses': 'attended_onboarding_event',
                'acquisition_types': 'earning_acquisition'
            }
            if key in col_map and values:
                # Filter out None and "All" values
                valid_values = [v for v in values if v is not None and v != "All"]
                if valid_values:
                    if key == 'event_statuses':
                        # Handle event status string values
                        status_conditions = []
                        for value in valid_values:
                            if value == 'Attended':
                                status_conditions.append(f"{col_map[key]} = TRUE")
                            elif value == 'Not Attended':
                                status_conditions.append(f"{col_map[key]} = FALSE")
                        if status_conditions:
                            conditions.append(f"({' OR '.join(status_conditions)})")
                    else:
                        # Use a single IN clause with the correct number of placeholders
                        placeholders = ','.join(['%s'] * len(valid_values))
                        conditions.append(f"{col_map[key]} IN ({placeholders})")
                        params.extend(valid_values)
    
    where_clause = " AND ".join(conditions) if conditions else ""
    return where_clause, params
