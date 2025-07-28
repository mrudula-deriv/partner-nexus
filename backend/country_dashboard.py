import pandas as pd
from utils import get_db_connection, get_supabase_client
import logging
from datetime import datetime
import json
from typing import Dict, List, Any
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from config import settings

logger = logging.getLogger(__name__)

def generate_country_dashboard_insights(dashboard_data: Dict[str, Any]) -> Dict[str, Any]:
    """Generate AI insights for country dashboard based on current data"""
    try:
        # Check if OpenAI is configured
        if not settings.openai.api_key or not settings.openai.base_url or not settings.openai.model_name:
            return {
                'insights': None,
                'error': 'AI insights not configured'
            }
        
        # Initialize LLM
        from utils import get_openai_client
        llm = get_openai_client()
        
        # Extract key metrics from dashboard data
        overview = dashboard_data.get('overview', {})
        financial = overview.get('financial_totals', {})
        country_data = overview.get('country_data', [])
        funnel = dashboard_data.get('funnel', {})
        top_partners = dashboard_data.get('top_partners', {}).get('top_partners', [])
        inactive_partners = dashboard_data.get('inactive_partners', {})
        performance_contribution = dashboard_data.get('performance_contribution', {})
        
        # Calculate key metrics
        total_partners = sum(c.get('total_partners', 0) for c in country_data)
        total_activated = sum(c.get('activated_partners', 0) for c in country_data)
        avg_activation_rate = (total_activated / total_partners * 100) if total_partners > 0 else 0
        
        # Find best and worst performing countries
        #sorted_countries = sorted(country_data, key=lambda x: x.get('activation_rate', 0), reverse=True)
        #best_countries = sorted_countries[:3] if len(sorted_countries) >= 3 else sorted_countries
        #worst_countries = [c for c in sorted_countries if c.get('total_partners', 0) >= 10][-3:] if len(sorted_countries) >= 3 else []
        
        # Prepare data summary for AI
        data_summary = f"""
        COUNTRY DASHBOARD ANALYSIS:
        
        Date Range: {dashboard_data.get('date_range', 90)} days
        Report Type: {dashboard_data.get('report_type', 'Monthly')}
        Period: {dashboard_data.get('start_date', 'N/A')} to {dashboard_data.get('end_date', 'N/A')}
        Selected Country: {dashboard_data.get('partner_country', 'All')}
        
        OVERVIEW METRICS:
        - Total Partners: {total_partners:,}
        - Activated Partners: {total_activated:,}
        - Average Activation Rate: {avg_activation_rate:.1f}%
        - Total Deposits: ${financial.get('total_deposits', 0):,.0f}
        - Total Volume: ${financial.get('total_volume_usd', 0):,.0f}
        - Total Earnings: ${financial.get('total_earnings', 0):,.0f}
        - Active Partners (Period): {financial.get('total_active_partners_period', 0):,}
        
        
        FUNNEL METRICS:
        - Applications: {funnel.get('total_applications', 0):,}
        - With Client Signups: {funnel.get('signup_activations', 0):,}
        - Earning Partners: {funnel.get('earning_activations', 0):,}
        - Approval Rate: {funnel.get('approval_rate', 0):.1f}%
        - Overall Activation Rate: {funnel.get('activation_rate', 0):.1f}%
        
        TOP PARTNERS PERFORMANCE:
        - Number of Top Partners: {len(top_partners)}
        - Combined Client Signups: {sum(p.get('total_new_client_signups', 0) for p in top_partners[:20]):,}
        - Combined Deposits: ${sum(p.get('total_deposit', 0) for p in top_partners[:20]):,.0f}
        - Combined Earnings: ${sum(p.get('total_direct_earnings', 0) + p.get('total_sub_affiliate_earnings', 0) for p in top_partners[:20]):,.0f}
        
        PARTNER HEALTH:
        - Inactive Partners (30+ days): {inactive_partners.get('summary', {}).get('total_inactive_partners', 0):,}
        - Platinum Tier Inactive: {inactive_partners.get('summary', {}).get('platinum_count', 0)}
        - Gold Tier Inactive: {inactive_partners.get('summary', {}).get('gold_count', 0)}
        
        REGIONAL PERFORMANCE:
        {chr(10).join(f"- {r['region']}: {r['new_application']} new apps, {r['earnings_pct']:.1f}% of earnings" 
                      for r in performance_contribution.get('performance_data', [])[:5] 
                      if r.get('region') != 'Overall')}
        """
        
        # Generate insights using LLM
        messages = [
            SystemMessage(content="""You are a senior business analyst specializing in partner affiliate programs and country performance analysis.

Analyze the country dashboard data and provide insights in 3 categories:

1. **Highlights** (2-3 positive findings or achievements)
2. **Concerns** (2-3 areas needing attention or improvement)
3. **Recommendations** (2-3 specific, actionable next steps)

Consider whether this is a Daily or Monthly report view when providing insights.
For Daily reports, focus on short-term trends and immediate actions.
For Monthly reports, focus on longer-term patterns and strategic recommendations.

Format as JSON object:
{
    "highlights": [
        "Strong activation rate of 15.2% in Brazil with 523 new partners, indicating effective local engagement strategies",
        "Top 20 partners generated $2.3M in deposits with average client signup rate of 47 per partner"
    ],
    "concerns": [
        "India shows only 3.1% activation despite 892 applications, suggesting onboarding or market fit issues",
        "47 Platinum-tier partners are inactive for 30+ days, risking $235K in monthly commission revenue"
    ],
    "recommendations": [
        "Launch targeted reactivation campaign for inactive Gold/Platinum partners with personalized incentives",
        "Investigate India's low activation through partner surveys and implement localized onboarding improvements"
    ]
}

Focus on:
- Country-specific performance patterns
- Partner activation and retention trends
- Revenue and growth opportunities
- High-value partner engagement
- Regional market dynamics

Be specific with numbers and percentages. Keep each point concise and actionable."""),
            HumanMessage(content=f"Analyze this country dashboard data and provide strategic insights:\n\n{data_summary}")
        ]
        
        response = llm.invoke(messages)
        
        # Parse the JSON response
        try:
            insights = json.loads(response.content)
            # Ensure all required keys exist
            if not all(key in insights for key in ['highlights', 'concerns', 'recommendations']):
                raise ValueError("Missing required keys in response")
        except:
            # Fallback to text parsing if JSON fails
            logger.warning("Failed to parse AI insights as JSON, using fallback")
            insights = {
                'highlights': ["Data analysis in progress"],
                'concerns': ["Unable to generate detailed insights"],
                'recommendations': ["Please refresh to retry AI analysis"]
            }
        
        return {
            'insights': insights,
            'generated_at': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error generating country dashboard insights: {str(e)}")
        return {
            'insights': None,
            'error': str(e)
        }

def get_country_performance_overview(date_range=90):
    """Get overall country performance metrics with financial data"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # First get the country breakdown data
            cursor.execute("""
                SELECT 
                    COALESCE(partner_country, 'Unknown') as country,
                    COUNT(DISTINCT partner_id) as total_partners,
                    COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) as activated_partners,
                    ROUND(
                        CAST(COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) AS NUMERIC) /
                        NULLIF(CAST(COUNT(DISTINCT partner_id) AS NUMERIC), 0) * 100,
                        2
                    ) as activation_rate,
                    ROUND(
                        AVG(CASE 
                            WHEN first_client_joined_date IS NOT NULL THEN 
                                EXTRACT(EPOCH FROM (first_client_joined_date::timestamp - date_joined::timestamp))::numeric / 86400
                            ELSE NULL 
                        END),
                        1
                    ) as avg_days_to_activation,
                    COUNT(DISTINCT CASE 
                        WHEN date_joined >= CURRENT_DATE - INTERVAL %s THEN partner_id 
                        END) as recent_signups
                FROM partner.partner_info
                WHERE is_internal = FALSE
                GROUP BY partner_country
                HAVING COUNT(DISTINCT partner_id) >= 5
                ORDER BY total_partners DESC
                LIMIT 20;
            """, (f"{date_range} days",))
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            columns = ['country', 'total_partners', 'activated_partners', 'activation_rate', 'avg_days_to_activation', 'recent_signups']
            data = []
            for row in results:
                data.append(dict(zip(columns, row)))
            
            # Get aggregated financial metrics from partner_summary_monthly for the date range
            cursor.execute("""
                SELECT 
                    COALESCE(SUM(ps.total_deposits), 0) as total_deposits,
                    COALESCE(SUM(ps.volume_usd), 0) as total_volume_usd,
                    COALESCE(SUM(ps.expected_revenue), 0) as total_company_revenue,
                    COALESCE(SUM(ps.expected_revenue), 0) as total_expected_revenue,
                    COALESCE(SUM(ps.total_earnings), 0) as total_earnings,
                    COUNT(DISTINCT CASE WHEN (
                        ps.client_signups > 0 OR 
                        ps.traded_clients > 0 OR 
                        ps.total_deposits > 0
                    ) THEN ps.partner_id END) as total_active_partners_period
                FROM partner.partner_summary_monthly ps
                INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                WHERE 
                    ps.month >= CURRENT_DATE - INTERVAL %s
                    AND ps.month <= CURRENT_DATE
                    AND (pi.is_internal = FALSE OR pi.is_internal IS NULL);
            """, (f"{date_range} days",))
            
            financial_result = cursor.fetchone()
            financial_columns = ['total_deposits', 'total_volume_usd', 'total_company_revenue', 
                               'total_expected_revenue', 'total_earnings', 'total_active_partners_period']
            financial_data = dict(zip(financial_columns, financial_result)) if financial_result else {}
            
            # Calculate partner retention rate using the provided SQL logic (without date filter to get all data)
            cursor.execute("""
                WITH monthly_active_partners AS (
                    -- Define what makes a partner "active" in a given month
                    SELECT 
                        partner_id,
                        month,
                        CASE WHEN 
                            client_signups > 0 OR 
                            traded_clients > 0 OR 
                            total_deposits > 0 OR
                            total_earnings > 0
                        THEN TRUE ELSE FALSE END AS is_active
                    FROM partner.partner_summary_monthly
                ),
                retention_calculation AS (
                    -- Join current month with previous month to check retention
                    SELECT 
                        curr.month,
                        COUNT(DISTINCT prev.partner_id) AS active_previous_month,
                        COUNT(DISTINCT CASE WHEN curr.is_active = TRUE THEN curr.partner_id END) AS retained_current_month
                    FROM monthly_active_partners curr
                    JOIN monthly_active_partners prev 
                        ON curr.partner_id = prev.partner_id
                        AND curr.month = (prev.month + INTERVAL '1 month')
                    WHERE prev.is_active = TRUE
                    GROUP BY curr.month
                )
                SELECT 
                    CASE 
                        WHEN active_previous_month = 0 THEN 0
                        ELSE ROUND((retained_current_month::numeric / active_previous_month) * 100, 2)
                    END AS retention_rate_percent
                FROM retention_calculation
                ORDER BY month DESC
                LIMIT 1;
            """)
            
            retention_result = cursor.fetchone()
            partner_retention_rate = float(retention_result[0]) if retention_result and retention_result[0] else 0.0
            
            # Add retention rate to financial data
            financial_data['partner_retention_rate'] = partner_retention_rate
            
            # Combine country data with financial totals
            return {
                'country_data': data,
                'financial_totals': financial_data
            }
    finally:
        conn.close()

def get_country_growth_trends(date_range=180):
    """Get country growth trends over time"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                WITH monthly_signups AS (
                    SELECT 
                        COALESCE(partner_country, 'Unknown') as country,
                        DATE_TRUNC('month', date_joined) as signup_month,
                        COUNT(DISTINCT partner_id) as monthly_signups
                    FROM partner.partner_info
                    WHERE is_internal = FALSE
                    AND date_joined >= CURRENT_DATE - INTERVAL %s
                    GROUP BY partner_country, DATE_TRUNC('month', date_joined)
                ),
                country_trends AS (
                    SELECT 
                        country,
                        signup_month,
                        monthly_signups,
                        LAG(monthly_signups, 1) OVER (PARTITION BY country ORDER BY signup_month) as prev_month_signups
                    FROM monthly_signups
                )
                SELECT 
                    country,
                    TO_CHAR(signup_month, 'Mon YYYY') as month_label,
                    monthly_signups,
                    CASE 
                        WHEN prev_month_signups > 0 THEN 
                            ROUND(((monthly_signups - prev_month_signups)::numeric / prev_month_signups * 100), 2)
                        ELSE NULL
                    END as growth_rate
                FROM country_trends
                WHERE country IN (
                    SELECT country 
                    FROM (
                        SELECT country, SUM(monthly_signups) as total
                        FROM monthly_signups 
                        GROUP BY country 
                        ORDER BY total DESC 
                        LIMIT 10
                    ) top_countries
                )
                ORDER BY country, signup_month;
            """, (f"{date_range} days",))
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            columns = ['country', 'month_label', 'monthly_signups', 'growth_rate']
            data = []
            for row in results:
                data.append(dict(zip(columns, row)))
            
            return data
    finally:
        conn.close()

def get_country_detailed_metrics(country_name, date_range=90):
    """Get detailed metrics for a specific country"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    partner_id,
                    COALESCE(aff_type, 'Unknown') as plan_type,
                    COALESCE(partner_platform, 'Unknown') as platform,
                    date_joined,
                    first_client_joined_date,
                    first_client_deposit_date,
                    first_client_trade_date,
                    first_earning_date,
                    CASE 
                        WHEN first_client_joined_date IS NOT NULL THEN 
                            EXTRACT(EPOCH FROM (first_client_joined_date::timestamp - date_joined::timestamp))::numeric / 86400
                        ELSE NULL 
                    END as days_to_activation,
                    attended_onboarding_event
                FROM partner.partner_info
                WHERE is_internal = FALSE
                AND COALESCE(partner_country, 'Unknown') = %s
                AND date_joined >= CURRENT_DATE - INTERVAL %s
                ORDER BY date_joined DESC
                LIMIT 100;
            """, (country_name, f"{date_range} days"))
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            columns = ['partner_id', 'plan_type', 'platform', 'date_joined', 'first_client_joined_date', 
                      'first_client_deposit_date', 'first_client_trade_date', 'first_earning_date', 
                      'days_to_activation', 'attended_onboarding_event']
            data = []
            for row in results:
                row_dict = dict(zip(columns, row))
                # Format dates
                for date_col in ['date_joined', 'first_client_joined_date', 'first_client_deposit_date', 
                               'first_client_trade_date', 'first_earning_date']:
                    if row_dict[date_col]:
                        row_dict[date_col] = row_dict[date_col].strftime('%Y-%m-%d') if hasattr(row_dict[date_col], 'strftime') else str(row_dict[date_col])
                    else:
                        row_dict[date_col] = None
                
                # Format days to activation
                if row_dict['days_to_activation'] is not None:
                    row_dict['days_to_activation'] = f"{row_dict['days_to_activation']:.1f}"
                
                data.append(row_dict)
            
            return data
    finally:
        conn.close()

def get_country_comparison_data(countries, date_range=90):
    """Compare metrics across multiple countries"""
    if not countries:
        return []
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(countries))
            query = f"""
                SELECT 
                    COALESCE(partner_country, 'Unknown') as country,
                    COUNT(DISTINCT partner_id) as total_partners,
                    COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) as signup_activations,
                    COUNT(DISTINCT CASE WHEN first_client_deposit_date IS NOT NULL THEN partner_id END) as deposit_activations,
                    COUNT(DISTINCT CASE WHEN first_client_trade_date IS NOT NULL THEN partner_id END) as trade_activations,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as earning_activations,
                    ROUND(
                        AVG(CASE 
                            WHEN first_client_joined_date IS NOT NULL THEN 
                                EXTRACT(EPOCH FROM (first_client_joined_date::timestamp - date_joined::timestamp))::numeric / 86400
                            ELSE NULL 
                        END),
                        1
                    ) as avg_days_to_signup,
                    COUNT(DISTINCT CASE WHEN attended_onboarding_event = TRUE THEN partner_id END) as attended_events,
                    ROUND(
                        CAST(COUNT(DISTINCT CASE WHEN attended_onboarding_event = TRUE THEN partner_id END) AS NUMERIC) /
                        NULLIF(CAST(COUNT(DISTINCT partner_id) AS NUMERIC), 0) * 100,
                        2
                    ) as event_attendance_rate
                FROM partner.partner_info
                WHERE is_internal = FALSE
                AND COALESCE(partner_country, 'Unknown') IN ({placeholders})
                AND date_joined >= CURRENT_DATE - INTERVAL %s
                GROUP BY partner_country
                ORDER BY total_partners DESC;
            """
            
            params = countries + [f"{date_range} days"]
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            columns = ['country', 'total_partners', 'signup_activations', 'deposit_activations', 
                      'trade_activations', 'earning_activations', 'avg_days_to_signup', 
                      'attended_events', 'event_attendance_rate']
            data = []
            for row in results:
                data.append(dict(zip(columns, row)))
            
            return data
    finally:
        conn.close()

def get_available_countries():
    """Get list of available countries with partner data"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COALESCE(partner_country, 'Unknown') as country,
                    COUNT(DISTINCT partner_id) as partner_count
                FROM partner.partner_info
                WHERE is_internal = FALSE
                GROUP BY partner_country
                HAVING COUNT(DISTINCT partner_id) >= 3
                ORDER BY partner_count DESC;
            """)
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = []
            for row in results:
                data.append({
                    'country': row[0],
                    'partner_count': row[1]
                })
            
            return data
    finally:
        conn.close() 

def get_partner_application_chart_data(date_range=90, period_type='monthly', start_date=None, end_date=None, partner_country=None):
    """Get partner application chart data showing applications attributed to events"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Get partner application data attributed to events
            if period_type.lower() == 'daily':
                date_format = 'YYYY-MM-DD'
                date_trunc = 'day'
                interval_step = '1 day'
            else:  # monthly
                date_format = 'YYYY-MM'
                date_trunc = 'month'
                interval_step = '1 month'
            
            # Determine date range - use start_date/end_date if provided, otherwise use date_range
            if start_date and end_date:
                start_filter = start_date
                end_filter = end_date
            else:
                start_filter = f"CURRENT_DATE - INTERVAL '{date_range} days'"
                end_filter = "CURRENT_DATE"
            
            # Query for partner applications attributed to events
            query_params = []
            if start_date and end_date:
                # Build country filter for event attribution (exact match when specific country selected)
                country_filter = ""
                events_country_params = []
                if partner_country and partner_country != 'All':
                    country_filter = "AND e.event_country = %s"
                    events_country_params.append(partner_country)
                
                if period_type.lower() == 'daily':
                    # For daily view, only show dates where events actually occurred
                    applications_query = f"""
                        SELECT 
                            DATE_TRUNC('{date_trunc}', p.date_joined)::date AS period_date,
                            TO_CHAR(DATE_TRUNC('{date_trunc}', p.date_joined)::date, '{date_format}') as period_label,
                            COUNT(DISTINCT p.partner_id) as application_count,
                            COUNT(DISTINCT CASE WHEN p.partner_country IS NOT NULL THEN p.partner_id END) as applications_with_country
                        FROM gp.event e
                        INNER JOIN partner.partner_info p ON p.partner_country = e.event_country
                        WHERE 
                            e.start_date >= %s::date
                            AND e.start_date <= %s::date
                            AND e.start_date IS NOT NULL
                            AND e.end_date IS NOT NULL
                            AND p.date_joined >= e.start_date
                            AND p.date_joined <= e.end_date
                            AND (p.is_internal = FALSE OR p.is_internal IS NULL)
                            {country_filter}
                        GROUP BY DATE_TRUNC('{date_trunc}', p.date_joined)::date
                        ORDER BY DATE_TRUNC('{date_trunc}', p.date_joined)::date;
                    """
                else:
                    # For monthly view, show complete date series (including zero months)
                    applications_query = f"""
                        WITH date_series AS (
                            SELECT generate_series(
                                DATE_TRUNC('{date_trunc}', %s::date),
                                DATE_TRUNC('{date_trunc}', %s::date),
                                INTERVAL '{interval_step}'
                            )::date AS period_date
                        ),
                        event_attributed_applications AS (
                            SELECT 
                                DATE_TRUNC('{date_trunc}', p.date_joined)::date AS period_date,
                                COUNT(DISTINCT p.partner_id) as application_count,
                                COUNT(DISTINCT CASE WHEN p.partner_country IS NOT NULL THEN p.partner_id END) as applications_with_country
                            FROM gp.event e
                            INNER JOIN partner.partner_info p ON p.partner_country = e.event_country
                            WHERE 
                                e.start_date >= %s::date
                                AND e.start_date <= %s::date
                                AND e.start_date IS NOT NULL
                                AND e.end_date IS NOT NULL
                                AND p.date_joined >= e.start_date
                                AND p.date_joined <= e.end_date
                                AND (p.is_internal = FALSE OR p.is_internal IS NULL)
                                {country_filter}
                            GROUP BY DATE_TRUNC('{date_trunc}', p.date_joined)::date
                        )
                        SELECT 
                            ds.period_date,
                            TO_CHAR(ds.period_date, '{date_format}') as period_label,
                            COALESCE(eaa.application_count, 0) as application_count,
                            COALESCE(eaa.applications_with_country, 0) as applications_with_country
                        FROM date_series ds
                        LEFT JOIN event_attributed_applications eaa ON ds.period_date = eaa.period_date
                        ORDER BY ds.period_date;
                    """
                if period_type.lower() == 'daily':
                    query_params = [start_date, end_date] + events_country_params
                else:
                    query_params = [start_date, end_date, start_date, end_date] + events_country_params
            else:
                # Build country filter for event attribution (relative date version)
                country_filter = ""
                events_country_params = []
                if partner_country and partner_country != 'All':
                    country_filter = "AND e.event_country = %s"
                    events_country_params.append(partner_country)
                
                if period_type.lower() == 'daily':
                    # For daily view, only show dates where events actually occurred
                    applications_query = f"""
                        SELECT 
                            DATE_TRUNC('{date_trunc}', p.date_joined)::date AS period_date,
                            TO_CHAR(DATE_TRUNC('{date_trunc}', p.date_joined)::date, '{date_format}') as period_label,
                            COUNT(DISTINCT p.partner_id) as application_count,
                            COUNT(DISTINCT CASE WHEN p.partner_country IS NOT NULL THEN p.partner_id END) as applications_with_country
                        FROM gp.event e
                        INNER JOIN partner.partner_info p ON p.partner_country = e.event_country
                        WHERE 
                            e.start_date >= CURRENT_DATE - INTERVAL '{date_range} days'
                            AND e.start_date <= CURRENT_DATE
                            AND e.start_date IS NOT NULL
                            AND e.end_date IS NOT NULL
                            AND p.date_joined >= e.start_date
                            AND p.date_joined <= e.end_date
                            AND (p.is_internal = FALSE OR p.is_internal IS NULL)
                            {country_filter}
                        GROUP BY DATE_TRUNC('{date_trunc}', p.date_joined)::date
                        ORDER BY DATE_TRUNC('{date_trunc}', p.date_joined)::date;
                    """
                else:
                    # For monthly view, show complete date series (including zero months)
                    applications_query = f"""
                        WITH date_series AS (
                            SELECT generate_series(
                                DATE_TRUNC('{date_trunc}', CURRENT_DATE - INTERVAL '{date_range} days'),
                                DATE_TRUNC('{date_trunc}', CURRENT_DATE),
                                INTERVAL '{interval_step}'
                            )::date AS period_date
                        ),
                        event_attributed_applications AS (
                            SELECT 
                                DATE_TRUNC('{date_trunc}', p.date_joined)::date AS period_date,
                                COUNT(DISTINCT p.partner_id) as application_count,
                                COUNT(DISTINCT CASE WHEN p.partner_country IS NOT NULL THEN p.partner_id END) as applications_with_country
                            FROM gp.event e
                            INNER JOIN partner.partner_info p ON p.partner_country = e.event_country
                            WHERE 
                                e.start_date >= CURRENT_DATE - INTERVAL '{date_range} days'
                                AND e.start_date <= CURRENT_DATE
                                AND e.start_date IS NOT NULL
                                AND e.end_date IS NOT NULL
                                AND p.date_joined >= e.start_date
                                AND p.date_joined <= e.end_date
                                AND (p.is_internal = FALSE OR p.is_internal IS NULL)
                                {country_filter}
                            GROUP BY DATE_TRUNC('{date_trunc}', p.date_joined)::date
                        )
                        SELECT 
                            ds.period_date,
                            TO_CHAR(ds.period_date, '{date_format}') as period_label,
                            COALESCE(eaa.application_count, 0) as application_count,
                            COALESCE(eaa.applications_with_country, 0) as applications_with_country
                        FROM date_series ds
                        LEFT JOIN event_attributed_applications eaa ON ds.period_date = eaa.period_date
                        ORDER BY ds.period_date;
                    """
                query_params = events_country_params
                
            cursor.execute(applications_query, query_params)
            
            applications_data = cursor.fetchall()
            applications_columns = ['period_date', 'period_label', 'application_count', 'applications_with_country']
            applications_list = []
            for row in applications_data:
                applications_list.append(dict(zip(applications_columns, row)))
            
            # Query for events in the same time period
            # Note: Handling potential permission issues with gp schema
            try:
                if start_date and end_date:
                    # Build regional filter for events (more relevant than strict country filter)
                    region_filter = ""
                    events_params = [start_date, end_date]
                    if partner_country and partner_country != 'All':
                        region_filter = """AND COALESCE(event_country, '') IN (
                            SELECT DISTINCT partner_country 
                            FROM partner.partner_info 
                            WHERE partner_region = (
                                SELECT partner_region 
                                FROM partner.partner_info 
                                WHERE partner_country = %s 
                                LIMIT 1
                            )
                            AND partner_region IS NOT NULL
                        )"""
                        events_params.append(partner_country)
                    
                    events_query = f"""
                        SELECT 
                            TO_CHAR(start_date, 'YYYY-MM') as event_month,
                            event_type,
                            STRING_AGG(
                                DISTINCT COALESCE(gp_region, 'Other'), 
                                ', ' ORDER BY COALESCE(gp_region, 'Other')
                            ) as regions_list,
                            MIN(start_date) as first_event_date
                        FROM gp.event 
                        WHERE 
                            start_date >= %s::date
                            AND start_date <= %s::date
                            AND start_date IS NOT NULL
                            AND event_type IS NOT NULL
                            {region_filter}
                        GROUP BY TO_CHAR(start_date, 'YYYY-MM'), event_type
                        ORDER BY first_event_date, event_type;
                    """
                    cursor.execute(events_query, events_params)
                else:
                    # Build regional filter for events (relative date version)
                    region_filter = ""
                    events_params = [f"{date_range} days"]
                    if partner_country and partner_country != 'All':
                        region_filter = """AND COALESCE(event_country, '') IN (
                            SELECT DISTINCT partner_country 
                            FROM partner.partner_info 
                            WHERE partner_region = (
                                SELECT partner_region 
                                FROM partner.partner_info 
                                WHERE partner_country = %s 
                                LIMIT 1
                            )
                            AND partner_region IS NOT NULL
                        )"""
                        events_params.append(partner_country)
                    
                    events_query = f"""
                        SELECT 
                            TO_CHAR(start_date, 'YYYY-MM') as event_month,
                            event_type,
                            STRING_AGG(
                                DISTINCT COALESCE(gp_region, 'Other'), 
                                ', ' ORDER BY COALESCE(gp_region, 'Other')
                            ) as regions_list,
                            MIN(start_date) as first_event_date
                        FROM gp.event 
                        WHERE 
                            start_date >= CURRENT_DATE - INTERVAL %s
                            AND start_date <= CURRENT_DATE
                            AND start_date IS NOT NULL
                            AND event_type IS NOT NULL
                            {region_filter}
                        GROUP BY TO_CHAR(start_date, 'YYYY-MM'), event_type
                        ORDER BY first_event_date, event_type;
                    """
                    cursor.execute(events_query, events_params)
                events_data = cursor.fetchall()
            except Exception as e:
                logger.warning(f"Could not access gp.event table: {str(e)}")
                events_data = []  # Return empty events if no access
            
            events_columns = ['event_month', 'event_type', 'regions_list', 'first_event_date']
            events_list = []
            for row in events_data:
                event_dict = dict(zip(events_columns, row))
                # Convert date to string for JSON serialization and remove first_event_date
                if event_dict['first_event_date']:
                    event_dict['first_event_date'] = event_dict['first_event_date'].strftime('%Y-%m-%d')
                # Remove first_event_date from final output as it's only used for sorting
                event_dict.pop('first_event_date', None)
                events_list.append(event_dict)
            
            # Get summary statistics for event-attributed applications
            try:
                if start_date and end_date:
                    # Build country filter for summary (exact match)
                    summary_country_filter = ""
                    summary_params = [end_date, end_date, start_date, end_date]
                    if partner_country and partner_country != 'All':
                        summary_country_filter = "AND e.event_country = %s"
                        summary_params.append(partner_country)
                    
                    summary_query = f"""
                        SELECT 
                            COUNT(DISTINCT p.partner_id) as total_applications,
                            COUNT(DISTINCT p.partner_country) as unique_countries,
                            COUNT(DISTINCT CASE WHEN p.date_joined >= %s::date - INTERVAL '30 days' THEN p.partner_id END) as recent_30_days,
                            COUNT(DISTINCT CASE WHEN p.date_joined >= %s::date - INTERVAL '7 days' THEN p.partner_id END) as recent_7_days
                        FROM gp.event e
                        INNER JOIN partner.partner_info p ON p.partner_country = e.event_country
                        WHERE 
                            e.start_date >= %s::date
                            AND e.start_date <= %s::date
                            AND e.start_date IS NOT NULL
                            AND e.end_date IS NOT NULL
                            AND p.date_joined >= e.start_date
                            AND p.date_joined <= e.end_date
                            AND (p.is_internal = FALSE OR p.is_internal IS NULL)
                            {summary_country_filter};
                    """
                    cursor.execute(summary_query, summary_params)
                else:
                    # Build country filter for summary (relative date version)
                    summary_country_filter = ""
                    summary_params = [f"{date_range} days"]
                    if partner_country and partner_country != 'All':
                        summary_country_filter = "AND e.event_country = %s"
                        summary_params.append(partner_country)
                    
                    summary_query = f"""
                        SELECT 
                            COUNT(DISTINCT p.partner_id) as total_applications,
                            COUNT(DISTINCT p.partner_country) as unique_countries,
                            COUNT(DISTINCT CASE WHEN p.date_joined >= CURRENT_DATE - INTERVAL '30 days' THEN p.partner_id END) as recent_30_days,
                            COUNT(DISTINCT CASE WHEN p.date_joined >= CURRENT_DATE - INTERVAL '7 days' THEN p.partner_id END) as recent_7_days
                        FROM gp.event e
                        INNER JOIN partner.partner_info p ON p.partner_country = e.event_country
                        WHERE 
                            e.start_date >= CURRENT_DATE - INTERVAL %s
                            AND e.start_date <= CURRENT_DATE
                            AND e.start_date IS NOT NULL
                            AND e.end_date IS NOT NULL
                            AND p.date_joined >= e.start_date
                            AND p.date_joined <= e.end_date
                            AND (p.is_internal = FALSE OR p.is_internal IS NULL)
                            {summary_country_filter};
                    """
                    cursor.execute(summary_query, summary_params)
            except Exception as e:
                logger.warning(f"Could not calculate event-attributed summary statistics: {str(e)}")
                # Fallback to zero counts
                cursor.execute("SELECT 0 as total_applications, 0 as unique_countries, 0 as recent_30_days, 0 as recent_7_days")
            
            
            summary_data = cursor.fetchone()
            summary_columns = ['total_applications', 'unique_countries', 'recent_30_days', 'recent_7_days']
            summary_dict = dict(zip(summary_columns, summary_data))
            
            return {
                'applications': applications_list,
                'events': events_list,
                'summary': summary_dict,
                'period_type': period_type,
                'date_range': date_range
            }
            
    finally:
        conn.close() 

def get_partner_application_chart_data_supabase(date_range=90, period_type='monthly'):
    """Get partner application chart data using Supabase client"""
    supabase = get_supabase_client()
    
    try:
        # Call the stored function in Supabase
        response = supabase.rpc('get_partner_application_chart', {
            'p_date_range': date_range,
            'p_period_type': period_type.lower()
        }).execute()
        
        if response.data:
            return response.data[0]  # The function returns a single JSON object
        else:
            logger.warning("No data returned from Supabase RPC")
            return {
                'applications': [],
                'events': [],
                'summary': {'total_applications': 0, 'unique_countries': 0, 'recent_30_days': 0, 'recent_7_days': 0},
                'period_type': period_type,
                'date_range': date_range
            }
            
    except Exception as e:
        logger.error(f"Error calling Supabase RPC: {str(e)}")
        raise

"""
CREATE OR REPLACE FUNCTION get_partner_application_chart(
    p_date_range INTEGER DEFAULT 90,
    p_period_type TEXT DEFAULT 'monthly'
)
RETURNS JSON AS $$
DECLARE
    date_format TEXT;
    date_trunc_val TEXT;
    interval_step TEXT;
    applications_data JSON;
    events_data JSON;
    summary_data JSON;
    result JSON;
BEGIN
    -- Set date formatting based on period type
    IF p_period_type = 'daily' THEN
        date_format := 'YYYY-MM-DD';
        date_trunc_val := 'day';
        interval_step := '1 day';
    ELSE -- monthly
        date_format := 'YYYY-MM';
        date_trunc_val := 'month';
        interval_step := '1 month';
    END IF;

    -- Get applications data
    EXECUTE FORMAT('
        WITH date_series AS (
            SELECT generate_series(
                DATE_TRUNC(%L, CURRENT_DATE - INTERVAL %L),
                DATE_TRUNC(%L, CURRENT_DATE),
                INTERVAL %L
            )::date AS period_date
        ),
        partner_applications AS (
            SELECT 
                DATE_TRUNC(%L, date_joined)::date AS period_date,
                COUNT(*) as application_count,
                COUNT(CASE WHEN partner_country IS NOT NULL THEN 1 END) as applications_with_country
            FROM partner.partner_info 
            WHERE 
                date_joined >= CURRENT_DATE - INTERVAL %L
                AND date_joined <= CURRENT_DATE
                AND (is_internal = FALSE OR is_internal IS NULL)
            GROUP BY DATE_TRUNC(%L, date_joined)::date
        )
        SELECT json_agg(
            json_build_object(
                ''period_date'', ds.period_date,
                ''period_label'', TO_CHAR(ds.period_date, %L),
                ''application_count'', COALESCE(pa.application_count, 0),
                ''applications_with_country'', COALESCE(pa.applications_with_country, 0)
            ) ORDER BY ds.period_date
        )
        FROM date_series ds
        LEFT JOIN partner_applications pa ON ds.period_date = pa.period_date',
        date_trunc_val, p_date_range || ' days', date_trunc_val, interval_step,
        date_trunc_val, p_date_range || ' days', date_trunc_val, date_format
    ) INTO applications_data;

    -- Get events data (handle permissions gracefully)
    BEGIN
        SELECT json_agg(
            json_build_object(
                'event_code', event_code,
                'event_type', event_type,
                'event_country', event_country,
                'event_city', event_city,
                'start_date', start_date::text,
                'end_date', end_date::text,
                'gp_region', gp_region,
                'estimated_budget_usd', estimated_budget_usd,
                'actual_spend_usd', actual_spend_usd
            ) ORDER BY start_date
        )
        FROM gp.event 
        WHERE 
            start_date >= CURRENT_DATE - INTERVAL (p_date_range || ' days')::interval
            AND start_date <= CURRENT_DATE
            AND start_date IS NOT NULL
        INTO events_data;
    EXCEPTION WHEN insufficient_privilege THEN
        events_data := '[]'::json;
    END;

    -- Get summary data
    SELECT json_build_object(
        'total_applications', COUNT(*),
        'unique_countries', COUNT(DISTINCT partner_country),
        'recent_30_days', COUNT(CASE WHEN date_joined >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END),
        'recent_7_days', COUNT(CASE WHEN date_joined >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END)
    )
    FROM partner.partner_info 
    WHERE 
        date_joined >= CURRENT_DATE - INTERVAL (p_date_range || ' days')::interval
        AND date_joined <= CURRENT_DATE
        AND (is_internal = FALSE OR is_internal IS NULL)
    INTO summary_data;

    -- Build final result
    result := json_build_object(
        'applications', COALESCE(applications_data, '[]'::json),
        'events', COALESCE(events_data, '[]'::json),
        'summary', summary_data,
        'period_type', p_period_type,
        'date_range', p_date_range
    );

    RETURN result;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
""" 

def get_partner_application_chart_data_client_only(date_range=90, period_type='monthly'):
    """Get partner application chart data using only Supabase client table queries"""
    supabase = get_supabase_client()
    
    try:
        from datetime import datetime, timedelta
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=date_range)
        
        # 1. Get partner applications data
        try:
            # Try using schema method first
            partners_response = supabase.schema('partner').from_('partner_info').select(
                'partner_id, date_joined, partner_country, is_internal'
            ).gte(
                'date_joined', start_date.strftime('%Y-%m-%d')
            ).lte(
                'date_joined', end_date.strftime('%Y-%m-%d')
            ).is_('is_internal', 'false').execute()
        except Exception as schema_error:
            # Fallback to RPC method
            logger.warning(f"Schema method failed: {schema_error}, trying RPC...")
            partners_response = supabase.rpc('get_partners_in_range', {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }).execute()
            # Restructure RPC response to match table response format
            partners_response = type('obj', (object,), {'data': partners_response.data or []})()
        
        # Process applications data locally
        applications_data = []
        partners_data = partners_response.data if partners_response.data else []
        
        # Group by period (daily or monthly)
        from collections import defaultdict
        period_counts = defaultdict(int)
        
        for partner in partners_data:
            if partner.get('date_joined'):
                try:
                    date_joined = datetime.strptime(partner['date_joined'], '%Y-%m-%d')
                    
                    if period_type.lower() == 'daily':
                        period_key = date_joined.strftime('%Y-%m-%d')
                    else:  # monthly
                        period_key = date_joined.strftime('%Y-%m')
                    
                    period_counts[period_key] += 1
                except ValueError:
                    continue
        
        # Create complete time series
        current_date = start_date
        while current_date <= end_date:
            if period_type.lower() == 'daily':
                period_key = current_date.strftime('%Y-%m-%d')
                period_label = current_date.strftime('%Y-%m-%d')
                current_date += timedelta(days=1)
            else:  # monthly
                period_key = current_date.strftime('%Y-%m')
                period_label = current_date.strftime('%Y-%m')
                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            
            applications_data.append({
                'period_date': period_key,
                'period_label': period_label,
                'application_count': period_counts.get(period_key, 0),
                'applications_with_country': period_counts.get(period_key, 0)  # Simplified
            })
        
        # 2. Get events data (with error handling)
        events_data = []
        try:
            # Try schema method for events
            try:
                events_response = supabase.schema('gp').from_('event').select(
                    'event_code, event_type, event_country, event_city, start_date, end_date, gp_region, estimated_budget_usd, actual_spend_usd'
                ).gte(
                    'start_date', start_date.strftime('%Y-%m-%d')
                ).lte(
                    'start_date', end_date.strftime('%Y-%m-%d')
                ).not_.is_('start_date', 'null').order('start_date').execute()
                
                events_data = events_response.data if events_response.data else []
                
            except Exception as schema_error:
                # Try RPC fallback for events
                logger.warning(f"Events schema access failed: {schema_error}, trying RPC...")
                events_response = supabase.rpc('get_events_in_range', {
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d')
                }).execute()
                events_data = events_response.data if events_response.data else []
            
        except Exception as e:
            logger.warning(f"Could not access events table: {str(e)}")
            events_data = []
        
        # 3. Calculate summary statistics
        total_applications = len(partners_data)
        unique_countries = len(set(p.get('partner_country') for p in partners_data if p.get('partner_country')))
        
        # Recent activity counts
        recent_30_days = len([p for p in partners_data if p.get('date_joined') and 
                             datetime.strptime(p['date_joined'], '%Y-%m-%d') >= (end_date - timedelta(days=30))])
        recent_7_days = len([p for p in partners_data if p.get('date_joined') and 
                            datetime.strptime(p['date_joined'], '%Y-%m-%d') >= (end_date - timedelta(days=7))])
        
        summary_data = {
            'total_applications': total_applications,
            'unique_countries': unique_countries,
            'recent_30_days': recent_30_days,
            'recent_7_days': recent_7_days
        }
        
        return {
            'applications': applications_data,
            'events': events_data,
            'summary': summary_data,
            'period_type': period_type,
            'date_range': date_range
        }
        
    except Exception as e:
        logger.error(f"Error in Supabase client function: {str(e)}")
        raise 

# Simple Example: Converting your notebook query
def get_partner_platforms_supabase():
    """Simple example: Get distinct partner platforms using Supabase client"""
    supabase = get_supabase_client()
    
    # Original SQL: SELECT DISTINCT partner_platform FROM partner.partner_info
    response = supabase.table('partner_info').select('partner_platform').execute()
    
    # Extract unique platforms
    platforms = list(set(row['partner_platform'] for row in response.data if row['partner_platform']))
    
    return platforms

# Alternative with Supabase distinct (if supported)
def get_partner_platforms_supabase_v2():
    """Alternative approach using Supabase distinct functionality"""
    supabase = get_supabase_client()
    
    # Using Supabase distinct query
    try:
        response = supabase.rpc('distinct_partner_platforms').execute()
        return response.data
    except:
        # Fallback to manual distinct
        response = supabase.table('partner_info').select('partner_platform').execute()
        platforms = list(set(row['partner_platform'] for row in response.data if row['partner_platform']))
        return [{'partner_platform': p} for p in platforms] 

"""
-- SQL Functions for RPC Fallback (Create these in Supabase if schema method fails)

-- Function to get partners in date range
CREATE OR REPLACE FUNCTION get_partners_in_range(
    start_date DATE,
    end_date DATE
)
RETURNS TABLE(
    partner_id TEXT,
    date_joined DATE,
    partner_country TEXT,
    is_internal BOOLEAN
) AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        pi.partner_id,
        pi.date_joined,
        pi.partner_country,
        pi.is_internal
    FROM partner.partner_info pi
    WHERE 
        pi.date_joined >= start_date
        AND pi.date_joined <= end_date
        AND (pi.is_internal = FALSE OR pi.is_internal IS NULL);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to get events in date range
CREATE OR REPLACE FUNCTION get_events_in_range(
    start_date DATE,
    end_date DATE
)
RETURNS TABLE(
    event_code TEXT,
    event_type TEXT,
    event_country TEXT,
    event_city TEXT,
    start_date DATE,
    end_date DATE,
    gp_region TEXT,
    estimated_budget_usd REAL,
    actual_spend_usd REAL
) AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        e.event_code,
        e.event_type,
        e.event_country,
        e.event_city,
        e.start_date,
        e.end_date,
        e.gp_region,
        e.estimated_budget_usd,
        e.actual_spend_usd
    FROM gp.event e
    WHERE 
        e.start_date >= start_date
        AND e.start_date <= end_date
        AND e.start_date IS NOT NULL
    ORDER BY e.start_date;
EXCEPTION WHEN insufficient_privilege THEN
    RETURN; -- Return empty if no permissions
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
""" 

def get_partner_funnel_data(date_range=90, partner_country=None):
    """Get basic partner funnel conversion data (signups -> approved -> active)
    
    Args:
        date_range: Number of days to look back for data
        partner_country: Optional country filter ('All' or specific country name)
    """
    from psycopg2.extras import RealDictCursor
    
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET statement_timeout = '30s'")
            
            # Build date filter clause
            if date_range > 0:
                date_filter = f"AND p.date_joined >= CURRENT_DATE - INTERVAL '{date_range} days'"
            else:
                date_filter = ""  # All time
                
            # Build country filter clause
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND p.partner_country = %(country)s"
            
            # Main funnel query - basic funnel only
            query = f"""
            SELECT 
                COUNT(DISTINCT partner_id) as total_applications,
                COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) as signup_activations,
                COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as earning_activations,
                ROUND(CAST(COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) AS NUMERIC) / NULLIF(COUNT(DISTINCT partner_id), 0) * 100, 1) as approval_rate,
                ROUND(CAST(COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) AS NUMERIC) / NULLIF(COUNT(DISTINCT partner_id), 0) * 100, 1) as activation_rate
            FROM partner.partner_info p
            WHERE p.is_internal = FALSE
                {date_filter}
                {country_filter};
            """
            
            params = {'country': partner_country} if (partner_country and partner_country != 'All') else {}
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result:
                return {
                    'total_applications': result['total_applications'],
                    'signup_activations': result['signup_activations'],
                    'earning_activations': result['earning_activations'],
                    'approval_rate': float(result['approval_rate']) if result['approval_rate'] else 0.0,
                    'activation_rate': float(result['activation_rate']) if result['activation_rate'] else 0.0,
                    'partner_country': partner_country,
                    'date_range': date_range
                }
            else:
                return {
                    'total_applications': 0,
                    'signup_activations': 0,
                    'earning_activations': 0,
                    'approval_rate': 0.0,
                    'activation_rate': 0.0,
                    'partner_country': partner_country,
                    'date_range': date_range
                }
            
    except Exception as e:
        logger.error(f"Error in partner funnel data: {str(e)}")
        return {
            'total_applications': 0,
            'signup_activations': 0,
            'earning_activations': 0,
            'approval_rate': 0.0,
            'activation_rate': 0.0,
            'partner_country': partner_country,
            'date_range': date_range,
            'error': str(e)
        }
    finally:
        conn.close() 

def get_partner_activation_chart_data(date_range=90, period_type='monthly', partner_country=None):
    """Get partner activation chart data over time periods"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Get partner activation data
            if period_type.lower() == 'daily':
                date_format = 'YYYY-MM-DD'
                date_trunc = 'day'
                interval_step = '1 day'
            else:  # monthly
                date_format = 'YYYY-MM'
                date_trunc = 'month'
                interval_step = '1 month'
            
            # Build country filter
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(partner_country, 'Unknown') = %s"
            
            # Query for partner activations grouped by time period
            query = f"""
                WITH date_series AS (
                    SELECT generate_series(
                        DATE_TRUNC('{date_trunc}', CURRENT_DATE - INTERVAL '{date_range} days'),
                        DATE_TRUNC('{date_trunc}', CURRENT_DATE),
                        INTERVAL '{interval_step}'
                    )::date AS period_date
                ),
                partner_activations AS (
                    SELECT 
                        DATE_TRUNC('{date_trunc}', first_earning_date)::date AS period_date,
                        COUNT(*) as activation_count,
                        COUNT(CASE WHEN partner_country IS NOT NULL THEN 1 END) as activations_with_country
                    FROM partner.partner_info 
                    WHERE 
                        first_earning_date >= CURRENT_DATE - INTERVAL '{date_range} days'
                        AND first_earning_date <= CURRENT_DATE
                        AND first_earning_date IS NOT NULL
                        AND (is_internal = FALSE OR is_internal IS NULL)
                        {country_filter}
                    GROUP BY DATE_TRUNC('{date_trunc}', first_earning_date)::date
                )
                SELECT 
                    ds.period_date,
                    TO_CHAR(ds.period_date, '{date_format}') as period_label,
                    COALESCE(pa.activation_count, 0) as activation_count,
                    COALESCE(pa.activations_with_country, 0) as activations_with_country
                FROM date_series ds
                LEFT JOIN partner_activations pa ON ds.period_date = pa.period_date
                ORDER BY ds.period_date;
            """
            
            # Execute query with proper parameters
            if partner_country and partner_country != 'All':
                cursor.execute(query, (partner_country,))
            else:
                cursor.execute(query)
            
            activations_data = cursor.fetchall()
            activations_columns = ['period_date', 'period_label', 'activation_count', 'activations_with_country']
            activations_list = []
            for row in activations_data:
                activations_list.append(dict(zip(activations_columns, row)))
            
            # Get summary statistics
            summary_query = f"""
                SELECT 
                    COUNT(*) as total_activations,
                    COUNT(DISTINCT partner_country) as unique_countries,
                    COUNT(CASE WHEN first_earning_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as recent_30_days,
                    COUNT(CASE WHEN first_earning_date >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_7_days
                FROM partner.partner_info 
                WHERE 
                    first_earning_date >= CURRENT_DATE - INTERVAL '{date_range} days'
                    AND first_earning_date <= CURRENT_DATE
                    AND first_earning_date IS NOT NULL
                    AND (is_internal = FALSE OR is_internal IS NULL)
                    {country_filter};
            """
            
            if partner_country and partner_country != 'All':
                cursor.execute(summary_query, (partner_country,))
            else:
                cursor.execute(summary_query)
            
            summary_data = cursor.fetchone()
            summary_columns = ['total_activations', 'unique_countries', 'recent_30_days', 'recent_7_days']
            summary_dict = dict(zip(summary_columns, summary_data))
            
            return {
                'activations': activations_list,
                'summary': summary_dict,
                'period_type': period_type,
                'date_range': date_range,
                'partner_country': partner_country
            }
            
    except Exception as e:
        logger.error(f"Error in get_partner_activation_chart_data: {str(e)}")
        raise
    finally:
        conn.close() 

def get_events_data(date_range=90, partner_country=None):
    """Get past and upcoming events data with filtering"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Build country filter - for events this might be event_country
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(event_country, 'Unknown') = %s"
            
            # Query for past events (within date range)
            try:
                past_events_query = f"""
                    SELECT 
                        event_code as event_name,
                        start_date as event_date,
                        event_type,
                        COALESCE(actual_spend_usd, estimated_budget_usd, 0) as spent,
                        0 as co_revenues, -- Placeholder as co_revenues logic needs to be defined
                        event_country,
                        event_city,
                        gp_region
                    FROM gp.event 
                    WHERE 
                        start_date >= CURRENT_DATE - INTERVAL '{date_range} days'
                        AND start_date <= CURRENT_DATE
                        AND start_date IS NOT NULL
                        {country_filter}
                    ORDER BY start_date DESC
                    LIMIT 20;
                """
                
                if partner_country and partner_country != 'All':
                    cursor.execute(past_events_query, (partner_country,))
                else:
                    cursor.execute(past_events_query)
                
                past_events_data = cursor.fetchall()
                past_events_columns = ['event_name', 'event_date', 'event_type', 'spent', 'co_revenues', 'event_country', 'event_city', 'gp_region']
                past_events_list = []
                
                for row in past_events_data:
                    event_dict = dict(zip(past_events_columns, row))
                    # Format date and spent amount
                    if event_dict['event_date']:
                        event_dict['event_date'] = event_dict['event_date'].strftime('%Y-%m-%d')
                    event_dict['spent'] = f"${event_dict['spent']:,.0f}" if event_dict['spent'] else "$0"
                    event_dict['co_revenues'] = f"${event_dict['co_revenues']:,.0f}" if event_dict['co_revenues'] else "$0"
                    past_events_list.append(event_dict)
                
            except Exception as e:
                logger.warning(f"Could not access past events from gp.event table: {str(e)}")
                past_events_list = []
            
            # Query for upcoming events (future dates)
            try:
                upcoming_events_query = f"""
                    SELECT 
                        event_code as event_name,
                        start_date as event_date,
                        event_type,
                        COALESCE(estimated_budget_usd, 0) as spent,
                        0 as co_revenues, -- Placeholder
                        event_country,
                        event_city,
                        gp_region
                    FROM gp.event 
                    WHERE 
                        start_date > CURRENT_DATE
                        AND start_date <= CURRENT_DATE + INTERVAL '90 days'
                        AND start_date IS NOT NULL
                        {country_filter}
                    ORDER BY start_date ASC
                    LIMIT 10;
                """
                
                if partner_country and partner_country != 'All':
                    cursor.execute(upcoming_events_query, (partner_country,))
                else:
                    cursor.execute(upcoming_events_query)
                
                upcoming_events_data = cursor.fetchall()
                upcoming_events_columns = ['event_name', 'event_date', 'event_type', 'spent', 'co_revenues', 'event_country', 'event_city', 'gp_region']
                upcoming_events_list = []
                
                for row in upcoming_events_data:
                    event_dict = dict(zip(upcoming_events_columns, row))
                    # Format date and spent amount
                    if event_dict['event_date']:
                        event_dict['event_date'] = event_dict['event_date'].strftime('%Y-%m-%d')
                    event_dict['spent'] = f"${event_dict['spent']:,.0f}" if event_dict['spent'] else "$0"
                    event_dict['co_revenues'] = f"${event_dict['co_revenues']:,.0f}" if event_dict['co_revenues'] else "$0"
                    upcoming_events_list.append(event_dict)
                
            except Exception as e:
                logger.warning(f"Could not access upcoming events from gp.event table: {str(e)}")
                upcoming_events_list = []
            
            return {
                'past_events': past_events_list,
                'upcoming_events': upcoming_events_list,
                'total_past_events': len(past_events_list),
                'total_upcoming_events': len(upcoming_events_list),
                'partner_country': partner_country,
                'date_range': date_range
            }
            
    except Exception as e:
        logger.error(f"Error in get_events_data: {str(e)}")
        return {
            'past_events': [],
            'upcoming_events': [],
            'total_past_events': 0,
            'total_upcoming_events': 0,
            'partner_country': partner_country,
            'date_range': date_range,
            'error': str(e)
        }
    finally:
        conn.close() 

def get_country_performance_contribution(date_range=90, partner_country=None):
    """Get country performance contribution to current regions with percentage breakdowns"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Build country filter
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(partner_country, 'Unknown') = %s"
            
            # First, get overall totals for percentage calculations
            overall_query = f"""
                SELECT 
                    COUNT(DISTINCT partner_id) as total_applications,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as total_activations,
                    COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '30 days' THEN partner_id END) as total_active_partners,
                    SUM(COALESCE(turnover_earnings, 0) + COALESCE(revenue_share_earnings, 0) + 
                        COALESCE(ib_earnings, 0) + COALESCE(cpa_deposit_earnings, 0)) as total_earnings,
                    COUNT(DISTINCT CASE WHEN first_client_deposit_date IS NOT NULL THEN partner_id END) as total_deposits,
                    COUNT(DISTINCT CASE WHEN first_client_trade_date IS NOT NULL THEN partner_id END) as total_volume_partners
                FROM partner.partner_info
                WHERE is_internal = FALSE
                AND date_joined >= CURRENT_DATE - INTERVAL '{date_range} days'
                {country_filter};
            """
            
            if partner_country and partner_country != 'All':
                cursor.execute(overall_query, (partner_country,))
            else:
                cursor.execute(overall_query)
            
            overall_totals = cursor.fetchone()
            
            # Get regional performance data using actual partner_region from database
            regional_query = f"""
                SELECT 
                    COALESCE(partner_region, 'Unknown') as region,
                    COUNT(DISTINCT partner_id) as new_applications,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as activations,
                    COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '30 days' THEN partner_id END) as active_partners,
                    SUM(COALESCE(turnover_earnings, 0) + COALESCE(revenue_share_earnings, 0) + 
                        COALESCE(ib_earnings, 0) + COALESCE(cpa_deposit_earnings, 0)) as region_earnings,
                    COUNT(DISTINCT CASE WHEN first_client_deposit_date IS NOT NULL THEN partner_id END) as deposit_partners,
                    COUNT(DISTINCT CASE WHEN first_client_trade_date IS NOT NULL THEN partner_id END) as volume_partners
                FROM partner.partner_info
                WHERE is_internal = FALSE
                AND date_joined >= CURRENT_DATE - INTERVAL '{date_range} days'
                {country_filter}
                GROUP BY partner_region
                ORDER BY new_applications DESC;
            """
            
            if partner_country and partner_country != 'All':
                cursor.execute(regional_query, (partner_country,))
            else:
                cursor.execute(regional_query)
            
            regional_data = cursor.fetchall()
            
            # Calculate percentages and format data
            performance_data = []
            
            for row in regional_data:
                region, new_apps, activations, active_partners, earnings, deposits, volume = row
                
                # Calculate percentages (avoiding division by zero)
                new_app_pct = (new_apps / overall_totals[0] * 100) if overall_totals[0] > 0 else 0
                activation_pct = (activations / overall_totals[1] * 100) if overall_totals[1] > 0 else 0
                active_partner_pct = (active_partners / overall_totals[2] * 100) if overall_totals[2] > 0 else 0
                earnings_pct = (earnings / overall_totals[3] * 100) if overall_totals[3] > 0 else 0
                deposit_pct = (deposits / overall_totals[4] * 100) if overall_totals[4] > 0 else 0
                volume_pct = (volume / overall_totals[5] * 100) if overall_totals[5] > 0 else 0
                
                performance_data.append({
                    'region': region,
                    'new_application': new_apps,
                    'new_activation_pct': round(activation_pct, 1),
                    'active_partner_pct': round(active_partner_pct, 1),
                    'earnings_pct': round(earnings_pct, 1),
                    'deposit_pct': round(deposit_pct, 1),
                    'volume_usd_pct': round(volume_pct, 1),
                    'company_revenue_pct': round(earnings_pct, 1)  # Using earnings as proxy for company revenue
                })
            
            # Add overall row
            performance_data.append({
                'region': 'Overall',
                'new_application': overall_totals[0],
                'new_activation_pct': 100.0,
                'active_partner_pct': 100.0,
                'earnings_pct': 100.0,
                'deposit_pct': 100.0,
                'volume_usd_pct': 100.0,
                'company_revenue_pct': 100.0
            })
            
            return {
                'performance_data': performance_data,
                'overall_totals': {
                    'total_applications': overall_totals[0],
                    'total_activations': overall_totals[1],
                    'total_active_partners': overall_totals[2],
                    'total_earnings': float(overall_totals[3]) if overall_totals[3] else 0,
                    'total_deposits': overall_totals[4],
                    'total_volume_partners': overall_totals[5]
                },
                'partner_country': partner_country,
                'date_range': date_range
            }
            
    except Exception as e:
        logger.error(f"Error in get_country_performance_contribution: {str(e)}")
        return {
            'performance_data': [],
            'overall_totals': {
                'total_applications': 0,
                'total_activations': 0,
                'total_active_partners': 0,
                'total_earnings': 0,
                'total_deposits': 0,
                'total_volume_partners': 0
            },
            'partner_country': partner_country,
            'date_range': date_range,
            'error': str(e)
        }
    finally:
        conn.close() 

def get_active_partners_chart_data(date_range=90, period_type='monthly', start_date=None, end_date=None, partner_country=None):
    """Get active partners chart data over time periods (partners with new client signups, trades, or deposits)"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Get active partners data
            if period_type.lower() == 'daily':
                date_format = 'YYYY-MM-DD'
                date_trunc = 'day'
                interval_step = '1 day'
                summary_table = 'partner.partner_summary_daily'
                date_column = 'date'
            else:  # monthly
                date_format = 'YYYY-MM'
                date_trunc = 'month'
                interval_step = '1 month'
                summary_table = 'partner.partner_summary_monthly'
                date_column = 'month'
            
            # Determine date range - use start_date/end_date if provided, otherwise use date_range
            if start_date and end_date:
                start_filter = start_date
                end_filter = end_date
            else:
                start_filter = f"CURRENT_DATE - INTERVAL '{date_range} days'"
                end_filter = "CURRENT_DATE"
            
            # Build country filter
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(pi.partner_country, 'Unknown') = %s"
            
            # Query for active partners grouped by time period
            if start_date and end_date:
                query = f"""
                    WITH date_series AS (
                        SELECT generate_series(
                            DATE_TRUNC('{date_trunc}', %s::date),
                            DATE_TRUNC('{date_trunc}', %s::date),
                            INTERVAL '{interval_step}'
                        )::date AS period_date
                    ),
                    active_partners AS (
                        SELECT 
                            DATE_TRUNC('{date_trunc}', ps.{date_column})::date AS period_date,
                            COUNT(DISTINCT ps.partner_id) as active_count,
                            COUNT(DISTINCT CASE WHEN pi.partner_country IS NOT NULL THEN ps.partner_id END) as active_with_country
                        FROM {summary_table} ps
                        INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                        WHERE 
                            ps.{date_column} >= %s::date
                            AND ps.{date_column} <= %s::date
                            AND (
                                ps.client_signups > 0 OR 
                                ps.traded_clients > 0 OR 
                                ps.total_deposits > 0
                            )
                            AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                            {country_filter}
                        GROUP BY DATE_TRUNC('{date_trunc}', ps.{date_column})::date
                    )
                    SELECT 
                        ds.period_date,
                        TO_CHAR(ds.period_date, '{date_format}') as period_label,
                        COALESCE(ap.active_count, 0) as active_count,
                        COALESCE(ap.active_with_country, 0) as active_with_country
                    FROM date_series ds
                    LEFT JOIN active_partners ap ON ds.period_date = ap.period_date
                    ORDER BY ds.period_date;
                """
                
                query_params = [start_date, end_date, start_date, end_date]
                if partner_country and partner_country != 'All':
                    query_params.append(partner_country)
            else:
                query = f"""
                    WITH date_series AS (
                        SELECT generate_series(
                            DATE_TRUNC('{date_trunc}', CURRENT_DATE - INTERVAL '{date_range} days'),
                            DATE_TRUNC('{date_trunc}', CURRENT_DATE),
                            INTERVAL '{interval_step}'
                        )::date AS period_date
                    ),
                    active_partners AS (
                        SELECT 
                            DATE_TRUNC('{date_trunc}', ps.{date_column})::date AS period_date,
                            COUNT(DISTINCT ps.partner_id) as active_count,
                            COUNT(DISTINCT CASE WHEN pi.partner_country IS NOT NULL THEN ps.partner_id END) as active_with_country
                        FROM {summary_table} ps
                        INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                        WHERE 
                            ps.{date_column} >= CURRENT_DATE - INTERVAL '{date_range} days'
                            AND ps.{date_column} <= CURRENT_DATE
                            AND (
                                ps.client_signups > 0 OR 
                                ps.traded_clients > 0 OR 
                                ps.total_deposits > 0
                            )
                            AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                            {country_filter}
                        GROUP BY DATE_TRUNC('{date_trunc}', ps.{date_column})::date
                    )
                    SELECT 
                        ds.period_date,
                        TO_CHAR(ds.period_date, '{date_format}') as period_label,
                        COALESCE(ap.active_count, 0) as active_count,
                        COALESCE(ap.active_with_country, 0) as active_with_country
                    FROM date_series ds
                    LEFT JOIN active_partners ap ON ds.period_date = ap.period_date
                    ORDER BY ds.period_date;
                """
                
                query_params = []
                if partner_country and partner_country != 'All':
                    query_params.append(partner_country)
            
            cursor.execute(query, query_params)
            
            active_data = cursor.fetchall()
            active_columns = ['period_date', 'period_label', 'active_count', 'active_with_country']
            active_list = []
            for row in active_data:
                active_list.append(dict(zip(active_columns, row)))
            
            # Get summary statistics
            if start_date and end_date:
                summary_query = f"""
                    SELECT 
                        COUNT(DISTINCT ps.partner_id) as total_active,
                        COUNT(DISTINCT pi.partner_country) as unique_countries,
                        COUNT(DISTINCT CASE WHEN ps.{date_column} >= %s::date - INTERVAL '30 days' THEN ps.partner_id END) as recent_30_days,
                        COUNT(DISTINCT CASE WHEN ps.{date_column} >= %s::date - INTERVAL '7 days' THEN ps.partner_id END) as recent_7_days
                    FROM {summary_table} ps
                    INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                    WHERE 
                        ps.{date_column} >= %s::date
                        AND ps.{date_column} <= %s::date
                        AND (
                            ps.client_signups > 0 OR 
                            ps.traded_clients > 0 OR 
                            ps.total_deposits > 0
                        )
                        AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        {country_filter};
                """
                
                summary_params = [end_date, end_date, start_date, end_date]
                if partner_country and partner_country != 'All':
                    summary_params.append(partner_country)
            else:
                summary_query = f"""
                    SELECT 
                        COUNT(DISTINCT ps.partner_id) as total_active,
                        COUNT(DISTINCT pi.partner_country) as unique_countries,
                        COUNT(DISTINCT CASE WHEN ps.{date_column} >= CURRENT_DATE - INTERVAL '30 days' THEN ps.partner_id END) as recent_30_days,
                        COUNT(DISTINCT CASE WHEN ps.{date_column} >= CURRENT_DATE - INTERVAL '7 days' THEN ps.partner_id END) as recent_7_days
                    FROM {summary_table} ps
                    INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                    WHERE 
                        ps.{date_column} >= CURRENT_DATE - INTERVAL '{date_range} days'
                        AND ps.{date_column} <= CURRENT_DATE
                        AND (
                            ps.client_signups > 0 OR 
                            ps.traded_clients > 0 OR 
                            ps.total_deposits > 0
                        )
                        AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        {country_filter};
                """
                
                summary_params = []
                if partner_country and partner_country != 'All':
                    summary_params.append(partner_country)
            
            cursor.execute(summary_query, summary_params)
            
            summary_data = cursor.fetchone()
            summary_columns = ['total_active', 'unique_countries', 'recent_30_days', 'recent_7_days']
            summary_dict = dict(zip(summary_columns, summary_data))
            
            return {
                'active_partners': active_list,
                'summary': summary_dict,
                'period_type': period_type,
                'date_range': date_range,
                'partner_country': partner_country
            }
            
    except Exception as e:
        logger.error(f"Error in get_active_partners_chart_data: {str(e)}")
        raise
    finally:
        conn.close() 

def get_performance_stats_data(date_range=90, period_type='monthly', start_date=None, end_date=None, partner_country=None):
    """Get performance stats data over time periods"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Get performance stats data
            if period_type.lower() == 'daily':
                date_format = 'YYYY-MM-DD'
                date_trunc = 'day'
                interval_step = '1 day'
                summary_table = 'partner.partner_summary_daily'
                date_column = 'date'
            else:  # monthly
                date_format = 'YYYY-MM'
                date_trunc = 'month'
                interval_step = '1 month'
                summary_table = 'partner.partner_summary_monthly'
                date_column = 'month'
            
            # Determine date range - use start_date/end_date if provided, otherwise use date_range
            if start_date and end_date:
                start_filter = start_date
                end_filter = end_date
            else:
                start_filter = f"CURRENT_DATE - INTERVAL '{date_range} days'"
                end_filter = "CURRENT_DATE"
            
            # Build country filter
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(pi.partner_country, 'Unknown') = %s"
            
            # Query for performance stats grouped by time period
            if start_date and end_date:
                query = f"""
                    WITH date_series AS (
                        SELECT generate_series(
                            DATE_TRUNC('{date_trunc}', %s::date),
                            DATE_TRUNC('{date_trunc}', %s::date),
                            INTERVAL '{interval_step}'
                        )::date AS period_date
                    ),
                    performance_stats AS (
                        SELECT 
                            DATE_TRUNC('{date_trunc}', ps.{date_column})::date AS period_date,
                            COUNT(DISTINCT CASE WHEN (
                                ps.client_signups > 0 OR 
                                ps.traded_clients > 0 OR 
                                ps.total_deposits > 0
                            ) THEN ps.partner_id END) as total_active_partners,
                            COALESCE(SUM(ps.total_deposits), 0) as total_deposit,
                            COALESCE(SUM(ps.volume_usd), 0) as total_volume_usd,
                            COALESCE(SUM(ps.expected_revenue), 0) as total_expected_revenue,
                            COALESCE(SUM(ps.total_earnings), 0) as total_earnings,
                            -- Calculate company revenue as sum of expected revenue (can be adjusted based on business logic)
                            COALESCE(SUM(ps.expected_revenue), 0) as total_company_revenue
                        FROM {summary_table} ps
                        INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                        WHERE 
                            ps.{date_column} >= %s::date
                            AND ps.{date_column} <= %s::date
                            AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                            {country_filter}
                        GROUP BY DATE_TRUNC('{date_trunc}', ps.{date_column})::date
                    )
                    SELECT 
                        ds.period_date,
                        TO_CHAR(ds.period_date, '{date_format}') as period_label,
                        COALESCE(ps.total_active_partners, 0) as total_active_partners,
                        COALESCE(ps.total_deposit, 0) as total_deposit,
                        COALESCE(ps.total_volume_usd, 0) as total_volume_usd,
                        COALESCE(ps.total_company_revenue, 0) as total_company_revenue,
                        COALESCE(ps.total_expected_revenue, 0) as total_expected_revenue,
                        COALESCE(ps.total_earnings, 0) as total_earnings
                    FROM date_series ds
                    LEFT JOIN performance_stats ps ON ds.period_date = ps.period_date
                    ORDER BY ds.period_date;
                """
                
                query_params = [start_date, end_date, start_date, end_date]
                if partner_country and partner_country != 'All':
                    query_params.append(partner_country)
            else:
                query = f"""
                    WITH date_series AS (
                        SELECT generate_series(
                            DATE_TRUNC('{date_trunc}', CURRENT_DATE - INTERVAL '{date_range} days'),
                            DATE_TRUNC('{date_trunc}', CURRENT_DATE),
                            INTERVAL '{interval_step}'
                        )::date AS period_date
                    ),
                    performance_stats AS (
                        SELECT 
                            DATE_TRUNC('{date_trunc}', ps.{date_column})::date AS period_date,
                            COUNT(DISTINCT CASE WHEN (
                                ps.client_signups > 0 OR 
                                ps.traded_clients > 0 OR 
                                ps.total_deposits > 0
                            ) THEN ps.partner_id END) as total_active_partners,
                            COALESCE(SUM(ps.total_deposits), 0) as total_deposit,
                            COALESCE(SUM(ps.volume_usd), 0) as total_volume_usd,
                            COALESCE(SUM(ps.expected_revenue), 0) as total_expected_revenue,
                            COALESCE(SUM(ps.total_earnings), 0) as total_earnings,
                            -- Calculate company revenue as sum of expected revenue (can be adjusted based on business logic)
                            COALESCE(SUM(ps.expected_revenue), 0) as total_company_revenue
                        FROM {summary_table} ps
                        INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                        WHERE 
                            ps.{date_column} >= CURRENT_DATE - INTERVAL '{date_range} days'
                            AND ps.{date_column} <= CURRENT_DATE
                            AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                            {country_filter}
                        GROUP BY DATE_TRUNC('{date_trunc}', ps.{date_column})::date
                    )
                    SELECT 
                        ds.period_date,
                        TO_CHAR(ds.period_date, '{date_format}') as period_label,
                        COALESCE(ps.total_active_partners, 0) as total_active_partners,
                        COALESCE(ps.total_deposit, 0) as total_deposit,
                        COALESCE(ps.total_volume_usd, 0) as total_volume_usd,
                        COALESCE(ps.total_company_revenue, 0) as total_company_revenue,
                        COALESCE(ps.total_expected_revenue, 0) as total_expected_revenue,
                        COALESCE(ps.total_earnings, 0) as total_earnings
                    FROM date_series ds
                    LEFT JOIN performance_stats ps ON ds.period_date = ps.period_date
                    ORDER BY ds.period_date;
                """
                
                query_params = []
                if partner_country and partner_country != 'All':
                    query_params.append(partner_country)
            
            cursor.execute(query, query_params)
            
            stats_data = cursor.fetchall()
            stats_columns = ['period_date', 'period_label', 'total_active_partners', 'total_deposit', 
                           'total_volume_usd', 'total_company_revenue', 'total_expected_revenue', 'total_earnings']
            stats_list = []
            for row in stats_data:
                row_dict = dict(zip(stats_columns, row))
                # Format monetary values
                row_dict['total_deposit'] = f"${row_dict['total_deposit']:,.0f}" if row_dict['total_deposit'] else "$0"
                row_dict['total_volume_usd'] = f"${row_dict['total_volume_usd']:,.0f}" if row_dict['total_volume_usd'] else "$0"
                row_dict['total_company_revenue'] = f"${row_dict['total_company_revenue']:,.0f}" if row_dict['total_company_revenue'] else "$0"
                row_dict['total_expected_revenue'] = f"${row_dict['total_expected_revenue']:,.0f}" if row_dict['total_expected_revenue'] else "$0"
                row_dict['total_earnings'] = f"${row_dict['total_earnings']:,.0f}" if row_dict['total_earnings'] else "$0"
                # Keep period_date as is for sorting, period_label for display
                stats_list.append(row_dict)
            
            return {
                'performance_stats': stats_list,
                'period_type': period_type,
                'date_range': date_range,
                'partner_country': partner_country
            }
            
    except Exception as e:
        logger.error(f"Error in get_performance_stats_data: {str(e)}")
        raise
    finally:
        conn.close() 

def get_earning_partners_chart_data(date_range=90, period_type='monthly', start_date=None, end_date=None, partner_country=None):
    """Get earning partners chart data over time periods (partners who generated commission during the period)"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Get earning partners data
            if period_type.lower() == 'daily':
                date_format = 'YYYY-MM-DD'
                date_trunc = 'day'
                interval_step = '1 day'
                summary_table = 'partner.partner_summary_daily'
                date_column = 'date'
            else:  # monthly
                date_format = 'YYYY-MM'
                date_trunc = 'month'
                interval_step = '1 month'
                summary_table = 'partner.partner_summary_monthly'
                date_column = 'month'
            
            # Determine date range - use start_date/end_date if provided, otherwise use date_range
            if start_date and end_date:
                start_filter = start_date
                end_filter = end_date
            else:
                start_filter = f"CURRENT_DATE - INTERVAL '{date_range} days'"
                end_filter = "CURRENT_DATE"
            
            # Build country filter
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(pi.partner_country, 'Unknown') = %s"
            
            # Query for earning partners grouped by time period
            if start_date and end_date:
                query = f"""
                    WITH date_series AS (
                        SELECT generate_series(
                            DATE_TRUNC('{date_trunc}', %s::date),
                            DATE_TRUNC('{date_trunc}', %s::date),
                            INTERVAL '{interval_step}'
                        )::date AS period_date
                    ),
                    earning_partners AS (
                        SELECT 
                            DATE_TRUNC('{date_trunc}', ps.{date_column})::date AS period_date,
                            COUNT(DISTINCT CASE WHEN ps.total_earnings > 0 THEN ps.partner_id END) as earning_count,
                            COUNT(DISTINCT CASE WHEN ps.total_earnings > 0 AND pi.partner_country IS NOT NULL THEN ps.partner_id END) as earning_with_country,
                            COALESCE(SUM(CASE WHEN ps.total_earnings > 0 THEN ps.total_earnings END), 0) as total_earnings_amount
                        FROM {summary_table} ps
                        INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                        WHERE 
                            ps.{date_column} >= %s::date
                            AND ps.{date_column} <= %s::date
                            AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                            {country_filter}
                        GROUP BY DATE_TRUNC('{date_trunc}', ps.{date_column})::date
                    )
                    SELECT 
                        ds.period_date,
                        TO_CHAR(ds.period_date, '{date_format}') as period_label,
                        COALESCE(ep.earning_count, 0) as earning_count,
                        COALESCE(ep.earning_with_country, 0) as earning_with_country,
                        COALESCE(ep.total_earnings_amount, 0) as total_earnings_amount
                    FROM date_series ds
                    LEFT JOIN earning_partners ep ON ds.period_date = ep.period_date
                    ORDER BY ds.period_date;
                """
                
                query_params = [start_date, end_date, start_date, end_date]
                if partner_country and partner_country != 'All':
                    query_params.append(partner_country)
            else:
                query = f"""
                    WITH date_series AS (
                        SELECT generate_series(
                            DATE_TRUNC('{date_trunc}', CURRENT_DATE - INTERVAL '{date_range} days'),
                            DATE_TRUNC('{date_trunc}', CURRENT_DATE),
                            INTERVAL '{interval_step}'
                        )::date AS period_date
                    ),
                    earning_partners AS (
                        SELECT 
                            DATE_TRUNC('{date_trunc}', ps.{date_column})::date AS period_date,
                            COUNT(DISTINCT CASE WHEN ps.total_earnings > 0 THEN ps.partner_id END) as earning_count,
                            COUNT(DISTINCT CASE WHEN ps.total_earnings > 0 AND pi.partner_country IS NOT NULL THEN ps.partner_id END) as earning_with_country,
                            COALESCE(SUM(CASE WHEN ps.total_earnings > 0 THEN ps.total_earnings END), 0) as total_earnings_amount
                        FROM {summary_table} ps
                        INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                        WHERE 
                            ps.{date_column} >= CURRENT_DATE - INTERVAL '{date_range} days'
                            AND ps.{date_column} <= CURRENT_DATE
                            AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                            {country_filter}
                        GROUP BY DATE_TRUNC('{date_trunc}', ps.{date_column})::date
                    )
                    SELECT 
                        ds.period_date,
                        TO_CHAR(ds.period_date, '{date_format}') as period_label,
                        COALESCE(ep.earning_count, 0) as earning_count,
                        COALESCE(ep.earning_with_country, 0) as earning_with_country,
                        COALESCE(ep.total_earnings_amount, 0) as total_earnings_amount
                    FROM date_series ds
                    LEFT JOIN earning_partners ep ON ds.period_date = ep.period_date
                    ORDER BY ds.period_date;
                """
                
                query_params = []
                if partner_country and partner_country != 'All':
                    query_params.append(partner_country)
            
            cursor.execute(query, query_params)
            
            earning_data = cursor.fetchall()
            earning_columns = ['period_date', 'period_label', 'earning_count', 'earning_with_country', 'total_earnings_amount']
            earning_list = []
            for row in earning_data:
                earning_list.append(dict(zip(earning_columns, row)))
            
            # Get summary statistics
            if start_date and end_date:
                summary_query = f"""
                    SELECT 
                        COUNT(DISTINCT CASE WHEN ps.total_earnings > 0 THEN ps.partner_id END) as total_earning,
                        COUNT(DISTINCT pi.partner_country) as unique_countries,
                        COUNT(DISTINCT CASE WHEN ps.{date_column} >= %s::date - INTERVAL '30 days' AND ps.total_earnings > 0 THEN ps.partner_id END) as recent_30_days,
                        COUNT(DISTINCT CASE WHEN ps.{date_column} >= %s::date - INTERVAL '7 days' AND ps.total_earnings > 0 THEN ps.partner_id END) as recent_7_days,
                        COALESCE(SUM(ps.total_earnings), 0) as total_earnings_sum
                    FROM {summary_table} ps
                    INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                    WHERE 
                        ps.{date_column} >= %s::date
                        AND ps.{date_column} <= %s::date
                        AND ps.total_earnings > 0
                        AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        {country_filter};
                """
                
                summary_params = [end_date, end_date, start_date, end_date]
                if partner_country and partner_country != 'All':
                    summary_params.append(partner_country)
            else:
                summary_query = f"""
                    SELECT 
                        COUNT(DISTINCT CASE WHEN ps.total_earnings > 0 THEN ps.partner_id END) as total_earning,
                        COUNT(DISTINCT pi.partner_country) as unique_countries,
                        COUNT(DISTINCT CASE WHEN ps.{date_column} >= CURRENT_DATE - INTERVAL '30 days' AND ps.total_earnings > 0 THEN ps.partner_id END) as recent_30_days,
                        COUNT(DISTINCT CASE WHEN ps.{date_column} >= CURRENT_DATE - INTERVAL '7 days' AND ps.total_earnings > 0 THEN ps.partner_id END) as recent_7_days,
                        COALESCE(SUM(ps.total_earnings), 0) as total_earnings_sum
                    FROM {summary_table} ps
                    INNER JOIN partner.partner_info pi ON ps.partner_id = pi.partner_id
                    WHERE 
                        ps.{date_column} >= CURRENT_DATE - INTERVAL '{date_range} days'
                        AND ps.{date_column} <= CURRENT_DATE
                        AND ps.total_earnings > 0
                        AND (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        {country_filter};
                """
                
                summary_params = []
                if partner_country and partner_country != 'All':
                    summary_params.append(partner_country)
            
            cursor.execute(summary_query, summary_params)
            
            summary_data = cursor.fetchone()
            summary_columns = ['total_earning', 'unique_countries', 'recent_30_days', 'recent_7_days', 'total_earnings_sum']
            summary_dict = dict(zip(summary_columns, summary_data))
            
            return {
                'earning_partners': earning_list,
                'summary': summary_dict,
                'period_type': period_type,
                'date_range': date_range,
                'partner_country': partner_country
            }

    except Exception as e:
        logger.error(f"Error getting earning partners chart data: {str(e)}")
        return {
            'earning_partners': [],
            'summary': {},
            'period_type': period_type,
            'date_range': date_range,
            'partner_country': partner_country
        }
    finally:
        conn.close()

def get_top_partners_data(date_range=90, partner_country=None, limit=20):
    """Get top 20 partners based on performance metrics"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Build country filter
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(pi.partner_country, 'Unknown') = %s"
            
            # Query to get top partners with aggregated data from monthly summary
            query = f"""
                SELECT 
                    pi.partner_id,
                    COALESCE(SUM(ps.client_signups), 0)::numeric as total_new_client_signups,
                    COALESCE(SUM(ps.sub_partner_signups), 0)::numeric as total_new_sub_aff_signups,
                    COALESCE(SUM(ps.traded_clients), 0)::numeric as total_active_clients,
                    COALESCE(SUM(ps.total_deposits), 0)::numeric as total_deposit,
                    COALESCE(SUM(ps.volume_usd), 0)::numeric as total_volume_usd,
                    COALESCE(SUM(ps.expected_revenue), 0)::numeric as total_company_revenue,
                    COALESCE(SUM(ps.expected_revenue), 0)::numeric as total_expected_revenue,
                    COALESCE(SUM(ps.total_earnings - COALESCE(ps.subaffiliate_earnings, 0)), 0)::numeric as total_direct_earnings,
                    COALESCE(SUM(ps.subaffiliate_earnings), 0)::numeric as total_sub_affiliate_earnings,
                    pi.partner_country,
                    pi.aff_type,
                    pi.date_joined,
                    pi.first_earning_date
                FROM partner.partner_info pi
                LEFT JOIN partner.partner_summary_monthly ps ON pi.partner_id = ps.partner_id
                    AND ps.month >= CURRENT_DATE - INTERVAL '%s days'
                    AND ps.month <= CURRENT_DATE
                WHERE 
                    (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                    {country_filter}
                GROUP BY 
                    pi.partner_id, pi.partner_country, pi.aff_type, 
                    pi.date_joined, pi.first_earning_date
                ORDER BY 
                    COALESCE(SUM(ps.client_signups), 0) DESC NULLS LAST,
                    COALESCE(SUM(ps.total_earnings), 0) DESC NULLS LAST,
                    COALESCE(SUM(ps.total_deposits), 0) DESC NULLS LAST
                LIMIT %s;
            """
            
            query_params = [date_range]
            if partner_country and partner_country != 'All':
                query_params.append(partner_country)
            query_params.append(limit)
            
            cursor.execute(query, query_params)
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            columns = [
                'partner_id', 'total_new_client_signups', 'total_new_sub_aff_signups', 
                'total_active_clients', 'total_deposit', 'total_volume_usd',
                'total_company_revenue', 'total_expected_revenue', 'total_direct_earnings',
                'total_sub_affiliate_earnings', 'partner_country', 'aff_type', 
                'date_joined', 'first_earning_date'
            ]
            
            data = []
            for row in results:
                partner_dict = dict(zip(columns, row))
                # Format dates
                if partner_dict['date_joined']:
                    partner_dict['date_joined'] = partner_dict['date_joined'].strftime('%Y-%m-%d')
                if partner_dict['first_earning_date']:
                    partner_dict['first_earning_date'] = partner_dict['first_earning_date'].strftime('%Y-%m-%d')
                # Convert Decimal to float for JSON serialization
                for key in ['total_new_client_signups', 'total_new_sub_aff_signups', 'total_active_clients',
                           'total_deposit', 'total_volume_usd', 'total_company_revenue', 
                           'total_expected_revenue', 'total_direct_earnings', 'total_sub_affiliate_earnings']:
                    if partner_dict[key] is not None:
                        partner_dict[key] = float(partner_dict[key])
                data.append(partner_dict)
            
            # Get summary statistics for context
            cursor.execute(f"""
                WITH partner_metrics AS (
                    SELECT 
                        pi.partner_id,
                        COALESCE(SUM(ps.total_earnings), 0) as partner_earnings,
                        COALESCE(SUM(ps.total_deposits), 0) as partner_deposits
                    FROM partner.partner_info pi
                    LEFT JOIN partner.partner_summary_monthly ps ON pi.partner_id = ps.partner_id
                        AND ps.month >= CURRENT_DATE - INTERVAL '%s days'
                        AND ps.month <= CURRENT_DATE
                    WHERE 
                        (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        {country_filter}
                    GROUP BY pi.partner_id
                )
                SELECT 
                    COUNT(DISTINCT partner_id) as total_partners_in_period,
                    COUNT(DISTINCT CASE WHEN partner_earnings > 0 THEN partner_id END) as earning_partners,
                    COALESCE(SUM(partner_earnings), 0) as total_earnings_sum,
                    COALESCE(SUM(partner_deposits), 0) as total_deposits_sum,
                    COALESCE(AVG(CASE WHEN partner_earnings > 0 THEN partner_earnings END), 0) as avg_earnings_per_partner
                FROM partner_metrics;
            """, query_params[:-1])  # Remove limit parameter
            
            summary_result = cursor.fetchone()
            summary_columns = ['total_partners_in_period', 'earning_partners', 'total_earnings_sum', 
                             'total_deposits_sum', 'avg_earnings_per_partner']
            summary_data = dict(zip(summary_columns, summary_result)) if summary_result else {}
            
            # Convert Decimal to float for JSON serialization
            for key in ['total_earnings_sum', 'total_deposits_sum', 'avg_earnings_per_partner']:
                if key in summary_data and summary_data[key] is not None:
                    summary_data[key] = float(summary_data[key])
            
            return {
                'top_partners': data,
                'summary': summary_data,
                'date_range': date_range,
                'partner_country': partner_country,
                'limit': limit
            }

    except Exception as e:
        logger.error(f"Error getting top partners data: {str(e)}")
        return {
            'top_partners': [],
            'summary': {},
            'date_range': date_range,
            'partner_country': partner_country,
            'limit': limit
        }
    finally:
        conn.close()

def get_inactive_partners_data(date_range=90, partner_country=None, limit=50):
    """Get inactive partners sorted by commission tiers based on 3-month average earnings"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Build country filter
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(pi.partner_country, 'Unknown') = %s"
            
            # Query to get inactive partners with their commission tiers
            query = f"""
                WITH partner_activity AS (
                    SELECT 
                        pi.partner_id,
                        pi.last_earning_date,
                        pi.last_client_joined_date as last_new_client_signup_date,
                        -- Get last sub-affiliate signup from partner_info
                        (SELECT MAX(date_joined) 
                         FROM partner.partner_info sub 
                         WHERE sub.parent_partner_id = pi.partner_id) as last_new_sub_aff_signup_date,
                        -- Calculate last active date as most recent of all activities
                        GREATEST(
                            COALESCE(pi.last_earning_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_joined_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_deposit_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_trade_date, '1900-01-01'::date)
                        ) as last_active_date,
                        -- Calculate days inactive
                        CURRENT_DATE - GREATEST(
                            COALESCE(pi.last_earning_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_joined_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_deposit_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_trade_date, '1900-01-01'::date)
                        ) as days_inactive,
                        -- Calculate 3-month average commission
                        COALESCE(
                            (SELECT SUM(ps.total_earnings) / 3.0
                             FROM partner.partner_summary_monthly ps
                             WHERE ps.partner_id = pi.partner_id
                             AND ps.month >= CURRENT_DATE - INTERVAL '3 months'
                             AND ps.month <= CURRENT_DATE),
                            0
                        ) as avg_monthly_commission,
                        pi.partner_country,
                        pi.aff_type,
                        pi.date_joined
                    FROM partner.partner_info pi
                    WHERE 
                        (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        AND pi.date_joined <= CURRENT_DATE - INTERVAL '%s days'
                        {country_filter}
                ),
                tiered_partners AS (
                    SELECT 
                        partner_id,
                        CASE 
                            WHEN last_active_date = '1900-01-01'::date THEN NULL
                            ELSE last_active_date 
                        END as last_active_date,
                        last_earning_date,
                        last_new_client_signup_date,
                        last_new_sub_aff_signup_date,
                        days_inactive,
                        partner_country,
                        aff_type,
                        date_joined,
                        avg_monthly_commission,
                        -- Commission tier based on 3-month average
                        CASE 
                            WHEN avg_monthly_commission > 5000 THEN 'Platinum'
                            WHEN avg_monthly_commission > 1000 THEN 'Gold'
                            WHEN avg_monthly_commission > 500 THEN 'Silver'
                            ELSE 'Bronze'
                        END as commission_tier
                    FROM partner_activity
                    WHERE days_inactive > 30  -- Only show partners inactive for more than 30 days
                )
                SELECT *
                FROM tiered_partners
                ORDER BY 
                    CASE commission_tier
                        WHEN 'Platinum' THEN 1
                        WHEN 'Gold' THEN 2
                        WHEN 'Silver' THEN 3
                        WHEN 'Bronze' THEN 4
                    END,
                    days_inactive DESC
                LIMIT %s;
            """
            
            query_params = [date_range]
            if partner_country and partner_country != 'All':
                query_params.append(partner_country)
            query_params.append(limit)
            
            cursor.execute(query, query_params)
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            columns = [
                'partner_id', 'last_active_date', 'last_earning_date', 
                'last_new_client_signup_date', 'last_new_sub_aff_signup_date',
                'days_inactive', 'partner_country', 'aff_type', 'date_joined',
                'avg_monthly_commission', 'commission_tier'
            ]
            
            data = []
            for row in results:
                partner_dict = dict(zip(columns, row))
                # Format dates
                for date_field in ['last_active_date', 'last_earning_date', 
                                 'last_new_client_signup_date', 'last_new_sub_aff_signup_date', 
                                 'date_joined']:
                    if partner_dict[date_field]:
                        partner_dict[date_field] = partner_dict[date_field].strftime('%Y-%m-%d')
                # Convert avg_monthly_commission to float
                if partner_dict['avg_monthly_commission'] is not None:
                    partner_dict['avg_monthly_commission'] = float(partner_dict['avg_monthly_commission'])
                data.append(partner_dict)
            
            # Get summary statistics for commission tiers
            cursor.execute(f"""
                WITH partner_activity AS (
                    SELECT 
                        pi.partner_id,
                        CURRENT_DATE - GREATEST(
                            COALESCE(pi.last_earning_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_joined_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_deposit_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_trade_date, '1900-01-01'::date)
                        ) as days_inactive,
                        COALESCE(
                            (SELECT SUM(ps.total_earnings) / 3.0
                             FROM partner.partner_summary_monthly ps
                             WHERE ps.partner_id = pi.partner_id
                             AND ps.month >= CURRENT_DATE - INTERVAL '3 months'
                             AND ps.month <= CURRENT_DATE),
                            0
                        ) as avg_monthly_commission
                    FROM partner.partner_info pi
                    WHERE 
                        (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        AND pi.date_joined <= CURRENT_DATE - INTERVAL '%s days'
                        {country_filter}
                        AND CURRENT_DATE - GREATEST(
                            COALESCE(pi.last_earning_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_joined_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_deposit_date, '1900-01-01'::date),
                            COALESCE(pi.last_client_trade_date, '1900-01-01'::date)
                        ) > 30  -- Only count inactive partners
                )
                SELECT 
                    COUNT(CASE WHEN avg_monthly_commission <= 500 THEN 1 END) as bronze_count,
                    COUNT(CASE WHEN avg_monthly_commission > 500 AND avg_monthly_commission <= 1000 THEN 1 END) as silver_count,
                    COUNT(CASE WHEN avg_monthly_commission > 1000 AND avg_monthly_commission <= 5000 THEN 1 END) as gold_count,
                    COUNT(CASE WHEN avg_monthly_commission > 5000 THEN 1 END) as platinum_count,
                    COUNT(*) as total_inactive_partners
                FROM partner_activity;
            """, query_params[:-1])  # Remove limit parameter
            
            summary_result = cursor.fetchone()
            summary_columns = ['bronze_count', 'silver_count', 'gold_count', 
                             'platinum_count', 'total_inactive_partners']
            summary_data = dict(zip(summary_columns, summary_result)) if summary_result else {}
            
            return {
                'inactive_partners': data,
                'summary': summary_data,
                'date_range': date_range,
                'partner_country': partner_country,
                'limit': limit
            }

    except Exception as e:
        logger.error(f"Error getting inactive partners data: {str(e)}")
        return {
            'inactive_partners': [],
            'summary': {},
            'date_range': date_range,
            'partner_country': partner_country,
            'limit': limit
        }
    finally:
        conn.close()

def get_new_partner_support_data(date_range=90, partner_country=None, limit=100):
    """Get new partners who need support - not yet activated or need help moving to next funnel stage"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Build country filter
            country_filter = ""
            if partner_country and partner_country != 'All':
                country_filter = "AND COALESCE(pi.partner_country, 'Unknown') = %s"
            
            # Query to get new partners with their funnel progress
            query = f"""
                WITH partner_metrics AS (
                    SELECT 
                        pi.partner_id,
                        pi.date_joined as partner_join_date,
                        pi.partner_country,
                        pi.aff_type,
                        pi.first_earning_date,
                        -- Get total client signups from summary data
                        COALESCE(
                            (SELECT SUM(ps.client_signups)
                             FROM partner.partner_summary_monthly ps
                             WHERE ps.partner_id = pi.partner_id
                             AND ps.month >= pi.date_joined::date
                             AND ps.month <= CURRENT_DATE),
                            0
                        ) as total_client_signups,
                        -- Get total sub-affiliate signups
                        COALESCE(
                            (SELECT SUM(ps.sub_partner_signups)
                             FROM partner.partner_summary_monthly ps
                             WHERE ps.partner_id = pi.partner_id
                             AND ps.month >= pi.date_joined::date
                             AND ps.month <= CURRENT_DATE),
                            0
                        ) as total_subaff_signups,
                        -- Get deposited clients count
                        COALESCE(
                            (SELECT COUNT(DISTINCT ps.partner_id)
                             FROM partner.partner_summary_monthly ps
                             WHERE ps.partner_id = pi.partner_id
                             AND ps.month >= pi.date_joined::date
                             AND ps.month <= CURRENT_DATE
                             AND ps.direct_deposits > 0),
                            0
                        ) as deposited_months,
                        -- Get traded clients count
                        COALESCE(
                            (SELECT SUM(ps.traded_clients)
                             FROM partner.partner_summary_monthly ps
                             WHERE ps.partner_id = pi.partner_id
                             AND ps.month >= pi.date_joined::date
                             AND ps.month <= CURRENT_DATE),
                            0
                        ) as total_traded_clients,
                        -- Calculate total deposits
                        COALESCE(
                            (SELECT SUM(ps.total_deposits)
                             FROM partner.partner_summary_monthly ps
                             WHERE ps.partner_id = pi.partner_id
                             AND ps.month >= pi.date_joined::date
                             AND ps.month <= CURRENT_DATE),
                            0
                        ) as total_deposits,
                        -- Days since joined
                        CURRENT_DATE - pi.date_joined::date as days_since_joined
                    FROM partner.partner_info pi
                    WHERE 
                        (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        AND pi.date_joined >= CURRENT_DATE - INTERVAL '%s days'
                        {country_filter}
                )
                SELECT 
                    partner_id,
                    partner_join_date,
                    total_client_signups as number_of_client_signups,
                    total_subaff_signups as number_of_subaff_signups,
                    CASE 
                        WHEN total_deposits > 0 THEN total_client_signups  -- Assume all clients who deposited
                        ELSE 0 
                    END as number_of_deposited_client,
                    total_traded_clients as number_of_traded_client,
                    days_since_joined,
                    partner_country,
                    aff_type,
                    first_earning_date,
                    -- Categorize partner status
                    CASE 
                        WHEN first_earning_date IS NOT NULL THEN 'Earning'
                        WHEN total_traded_clients > 0 THEN 'Has Traded Clients'
                        WHEN total_deposits > 0 THEN 'Has Deposits'
                        WHEN total_client_signups > 0 THEN 'Has Signups'
                        ELSE 'No Activity'
                    END as partner_status,
                    -- Flag potential high-value partners
                    CASE 
                        WHEN total_client_signups >= 10 AND first_earning_date IS NULL THEN TRUE
                        WHEN total_subaff_signups >= 5 AND first_earning_date IS NULL THEN TRUE
                        WHEN days_since_joined > 30 AND total_client_signups > 0 AND first_earning_date IS NULL THEN TRUE
                        ELSE FALSE
                    END as high_potential
                FROM partner_metrics
                WHERE 
                    (first_earning_date IS NULL  -- Not yet earning
                     OR (days_since_joined <= 30))  -- Or very new partners
                ORDER BY 
                    high_potential DESC,
                    total_client_signups DESC,
                    days_since_joined DESC
                LIMIT %s;
            """
            
            query_params = [date_range]
            if partner_country and partner_country != 'All':
                query_params.append(partner_country)
            query_params.append(limit)
            
            cursor.execute(query, query_params)
            
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            columns = [
                'partner_id', 'partner_join_date', 'number_of_client_signups', 
                'number_of_subaff_signups', 'number_of_deposited_client',
                'number_of_traded_client', 'days_since_joined', 'partner_country',
                'aff_type', 'first_earning_date', 'partner_status', 'high_potential'
            ]
            
            data = []
            for row in results:
                partner_dict = dict(zip(columns, row))
                # Format dates
                if partner_dict['partner_join_date']:
                    partner_dict['partner_join_date'] = partner_dict['partner_join_date'].strftime('%Y-%m-%d')
                if partner_dict['first_earning_date']:
                    partner_dict['first_earning_date'] = partner_dict['first_earning_date'].strftime('%Y-%m-%d')
                # Convert numeric fields to int
                for field in ['number_of_client_signups', 'number_of_subaff_signups', 
                            'number_of_deposited_client', 'number_of_traded_client']:
                    partner_dict[field] = int(partner_dict[field])
                data.append(partner_dict)
            
            # Get summary statistics
            cursor.execute(f"""
                WITH new_partners AS (
                    SELECT 
                        pi.partner_id,
                        pi.first_earning_date,
                        pi.date_joined,
                        CURRENT_DATE - pi.date_joined::date as days_since_joined,
                        COALESCE(
                            (SELECT SUM(ps.client_signups)
                             FROM partner.partner_summary_monthly ps
                             WHERE ps.partner_id = pi.partner_id),
                            0
                        ) as total_signups
                    FROM partner.partner_info pi
                    WHERE 
                        (pi.is_internal = FALSE OR pi.is_internal IS NULL)
                        AND pi.date_joined >= CURRENT_DATE - INTERVAL '%s days'
                        {country_filter}
                )
                SELECT 
                    COUNT(*) as total_new_partners,
                    COUNT(CASE WHEN first_earning_date IS NULL THEN 1 END) as not_yet_earning,
                    COUNT(CASE WHEN first_earning_date IS NOT NULL THEN 1 END) as already_earning,
                    COUNT(CASE WHEN total_signups > 0 AND first_earning_date IS NULL THEN 1 END) as has_activity_no_earnings,
                    COUNT(CASE WHEN days_since_joined > 30 AND first_earning_date IS NULL THEN 1 END) as stuck_over_30_days
                FROM new_partners;
            """, query_params[:-1])  # Remove limit parameter
            
            summary_result = cursor.fetchone()
            summary_columns = ['total_new_partners', 'not_yet_earning', 'already_earning', 
                             'has_activity_no_earnings', 'stuck_over_30_days']
            summary_data = dict(zip(summary_columns, summary_result)) if summary_result else {}
            
            return {
                'new_partners': data,
                'summary': summary_data,
                'date_range': date_range,
                'partner_country': partner_country,
                'limit': limit
            }

    except Exception as e:
        logger.error(f"Error getting new partner support data: {str(e)}")
        return {
            'new_partners': [],
            'summary': {},
            'date_range': date_range,
            'partner_country': partner_country,
            'limit': limit
        }
    finally:
        conn.close()