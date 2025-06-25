import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timedelta
import json
from typing import Dict, List, Any, Tuple
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

# Database configuration
db_params = {
    'host': os.getenv('host'),
    'port': os.getenv('port'),
    'database': os.getenv('dbname'),
    'user': os.getenv('user'),
    'password': os.getenv('password')
}

# OpenAI configuration for insights
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
API_BASE_URL = os.getenv('API_BASE_URL')
OPENAI_MODEL_NAME = os.getenv('OPENAI_MODEL_NAME')

def get_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(**db_params)

def get_spotlight_dashboard_data(date_range: int = 90) -> Dict[str, Any]:
    """Get comprehensive spotlight dashboard data with all insights
    
    Args:
        date_range: Number of days to look back for data (30, 60, 90, 180, 365, or 0 for all time)
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET statement_timeout = '30s'")
            
            # Build date filter clause
            if date_range > 0:
                date_filter = f"AND date_joined >= CURRENT_DATE - INTERVAL '{date_range} days'"
                date_filter_earnings = f"AND first_earning_date >= CURRENT_DATE - INTERVAL '{date_range} days'"
            else:
                date_filter = ""  # All time
                date_filter_earnings = ""
            
            # Calculate total applications and activation rate for the selected period
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT partner_id) as total_applications,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as activated_partners,
                    ROUND(
                        (COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END)::numeric / 
                         NULLIF(COUNT(DISTINCT partner_id), 0)) * 100, 2
                    ) as overall_activation_rate
                FROM partner.partner_info
                WHERE is_internal = FALSE
                {date_filter}
            """)
            overview_metrics = cursor.fetchone()
            
            # 1. Partner Acquisition Effectiveness (Events & VAN Trips)
            # VAN Trip Countries Performance
            cursor.execute(f"""
                SELECT 
                    partner_country as country,
                    COUNT(DISTINCT partner_id) as total_applications,
                    COUNT(DISTINCT CASE WHEN deriv_van_count > 0 THEN partner_id END) as van_trip_partners,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as activated_partners,
                    COUNT(DISTINCT CASE WHEN deriv_van_count > 0 AND first_earning_date IS NOT NULL 
                          THEN partner_id END) as van_activated,
                    ROUND(
                        CASE 
                            WHEN COUNT(DISTINCT CASE WHEN deriv_van_count > 0 THEN partner_id END) > 0
                            THEN (COUNT(DISTINCT CASE WHEN deriv_van_count > 0 AND first_earning_date IS NOT NULL 
                                  THEN partner_id END)::numeric / 
                                  COUNT(DISTINCT CASE WHEN deriv_van_count > 0 THEN partner_id END)) * 100
                            ELSE 0
                        END, 2
                    ) as van_activation_rate,
                    SUM(CASE WHEN deriv_van_count > 0 
                        THEN COALESCE(turnover_earnings, 0) + COALESCE(revenue_share_earnings, 0) + 
                             COALESCE(ib_earnings, 0) + COALESCE(cpa_deposit_earnings, 0) 
                        ELSE 0 END) as van_earnings
                FROM partner.partner_info
                WHERE is_internal = FALSE
                {date_filter}
                AND partner_country IS NOT NULL
                GROUP BY partner_country
                HAVING COUNT(DISTINCT partner_id) >= 10
                ORDER BY van_earnings DESC
                LIMIT 15
            """)
            van_trip_effectiveness = cursor.fetchall()
            
            # Calculate VAN Trip ROI (total earnings from VAN trip partners)
            cursor.execute(f"""
                SELECT 
                    SUM(COALESCE(turnover_earnings, 0) + COALESCE(revenue_share_earnings, 0) + 
                        COALESCE(ib_earnings, 0) + COALESCE(cpa_deposit_earnings, 0)) as total_van_earnings,
                    COUNT(DISTINCT partner_id) as total_van_partners
                FROM partner.partner_info
                WHERE is_internal = FALSE
                AND deriv_van_count > 0
                {date_filter if date_range > 0 else ""}
            """)
            van_roi_data = cursor.fetchone()
            
            # Event Impact by Type
            cursor.execute(f"""
                WITH event_partners AS (
                    SELECT 
                        partner_id,
                        partner_country,
                        date_joined,
                        first_earning_date,
                        CASE 
                            WHEN deriv_van_count > 0 THEN 'VAN Trip'
                            WHEN conference_count > 0 THEN 'Conference'
                            WHEN webinar_count > 0 THEN 'Webinar'
                            WHEN seminar_count > 0 THEN 'Seminar'
                            WHEN sponsorship_event_count > 0 THEN 'Sponsorship'
                            ELSE 'No Event'
                        END as event_type,
                        (webinar_count + seminar_count + sponsorship_event_count + 
                         deriv_van_count + conference_count) as total_events
                    FROM partner.partner_info
                    WHERE is_internal = FALSE
                    {date_filter}
                )
                SELECT 
                    event_type,
                    COUNT(DISTINCT partner_id) as partner_count,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as activated_count,
                    ROUND(
                        (COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END)::numeric / 
                         COUNT(DISTINCT partner_id)) * 100, 2
                    ) as activation_rate
                FROM event_partners
                GROUP BY event_type
                ORDER BY activation_rate DESC
            """)
            event_impact = cursor.fetchall()
            
            # 2. Conversion & Activation Funnel
            # Country-wise conversion funnel
            cursor.execute(f"""
                SELECT 
                    partner_country as country,
                    COUNT(DISTINCT partner_id) as applications,
                    COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) as with_signups,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as activated,
                    ROUND(
                        (COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END)::numeric / 
                         NULLIF(COUNT(DISTINCT partner_id), 0)) * 100, 2
                    ) as signup_rate,
                    ROUND(
                        (COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END)::numeric / 
                         NULLIF(COUNT(DISTINCT partner_id), 0)) * 100, 2
                    ) as activation_rate,
                    ROUND(
                        AVG(CASE 
                            WHEN first_earning_date IS NOT NULL 
                            THEN EXTRACT(EPOCH FROM (first_earning_date::timestamp - date_joined::timestamp)) / 86400.0
                            ELSE NULL 
                        END), 1
                    ) as avg_days_to_activate
                FROM partner.partner_info
                WHERE is_internal = FALSE
                {date_filter}
                AND partner_country IS NOT NULL
                GROUP BY partner_country
                HAVING COUNT(DISTINCT partner_id) >= 5
                ORDER BY activation_rate DESC
                LIMIT 20
            """)
            conversion_funnel = cursor.fetchall()
            
            # 3. Quality and Retention of Partner Network
            # Platform comparison (DW vs MyAffiliates)
            cursor.execute(f"""
                WITH platform_metrics AS (
                    SELECT 
                        partner_platform,
                        COUNT(DISTINCT partner_id) as total_partners,
                        COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as active_partners,
                        COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END) as currently_active,
                        COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '90 days'
                              AND last_earning_date < CURRENT_DATE - INTERVAL '30 days'
                              THEN partner_id END) as at_risk,
                        AVG(COALESCE(turnover_earnings, 0) + COALESCE(revenue_share_earnings, 0) + 
                            COALESCE(ib_earnings, 0) + COALESCE(cpa_deposit_earnings, 0)) as avg_lifetime_value
                    FROM partner.partner_info
                    WHERE is_internal = FALSE
                    {date_filter if date_range > 0 else ""}
                    GROUP BY partner_platform
                )
                SELECT 
                    partner_platform,
                    total_partners,
                    active_partners,
                    currently_active,
                    at_risk,
                    ROUND((currently_active::numeric / NULLIF(active_partners, 0)) * 100, 2) as retention_rate,
                    ROUND(avg_lifetime_value::numeric, 2) as avg_lifetime_value
                FROM platform_metrics
                ORDER BY partner_platform
            """)
            platform_comparison = cursor.fetchall()
            
            # Calculate Network Retention (% of partners who earned in last 30 days vs those who ever earned)
            cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '30 days' 
                          THEN partner_id END) as active_30d,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as total_activated,
                    ROUND(
                        (COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END)::numeric / 
                         NULLIF(COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END), 0)) * 100, 2
                    ) as network_retention_rate
                FROM partner.partner_info
                WHERE is_internal = FALSE
                {date_filter if date_range > 0 else ""}
            """)
            network_retention = cursor.fetchone()
            
            # Partner Retention Cohorts
            cursor.execute(f"""
                WITH cohort_data AS (
                    SELECT 
                        TO_CHAR(DATE_TRUNC('month', date_joined), 'Mon YY') as cohort_month,
                        DATE_TRUNC('month', date_joined) as cohort_date,
                        COUNT(DISTINCT partner_id) as cohort_size,
                        COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END) as m0_active,
                        COUNT(DISTINCT CASE WHEN last_earning_date >= DATE_TRUNC('month', date_joined) + INTERVAL '1 month'
                              THEN partner_id END) as m1_active,
                        COUNT(DISTINCT CASE WHEN last_earning_date >= DATE_TRUNC('month', date_joined) + INTERVAL '3 months'
                              THEN partner_id END) as m3_active,
                        COUNT(DISTINCT CASE WHEN last_earning_date >= DATE_TRUNC('month', date_joined) + INTERVAL '6 months'
                              THEN partner_id END) as m6_active
                    FROM partner.partner_info
                    WHERE is_internal = FALSE
                    AND first_earning_date IS NOT NULL
                    AND date_joined >= CURRENT_DATE - INTERVAL '12 months'
                    GROUP BY DATE_TRUNC('month', date_joined)
                )
                SELECT 
                    cohort_month,
                    cohort_size,
                    ROUND((m0_active::numeric / cohort_size) * 100, 2) as current_retention,
                    ROUND((m1_active::numeric / cohort_size) * 100, 2) as m1_retention,
                    ROUND((m3_active::numeric / cohort_size) * 100, 2) as m3_retention,
                    ROUND((m6_active::numeric / cohort_size) * 100, 2) as m6_retention
                FROM cohort_data
                ORDER BY cohort_date DESC
                LIMIT 12
            """)
            retention_cohorts = cursor.fetchall()
            
            # 4. Country & Regional ROI
            cursor.execute(f"""
                WITH country_roi AS (
                    SELECT 
                        p.partner_country as country,
                        p.partner_region as region,
                        COUNT(DISTINCT p.partner_id) as total_partners,
                        COUNT(DISTINCT CASE WHEN p.first_earning_date IS NOT NULL THEN p.partner_id END) as active_partners,
                        COUNT(DISTINCT CASE WHEN p.last_earning_date >= CURRENT_DATE - INTERVAL '30 days' 
                              THEN p.partner_id END) as currently_active,
                        SUM(COALESCE(p.turnover_earnings, 0) + COALESCE(p.revenue_share_earnings, 0) + 
                            COALESCE(p.ib_earnings, 0) + COALESCE(p.cpa_deposit_earnings, 0)) as total_earnings,
                        COUNT(DISTINCT CASE WHEN p.date_joined >= CURRENT_DATE - INTERVAL '30 days' 
                              THEN p.partner_id END) as new_partners_30d,
                        COUNT(DISTINCT CASE WHEN p.last_earning_date >= CURRENT_DATE - INTERVAL '30 days'
                              AND p.first_earning_date < CURRENT_DATE - INTERVAL '30 days'
                              THEN p.partner_id END) as reactivated_30d
                    FROM partner.partner_info p
                    WHERE p.is_internal = FALSE
                    AND p.partner_country IS NOT NULL
                    {date_filter if date_range > 0 else ""}
                    GROUP BY p.partner_country, p.partner_region
                    HAVING COUNT(DISTINCT p.partner_id) >= 5
                )
                SELECT 
                    country,
                    region,
                    total_partners,
                    active_partners,
                    currently_active,
                    ROUND(total_earnings::numeric, 2) as total_earnings,
                    ROUND((total_earnings::numeric / NULLIF(active_partners, 0)), 2) as earnings_per_partner,
                    new_partners_30d,
                    reactivated_30d,
                    ROUND((currently_active::numeric / NULLIF(active_partners, 0)) * 100, 2) as retention_rate
                FROM country_roi
                ORDER BY total_earnings DESC
                LIMIT 20
            """)
            country_roi = cursor.fetchall()
            
            # 5. Underperforming Countries - prioritize countries with significant volume
            cursor.execute(f"""
                WITH country_performance AS (
                    SELECT 
                        partner_country as country,
                        COUNT(DISTINCT CASE WHEN date_joined >= CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END) as recent_signups,
                        COUNT(DISTINCT CASE WHEN date_joined >= CURRENT_DATE - INTERVAL '60 days'
                              AND date_joined < CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END) as previous_signups,
                        COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END) as active_partners,
                        COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '60 days'
                              AND last_earning_date < CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END) as previously_active,
                        COUNT(DISTINCT partner_id) as total_partners
                    FROM partner.partner_info
                    WHERE is_internal = FALSE
                    AND partner_country IS NOT NULL
                    {date_filter if date_range > 0 else ""}
                    GROUP BY partner_country
                    HAVING COUNT(DISTINCT CASE WHEN date_joined >= CURRENT_DATE - INTERVAL '30 days' 
                           THEN partner_id END) >= 15  -- At least 15 recent applications
                    AND COUNT(DISTINCT CASE WHEN last_earning_date >= CURRENT_DATE - INTERVAL '60 days' 
                        THEN partner_id END) > 0  -- Must have had some activity to compare
                )
                SELECT 
                    country,
                    recent_signups,
                    active_partners,
                    ROUND(
                        CASE 
                            WHEN previous_signups = 0 THEN 0
                            ELSE ((recent_signups - previous_signups)::numeric / previous_signups) * 100
                        END, 2
                    ) as signup_change,
                    ROUND(
                        CASE 
                            WHEN previously_active = 0 THEN 0
                            ELSE ((active_partners - previously_active)::numeric / previously_active) * 100
                        END, 2
                    ) as activity_change,
                    ROUND((active_partners::numeric / NULLIF(recent_signups, 0)) * 100, 2) as activation_rate
                FROM country_performance
                WHERE (active_partners::numeric / NULLIF(recent_signups, 0)) < 0.2  -- Less than 20% activation rate
                OR active_partners < previously_active * 0.8  -- 20% or more decline in active partners
                ORDER BY recent_signups DESC, activity_change ASC
                LIMIT 10
            """)
            underperforming_countries = cursor.fetchall()
            
            # 6. Monthly Trends for Chart
            cursor.execute(f"""
                SELECT 
                    TO_CHAR(DATE_TRUNC('month', date_joined), 'Mon YY') as month,
                    COUNT(DISTINCT partner_id) as applications,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as activations,
                    ROUND(
                        (COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END)::numeric / 
                         NULLIF(COUNT(DISTINCT partner_id), 0)) * 100, 2
                    ) as activation_rate
                FROM partner.partner_info
                WHERE is_internal = FALSE
                AND date_joined >= CURRENT_DATE - INTERVAL '12 months'
                GROUP BY DATE_TRUNC('month', date_joined)
                ORDER BY DATE_TRUNC('month', date_joined)
            """)
            monthly_trends = cursor.fetchall()
            
            # 7. Top Growing Countries
            cursor.execute("""
                WITH country_growth AS (
                    SELECT 
                        partner_country as country,
                        COUNT(DISTINCT CASE WHEN date_joined >= CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END) as current_signups,
                        COUNT(DISTINCT CASE WHEN date_joined >= CURRENT_DATE - INTERVAL '60 days' 
                              AND date_joined < CURRENT_DATE - INTERVAL '30 days' 
                              THEN partner_id END) as previous_signups
                    FROM partner.partner_info
                    WHERE is_internal = FALSE
                    AND partner_country IS NOT NULL
                    AND date_joined >= CURRENT_DATE - INTERVAL '60 days'
                    GROUP BY partner_country
                    HAVING COUNT(DISTINCT CASE WHEN date_joined >= CURRENT_DATE - INTERVAL '30 days' 
                           THEN partner_id END) >= 5
                )
                SELECT 
                    country,
                    current_signups,
                    previous_signups,
                    ROUND(
                        CASE 
                            WHEN previous_signups = 0 THEN 100
                            ELSE ((current_signups - previous_signups)::numeric / previous_signups) * 100
                        END, 2
                    ) as growth_rate
                FROM country_growth
                WHERE current_signups > 0
                ORDER BY growth_rate DESC
                LIMIT 10
            """)
            top_growing_countries = cursor.fetchall()
            
            # Generate AI insights if API key is available
            ai_insights = None
            if OPENAI_API_KEY and API_BASE_URL and OPENAI_MODEL_NAME:
                try:
                    llm = ChatOpenAI(
                        api_key=OPENAI_API_KEY,
                        base_url=API_BASE_URL,
                        model=OPENAI_MODEL_NAME,
                        temperature=0.7
                    )
                    
                    # Prepare data summary for AI
                    data_summary = f"""
                    Partner Acquisition Analysis:
                    - Top VAN Trip country: {van_trip_effectiveness[0]['country'] if van_trip_effectiveness else 'N/A'} 
                      with {van_trip_effectiveness[0]['van_activation_rate'] if van_trip_effectiveness else 0}% activation rate
                    - Most effective event type: {event_impact[0]['event_type'] if event_impact else 'N/A'} 
                      with {event_impact[0]['activation_rate'] if event_impact else 0}% activation rate
                    - Best converting country: {conversion_funnel[0]['country'] if conversion_funnel else 'N/A'} 
                      with {conversion_funnel[0]['activation_rate'] if conversion_funnel else 0}% activation rate
                    - Network retention rate: {network_retention['network_retention_rate'] if network_retention else 0}%
                    - Underperforming countries: {len(underperforming_countries)} countries with high volume but low activation
                    """
                    
                    messages = [
                        SystemMessage(content="""You are a partner analytics expert. Provide exactly 4 actionable insights.
                        Format your response as a JSON array with this structure:
                        [
                            {
                                "title": "Short actionable title",
                                "insight": "Brief explanation of the finding",
                                "recommendation": "Specific action to take"
                            }
                        ]
                        Keep each part concise and focused."""),
                        HumanMessage(content=f"Based on this partner acquisition data, what are the key insights and recommendations?\n\n{data_summary}")
                    ]
                    
                    response = llm.invoke(messages)
                    # Try to parse as JSON, fallback to text if fails
                    try:
                        ai_insights = json.loads(response.content)
                    except:
                        ai_insights = response.content
                    
                except Exception as e:
                    logger.error(f"Error generating AI insights: {str(e)}")
                    ai_insights = None
            
            return {
                'overview_metrics': overview_metrics,
                'van_trip_effectiveness': van_trip_effectiveness,
                'van_roi_data': van_roi_data,
                'event_impact': event_impact,
                'conversion_funnel': conversion_funnel,
                'platform_comparison': platform_comparison,
                'network_retention': network_retention,
                'retention_cohorts': retention_cohorts,
                'country_roi': country_roi,
                'underperforming_countries': underperforming_countries,
                'monthly_trends': monthly_trends,
                'top_growing_countries': top_growing_countries,
                'ai_insights': ai_insights,
                'date_range': date_range,
                'last_updated': datetime.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error in spotlight dashboard: {str(e)}")
        return {
            'overview_metrics': {'total_applications': 0, 'activated_partners': 0, 'overall_activation_rate': 0},
            'van_trip_effectiveness': [],
            'van_roi_data': {'total_van_earnings': 0, 'total_van_partners': 0},
            'event_impact': [],
            'conversion_funnel': [],
            'platform_comparison': [],
            'network_retention': {'network_retention_rate': 0},
            'retention_cohorts': [],
            'country_roi': [],
            'underperforming_countries': [],
            'monthly_trends': [],
            'top_growing_countries': [],
            'ai_insights': None,
            'date_range': date_range,
            'last_updated': datetime.now().isoformat(),
            'error': str(e)
        }
    finally:
        conn.close() 

def get_funnel_metrics(date_range: int = 90, country: str = None) -> Dict[str, Any]:
    """Get detailed conversion funnel metrics
    
    Args:
        date_range: Number of days to look back for data (30, 60, 90, 180, 365, or 0 for all time)
        country: Optional country filter
    """
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
            if country:
                country_filter = "AND p.partner_country = %(country)s"
            
            # Calculate previous period for comparison
            if date_range > 0:
                prev_start = f"CURRENT_DATE - INTERVAL '{date_range * 2} days'"
                prev_end = f"CURRENT_DATE - INTERVAL '{date_range} days'"
            else:
                # For all time, compare to last 90 days
                prev_start = "CURRENT_DATE - INTERVAL '180 days'"
                prev_end = "CURRENT_DATE - INTERVAL '90 days'"
            
            # Main funnel query with client data
            query = f"""
            WITH partner_clients AS (
                SELECT 
                    p.partner_id,
                    p.partner_country,
                    p.date_joined as partner_joined_date,
                    p.first_client_joined_date,
                    p.first_client_deposit_date,
                    p.first_client_trade_date,
                    p.first_earning_date,
                    CASE WHEN p.first_client_joined_date IS NOT NULL THEN 
                        EXTRACT(EPOCH FROM (p.first_client_joined_date::timestamp - p.date_joined::timestamp)) / 86400.0
                    END as days_to_first_client,
                    CASE WHEN p.last_earning_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 ELSE 0 END as is_active_last_30d
                FROM partner.partner_info p
                WHERE p.is_internal = FALSE
                    {date_filter}
                    {country_filter}
            ),
            previous_period_metrics AS (
                SELECT 
                    COUNT(DISTINCT partner_id) as prev_total_applications
                FROM partner.partner_info p
                WHERE p.is_internal = FALSE
                    AND p.date_joined >= {prev_start}
                    AND p.date_joined < {prev_end}
                    {country_filter}
            ),
            current_period_metrics AS (
                SELECT 
                    COUNT(DISTINCT partner_id) as total_applications,
                    COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) as signup_activations,
                    COUNT(DISTINCT CASE WHEN first_client_deposit_date IS NOT NULL THEN partner_id END) as deposit_activations,
                    COUNT(DISTINCT CASE WHEN first_client_trade_date IS NOT NULL THEN partner_id END) as trade_activations,
                    COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) as earning_activations,
                    AVG(days_to_first_client) as avg_days_to_activation,
                    COUNT(DISTINCT CASE WHEN is_active_last_30d = 1 THEN partner_id END) as active_partners_30d
                FROM partner_clients
            )
            SELECT 
                cm.*,
                pm.prev_total_applications,
                ROUND(CAST(cm.signup_activations AS NUMERIC) / NULLIF(cm.total_applications, 0) * 100, 1) as activation_rate,
                ROUND(CAST(cm.active_partners_30d AS NUMERIC) / NULLIF(cm.total_applications, 0) * 100, 1) as active_partners_rate,
                ROUND(CAST((cm.total_applications - pm.prev_total_applications) AS NUMERIC) / NULLIF(pm.prev_total_applications, 0) * 100, 1) as application_growth_rate,
                -- Stage-by-stage conversion rates
                ROUND(CAST(cm.signup_activations AS NUMERIC) / NULLIF(cm.total_applications, 0) * 100, 1) as apps_to_signup_rate,
                ROUND(CAST(cm.deposit_activations AS NUMERIC) / NULLIF(cm.signup_activations, 0) * 100, 1) as signup_to_deposit_rate,
                ROUND(CAST(cm.trade_activations AS NUMERIC) / NULLIF(cm.deposit_activations, 0) * 100, 1) as deposit_to_trade_rate,
                ROUND(CAST(cm.earning_activations AS NUMERIC) / NULLIF(cm.trade_activations, 0) * 100, 1) as trade_to_earning_rate
            FROM current_period_metrics cm
            CROSS JOIN previous_period_metrics pm;
            """
            
            params = {'country': country} if country else {}
            cursor.execute(query, params)
            funnel_overview = cursor.fetchone()
            
            # Get available countries for filter
            cursor.execute("""
                SELECT DISTINCT partner_country
                FROM partner.partner_info
                WHERE is_internal = FALSE
                    AND partner_country IS NOT NULL
                ORDER BY partner_country;
            """)
            available_countries = [row['partner_country'] for row in cursor.fetchall()]
            
            # Country performance for table (unfiltered to show comparisons)
            country_query = f"""
            WITH partner_clients AS (
                SELECT 
                    p.partner_id,
                    p.partner_country,
                    p.date_joined as partner_joined_date,
                    p.first_client_joined_date,
                    p.first_client_deposit_date,
                    p.first_client_trade_date,
                    p.first_earning_date,
                    CASE WHEN p.first_client_joined_date IS NOT NULL THEN 
                        EXTRACT(EPOCH FROM (p.first_client_joined_date::timestamp - p.date_joined::timestamp)) / 86400.0
                    END as days_to_activation,
                    CASE WHEN p.last_earning_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 ELSE 0 END as is_active_last_30d
                FROM partner.partner_info p
                WHERE p.is_internal = FALSE
                    {date_filter}
                    AND p.partner_country IS NOT NULL
            ),
            country_metrics AS (
                SELECT 
                    partner_country,
                    COUNT(DISTINCT partner_id) as total_applications,
                    COUNT(DISTINCT CASE WHEN first_client_joined_date IS NOT NULL THEN partner_id END) as activated_partners,
                    AVG(days_to_activation) as avg_days_to_activation,
                    ROUND(
                        CAST(COUNT(DISTINCT CASE WHEN is_active_last_30d = 1 THEN partner_id END) AS NUMERIC) /
                        NULLIF(COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END), 0) * 100,
                        1
                    ) as retention_rate,
                    ROUND(
                        CAST(COUNT(DISTINCT CASE WHEN first_earning_date IS NOT NULL THEN partner_id END) AS NUMERIC) /
                        NULLIF(COUNT(DISTINCT partner_id), 0) * 100,
                        1
                    ) as activation_rate
                FROM partner_clients
                GROUP BY partner_country
                HAVING COUNT(DISTINCT partner_id) >= 5
            )
            SELECT *,
                RANK() OVER (ORDER BY activation_rate DESC) as rank_by_activation,
                RANK() OVER (ORDER BY retention_rate DESC) as rank_by_retention
            FROM country_metrics
            ORDER BY activation_rate DESC;
            """
            
            cursor.execute(country_query)
            country_performance = cursor.fetchall()
            
            return {
                'funnel_overview': funnel_overview,
                'country_performance': country_performance,
                'available_countries': available_countries,
                'selected_country': country,
                'date_range': date_range,
                'last_updated': datetime.now().isoformat()
            }
            
    except Exception as e:
        logger.error(f"Error in funnel metrics: {str(e)}")
        return {
            'funnel_overview': {
                'total_applications': 0,
                'signup_activations': 0,
                'deposit_activations': 0,
                'trade_activations': 0,
                'earning_activations': 0,
                'activation_rate': 0,
                'active_partners_rate': 0,
                'application_growth_rate': 0,
                'avg_days_to_activation': 0,
                'apps_to_signup_rate': 0,
                'signup_to_deposit_rate': 0,
                'deposit_to_trade_rate': 0,
                'trade_to_earning_rate': 0
            },
            'country_performance': [],
            'available_countries': [],
            'selected_country': country,
            'date_range': date_range,
            'last_updated': datetime.now().isoformat(),
            'error': str(e)
        }
    finally:
        conn.close() 