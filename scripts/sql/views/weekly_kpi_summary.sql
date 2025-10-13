-- Weekly KPI Summary View (PostgreSQL)
CREATE OR REPLACE VIEW weekly_kpi_summary AS
SELECT 
    week_start,
    total_subscribers,
    qualifying_subscribers,
    total_revenue,
    qualification_rate,
    total_revenue / NULLIF(qualifying_subscribers, 0) AS avg_revenue_per_qualifier,
    total_events,
    qualifying_subscribers - LAG(qualifying_subscribers) OVER (ORDER BY week_start) AS subscriber_growth,
    total_revenue - LAG(total_revenue) OVER (ORDER BY week_start) AS revenue_growth
FROM weekly_summary_trends;