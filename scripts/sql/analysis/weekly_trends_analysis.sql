-- Weekly Trends Analysis (PostgreSQL)
-- FIX total subcribers and qualification rate
WITH weekly_aggregates AS (
    SELECT 
        DATE_TRUNC('week', usage_event_date_time)::DATE AS week_start,
        COUNT(DISTINCT msisdn) AS total_subscribers,
        SUM(usage_event_revenue) AS total_revenue,
        COUNT(*) AS total_events
    FROM usage_records
    WHERE usage_event_date_time IS NOT NULL
    GROUP BY DATE_TRUNC('week', usage_event_date_time)::DATE
),
qualifying_subscribers AS (
    SELECT 
        DATE_TRUNC('week', usage_event_date_time)::DATE AS week_start,
        msisdn,
        SUM(usage_event_revenue) AS weekly_revenue
    FROM usage_records
    WHERE usage_event_date_time IS NOT NULL
    GROUP BY DATE_TRUNC('week', usage_event_date_time)::DATE, msisdn
    HAVING SUM(usage_event_revenue) >= 30
),
weekly_qualifiers AS (
    SELECT 
        week_start,
        COUNT(*) AS qualifying_subscribers,
        AVG(weekly_revenue) AS avg_qualifier_revenue
    FROM qualifying_subscribers
    GROUP BY week_start
)
SELECT 
    w.week_start,
    w.total_subscribers,
    w.total_revenue,
    w.total_events,
    COALESCE(q.qualifying_subscribers, 0) AS qualifying_subscribers,
    COALESCE(q.avg_qualifier_revenue, 0) AS avg_qualifier_revenue,
    CASE 
        WHEN w.total_subscribers > 0 THEN 
            ROUND((COALESCE(q.qualifying_subscribers, 0) * 100.0 / w.total_subscribers), 2)
        ELSE 0 
    END AS qualification_rate
FROM weekly_aggregates w
LEFT JOIN weekly_qualifiers q ON w.week_start = q.week_start
ORDER BY w.week_start;