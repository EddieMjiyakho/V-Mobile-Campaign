-- Subscriber Details for Drill-Through Analysis (PostgreSQL)
WITH weekly_subscriber_revenue AS (
    SELECT 
        msisdn,
        DATE_TRUNC('week', usage_event_date_time)::DATE AS week_start,
        SUM(usage_event_revenue) AS weekly_revenue,
        COUNT(*) AS total_events,
        COUNT(CASE WHEN usage_event_type_id IN (6, 10) THEN 1 END) AS sms_events,
        COUNT(CASE WHEN usage_event_type_id IN (3, 4, 5, 8, 9) THEN 1 END) AS voice_events
    FROM usage_records
    WHERE usage_event_date_time IS NOT NULL
    GROUP BY msisdn, DATE_TRUNC('week', usage_event_date_time)::DATE
)
SELECT 
    wr.msisdn,
    wr.week_start,
    wr.weekly_revenue,
    wr.total_events,
    wr.sms_events,
    wr.voice_events,
    CASE WHEN wr.weekly_revenue >= 30 THEN 1 ELSE 0 END AS is_qualifying,
    ms.first_name,
    ms.last_name,
    ms.region,
    ms.date_of_birth,
    ms.sim_activation_date,
    ms.source_system_name
FROM weekly_subscriber_revenue wr
LEFT JOIN master_subscribers ms ON wr.msisdn::bigint = ms.cell_phone_number
ORDER BY wr.week_start, wr.weekly_revenue DESC;