-- Regional Analysis with City Lookup (PostgreSQL)
WITH subscriber_regions AS (
    SELECT 
        ur.msisdn,
        ur.usage_event_date_time,
        ur.usage_event_revenue,
        COALESCE(ms.region, cl."CITY_NAME", 'Unknown') AS region_name,
        cl."PROVINCE_NAME" AS province_name,
        DATE_TRUNC('week', ur.usage_event_date_time)::DATE AS week_start
    FROM usage_records ur
    LEFT JOIN master_subscribers ms ON ur.msisdn::bigint = ms.cell_phone_number
    LEFT JOIN city_lookup cl ON ur.usage_event_city_id = cl."CITY_ID"
    WHERE ur.usage_event_date_time IS NOT NULL
),
weekly_regional_usage AS (
    SELECT 
        week_start,
        region_name,
        province_name,
        COUNT(DISTINCT msisdn) AS total_subscribers,
        SUM(usage_event_revenue) AS total_revenue
    FROM subscriber_regions
    GROUP BY week_start, region_name, province_name
),
weekly_regional_qualifiers AS (
    SELECT 
        week_start,
        region_name,
        province_name,
        msisdn,
        SUM(usage_event_revenue) AS weekly_revenue
    FROM subscriber_regions
    GROUP BY week_start, region_name, province_name, msisdn
    HAVING SUM(usage_event_revenue) >= 30
),
weekly_regional_qualifier_count AS (
    SELECT 
        week_start,
        region_name,
        province_name,
        COUNT(*) AS qualifying_subscribers,
        AVG(weekly_revenue) AS avg_qualifier_revenue
    FROM weekly_regional_qualifiers
    GROUP BY week_start, region_name, province_name
)
SELECT 
    w.week_start,
    w.region_name,
    w.province_name,
    w.total_subscribers,
    w.total_revenue,
    COALESCE(q.qualifying_subscribers, 0) AS qualifying_subscribers,
    COALESCE(q.avg_qualifier_revenue, 0) AS avg_qualifier_revenue,
    CASE 
        WHEN w.total_subscribers > 0 THEN 
            ROUND((COALESCE(q.qualifying_subscribers, 0) * 100.0 / w.total_subscribers), 2)
        ELSE 0 
    END AS qualification_rate
FROM weekly_regional_usage w
LEFT JOIN weekly_regional_qualifier_count q 
    ON w.week_start = q.week_start 
    AND w.region_name = q.region_name
ORDER BY w.week_start, w.total_revenue DESC;