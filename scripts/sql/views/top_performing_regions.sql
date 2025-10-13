-- Top Performing Regions View (PostgreSQL)
CREATE OR REPLACE VIEW top_performing_regions AS
SELECT 
    region_name,
    province_name,
    SUM(total_subscribers) AS total_subscribers,
    SUM(qualifying_subscribers) AS total_qualifiers,
    SUM(total_revenue) AS total_revenue,
    AVG(qualification_rate) AS avg_qualification_rate,
    RANK() OVER (ORDER BY SUM(qualifying_subscribers) DESC) AS performance_rank
FROM regional_analysis
GROUP BY region_name, province_name;