-- Query: Advanced Payment Adoption
-- Description: Tracks adoption of advanced payment models (ADV PAY, AIM, AIP) by track type, highlighting which tracks are most engaged with alternative payment structures.
-- Table: healthcare_records
-- Author: Tevin S.
-- Date: 2026-03

SELECT
    track_type,
    COUNT(*) AS total_acos,
    SUM(CASE WHEN adv_pay = '1' THEN 1 ELSE 0 END) AS adv_pay_count,
    SUM(CASE WHEN aim = '1' THEN 1 ELSE 0 END) AS aim_count,
    SUM(CASE WHEN aip = '1' THEN 1 ELSE 0 END) AS aip_count
FROM healthcare_records
GROUP BY track_type
ORDER BY total_acos DESC;
