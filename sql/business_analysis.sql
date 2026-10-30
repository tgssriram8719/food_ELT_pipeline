USE food_orders_analytics_db;


-- 1. Total Order Value

SELECT
    COUNT(order_value) AS records_included,
    SUM(order_value) AS total_order_value

FROM cleaned_food_orders;


-- 2. Average Order Value

SELECT
    COUNT(order_value) AS records_included,
    AVG(order_value) AS average_order_value

FROM cleaned_food_orders;


-- 3. Payment Method with Highest Number of Orders

SELECT
    payment_method,
    COUNT(*) AS order_count

FROM cleaned_food_orders

WHERE payment_method <> 'Unknown'

GROUP BY payment_method

ORDER BY order_count DESC

LIMIT 1;


-- 4. Order Channel with Highest Number of Orders

SELECT
    order_channel,
    COUNT(*) AS order_count

FROM cleaned_food_orders

GROUP BY order_channel

ORDER BY order_count DESC

LIMIT 1;


-- 5. Average Delivery Time

SELECT
    COUNT(delivery_time_minutes) AS records_included,
    AVG(delivery_time_minutes) AS average_delivery_time_minutes

FROM cleaned_food_orders;


-- 6. Total Platform Net Revenue

SELECT
    COUNT(platform_net_revenue) AS records_included,
    SUM(platform_net_revenue) AS total_platform_net_revenue

FROM cleaned_food_orders;


-- Additional Analysis: Rejected Records

SELECT
    rejection_reason,
    COUNT(*) AS rejected_count

FROM rejected_food_orders

GROUP BY rejection_reason

ORDER BY rejected_count DESC;

