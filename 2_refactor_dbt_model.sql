{{
    config(
        materialized='table'
    )
}}

WITH customer_orders AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', order_date) AS order_month,
        COUNT(DISTINCT order_id) AS monthly_orders,
        SUM(order_amount) AS monthly_revenue,
        AVG(order_amount) AS avg_order_value
    FROM {{ ref('stg_orders') }}
    WHERE order_status = 'completed'
    GROUP BY 1, 2
),
customer_metrics_with_lag AS (
    SELECT
        customer_id,
        order_month,
        monthly_orders,
        monthly_revenue,
        avg_order_value,
        LAG(monthly_orders, 1) OVER (
            PARTITION BY customer_id 
            ORDER BY order_month
        ) AS prev_month_orders,
        LAG(monthly_revenue, 1) OVER (
            PARTITION BY customer_id 
            ORDER BY order_month
        ) AS prev_month_revenue,
        LAG(avg_order_value, 1) OVER (
            PARTITION BY customer_id 
            ORDER BY order_month
        ) AS prev_avg_order_value
    FROM customer_orders
),
customer_kpi AS (
    SELECT
        customer_id,
        order_month,
        monthly_orders,
        monthly_revenue,
        avg_order_value,
        prev_month_orders,
        prev_month_revenue,
        prev_avg_order_value,
        CASE 
            WHEN prev_month_orders IS NULL OR prev_month_orders = 0 
            THEN NULL
            ELSE ROUND(
                (monthly_orders - prev_month_orders) * 100.0 / prev_month_orders, 
                2
            )
        END AS orders_mom_growth_pct,
        
        CASE 
            WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 
            THEN NULL
            ELSE ROUND(
                (monthly_revenue - prev_month_revenue) * 100.0 / prev_month_revenue, 
                2
            )
        END AS revenue_mom_growth_pct,
        
        CASE 
            WHEN prev_avg_order_value IS NULL OR prev_avg_order_value = 0 
            THEN NULL
            ELSE ROUND(
                (avg_order_value - prev_avg_order_value) * 100.0 / prev_avg_order_value, 
                2
            )
        END AS aov_mom_growth_pct,
        LAG(monthly_orders, 12) OVER (
            PARTITION BY customer_id 
            ORDER BY order_month
        ) AS prev_year_orders,
        
        CASE 
            WHEN LAG(monthly_orders, 12) OVER (
                PARTITION BY customer_id 
                ORDER BY order_month
            ) IS NULL 
            OR LAG(monthly_orders, 12) OVER (
                PARTITION BY customer_id 
                ORDER BY order_month
            ) = 0 
            THEN NULL
            ELSE ROUND(
                (monthly_orders - LAG(monthly_orders, 12) OVER (
                    PARTITION BY customer_id 
                    ORDER BY order_month
                )) * 100.0 / LAG(monthly_orders, 12) OVER (
                    PARTITION BY customer_id 
                    ORDER BY order_month
                ), 
                2
            )
        END AS orders_yoy_growth_pct
        
    FROM customer_metrics_with_lag
),
customer_segments AS (
    SELECT
        customer_id,
        order_month,
        CASE 
            WHEN monthly_revenue >= 10000 THEN 'VIP'
            WHEN monthly_revenue >= 5000 THEN 'Premium'
            WHEN monthly_revenue >= 1000 THEN 'Regular'
            ELSE 'Basic'
        END AS revenue_segment,
        
        CASE 
            WHEN monthly_orders >= 20 THEN 'Frequent'
            WHEN monthly_orders >= 10 THEN 'Active'
            WHEN monthly_orders >= 1 THEN 'Occasional'
            ELSE 'Inactive'
        END AS frequency_segment,
        
        CASE 
            WHEN avg_order_value >= 500 THEN 'High AOV'
            WHEN avg_order_value >= 200 THEN 'Medium AOV'
            ELSE 'Low AOV'
        END AS aov_segment
        
    FROM customer_orders
),
customer_retention AS (
    SELECT
        customer_id,
        order_month,
        LAG(order_month, 1) OVER (
            PARTITION BY customer_id 
            ORDER BY order_month
        ) AS prev_order_month,
        
        CASE 
            WHEN DATE_PART('month', order_month) - DATE_PART(
                'month', 
                LAG(order_month, 1) OVER (
                    PARTITION BY customer_id 
                    ORDER BY order_month
                )
            ) = 1 
            THEN 'Retained'
            WHEN LAG(order_month, 1) OVER (
                PARTITION BY customer_id 
                ORDER BY order_month
            ) IS NULL 
            THEN 'New'
            ELSE 'Reactivated'
        END AS retention_status
    FROM customer_orders
)
SELECT
    kpi.customer_id,
    kpi.order_month,
    kpi.monthly_orders,
    kpi.monthly_revenue,
    kpi.avg_order_value,
    kpi.prev_month_orders,
    kpi.prev_month_revenue,
    kpi.orders_mom_growth_pct,
    kpi.revenue_mom_growth_pct,
    kpi.aov_mom_growth_pct,
    kpi.orders_yoy_growth_pct,
    seg.revenue_segment,
    seg.frequency_segment,
    seg.aov_segment,
    ret.retention_status
FROM customer_kpi kpi
LEFT JOIN customer_segments seg 
    ON kpi.customer_id = seg.customer_id 
    AND kpi.order_month = seg.order_month
LEFT JOIN customer_retention ret 
    ON kpi.customer_id = ret.customer_id 
    AND kpi.order_month = ret.order_month
ORDER BY kpi.customer_id, kpi.order_month