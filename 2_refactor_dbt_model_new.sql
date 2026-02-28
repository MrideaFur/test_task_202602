{{
    config(
        materialized='incremental',
		unique_key='customer_id || order_month'
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
    {% if is_incremental() %}
        AND DATE_TRUNC('month', order_date) >= (SELECT MAX(order_month) FROM {{ this }})
    {% endif %}
    GROUP BY 1, 2
),
customer_orders_last AS (
	SELECT
		customer_id,
		max(order_month)
	FROM {{ this }}
),
customer_orders_data as (
	select
		co0.customer_id,
		co0.order_month,
		COALESCE(co_1m.order_month, col0.order_month) as prev_order_month,
		co0.monthly_orders,
		co0.monthly_revenue,
		co0.avg_order_value,
		COALESCE(co_1m.monthly_orders, co_1m_new.monthly_orders) 	as prev_month_orders,
		COALESCE(co_1m.monthly_revenue, co_1m_new.monthly_revenue) 	as prev_month_revenue,
		COALESCE(co_1m.avg_order_value, co_1m_new.avg_order_value) 	as prev_avg_order_value,
		COALESCE(co_1y.monthly_orders, co_1y_new.monthly_orders) 	as prev_year_orders

	from customer_orders co0
	left join {{ this }} as co_1m on 1=1
		and co_1m.customer_id=co0.customer_id
		and co_1m.order_month=co0.order_month - INTERVAL '1 month'
	left join customer_orders as co_1m_new on 1=1
		and co_1m_new.customer_id=co0.customer_id
		and co_1m_new.order_month=co0.order_month - INTERVAL '1 month'
	left join {{ this }} as co_1y on 1=1
		and co_1y.customer_id=co0.customer_id
		and co_1y.order_month=co0.order_month - INTERVAL '1 year'
	left join customer_orders as co_1y_new on 1=1
		and co_1y_new.customer_id=co0.customer_id
		and co_1y_new.order_month=co0.order_month - INTERVAL '1 year'
	left join customer_orders_last as col0 on 1=1
		and col0.customer_id=co0.customer_id
)

select 
    cod0.customer_id,
    cod0.order_month,
    cod0.monthly_orders,
    cod0.monthly_revenue,
    cod0.avg_order_value,
    cod0.prev_month_orders,
    cod0.prev_month_revenue,
	cod0.prev_year_orders,
	
	CASE 
		WHEN NULLIF(cod0.prev_month_orders,0) = 0
		THEN NULL
		ELSE ROUND(
			(cod0.monthly_orders - cod0.prev_month_orders) * 100.0 / cod0.prev_month_orders, 
			2
		)
	END AS orders_mom_growth_pct,
	
	CASE 
		WHEN NULLIF(cod0.prev_month_revenue,0) = 0
		THEN NULL
		ELSE ROUND(
			(cod0.monthly_revenue - cod0.prev_month_revenue) * 100.0 / cod0.prev_month_revenue, 
			2
		)
	END AS revenue_mom_growth_pct,
	
	CASE 
		WHEN NULLIF(cod0.prev_avg_order_value,0) = 0
		THEN NULL
		ELSE ROUND(
			(cod0.avg_order_value - cod0.prev_avg_order_value) * 100.0 / cod0.prev_avg_order_value, 
			2
		)
	END AS aov_mom_growth_pct,
	
	CASE 
		WHEN NULLIF(cod0.prev_year_orders,0) = 0 
		THEN NULL
		ELSE ROUND(
			(cod0.monthly_orders - cod0.prev_year_orders) * 100.0 / cod0.prev_year_orders, 
			2
		)
	END AS orders_yoy_growth_pct,
	
	CASE 
		WHEN cod0.monthly_revenue >= 10000 THEN 'VIP'
		WHEN cod0.monthly_revenue >= 5000 THEN 'Premium'
		WHEN cod0.monthly_revenue >= 1000 THEN 'Regular'
		ELSE 'Basic'
	END AS revenue_segment,
	
	CASE 
		WHEN cod0.monthly_orders >= 20 THEN 'Frequent'
		WHEN cod0.monthly_orders >= 10 THEN 'Active'
		WHEN cod0.monthly_orders >= 1 THEN 'Occasional'
		ELSE 'Inactive'
	END AS frequency_segment,
	
	CASE 
		WHEN cod0.avg_order_value >= 500 THEN 'High AOV'
		WHEN cod0.avg_order_value >= 200 THEN 'Medium AOV'
		ELSE 'Low AOV'
	END AS aov_segment,
	
	CASE 
		WHEN DATE_PART('month', cod0.order_month) - DATE_PART('month', cod0.prev_order_month) = 1 
		THEN 'Retained'
		WHEN cod0.prev_order_month IS NULL 
		THEN 'New'
		ELSE 'Reactivated'
	END AS retention_status

from customer_orders_data as cod0