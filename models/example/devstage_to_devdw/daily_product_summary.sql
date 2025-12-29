{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['summary_date', 'dw_product_id']
) }}

-- =================================================
-- 1. ETL batch metadata
-- =================================================
with metadata as (
    select
        m.etl_batch_no,
        m.etl_batch_date
    from {{ source('metadata', 'batch_control') }} m
),

-- =================================================
-- 2. Product sales
-- =================================================
product_sales_cte as (
    select
        cast(o.orderDate as date) as summary_date,
        od.dw_product_id,
        count(distinct o.dw_customer_id) as customer_apd,
        sum(od.priceEach * od.quantityOrdered) as product_cost_amount,
        sum(p.MSRP * od.quantityOrdered) as product_mrp_amount,
        0 as cancelled_product_qty,
        0 as cancelled_cost_amount,
        0 as cancelled_mrp_amount,
        0 as cancelled_order_apd
    from {{ source('j25amit_devdw', 'orders') }} o
    join {{ source('j25amit_devdw', 'orderdetails') }} od
        on o.dw_order_id = od.dw_order_id
    join {{ source('j25amit_devdw', 'products') }} p
        on od.dw_product_id = p.dw_product_id
    cross join metadata m
    where cast(o.orderDate as date) >= m.etl_batch_date
    group by cast(o.orderDate as date), od.dw_product_id
),

-- =================================================
-- 3. Cancelled products
-- =================================================
cancelled_products_cte as (
    select
        cast(o.cancelledDate as date) as summary_date,
        od.dw_product_id,
        0 as customer_apd,
        0 as product_cost_amount,
        0 as product_mrp_amount,
        sum(od.quantityOrdered) as cancelled_product_qty,
        sum(od.priceEach * od.quantityOrdered) as cancelled_cost_amount,
        sum(p.MSRP * od.quantityOrdered) as cancelled_mrp_amount,
        count(distinct o.dw_order_id) as cancelled_order_apd
    from {{ ref('orders') }} o
    join {{ ref('orderdetails') }} od
        on o.dw_order_id = od.dw_order_id
    join {{ ref('products') }} p
        on od.dw_product_id = p.dw_product_id
    cross join metadata m
    where lower(trim(o.status)) = 'cancelled'
      and cast(o.cancelledDate as date) >= m.etl_batch_date
    group by cast(o.cancelledDate as date), od.dw_product_id
),

-- =================================================
-- 4. Combine sales + cancelled
-- =================================================
combined_cte as (
    select * from product_sales_cte
    union all
    select * from cancelled_products_cte
),

-- =================================================
-- 5. Aggregate per product per day
-- =================================================
final as (
    select
        c.summary_date,
        c.dw_product_id,
        max(c.customer_apd) as customer_apd,
        max(c.product_cost_amount) as product_cost_amount,
        max(c.product_mrp_amount) as product_mrp_amount,
        max(c.cancelled_product_qty) as cancelled_product_qty,
        max(c.cancelled_cost_amount) as cancelled_cost_amount,
        max(c.cancelled_mrp_amount) as cancelled_mrp_amount,
        max(c.cancelled_order_apd) as cancelled_order_apd,
        current_timestamp as dw_create_timestamp,
        current_timestamp as dw_update_timestamp,
        max(m.etl_batch_no) as etl_batch_no,
        max(m.etl_batch_date) as etl_batch_date
    from combined_cte c
    cross join metadata m
    group by c.summary_date, c.dw_product_id
)

-- =================================================
-- 6. Final output
-- =================================================
select *
from final
