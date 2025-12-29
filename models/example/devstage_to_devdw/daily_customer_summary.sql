{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
     unique_key = ['summary_date', 'dw_customer_id']
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
-- 2. Orders
-- =================================================
orders_cte as (
    select
        cast(o.orderDate as date)        as summary_date,
        o.dw_customer_id                as dw_customer_id,

        count(distinct o.dw_order_id)   as order_count,
        1                               as order_apd,
        sum(od.priceEach * od.quantityOrdered) as order_cost_amount,

        0 as cancelled_order_count,
        0 as cancelled_order_amount,
        0 as cancelled_order_apd,

        0 as shipped_order_count,
        0 as shipped_order_amount,
        0 as shipped_order_apd,

        0 as payment_apd,
        0 as payment_amount,

        count(distinct od.dw_product_id) as products_ordered_qty,
        count(od.quantityOrdered)        as products_items_qty,
        sum(p.MSRP * od.quantityOrdered) as order_mrp_amount,

        0 as new_customer_apd,
        0 as new_customer_paid_apd
    from {{ ref('orders') }} o
    join {{ ref('orderdetails') }} od
        on o.dw_order_id = od.dw_order_id
    join {{ ref('products') }} p
        on od.dw_product_id = p.dw_product_id
    cross join metadata m
    where cast(o.orderDate as date) >= m.etl_batch_date
    group by
        cast(o.orderDate as date),
        o.dw_customer_id
),

-- =================================================
-- 3. New customers
-- =================================================
customers_cte as (
    select
        cast(c.src_create_timestamp as date) as summary_date,
        c.dw_customer_id                     as dw_customer_id,

        0 as order_count,
        0 as order_apd,
        0 as order_cost_amount,

        0 as cancelled_order_count,
        0 as cancelled_order_amount,
        0 as cancelled_order_apd,

        0 as shipped_order_count,
        0 as shipped_order_amount,
        0 as shipped_order_apd,

        0 as payment_apd,
        0 as payment_amount,

        0 as products_ordered_qty,
        0 as products_items_qty,
        0 as order_mrp_amount,

        1 as new_customer_apd,
        0 as new_customer_paid_apd
    from {{ source('j25amit_devdw', 'customers') }} c
    cross join metadata m
    where cast(c.src_create_timestamp as date) >= m.etl_batch_date
),

-- =================================================
-- 4. Cancelled orders
-- =================================================
cancelled_cte as (
    select
        cast(o.cancelledDate as date)  as summary_date,
        o.dw_customer_id               as dw_customer_id,

        0 as order_count,
        0 as order_apd,
        0 as order_cost_amount,

        count(o.dw_order_id)           as cancelled_order_count,
        sum(od.priceEach * od.quantityOrdered) as cancelled_order_amount,
        1                               as cancelled_order_apd,

        0 as shipped_order_count,
        0 as shipped_order_amount,
        0 as shipped_order_apd,

        0 as payment_apd,
        0 as payment_amount,

        0 as products_ordered_qty,
        0 as products_items_qty,
        0 as order_mrp_amount,

        0 as new_customer_apd,
        0 as new_customer_paid_apd
    from {{ source('j25amit_devdw', 'orders') }} o
    join {{ source('j25amit_devdw', 'orderdetails') }} od
        on o.dw_order_id = od.dw_order_id
    cross join metadata m
    where cast(o.cancelledDate as date) >= m.etl_batch_date
      and o.status = 'Cancelled'
    group by
        cast(o.cancelledDate as date),
        o.dw_customer_id
),

-- =================================================
-- 5. Payments
-- =================================================
payments_cte as (
    select
        cast(p.paymentDate as date) as summary_date,
        p.dw_customer_id            as dw_customer_id,

        0 as order_count,
        0 as order_apd,
        0 as order_cost_amount,

        0 as cancelled_order_count,
        0 as cancelled_order_amount,
        0 as cancelled_order_apd,

        0 as shipped_order_count,
        0 as shipped_order_amount,
        0 as shipped_order_apd,

        1 as payment_apd,
        sum(p.amount)               as payment_amount,

        0 as products_ordered_qty,
        0 as products_items_qty,
        0 as order_mrp_amount,

        0 as new_customer_apd,
        1 as new_customer_paid_apd
    from {{ source('j25amit_devdw', 'payments') }} p
    cross join metadata m
    where cast(p.paymentDate as date) >= m.etl_batch_date
    group by
        cast(p.paymentDate as date),
        p.dw_customer_id
),

-- =================================================
-- 6. Shipped orders
-- =================================================
shipped_cte as (
    select
        cast(o.shippedDate as date)  as summary_date,
        o.dw_customer_id             as dw_customer_id,

        0 as order_count,
        0 as order_apd,
        0 as order_cost_amount,

        0 as cancelled_order_count,
        0 as cancelled_order_amount,
        0 as cancelled_order_apd,

        count(o.dw_order_id)         as shipped_order_count,
        sum(od.priceEach * od.quantityOrdered) as shipped_order_amount,
        1                            as shipped_order_apd,

        0 as payment_apd,
        0 as payment_amount,

        0 as products_ordered_qty,
        0 as products_items_qty,
        0 as order_mrp_amount,

        0 as new_customer_apd,
        0 as new_customer_paid_apd
    from {{ source('j25amit_devdw', 'orders') }} o
    join {{ source('j25amit_devdw', 'orderdetails') }} od
        on o.dw_order_id = od.dw_order_id
    cross join metadata m
    where cast(o.shippedDate as date) >= m.etl_batch_date
      and o.status = 'Shipped'
    group by
        cast(o.shippedDate as date),
        o.dw_customer_id
),

-- =================================================
-- 7. Combine all metrics
-- =================================================
combined as (
    select * from orders_cte
    union all
    select * from customers_cte
    union all
    select * from cancelled_cte
    union all
    select * from payments_cte
    union all
    select * from shipped_cte
)

-- =================================================
-- 8. Final daily customer summary
-- =================================================
select
    c.summary_date                    as summary_date,
    c.dw_customer_id                  as dw_customer_id,

    max(c.order_count)               as order_count,
    max(c.order_apd)                 as order_apd,
    max(c.order_cost_amount)         as order_cost_amount,

    max(c.cancelled_order_count)     as cancelled_order_count,
    max(c.cancelled_order_amount)    as cancelled_order_amount,
    max(c.cancelled_order_apd)       as cancelled_order_apd,

    max(c.shipped_order_count)       as shipped_order_count,
    max(c.shipped_order_amount)      as shipped_order_amount,
    max(c.shipped_order_apd)         as shipped_order_apd,

    max(c.payment_apd)               as payment_apd,
    max(c.payment_amount)            as payment_amount,

    max(c.products_ordered_qty)      as products_ordered_qty,
    max(c.products_items_qty)        as products_items_qty,
    max(c.order_mrp_amount)          as order_mrp_amount,

    max(c.new_customer_apd)          as new_customer_apd,
    max(c.new_customer_paid_apd)     as new_customer_paid_apd,

    current_timestamp                as dw_create_timestamp,
    current_timestamp                as dw_update_timestamp,
    max(m.etl_batch_no)              as etl_batch_no,
    max(m.etl_batch_date)            as etl_batch_date
from combined c
cross join metadata m
group by
    c.summary_date,
    c.dw_customer_id
