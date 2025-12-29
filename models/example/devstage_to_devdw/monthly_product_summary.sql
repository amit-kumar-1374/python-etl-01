{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['start_of_the_month_date', 'dw_product_id']
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
-- 2. Aggregate daily product summary into monthly
-- =================================================
monthly_agg as (
    select
        date_trunc('month', d.summary_date) as start_of_the_month_date,
        d.dw_product_id,
        max(d.customer_apd) as customer_apd,
        1 as customer_apm,
        sum(d.product_cost_amount) as product_cost_amount,
        sum(d.product_mrp_amount) as product_mrp_amount,
        sum(d.cancelled_product_qty) as cancelled_product_qty,
        sum(d.cancelled_cost_amount) as cancelled_cost_amount,
        sum(d.cancelled_mrp_amount) as cancelled_mrp_amount,
        max(d.cancelled_order_apd) as cancelled_order_apd,
        sum(d.cancelled_order_apd) as cancelled_order_apm,
        max(d.dw_create_timestamp) as dw_create_timestamp,
        max(d.dw_update_timestamp) as dw_update_timestamp,
        max(d.etl_batch_no) as etl_batch_no,
        max(d.etl_batch_date) as etl_batch_date
    from {{ ref('daily_product_summary') }} d
    cross join metadata m
    where d.etl_batch_date >= m.etl_batch_date
    group by date_trunc('month', d.summary_date), d.dw_product_id
),

-- =================================================
-- 3. Insert new records
-- =================================================
new_records as (
    select
        a.start_of_the_month_date,
        a.dw_product_id,
        a.customer_apd,
        a.customer_apm,
        a.product_cost_amount,
        a.product_mrp_amount,
        a.cancelled_product_qty,
        a.cancelled_cost_amount,
        a.cancelled_mrp_amount,
        a.cancelled_order_apd,
        a.cancelled_order_apm,
        a.dw_create_timestamp,
        a.dw_update_timestamp,
        a.etl_batch_no,
        a.etl_batch_date
    from monthly_agg a
    left join {{ this }} m
      on a.start_of_the_month_date = m.start_of_the_month_date
     and a.dw_product_id = m.dw_product_id
    where m.dw_product_id is null
),

-- =================================================
-- 4. Update existing records (aggregate existing + new)
-- =================================================
update_records as (
    select
        m.start_of_the_month_date,
        m.dw_product_id,
        m.customer_apd + a.customer_apd as customer_apd,
        m.customer_apm + a.customer_apm as customer_apm,
        m.product_cost_amount + a.product_cost_amount as product_cost_amount,
        m.product_mrp_amount + a.product_mrp_amount as product_mrp_amount,
        m.cancelled_product_qty + a.cancelled_product_qty as cancelled_product_qty,
        m.cancelled_cost_amount + a.cancelled_cost_amount as cancelled_cost_amount,
        m.cancelled_mrp_amount + a.cancelled_mrp_amount as cancelled_mrp_amount,
        m.cancelled_order_apd + a.cancelled_order_apd as cancelled_order_apd,
        m.cancelled_order_apm + a.cancelled_order_apm as cancelled_order_apm,
        m.dw_create_timestamp,
        current_timestamp as dw_update_timestamp,
        a.etl_batch_no,
        a.etl_batch_date
    from {{ this }} m
    join monthly_agg a
      on m.start_of_the_month_date = a.start_of_the_month_date
     and m.dw_product_id = a.dw_product_id
)

-- =================================================
-- 5. Final union for incremental insert/merge
-- =================================================
select * from new_records
union all
select * from update_records
