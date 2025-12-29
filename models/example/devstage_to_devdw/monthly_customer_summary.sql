{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['start_of_the_month_date', 'dw_customer_id']
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
-- 2. Filter daily rows for incremental window
-- =================================================
daily_filtered as (
    select
        d.summary_date,
        d.dw_customer_id,

        d.order_count,
        d.order_apd,
        d.order_cost_amount,

        d.cancelled_order_count,
        d.cancelled_order_amount,
        d.cancelled_order_apd,

        d.shipped_order_count,
        d.shipped_order_amount,
        d.shipped_order_apd,

        d.payment_apd,
        d.payment_amount,

        d.products_ordered_qty,
        d.products_items_qty,
        d.order_mrp_amount,

        d.new_customer_apd,
        d.new_customer_paid_apd,

        d.dw_create_timestamp,
        d.dw_update_timestamp,
        d.etl_batch_no,
        d.etl_batch_date
    from {{ ref('daily_customer_summary') }} d
    cross join metadata m
    where d.etl_batch_date >= m.etl_batch_date
),

-- =================================================
-- 3. Monthly aggregation
-- =================================================
monthly_agg as (
    select
        date_trunc('month', df.summary_date)::date
            as start_of_the_month_date,

        df.dw_customer_id
            as dw_customer_id,

        sum(df.order_count)                    as order_count,
        sum(df.order_apd)                      as order_apd,
        count(distinct df.summary_date)        as order_apm,

        sum(df.order_cost_amount)              as order_cost_amount,

        sum(df.cancelled_order_count)          as cancelled_order_count,
        sum(df.cancelled_order_amount)         as cancelled_order_amount,
        sum(df.cancelled_order_apd)            as cancelled_order_apd,
        count(distinct case
            when df.cancelled_order_count > 0 then df.summary_date
        end)                                   as cancelled_order_apm,

        sum(df.shipped_order_count)            as shipped_order_count,
        sum(df.shipped_order_amount)           as shipped_order_amount,
        sum(df.shipped_order_apd)              as shipped_order_apd,
        count(distinct case
            when df.shipped_order_count > 0 then df.summary_date
        end)                                   as shipped_order_apm,

        sum(df.payment_apd)                    as payment_apd,
        count(distinct case
            when df.payment_amount > 0 then df.summary_date
        end)                                   as payment_apm,
        sum(df.payment_amount)                 as payment_amount,

        sum(df.products_ordered_qty)           as products_ordered_qty,
        sum(df.products_items_qty)             as products_items_qty,
        sum(df.order_mrp_amount)               as order_mrp_amount,

        sum(df.new_customer_apd)               as new_customer_apd,
        count(distinct case
            when df.new_customer_apd > 0 then df.summary_date
        end)                                   as new_customer_apm,

        sum(df.new_customer_paid_apd)          as new_customer_paid_apd,
        count(distinct case
            when df.new_customer_paid_apd > 0 then df.summary_date
        end)                                   as new_customer_paid_apm,

        max(df.dw_create_timestamp)            as dw_create_timestamp,
        current_timestamp                      as dw_update_timestamp,
        max(df.etl_batch_no)                   as etl_batch_no,
        max(df.etl_batch_date)                 as etl_batch_date
    from daily_filtered df
    group by
        date_trunc('month', df.summary_date)::date,
        df.dw_customer_id
)

-- =================================================
-- 4. Final output
-- =================================================
select
    ma.start_of_the_month_date,
    ma.dw_customer_id,

    ma.order_count,
    ma.order_apd,
    ma.order_apm,
    ma.order_cost_amount,

    ma.cancelled_order_count,
    ma.cancelled_order_amount,
    ma.cancelled_order_apd,
    ma.cancelled_order_apm,

    ma.shipped_order_count,
    ma.shipped_order_amount,
    ma.shipped_order_apd,
    ma.shipped_order_apm,

    ma.payment_apd,
    ma.payment_apm,
    ma.payment_amount,

    ma.products_ordered_qty,
    ma.products_items_qty,
    ma.order_mrp_amount,

    ma.new_customer_apd,
    ma.new_customer_apm,
    ma.new_customer_paid_apd,
    ma.new_customer_paid_apm,

    ma.dw_create_timestamp,
    ma.dw_update_timestamp,
    ma.etl_batch_no,
    ma.etl_batch_date
from monthly_agg ma
