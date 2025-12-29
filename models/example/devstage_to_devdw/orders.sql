{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='src_orderNumber'
) }}

-- =================================================
-- 1. ETL batch metadata
-- =================================================
with metadata as (
    select
        etl_batch_no,
        etl_batch_date
    from {{ source('metadata', 'batch_control') }}
),

-- =================================================
-- 2. Deduplicate stage orders
-- =================================================
source_dedup as (
    select
        s.orderNumber            as src_orderNumber,
        s.orderDate,
        s.requiredDate,
        s.shippedDate,
        s.status,
        s.comments,
        s.customerNumber         as src_customerNumber,
        s.cancelledDate,
        s.create_timestamp       as src_create_timestamp,
        s.update_timestamp       as src_update_timestamp,
        m.etl_batch_no,
        m.etl_batch_date,
        row_number() over (
            partition by s.orderNumber
            order by s.update_timestamp desc
        ) as rn
    from {{ source('stage', 'orders') }} s
    cross join metadata m
),

-- =================================================
-- 3. Keep latest row per order
-- =================================================
source_latest as (
    select *
    from source_dedup
    where rn = 1
),

-- =================================================
-- 4. Join DW customers + target orders
-- =================================================
source_with_target as (
    select
        s.*,
        c.dw_customer_id,
        t.dw_order_id,
        t.src_update_timestamp      as tgt_update_timestamp,
        t.dw_create_timestamp       as tgt_create_timestamp,
        current_timestamp           as dw_update_timestamp
    from source_latest s

    left join {{ ref('customers') }} c
        on s.src_customerNumber = c.src_customerNumber

    {% if is_incremental() %}
    left join {{ this }} t
        on s.src_orderNumber = t.src_orderNumber
    {% else %}
    left join (
        select
            null::bigint as dw_order_id,
            null::timestamp as src_update_timestamp,
            null::timestamp as dw_create_timestamp
    ) t on 1 = 0
    {% endif %}
),

-- =================================================
-- 5. Filter NEW + UPDATED orders
-- =================================================
filtered as (
    select *
    from source_with_target
    where
        dw_order_id is null
        or src_update_timestamp > tgt_update_timestamp
),

-- =================================================
-- 6. Max surrogate key for incremental inserts
-- =================================================
max_key as (
    {% if is_incremental() %}
        select coalesce(max(dw_order_id), 0) as max_val
        from {{ this }}
    {% else %}
        select 0 as max_val
    {% endif %}
),

-- =================================================
-- 7. Final dataset (MERGE-safe)
-- =================================================
final as (
    select
        coalesce(
            dw_order_id,
            max_val + row_number() over(order by src_orderNumber)
        )::bigint as dw_order_id,

        src_orderNumber,
        orderDate,
        requiredDate,
        shippedDate,
        status,
        comments,
        src_customerNumber,
        dw_customer_id,
        cancelledDate,
        src_create_timestamp,
        src_update_timestamp,
        coalesce(tgt_create_timestamp, current_timestamp) as dw_create_timestamp,
        dw_update_timestamp,
        etl_batch_no,
        etl_batch_date
    from filtered
    cross join max_key
)

select * from final
