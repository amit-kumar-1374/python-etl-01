{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['src_orderNumber', 'src_productCode']
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
-- 2. Deduplicate stage orderdetails
-- =================================================
source_dedup as (
    select
        s.orderNumber          as src_orderNumber,
        s.productCode          as src_productCode,
        s.quantityOrdered,
        s.priceEach,
        s.orderLineNumber,
        s.create_timestamp     as src_create_timestamp,
        s.update_timestamp     as src_update_timestamp,
        m.etl_batch_no,
        m.etl_batch_date,
        row_number() over (
            partition by s.orderNumber, s.productCode
            order by s.update_timestamp desc
        ) as rn
    from {{ source('stage', 'orderdetails') }} s
    cross join metadata m
),

-- =================================================
-- 3. Keep latest row per orderdetail
-- =================================================
source_latest as (
    select *
    from source_dedup
    where rn = 1
),

-- =================================================
-- 4. Join DW orders + products + target orderdetails
-- =================================================
source_with_target as (
    select
        s.*,
        o.dw_order_id,
        p.dw_product_id,
        t.dw_orderdetail_id,
        t.src_update_timestamp      as tgt_update_timestamp,
        t.dw_create_timestamp       as tgt_create_timestamp,
        current_timestamp           as dw_update_timestamp
    from source_latest s
    left join {{ ref('orders') }} o
        on s.src_orderNumber = o.src_orderNumber
    left join {{ ref('products') }} p
        on s.src_productCode = p.src_productCode

    {% if is_incremental() %}
    left join (
        select
            dw_orderdetail_id,
            src_orderNumber,
            src_productCode,
            src_update_timestamp,
            dw_create_timestamp
        from {{ this }}
    ) t
        on s.src_orderNumber = t.src_orderNumber
       and s.src_productCode = t.src_productCode
    {% else %}
    left join (
        select
            null::bigint as dw_orderdetail_id,
            null::varchar as src_orderNumber,
            null::varchar as src_productCode,
            null::timestamp as src_update_timestamp,
            null::timestamp as dw_create_timestamp
    ) t on 1 = 0
    {% endif %}
),

-- =================================================
-- 5. Filter NEW + UPDATED orderdetails
-- =================================================
filtered as (
    select *
    from source_with_target
    where
        dw_orderdetail_id is null
        or src_update_timestamp > tgt_update_timestamp
),

-- =================================================
-- 6. Max surrogate key for incremental inserts
-- =================================================
max_key as (
    {% if is_incremental() %}
        select coalesce(max(dw_orderdetail_id), 0) as max_val
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
            dw_orderdetail_id,
            max_val + row_number() over(order by src_orderNumber, src_productCode)
        )::bigint as dw_orderdetail_id,

        src_orderNumber,
        src_productCode,
        quantityOrdered,
        priceEach,
        orderLineNumber,
        dw_order_id,
        dw_product_id,
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
