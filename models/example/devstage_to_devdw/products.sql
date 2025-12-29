{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='src_productCode'
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
-- 2. Deduplicate stage products
-- =================================================
source_dedup as (
    select
        s.productCode           as src_productCode,
        s.productName,
        s.productLine,
        s.productScale,
        s.productVendor,
        s.productDescription,
        s.quantityInStock,
        s.buyPrice,
        s.MSRP,
        s.create_timestamp      as src_create_timestamp,
        s.update_timestamp      as src_update_timestamp,
        m.etl_batch_no,
        m.etl_batch_date,
        row_number() over (
            partition by s.productCode
            order by s.update_timestamp desc
        ) as rn
    from {{ source('stage', 'products') }} s
    cross join metadata m
),

-- =================================================
-- 3. Keep latest row per product
-- =================================================
source_latest as (
    select *
    from source_dedup
    where rn = 1
),

-- =================================================
-- 4. Join DW productlines + target products
-- =================================================
source_with_target as (
    select
        s.*,
        pl.dw_product_line_id,
        t.dw_product_id,
        t.src_update_timestamp      as tgt_update_timestamp,
        t.dw_create_timestamp       as tgt_create_timestamp,
        current_timestamp           as dw_update_timestamp
    from source_latest s
    left join {{ ref('productlines') }} pl
        on s.productLine = pl.productLine

    {% if is_incremental() %}
    left join {{ this }} t
        on s.src_productCode = t.src_productCode
    {% else %}
    left join (
        select
            null::bigint as dw_product_id,
            null::timestamp as src_update_timestamp,
            null::timestamp as dw_create_timestamp
    ) t on 1 = 0
    {% endif %}
),

-- =================================================
-- 5. Filter NEW + UPDATED products
-- =================================================
filtered as (
    select *
    from source_with_target
    where
        dw_product_id is null
        or src_update_timestamp > tgt_update_timestamp
),

-- =================================================
-- 6. Max surrogate key for incremental inserts
-- =================================================
max_key as (
    {% if is_incremental() %}
        select coalesce(max(dw_product_id), 0) as max_val
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
            dw_product_id,
            max_val + row_number() over(order by src_productCode)
        )::bigint as dw_product_id,

        src_productCode,
        productName,
        productLine,
        productScale,
        productVendor,
        productDescription,
        quantityInStock,
        buyPrice,
        MSRP,
        dw_product_line_id,
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
