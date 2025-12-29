{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='productLine'
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
-- 2. Deduplicate stage productlines
-- =================================================
source_dedup as (
    select
        s.productLine,
        s.textDescription,
        s.htmlDescription,
        s.image,
        s.create_timestamp       as src_create_timestamp,
        s.update_timestamp       as src_update_timestamp,
        m.etl_batch_no,
        m.etl_batch_date,
        row_number() over (
            partition by s.productLine
            order by s.update_timestamp desc
        ) as rn
    from {{ source('stage', 'productlines') }} s
    cross join metadata m
),

-- =================================================
-- 3. Keep latest row per productLine
-- =================================================
source_latest as (
    select *
    from source_dedup
    where rn = 1
),

-- =================================================
-- 4. Join target productlines
-- =================================================
source_with_target as (
    select
        s.*,
        t.dw_product_line_id,
        t.src_update_timestamp      as tgt_update_timestamp,
        t.dw_create_timestamp       as tgt_create_timestamp,
        current_timestamp           as dw_update_timestamp
    from source_latest s

    {% if is_incremental() %}
    left join {{ this }} t
        on s.productLine = t.productLine
    {% else %}
    left join (
        select
            null::bigint as dw_product_line_id,
            null::timestamp as src_update_timestamp,
            null::timestamp as dw_create_timestamp
    ) t on 1 = 0
    {% endif %}
),

-- =================================================
-- 5. Filter NEW + UPDATED productlines
-- =================================================
filtered as (
    select *
    from source_with_target
    where
        dw_product_line_id is null
        or src_update_timestamp > tgt_update_timestamp
),

-- =================================================
-- 6. Max surrogate key for incremental inserts
-- =================================================
max_key as (
    {% if is_incremental() %}
        select coalesce(max(dw_product_line_id), 0) as max_val
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
            dw_product_line_id,
            max_val + row_number() over(order by productLine)
        )::bigint as dw_product_line_id,

        productLine,
        textDescription,
        htmlDescription,
        image,
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
