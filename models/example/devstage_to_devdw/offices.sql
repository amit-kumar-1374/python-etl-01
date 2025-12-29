{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'officeCode'
) }}

-- =================================================
-- 1. Latest ETL batch metadata
-- =================================================
with metadata as (
    select
        etl_batch_no,
        etl_batch_date
    from {{ source('metadata', 'batch_control') }}
),

-- =================================================
-- 2. Deduplicate SOURCE (ONE row per officeCode)
-- =================================================
source_dedup as (
    select
        s.officeCode,
        s.city,
        s.phone,
        s.addressLine1,
        s.addressLine2,
        s.state,
        s.country,
        s.postalCode,
        s.territory,
        s.create_timestamp as src_create_timestamp,
        s.update_timestamp as src_update_timestamp,
        m.etl_batch_no,
        m.etl_batch_date,
        row_number() over (
            partition by s.officeCode
            order by s.update_timestamp desc
        ) as rn
    from {{ source('stage', 'offices') }} s
    cross join metadata m
),

-- =================================================
-- 3. Keep ONLY latest row per officeCode
-- =================================================
source_latest as (
    select *
    from source_dedup
    where rn = 1
),

-- =================================================
-- 4. Join with target for CDC
-- =================================================
source_with_target as (
    select
        s.*,
        t.dw_office_id,
        t.src_update_timestamp as tgt_update_timestamp,
        t.dw_create_timestamp as tgt_create_timestamp,
        current_timestamp as dw_update_timestamp
    from source_latest s

    {% if is_incremental() %}
    left join {{ this }} t
        on s.officeCode = t.officeCode
    {% else %}
    left join (
        select
            null::varchar as officeCode,
            null::bigint  as dw_office_id,
            null::timestamp as src_update_timestamp,
            null::timestamp as dw_create_timestamp
    ) t on 1 = 0
    {% endif %}
),

-- =================================================
-- 5. Filter NEW + UPDATED rows
-- =================================================
filtered as (
    select *
    from source_with_target
    where
        dw_office_id is null
        or src_update_timestamp > tgt_update_timestamp
),

-- =================================================
-- 6. Max surrogate key
-- =================================================
max_key as (
    {% if is_incremental() %}
        select coalesce(max(dw_office_id), 0) as max_val
        from {{ this }}
    {% else %}
        select 0 as max_val
    {% endif %}
),

-- =================================================
-- 7. Final dataset
-- =================================================
final as (
    select
        coalesce(
            dw_office_id,
            max_val + row_number() over(order by officeCode)
        )::bigint as dw_office_id,

        officeCode,
        city,
        phone,
        addressLine1,
        addressLine2,
        state,
        country,
        postalCode,
        territory,
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
