{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'dw_customer_id'
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
-- 2. Deduplicate DEV DW customers
-- =================================================
source_dedup as (
    select
        s.dw_customer_id,
        s.creditLimit,
        s.src_create_timestamp,
        s.src_update_timestamp,
        m.etl_batch_no,
        m.etl_batch_date,
        row_number() over (
            partition by s.dw_customer_id
            order by s.src_update_timestamp desc
        ) as rn
    from {{ ref('customers') }} s
    cross join metadata m
),

-- =================================================
-- 3. Keep latest customer row
-- =================================================
source_latest as (
    select *
    from source_dedup
    where rn = 1
),

-- =================================================
-- 4. Join with customer_history (SAFE for first run)
-- =================================================
source_with_target as (
    select
        s.*,
        t.creditLimit          as tgt_creditLimit,
        t.dw_active_record_ind,
        t.effective_from_date,
        t.dw_create_timestamp
    from source_latest s

    {% if is_incremental() %}
    left join {{ this }} t
        on s.dw_customer_id = t.dw_customer_id
       and t.dw_active_record_ind = 1
    {% else %}
    left join (
        select
            null::numeric   as creditLimit,
            null::int       as dw_active_record_ind,
            null::date      as effective_from_date,
            null::timestamp as dw_create_timestamp,
            null::int       as dw_customer_id
    ) t on 1 = 0
    {% endif %}
),

-- =================================================
-- 5. Identify NEW or CHANGED customers
-- =================================================
changed as (
    select *
    from source_with_target
    where
          dw_active_record_ind is null
       or creditLimit <> tgt_creditLimit
),

-- =================================================
-- 6. Expire old active records (SCD2 close)
-- =================================================
expired as (
    select
        dw_customer_id,
        tgt_creditLimit              as creditLimit,
        effective_from_date,
        etl_batch_date - interval '1 day' as effective_to_date,
        0                             as dw_active_record_ind,
        dw_create_timestamp,
        current_timestamp             as dw_update_timestamp,
        null                          as create_etl_batch_no,
        null                          as create_etl_batch_date,
        etl_batch_no                  as update_etl_batch_no,
        etl_batch_date                as update_etl_batch_date
    from changed
    where dw_active_record_ind = 1
),

-- =================================================
-- 7. Insert new active records
-- =================================================
new_records as (
    select
        dw_customer_id,
        creditLimit,
        etl_batch_date                as effective_from_date,
        null                          as effective_to_date,
        1                             as dw_active_record_ind,
        current_timestamp             as dw_create_timestamp,
        current_timestamp             as dw_update_timestamp,
        etl_batch_no                  as create_etl_batch_no,
        etl_batch_date                as create_etl_batch_date,
        null                          as update_etl_batch_no,
        null                          as update_etl_batch_date
    from changed
)

-- =================================================
-- 8. Final incremental output
-- =================================================
select * from expired
union all
select * from new_records

