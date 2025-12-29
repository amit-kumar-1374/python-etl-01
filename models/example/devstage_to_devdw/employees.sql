{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'employeeNumber',

    post_hook = """
        -- Resolve reporting hierarchy AFTER load
        update {{ this }} d
        set dw_reporting_employee_id = s.dw_employee_id
        from {{ this }} s
        where d.reportsTo = s.employeeNumber;

        -- Safety re-sync office FK
        update {{ this }} d
        set dw_office_id = o.dw_office_id
        from {{ ref('offices') }} o
        where d.officeCode = o.officeCode;
    """
) }}

-- =================================================
-- 1. Latest ETL metadata
-- =================================================
with metadata as (
    select
        etl_batch_no,
        etl_batch_date
    from {{ source('metadata', 'batch_control') }}
),

-- =================================================
-- 2. Deduplicate SOURCE (latest per employee)
-- =================================================
source_dedup as (
    select
        s.employeeNumber,
        s.lastName,
        s.firstName,
        s.extension,
        s.email,
        s.officeCode,
        s.reportsTo,
        s.jobTitle,
        s.create_timestamp as src_create_timestamp,
        s.update_timestamp as src_update_timestamp,
        m.etl_batch_no,
        m.etl_batch_date,
        row_number() over (
            partition by s.employeeNumber
            order by s.update_timestamp desc
        ) as rn
    from {{ source('stage', 'employees') }} s
    cross join metadata m
),

-- =================================================
-- 3. Keep ONLY latest record per employee
-- =================================================
source_latest as (
    select *
    from source_dedup
    where rn = 1
),

-- =================================================
-- 4. Join target for CDC comparison
-- =================================================
source_with_target as (
    select
        s.*,
        t.dw_employee_id,
        t.src_update_timestamp as tgt_update_timestamp,
        t.dw_create_timestamp as tgt_create_timestamp,
        o.dw_office_id,
        current_timestamp as dw_update_timestamp
    from source_latest s

    left join {{ ref('offices') }} o
        on s.officeCode = o.officeCode

    {% if is_incremental() %}
    left join {{ this }} t
        on s.employeeNumber = t.employeeNumber
    {% else %}
    left join (
        select
            null::bigint as dw_employee_id,
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
        dw_employee_id is null
        or src_update_timestamp > tgt_update_timestamp
),

-- =================================================
-- 6. Max surrogate key
-- =================================================
max_key as (
    {% if is_incremental() %}
        select coalesce(max(dw_employee_id), 0) as max_val
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
            dw_employee_id,
            max_val + row_number() over(order by employeeNumber)
        )::bigint as dw_employee_id,

        employeeNumber,
        lastName,
        firstName,
        extension,
        email,
        officeCode,
        reportsTo,
        jobTitle,
        dw_office_id,

        cast(null as bigint) as dw_reporting_employee_id,

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
