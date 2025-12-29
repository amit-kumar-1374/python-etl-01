{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'src_customerNumber'
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
-- 2. Deduplicate stage customers
-- =================================================
source_dedup as (
    select
        s.customerNumber            as src_customerNumber,
        s.customerName,
        s.contactLastName,
        s.contactFirstName,
        s.phone,
        s.addressLine1,
        s.addressLine2,
        s.city,
        s.state,
        s.postalCode,
        s.country,
        s.salesRepEmployeeNumber,
        s.creditLimit,
        s.create_timestamp          as src_create_timestamp,
        s.update_timestamp          as src_update_timestamp,
        m.etl_batch_no,
        m.etl_batch_date,
        row_number() over (
            partition by s.customerNumber
            order by s.update_timestamp desc
        ) as rn
    from {{ source('stage', 'customers') }} s
    cross join metadata m
),

-- =================================================
-- 3. Keep latest row per customer
-- =================================================
source_latest as (
    select *
    from source_dedup
    where rn = 1
),

-- =================================================
-- 4. Join employee + target safely
-- =================================================
source_with_target as (
    select
        s.*,
        e.dw_employee_id            as dw_sales_rep_employee_id,
        t.dw_customer_id,
        t.src_update_timestamp      as tgt_update_timestamp,
        t.dw_create_timestamp       as tgt_create_timestamp,
        current_timestamp           as dw_update_timestamp
    from source_latest s

    left join {{ ref('employees') }} e
        on s.salesRepEmployeeNumber = e.employeeNumber

    {% if is_incremental() %}
    left join {{ this }} t
        on s.src_customerNumber = t.src_customerNumber
    {% else %}
    left join (
        select
            null::bigint    as dw_customer_id,
            null::timestamp as src_update_timestamp,
            null::timestamp as dw_create_timestamp
    ) t on 1 = 0
    {% endif %}
),

-- =================================================
-- 5. Filter NEW + UPDATED customers
-- =================================================
filtered as (
    select *
    from source_with_target
    where
        dw_customer_id is null
        or src_update_timestamp > tgt_update_timestamp
),

-- =================================================
-- 6. Max surrogate key for incremental inserts
-- =================================================
max_key as (
    {% if is_incremental() %}
        select coalesce(max(dw_customer_id), 0) as max_val
        from {{ this }}
    {% else %}
        select 0 as max_val
    {% endif %}
),

-- =================================================
-- 7. Final output with dw_customer_id
-- =================================================
final as (
    select
        coalesce(
            dw_customer_id,
            max_val + row_number() over(order by src_customerNumber)
        )::bigint as dw_customer_id,

        src_customerNumber,
        customerName,
        contactLastName,
        contactFirstName,
        phone,
        addressLine1,
        addressLine2,
        city,
        state,
        postalCode,
        country,
        salesRepEmployeeNumber,
        dw_sales_rep_employee_id,
        creditLimit,  -- <-- Added creditLimit here

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

