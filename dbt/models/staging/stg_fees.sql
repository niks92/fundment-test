/*
    Staging model: stg_fees

    Deduplication strategy:
    - Generate a hash of (client_id, adviser_id, fee_date, fee_amount)
    - Keep only distinct combinations
    - This handles exact duplicates from source system
*/

with source as (
    select * from {{ source('raw', 'fees') }}
    {% if is_incremental() %}
    where fee_date >= (select date_sub(max(fee_date), interval 3 day) from {{ this }})
    {% endif %}
),

deduplicated as (
    select distinct
        client_id,
        client_nino,
        adviser_id,
        fee_date,
        fee_amount,
        -- row hash for deduplication and idempotency
        {{ dbt_utils.generate_surrogate_key(['client_id', 'adviser_id', 'fee_date', 'fee_amount']) }} as fee_id
    from source
    where
        -- Data quality filters (NULLs only)
        -- Note: Negative fees are kept as they represent corrections/refunds
        client_id is not null
        and adviser_id is not null
        and client_nino is not null
        and fee_date is not null
        and fee_amount is not null
)

select
    fee_id,
    client_id,
    client_nino,
    adviser_id,
    fee_date,
    fee_amount
from deduplicated
