/*
    Dimension: dim_clients

    Purpose:
    - Establish client cohort (month of first fee)
    - Capture first adviser for LTV attribution
    - Single source of truth for client-level attributes

    Attribution strategy:
    - First-touch: The adviser associated with the client's FIRST fee
      gets credit for the client's entire lifetime value.
    - This rewards client acquisition and is simple to audit.
*/

with first_fees as (
    select
        client_id,
        adviser_id,
        fee_date,
        row_number() over (
            partition by client_id
            order by fee_date asc, fee_id asc  -- deterministic tie-breaker
        ) as fee_rank
    from {{ ref('stg_fees') }}
)

select
    client_id,
    fee_date as first_fee_date,
    adviser_id as first_adviser_id,
    format_date('%Y-%m', fee_date) as cohort_month
from first_fees
where fee_rank = 1
