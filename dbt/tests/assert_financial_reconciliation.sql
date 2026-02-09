/*
    Test: Financial reconciliation across layers.

    SUM(fee_amount) must be preserved from staging through to reporting.
    - stg_fees total should match rpt_client_ltv total (ltv_total = all-time fees)
    - Any mismatch means fees were gained or lost during transformation.

    Tolerance: 0.01 for floating-point rounding.
    Test passes when no rows are returned.
*/

with staging_total as (
    select round(sum(fee_amount), 2) as total_fees
    from {{ ref('stg_fees') }}
),

reporting_total as (
    select round(sum(ltv_total), 2) as total_fees
    from {{ ref('rpt_client_ltv') }}
)

select
    s.total_fees as staging_total_fees,
    r.total_fees as reporting_total_fees,
    round(s.total_fees - r.total_fees, 2) as discrepancy
from staging_total s
cross join reporting_total r
where abs(s.total_fees - r.total_fees) > 0.01
