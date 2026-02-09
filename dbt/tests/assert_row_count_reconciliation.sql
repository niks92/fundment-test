/*
    Test: Row count reconciliation between raw.fees and stg_fees.

    stg_fees removes exact duplicates and null rows from raw.fees, so:
    - stg_fees row count should never EXCEED raw.fees
    - stg_fees should not lose more than 5% of rows (unexpected data quality issue)

    Test passes when no rows are returned.
*/

with counts as (
    select
        (select count(*) from {{ source('raw', 'fees') }}) as raw_count,
        (select count(*) from {{ ref('stg_fees') }}) as staging_count
)

select
    raw_count,
    staging_count,
    raw_count - staging_count as rows_removed,
    round((raw_count - staging_count) / nullif(raw_count, 0) * 100, 2) as pct_removed
from counts
where
    -- staging should never have more rows than raw
    staging_count > raw_count
    -- alert if more than 5% of rows are lost to dedup/null filtering
    or (raw_count - staging_count) > raw_count * 0.05
