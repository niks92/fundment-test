/*
    Purpose:
    - Aggregate LTV metrics by cohort (month of first fee)
    - Support cohort-based performance analysis

    Definition:
    - A cohort is defined by the month in which a client first paid a fee
    - Example: "2025-01" cohort = all clients whose first fee was in January 2025
*/

with cohort_metrics as (
    select
        c.cohort_month,
        count(distinct l.client_id) as client_count,

        -- Average LTV by window
        round(avg(l.ltv_1_month), 2) as avg_ltv_1_month,
        round(avg(l.ltv_3_month), 2) as avg_ltv_3_month,
        round(avg(l.ltv_6_month), 2) as avg_ltv_6_month,

        -- Total LTV by window (sum across cohort)
        round(sum(l.ltv_1_month), 2) as total_ltv_1_month,
        round(sum(l.ltv_3_month), 2) as total_ltv_3_month,
        round(sum(l.ltv_6_month), 2) as total_ltv_6_month,

        -- Median LTV (using APPROX_QUANTILES for efficiency)
        round(approx_quantiles(l.ltv_6_month, 2)[offset(1)], 2) as median_ltv_6_month

    from {{ ref('dim_clients') }} c
    inner join {{ ref('rpt_client_ltv') }} l
        on c.client_id = l.client_id
    group by c.cohort_month
)

select
    cohort_month,
    client_count,
    avg_ltv_1_month,
    avg_ltv_3_month,
    avg_ltv_6_month,
    total_ltv_1_month,
    total_ltv_3_month,
    total_ltv_6_month,
    median_ltv_6_month
from cohort_metrics
order by cohort_month
