/*
    Purpose:
    - Calculate lifetime value (accumulated fees) at 1, 3, and 6 month windows
    - LTV windows are measured from each client's first fee date
*/

with client_fees as (
    select
        f.client_id,
        f.fee_date,
        f.fee_amount,
        c.first_fee_date
    from {{ ref('stg_fees') }} f
    inner join {{ ref('dim_clients') }} c
        on f.client_id = c.client_id
),

ltv_calculations as (
    select
        client_id,
        first_fee_date,

        -- 1-month LTV: fees within first month
        sum(case
            when fee_date < date_add(first_fee_date, interval 1 month)
            then fee_amount
            else 0
        end) as ltv_1_month,

        -- 3-month LTV: fees within first 3 months
        sum(case
            when fee_date < date_add(first_fee_date, interval 3 month)
            then fee_amount
            else 0
        end) as ltv_3_month,

        -- 6-month LTV: fees within first 6 months
        sum(case
            when fee_date < date_add(first_fee_date, interval 6 month)
            then fee_amount
            else 0
        end) as ltv_6_month,

        -- Total LTV (all time) for reference
        sum(fee_amount) as ltv_total

    from client_fees
    group by client_id, first_fee_date
)

select
    client_id,
    first_fee_date,
    round(ltv_1_month, 2) as ltv_1_month,
    round(ltv_3_month, 2) as ltv_3_month,
    round(ltv_6_month, 2) as ltv_6_month,
    round(ltv_total, 2) as ltv_total
from ltv_calculations
