/*
    Test: Validate rpt_client_ltv calculations against raw fee data

    This test recalculates LTV from staging and compares to mart values.
    Fails if any client has a mismatch > 0.01 (rounding tolerance).
*/

with client_first_fee as (
    select
        client_id,
        min(fee_date) as first_fee_date
    from {{ ref('stg_fees') }}
    group by client_id
),

expected_ltv as (
    select
        f.client_id,
        cff.first_fee_date,
        sum(case
            when f.fee_date < date_add(cff.first_fee_date, interval 1 month)
            then f.fee_amount else 0
        end) as expected_ltv_1_month,
        sum(case
            when f.fee_date < date_add(cff.first_fee_date, interval 3 month)
            then f.fee_amount else 0
        end) as expected_ltv_3_month,
        sum(case
            when f.fee_date < date_add(cff.first_fee_date, interval 6 month)
            then f.fee_amount else 0
        end) as expected_ltv_6_month,
        sum(f.fee_amount) as expected_ltv_total
    from {{ ref('stg_fees') }} f
    inner join client_first_fee cff on f.client_id = cff.client_id
    group by f.client_id, cff.first_fee_date
),

comparison as (
    select
        e.client_id,
        e.expected_ltv_1_month,
        m.ltv_1_month as actual_ltv_1_month,
        e.expected_ltv_3_month,
        m.ltv_3_month as actual_ltv_3_month,
        e.expected_ltv_6_month,
        m.ltv_6_month as actual_ltv_6_month,
        e.expected_ltv_total,
        m.ltv_total as actual_ltv_total
    from expected_ltv e
    inner join {{ ref('rpt_client_ltv') }} m on e.client_id = m.client_id
)

-- Return rows where expected != actual (with 0.01 tolerance for rounding)
select *
from comparison
where abs(expected_ltv_1_month - actual_ltv_1_month) > 0.01
   or abs(expected_ltv_3_month - actual_ltv_3_month) > 0.01
   or abs(expected_ltv_6_month - actual_ltv_6_month) > 0.01
   or abs(expected_ltv_total - actual_ltv_total) > 0.01
