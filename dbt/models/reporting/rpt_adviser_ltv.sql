/*
    Attribution methodology: FIRST-TOUCH ATTRIBUTION
    - Each client is attributed to exactly ONE adviser: the adviser
      associated with the client's FIRST fee payment
    - The client's entire LTV (1, 3, 6 month) is credited to that adviser
    - This approach rewards client acquisition/onboarding
*/

with adviser_clients as (
    select
        c.first_adviser_id as adviser_id,
        c.client_id,
        l.ltv_1_month,
        l.ltv_3_month,
        l.ltv_6_month,
        l.ltv_total
    from {{ ref('dim_clients') }} c
    inner join {{ ref('rpt_client_ltv') }} l
        on c.client_id = l.client_id
),

adviser_aggregates as (
    select
        adviser_id,
        count(distinct client_id) as client_count,

        -- Total LTV attributed to adviser
        round(sum(ltv_1_month), 2) as total_client_ltv_1_month,
        round(sum(ltv_3_month), 2) as total_client_ltv_3_month,
        round(sum(ltv_6_month), 2) as total_client_ltv_6_month,
        round(sum(ltv_total), 2) as total_client_ltv_all_time,

        -- Average LTV per client
        round(avg(ltv_6_month), 2) as avg_client_ltv_6_month,

        -- Rank by 6-month LTV
        rank() over (order by sum(ltv_6_month) desc) as rank_by_ltv_6_month

    from adviser_clients
    group by adviser_id
)

select
    adviser_id,
    client_count,
    total_client_ltv_1_month,
    total_client_ltv_3_month,
    total_client_ltv_6_month,
    total_client_ltv_all_time,
    avg_client_ltv_6_month,
    rank_by_ltv_6_month
from adviser_aggregates
order by rank_by_ltv_6_month
