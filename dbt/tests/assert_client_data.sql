/*
    Test: Each client should have exactly one NINO.

    A client_id mapping to multiple client_nino values indicates
    a data quality issue upstream or a hash collision.

    Test passes when no rows are returned.
*/

select
    client_id,
    count(distinct client_nino) as nino_count
from {{ ref('stg_fees') }}
group by client_id
having count(distinct client_nino) > 1
