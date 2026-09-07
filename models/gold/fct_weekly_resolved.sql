{{ config(materialized='table') }}

select
    date_trunc('week', closed_at)::date               as week,
    case when is_pull_request then 'pull_request'
         else 'issue' end                             as item_type,
    count(*)                                          as items_resolved,
    count(distinct author)                            as distinct_authors,
    avg(datediff(hour, created_at, closed_at))        as avg_hours_to_close
from {{ ref('stg_issues') }}
where closed_at is not null
  and closed_at >= '2026-06-01'
group by 1, 2