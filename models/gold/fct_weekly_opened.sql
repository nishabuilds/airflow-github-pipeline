{{ config(materialized='table') }}

select
    date_trunc('week', created_at)::date              as week,
    case when is_pull_request then 'pull_request'
         else 'issue' end                             as item_type,
    count(*)                                          as items_opened,
    count(distinct author)                            as distinct_authors,
    avg(comment_count)                                as avg_comments
from {{ ref('stg_issues') }}
where created_at >= '2026-06-01'
group by 1, 2