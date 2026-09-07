{{ config(
    materialized='incremental',
    unique_key='issue_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with source as (
    select
        payload:id::string                      as issue_id,
        payload:number::int                     as issue_number,
        payload:title::string                   as title,
        payload:state::string                   as state,
        payload:user.login::string              as author,
        payload:comments::int                   as comment_count,
        payload:created_at::timestamp           as created_at,
        payload:updated_at::timestamp           as updated_at,
        payload:closed_at::timestamp            as closed_at,
        payload:pull_request is not null        as is_pull_request,
        ingested_at
    from {{ source('bronze', 'raw_airflow_issues') }}
    {% if is_incremental() %}
        where ingested_at >= (select max(ingested_at) from {{ this }})
    {% endif %}
),

deduped as (
    select * except(rn) from (
        select *, row_number() over (
            partition by issue_id
            order by updated_at desc, ingested_at desc
        ) as rn
        from source
    ) where rn = 1
)

select * from deduped