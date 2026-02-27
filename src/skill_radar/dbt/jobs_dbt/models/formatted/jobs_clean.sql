{{ config(materialized='table') }}

select
    job_id,

    lower(trim(title)) as title,

    lower(trim(company_name)) as company_name,

    lower(trim(location_name)) as location_name,

    -- conversion date string → timestamp
    try_cast(created_raw as timestamp) as created_at,

    url,

    lower(trim(contract_time)) as contract_time,

    lower(trim(contract_type)) as contract_type,

    description

from {{ ref('stg_jobs') }}

where job_id is not null
