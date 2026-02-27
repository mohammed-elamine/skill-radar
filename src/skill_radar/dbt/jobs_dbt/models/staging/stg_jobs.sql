{{ config(materialized='view') }}

select
  -- identifiant (Adzuna fournit souvent "id")
  cast(job_json->>'id' as varchar) as job_id,

  -- champs textuels
  job_json->>'title' as title,
  job_json->'company'->>'display_name' as company_name,
  job_json->'location'->>'display_name' as location_name,

  -- dates
  job_json->>'created' as created_raw,

  -- url/source
  job_json->>'redirect_url' as url,
  job_json->>'category' as category_raw,

  -- description brute (utile pour extraction skills plus tard)
  job_json->>'description' as description,

  -- remote (selon dispo)
  job_json->>'contract_time' as contract_time,
  job_json->>'contract_type' as contract_type

from raw_jobs
