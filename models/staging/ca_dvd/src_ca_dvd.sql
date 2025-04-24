{{ config(materialized='view') }}

with raw as (
  select *, FILE_NAME as file_name
  from {{ source('raw_csv','ca_dvd') }}
),

parsed as (
  select
    trim(Villes)                           as city,
    safe_cast(O_mois    as float64)        as objectif,
    safe_cast(R_mois    as float64)        as realisation,
    case
      when regexp_contains(file_name, r'HISTO')  then 'HISTO'
      when regexp_contains(file_name, r'BUDGET') then 'BUDGET'
      else 'MENSUEL'
    end                                    as file_type,
    case
      when regexp_contains(file_name, r'Janvier') then '2025-01-01'
      -- ajouter d'autres mois ici...
    end                                    as month_start_str
  from raw
),

clean as (
  select
    city,
    objectif,
    realisation,
    file_type,
    parse_date('%Y-%m-%d', month_start_str) as month
  from parsed
  where objectif >= 0
    and realisation >= 0
    and city is not null
)

select * from clean;