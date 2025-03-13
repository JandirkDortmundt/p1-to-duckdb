{{
  config(
    materialized='incremental',
    unique_key='timestamp',
    incremental_strategy='append'
  )
}}

WITH source_data AS (
    {{ sqlite_source('e_history_min') }}
)

SELECT * FROM source_data

{% if is_incremental() %}
    WHERE timestamp > (SELECT max(timestamp) FROM {{ this }})
{% endif %}
