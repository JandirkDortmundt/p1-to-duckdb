{{ config(
    materialized='incremental',
    unique_key='timestamp',  -- Adjust based on your actual primary key
    incremental_strategy='merge'
  ) }}

WITH source_data AS (
    {{ sqlite_source('e_history_uur') }}
)

SELECT * FROM source_data

{% if is_incremental() %}
    WHERE timestamp > (SELECT max(timestamp) FROM {{ this }})
{% endif %}
