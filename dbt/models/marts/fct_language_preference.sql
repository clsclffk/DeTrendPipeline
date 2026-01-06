{{ config(
    materialized='table',
    partition_by={
      "field": "execution_date",
      "data_type": "date"
    }
) }}

SELECT
    execution_date,
    language,
    keyword,
    COUNT(DISTINCT repo_id) as project_count,
    SUM(stars) as total_stars,
    ROUND(AVG(stars), 2) as avg_stars,
    
    ROUND(
        COUNT(DISTINCT repo_id) * 100.0 / 
        SUM(COUNT(DISTINCT repo_id)) OVER (PARTITION BY execution_date, language),
    2) as keyword_share_in_language

FROM {{ ref('fct_github_trends') }}
WHERE language IS NOT NULL
GROUP BY execution_date, language, keyword
HAVING project_count >= 3