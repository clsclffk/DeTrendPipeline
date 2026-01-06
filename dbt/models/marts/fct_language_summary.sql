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
    
    COUNT(DISTINCT repo_id) as total_projects,
    SUM(stars) as total_stars,
    ROUND(AVG(stars), 2) as avg_stars,
    ROUND(AVG(health_score), 2) as avg_health_score

FROM {{ ref('fct_github_trends') }}
WHERE language IS NOT NULL
GROUP BY execution_date, language
HAVING total_projects >= 5