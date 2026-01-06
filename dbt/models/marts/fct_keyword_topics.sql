{{ config(
    materialized='table',
    partition_by={
      "field": "execution_date",
      "data_type": "date"
    }
) }}

WITH topic_exploded AS (
    SELECT
        execution_date,
        keyword,
        repo_id,
        stars,
        TRIM(topic) as topic
    FROM {{ ref('fct_github_trends') }},
    UNNEST(SPLIT(topics, ',')) as topic
    WHERE topics IS NOT NULL AND topics != ''
),

topic_stats AS (
    SELECT
        execution_date,
        topic,
        COUNT(DISTINCT keyword) as keyword_count,
        STRING_AGG(DISTINCT keyword ORDER BY keyword) as keywords,
        COUNT(DISTINCT repo_id) as project_count,
        ROUND(AVG(stars), 2) as avg_stars
    FROM topic_exploded
    GROUP BY execution_date, topic
)

SELECT *
FROM topic_stats
WHERE keyword_count >= 2  -- 2개 이상 키워드에 공통으로 등장
  AND project_count >= 5   -- 5개 이상 프로젝트에 등장
