{{ config(
    materialized='table',
    partition_by={
      "field": "execution_date",
      "data_type": "date"
    }
) }}

SELECT
    execution_date,
    keyword,
    
    -- 기본 통계
    COUNT(DISTINCT repo_id) as total_projects,
    SUM(stars) as total_stars,
    ROUND(AVG(stars), 2) as avg_stars,
    MAX(stars) as max_stars,
    
    -- 활성도
    ROUND(AVG(health_score), 2) as avg_health_score,
    COUNTIF(activity_level = '매우 활발') as very_active_count,
    COUNTIF(activity_level IN ('매우 활발', '활발')) as active_count,
    
    -- 스타 증가량 
    SUM(star_increase) as total_star_increase,
    ROUND(AVG(star_increase), 2) as avg_star_increase,
    
    -- 언어 다양성
    COUNT(DISTINCT language) as language_count

FROM {{ ref('fct_github_trends') }}
GROUP BY execution_date, keyword