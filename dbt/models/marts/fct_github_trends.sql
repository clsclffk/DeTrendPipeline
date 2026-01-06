-- 날짜 기준 파티셔닝 (특정 날짜만 조회 시 스캔 비용 감소)
-- keyword 기준 클러스터링 (같은 키워드 값들이 물리적으로 가까이 저장)
{{ config(
    materialized='table',
    partition_by={
      "field": "execution_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['keyword']
) }}

WITH daily_stats AS (
    SELECT
        *,
        -- 키워드별 스타 순위 
        RANK() OVER (
            PARTITION BY execution_date, keyword 
            ORDER BY stars DESC
        ) as star_rank,

        -- 전일 스타 수 
        LAG(stars) OVER (
            PARTITION BY repo_id 
            ORDER BY execution_date
        ) as prev_stars,

        -- 마지막 push 이후 일수
        DATE_DIFF(CURRENT_DATE(), DATE(pushed_at), DAY) as days_since_push


    FROM {{ ref('stg_github_repos') }}
)

SELECT
    execution_date,
    keyword,
    repo_id,
    full_name,
    stars,
    forks,
    open_issues,
    language,
    description,
    pushed_at,
    topics,
    star_rank,
    days_since_push,

    -- 하루 동안 스타가 얼마나 늘었는가 
    COALESCE(stars - prev_stars, 0) as star_increase,
    
    -- 활성도 점수 (간소화)
    ROUND(
        LEAST(stars * 0.001, 40) +
        LEAST(forks * 0.002, 30) +
        LEAST(100.0 / (COALESCE(days_since_push, 0) + 1), 20) +
        LEAST(100.0 / (COALESCE(open_issues, 0) + 1), 10),
    2) as health_score,
    
    CASE 
        WHEN ROUND(
            LEAST(stars * 0.001, 40) +
            LEAST(forks * 0.002, 30) +
            LEAST(100.0 / (COALESCE(days_since_push, 0) + 1), 20) +
            LEAST(100.0 / (COALESCE(open_issues, 0) + 1), 10),
        2) >= 70 THEN '매우 활발'
        WHEN ROUND(
            LEAST(stars * 0.001, 40) +
            LEAST(forks * 0.002, 30) +
            LEAST(100.0 / (COALESCE(days_since_push, 0) + 1), 20) +
            LEAST(100.0 / (COALESCE(open_issues, 0) + 1), 10),
        2) >= 50 THEN '활발'
        WHEN ROUND(
            LEAST(stars * 0.001, 40) +
            LEAST(forks * 0.002, 30) +
            LEAST(100.0 / (COALESCE(days_since_push, 0) + 1), 20) +
            LEAST(100.0 / (COALESCE(open_issues, 0) + 1), 10),
        2) >= 30 THEN '보통'
        ELSE '저조'
    END as activity_level

FROM daily_stats
WHERE star_rank <= 100