{{ config(
    materialized='view'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw_data_source', 'github_repos') }}
),

-- 중복 제거 + 타입 정제 
deduped AS (
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
        
        -- 문자열 → TIMESTAMP 변환
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        CAST(pushed_at AS TIMESTAMP) AS pushed_at,
        CAST(loaded_at AS TIMESTAMP) AS loaded_at,
        topics,
        
        -- loaded_at 기준으로 중복 제거
        ROW_NUMBER() OVER (
            PARTITION BY execution_date, repo_id 
            ORDER BY loaded_at DESC  -- 가장 최근 수집 데이터 선택
        ) AS rn
        
    FROM source
)

SELECT * EXCEPT(rn)
FROM deduped
WHERE rn = 1