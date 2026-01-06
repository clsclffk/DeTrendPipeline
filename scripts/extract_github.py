import requests
import pandas as pd
import os
from datetime import datetime
import time
from pathlib import Path
from airflow.models import Variable

GITHUB_TOKEN = Variable.get("GITHUB_TOKEN")
KEYWORDS = ["airflow", "dagster", "prefect", "spark", "flink", "kafka", 
            "data-lake", "data-warehouse", "lakehouse", "iceberg", "delta-lake", "dbt", "bigquery"]

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json" 
}

def search_github_repos(keyword, per_page=100):
    """키워드로 GitHub 인기 레포지토리 Top N을 가져오는 함수"""
    url = (
        f"https://api.github.com/search/repositories"
        f"?q={keyword}+in:name,description,topics"
        f"&sort=stars&order=desc&per_page={per_page}"
    )
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json().get("items", [])
    
    # API 호출 실패 시 에러 코드 출력 
    else:
        print(f"Error {response.status_code}: {keyword}")
        return []

def extract_github_data(**context):
    """
    Airflow Task로 실행될 함수
    - 여러 키워드의 레포지토리 수집 및 실행 날짜 기준 parquet 파일 생성
    """
    
    # Airflow에서 자동으로 제공하는 execution_date
    execution_date = context['ds']  # date string 
    
    print(f"데이터 수집 시작: {execution_date}")
    
    all_results = []
    for kw in KEYWORDS:
        print(f"키워드 : {kw}")
        repos = search_github_repos(kw)
        
        for repo in repos:
            all_results.append({
                "execution_date": execution_date,
                "keyword": kw,
                "repo_id": repo["id"],
                "full_name": repo["full_name"],
                "stars": repo["stargazers_count"],
                "forks": repo["forks_count"],
                "open_issues": repo["open_issues_count"],
                "language": repo.get("language"),
                "description": repo.get("description"),
                "created_at": repo["created_at"],
                "updated_at": repo["updated_at"],
                "pushed_at": repo["pushed_at"],
                "topics": ",".join(repo.get("topics", [])),
                "loaded_at": datetime.utcnow().isoformat(),
            })
        # rate limit 방지 
        time.sleep(2)
    
    df = pd.DataFrame(all_results)
    print(f"총 {len(df)}개 레포지토리 수집")
    
    # Airflow GCS 마운트 경로에 저장
    data_dir = Path("/home/airflow/gcs/dags/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = data_dir / f"github_repos_{execution_date}.parquet"
    df.to_parquet(output_path, index=False)
    
    print(f"저장 완료: {output_path}")
    return str(output_path)

# 로컬 테스트용
if __name__ == "__main__":
    # 로컬에서는 .env 사용
    from dotenv import load_dotenv
    load_dotenv()
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    extract_github_data(ds=datetime.utcnow().date().isoformat())