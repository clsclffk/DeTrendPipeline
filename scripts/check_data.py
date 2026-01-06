import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
data_dir = BASE_DIR / "data"

# 가장 최근에 생성된 parquet 파일
parquet_files = list(data_dir.glob("github_repos_raw_execution_date=*.parquet"))
latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
df = pd.read_parquet(latest_file)

print("데이터 요약")
print("-" * 20)
print(f"전체 데이터 수 : {total_rows:,}행")
print(f"레포지토리 고유 수 : {unique_repos:,}개")
print(f"중복된 데이터 수 : {duplicate_rows:,}행")

"""
데이터 요약
--------------------
전체 데이터 수 : 1,300행
레포지토리 고유 수 : 1,104개
중복된 데이터 수 : 196행
"""
