from google.cloud import storage
from pathlib import Path
from datetime import datetime

BUCKET_NAME = "de-trend-pipeline-bucket"

def upload_to_gcs(**context):
    """
    Airflow Task로 실행될 함수
    /home/airflow/gcs/dags/data/ 에서 파일을 읽어서
    실제 GCS 버킷(de-trend-pipeline-bucket)으로 복사
    """
    # Airflow context에서 execution_date 가져오기
    execution_date = context.get('ds') 
    
    # 파일 경로
    source_path = Path(f"/home/airflow/gcs/dags/data/github_repos_{execution_date}.parquet")
    
    if not source_path.exists():
        raise FileNotFoundError(f"파일 없음: {source_path}")
    
    # GCS 클라이언트
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Hive 파티셔닝 경로
    gcs_path = f"raw/github_repos/execution_date={execution_date}/data.parquet"

    # blob 객체 생성 
    blob = bucket.blob(gcs_path)
    
    # 업로드 (덮어쓰기)
    blob.upload_from_filename(str(source_path))
    
    print(f"GCS 업로드: gs://{BUCKET_NAME}/{gcs_path}")
    return gcs_path