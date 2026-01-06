from google.cloud import bigquery

# 전역 설정
PROJECT_ID = "de-trend-pipeline"
DATASET_ID = "raw_data"
TABLE_ID = "github_repos"
BUCKET_NAME = "de-trend-pipeline-bucket"

def create_external_table(**context):
    """
    GCS에 저장된 Parquet 파일을
    Hive 파티셔닝 방식의 BigQuery External Table로 생성
    """

    # BigQuery 클라이언트 생성 
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # 1. 데이터셋 확인 및 생성
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset.location = "asia-northeast3"
    client.create_dataset(dataset, exists_ok=True)
    
    # 2. External Table 스키마 정의
    schema = [
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("repo_id", "INTEGER"),
        bigquery.SchemaField("full_name", "STRING"),
        bigquery.SchemaField("stars", "INTEGER"),
        bigquery.SchemaField("forks", "INTEGER"),
        bigquery.SchemaField("open_issues", "INTEGER"),
        bigquery.SchemaField("language", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("created_at", "STRING"),
        bigquery.SchemaField("updated_at", "STRING"),
        bigquery.SchemaField("pushed_at", "STRING"),
        bigquery.SchemaField("topics", "STRING"),
        bigquery.SchemaField("loaded_at", "STRING"),
    ]
    
    # 3. External Table + Hive 파티셔닝 설정
    table = bigquery.Table(table_ref, schema=schema)
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [f"gs://{BUCKET_NAME}/raw/github_repos/*"]
    
    hive_options = bigquery.HivePartitioningOptions()

    # execution_date=YYYY-MM-DD 디렉터리를 파티션 컬럼으로 인식
    hive_options.source_uri_prefix = f"gs://{BUCKET_NAME}/raw/github_repos/"
    
    # 디렉터리 구조 기반으로 파티션 컬럼 자동 추론
    hive_options.mode = "AUTO" 
    external_config.hive_partitioning = hive_options
    table.external_data_configuration = external_config
    
    # 4. 스키마/옵션 변경 시 테이블 재생성 
    client.delete_table(table_ref, not_found_ok=True)
    client.create_table(table)
    
    print(f"External table {table_ref} created successfully")
    return table_ref