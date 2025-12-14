"""S3에서 Snowflake로 대기질 데이터를 적재하는 DAG."""

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.external_task import ExternalTaskSensor
from plugin.slack_alert import send_slack_alert


def get_snowflake_connection():
    """Snowflake 연결을 생성하고 커서를 반환."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_id")
    return hook.get_conn().cursor()

def upload_to_snowflake(**context):
    """Task: S3에서 데이터를 읽어와 Snowflake에 업로드."""
    from io import BytesIO

    logging.info("TASK: S3에서 Snowflake로 데이터 업로드 시작")
    logging.info("=" * 70)

    # ETL DAG과 동일한 Variable 이름 사용
    s3_bucket = Variable.get("AIR_s3_bucket_name")

    # S3Hook으로 최신 파일 찾기
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")

    # air_quality/ 폴더에서 최신 parquet 파일 찾기
    prefix = "air_quality/"
    keys = s3_hook.list_keys(bucket_name=s3_bucket, prefix=prefix)

    if not keys:
        logging.error(f"S3 버킷에서 파일을 찾을 수 없습니다: s3://{s3_bucket}/{prefix}")
        return

    # 가장 최신 파일 선택 (파일명에 타임스탬프 포함)
    latest_key = sorted(keys)[-1]
    logging.info(f"최신 파일 선택: {latest_key}")

    # S3에서 Parquet 파일 읽기
    obj = s3_hook.get_key(latest_key, bucket_name=s3_bucket)
    parquet_data = obj.get()["Body"].read()
    df = pd.read_parquet(BytesIO(parquet_data))

    logging.info(f"데이터 로드 완료: {len(df)}행")
    logging.info(f"컬럼: {df.columns.tolist()}")

    # Snowflake에 데이터 업로드
    cursor = get_snowflake_connection()

    # 컬럼명 매핑 (한글 -> 영문)
    for _, row in df.iterrows():
        insert_query = f"""
        INSERT INTO air_quality_data (
            data_time, sido_name, pm10_value, pm25_value,
            o3_value, no2_value, co_value, so2_value, iso_code
        )
        VALUES (
            '{row["측정일시"]}', '{row["시도명"]}', {row["미세먼지"] if pd.notna(row["미세먼지"]) else "NULL"},
            {row["초미세먼지"] if pd.notna(row["초미세먼지"]) else "NULL"}, {row["오존"] if pd.notna(row["오존"]) else "NULL"},
            {row["이산화질소"] if pd.notna(row["이산화질소"]) else "NULL"}, {row["일산화탄소"] if pd.notna(row["일산화탄소"]) else "NULL"},
            {row["아황산가스"] if pd.notna(row["아황산가스"]) else "NULL"}, '{row["ISO_코드"]}'
        )
        """
        cursor.execute(insert_query)

    cursor.close()
    logging.info(f"V 데이터가 성공적으로 Snowflake에 업로드되었습니다: {len(df)}행")
    logging.info("=" * 70)

with DAG(
    dag_id="mise_s3_to_snowflake",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": send_slack_alert,  # 실패 시 Slack 알림
    },
    description="S3에서 Snowflake로 대기질 데이터를 적재하는 DAG",
    schedule="0 6 * * *",  # mise_data_to_s3와 동일한 스케줄
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["air_quality", "s3", "snowflake"],
) as dag:
    # mise_data_to_s3 DAG의 load_data_to_s3 작업이 완료될 때까지 대기
    wait_for_s3_load = ExternalTaskSensor(
        task_id="wait_for_s3_load",
        external_dag_id="mise_data_to_s3",
        external_task_id="load_data_to_s3",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,  # 60초마다 체크
        timeout=3600,  # 1시간 타임아웃
    )

    upload_task = PythonOperator(
        task_id="upload_data_to_snowflake",
        python_callable=upload_to_snowflake,
        provide_context=True,
    )

    # 의존성 설정: S3 적재 완료 후 Snowflake 업로드 실행
    wait_for_s3_load >> upload_task
