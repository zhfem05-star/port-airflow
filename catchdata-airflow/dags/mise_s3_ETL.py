"""대기질 데이터를 S3에 Parquet 형식으로 적재하는 DAG."""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from plugin.slack_alert import send_slack_alert

KR_ISO_CODES = {
    "서울": "KR-11",
    "부산": "KR-26",
    "대구": "KR-27",
    "인천": "KR-28",
    "광주": "KR-29",
    "대전": "KR-30",
    "울산": "KR-31",
    "세종": "KR-50",
    "경기": "KR-41",
    "강원": "KR-42",
    "충북": "KR-43",
    "충남": "KR-44",
    "전북": "KR-45",
    "전남": "KR-46",
    "경북": "KR-47",
    "경남": "KR-48",
    "제주": "KR-49",
}

def extract_air_quality_data(**context) -> list:
    """Task 1: API에서 대기질 데이터 추출."""
    logging.info("TASK 1: 대기질 데이터 추출 시작")
    logging.info("=" * 70)

    # API 설정
    base_url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
    service_key = Variable.get("AIR_sido_api_key")  # Air Quality API Key

    all_data = []
    num_of_rows = 1000
    page_no = 1
    max_pages = 1000

    params = {
        "serviceKey": service_key,
        "returnType": "json",
        "numOfRows": num_of_rows,
        "pageNo": 1,
        "sidoName": "전국",
        "ver": "1.0",
    }

    while page_no <= max_pages:
        try:
            params["pageNo"] = page_no

            response = requests.get(base_url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            # API 오류 체크
            result_code = data["response"]["header"]["resultCode"]
            if result_code != "00":
                logging.error(f"  X 페이지 {page_no}: API 오류 코드 {result_code}")
                break

            # 데이터 추출
            items = data["response"]["body"]["items"]

            # 빈 응답 체크
            if not items or (isinstance(items, list) and len(items) == 0):
                logging.info(f"  페이지 {page_no}: 데이터 없음 → 수집 종료")
                break

            if isinstance(items, dict):
                items = [items]

            all_data.extend(items)
            logging.info(f"  페이지 {page_no}: {len(items)}개 수집 (누적: {len(all_data)}개)")

            # 마지막 페이지 감지
            if len(items) < num_of_rows:
                logging.info(f"  페이지 {page_no}: 마지막 페이지 도달")
                break

            page_no += 1
            time.sleep(1)

        except Exception as e:
            logging.error(f"  X 페이지 {page_no}: {e}")
            break

    if len(all_data) == 0:
        msg = "X 데이터 추출 실패 또는 없음"
        raise ValueError(msg)
    if len(all_data) <= 400:
        logging.warning(f"  ! 추출된 데이터가 적습니다: {len(all_data)}개")
    logging.info(f"V 총 {len(all_data)}개 데이터 추출 완료")
    logging.info("=" * 70)
    # XCom으로 다음 task에 데이터 전달
    return all_data

def transform_data(**context) -> pd.DataFrame:
    """Task2: 데이터 pd dataframe으로 변환 및 전처리."""
    logging.info("TASK 2: 데이터 전처리")
    logging.info("=" * 70)

    # XCom에서 추출된 데이터 가져오기
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_air_quality_data")

    # DataFrame으로 변환
    df = pd.DataFrame(raw_data)
    logging.debug(f"V 원본 데이터프레임 크기: {df.shape}")

    # 필요한 컬럼 선택 및 이름 변경
    selected_columns = {
        "dataTime": "측정일시",
        "sidoName": "시도명",
        "pm10Value": "미세먼지",
        "pm25Value": "초미세먼지",
        "o3Value": "오존",
        "no2Value": "이산화질소",
        "coValue": "일산화탄소",
        "so2Value": "아황산가스",
        "isoCode": "ISO_코드",
    }

    df = df[list(selected_columns.keys())]
    df.rename(columns=selected_columns, inplace=True)
    # ISO 코드 매핑
    df["ISO_코드"] = df["시도명"].map(KR_ISO_CODES)
    # 결측치 처리
    df.replace("-", None, inplace=True)

    # 데이터 타입 변환
    df["측정일시"] = pd.to_datetime(df["측정일시"], format="%Y-%m-%d %H:%M")
    numeric_columns = ["미세먼지", "초미세먼지", "오존", "이산화질소", "일산화탄소", "아황산가스"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logging.debug(f"V 전처리된 데이터프레임 크기: {df.shape}")
    logging.info("V 데이터 전처리 완료")
    # XCom으로 다음 task에 데이터 전달
    return df.to_dict(orient="records")

def load_data_to_s3(**context):
    """Task 3: 전처리된 데이터를 S3에 Parquet 형식으로 업로드."""
    from io import BytesIO

    logging.info("TASK 3: S3에 데이터 업로드 (Parquet)")
    logging.info("=" * 70)

    # XCom에서 전처리된 데이터 가져오기
    ti = context["ti"]
    processed_data = ti.xcom_pull(task_ids="transform_data")

    # DataFrame으로 변환
    df = pd.DataFrame(processed_data)

    # Parquet으로 저장
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)

    # S3에 업로드
    s3_hook = S3Hook(aws_conn_id="my_aws_connection")
    bucket_name = Variable.get("AIR_s3_bucket_name")
    s3_key = f"air_quality/mise_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

    s3_hook.load_bytes(
        bytes_data=parquet_buffer.getvalue(),
        key=s3_key,
        bucket_name=bucket_name,
        replace=True,
    )

    logging.info(f"V 데이터가 S3 버킷 '{bucket_name}'의 '{s3_key}'에 Parquet 형식으로 업로드되었습니다.")

with DAG(
    dag_id="mise_data_to_s3",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": send_slack_alert,  # 실패 시 Slack 알림
    },
    description="대기질 데이터를 S3에 Parquet 형식으로 적재하는 DAG",
    schedule="0 6 * * *",  # 매일 6시에 실행
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["air_quality", "s3", "parquet"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_air_quality_data",
        python_callable=extract_air_quality_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_data_to_s3",
        python_callable=load_data_to_s3,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
