# Slack Alert Plugin

DAG 실행 결과(성공/실패/재시도)를 Slack으로 알림 전송하는 Airflow 플러그인

## 설정 방법

### 1. Slack Webhook URL 생성

1. Slack 워크스페이스의 [Incoming Webhooks](https://api.slack.com/messaging/webhooks) 설정
2. Webhook URL 복사

### 2. Airflow Variable 설정

Airflow UI에서 다음 Variable을 설정합니다:

```
Key: SLACK_WEBHOOK_URL
Value: https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

또는 CLI로 설정:

```bash
airflow variables set SLACK_WEBHOOK_URL "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### 3. Airflow Connection 설정 (선택사항)

Airflow UI의 Admin > Connections에서:

- **Conn Id**: `slack_webhook`
- **Conn Type**: `HTTP`
- **Host**: (비워둠)

## 사용 방법

### 기본 사용법

DAG에서 `default_args`에 콜백 함수를 추가합니다:

```python
from plugin.slack_alert import send_slack_alert, send_slack_success_alert, send_slack_retry_alert
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_slack_alert,  # 실패 시 알림
    "on_success_callback": send_slack_success_alert,  # 성공 시 알림 (선택)
    "on_retry_callback": send_slack_retry_alert,  # 재시도 시 알림 (선택)
}

with DAG(
    dag_id="example_dag",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # tasks...
```

### 특정 Task에만 적용

```python
from airflow.operators.python import PythonOperator
from plugin.slack_alert import send_slack_alert

task = PythonOperator(
    task_id="important_task",
    python_callable=my_function,
    on_failure_callback=send_slack_alert,
)
```

### DAG 레벨에서 적용

```python
with DAG(
    dag_id="example_dag",
    default_args=default_args,
    on_failure_callback=send_slack_alert,  # DAG 전체 실패 시
) as dag:
    # tasks...
```

## 제공되는 함수

### `send_slack_alert(context)`
- DAG/Task 실패 시 Slack 알림 전송
- 빨간색 X 아이콘 표시
- 실패한 DAG ID, Task ID, 실행 시간, 로그 URL 포함

### `send_slack_success_alert(context)`
- DAG/Task 성공 시 Slack 알림 전송
- 녹색 체크 아이콘 표시
- 성공한 DAG ID, Task ID, 실행 시간, 로그 URL 포함

### `send_slack_retry_alert(context)`
- DAG/Task 재시도 시 Slack 알림 전송
- 노란색 경고 아이콘 표시
- 재시도 횟수 정보 포함

## 알림 메시지 형식

### 실패 알림
```
:x: DAG 실패 알림

DAG ID: `example_dag`
Task ID: `task_name`
실행 시간: 2023-01-01 06:00:00
로그 URL: http://localhost:8080/log?...

작업이 실패했습니다. 로그를 확인해주세요.
```

### 성공 알림
```
:white_check_mark: DAG 성공 알림

DAG ID: `example_dag`
Task ID: `task_name`
실행 시간: 2023-01-01 06:00:00
로그 URL: http://localhost:8080/log?...

작업이 성공적으로 완료되었습니다.
```

### 재시도 알림
```
:warning: DAG 재시도 알림

DAG ID: `example_dag`
Task ID: `task_name`
실행 시간: 2023-01-01 06:00:00
재시도 횟수: 1/3
로그 URL: http://localhost:8080/log?...

작업이 재시도되고 있습니다.
```

## 트러블슈팅

### 알림이 전송되지 않는 경우

1. **SLACK_WEBHOOK_URL 변수 확인**
   ```bash
   airflow variables get SLACK_WEBHOOK_URL
   ```

2. **Slack Provider 패키지 설치 확인**
   ```bash
   pip install apache-airflow-providers-slack
   ```

3. **Airflow 로그 확인**
   - Task 로그에서 "Slack 알림 전송 실패" 메시지 확인
   - Webhook URL이 올바른지 확인

4. **네트워크 연결 확인**
   - Airflow가 외부 인터넷에 접근 가능한지 확인
   - 방화벽 설정 확인

## 커스터마이징

메시지 형식이나 내용을 변경하려면 `slack_alert.py` 파일의 각 함수에서 `slack_msg` 변수를 수정하세요.

```python
slack_msg = f"""
:custom_emoji: *커스텀 알림*

*프로젝트:* Production
*DAG:* `{dag_id}`
*상태:* 실패
*담당자:* @team-data
"""
```
