"""Airflow DAG 실패 시 Slack 알림을 전송하는 플러그인."""

import logging
from typing import Any

from airflow.models import Variable
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def send_slack_alert(context: dict[str, Any]) -> None:
    """
    DAG 실패 시 Slack으로 알림을 전송.

    Args:
        context: Airflow task context
    """
    # Slack Webhook URL을 Airflow Variable에서 가져오기
    try:
        slack_webhook_token = Variable.get("SLACK_WEBHOOK_URL")
    except KeyError:
        logging.error("SLACK_WEBHOOK_URL 변수가 설정되지 않았습니다.")
        return

    # Context에서 필요한 정보 추출
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url if task_instance else "N/A"

    # 실패 메시지 구성
    slack_msg = f"""
:x: *DAG 실패 알림*

*DAG ID:* `{dag_id}`
*Task ID:* `{task_id}`
*실행 시간:* {execution_date}
*로그 URL:* {log_url}

작업이 실패했습니다. 로그를 확인해주세요.
    """

    # Slack Hook 생성 및 메시지 전송
    slack_hook = SlackWebhookHook(
        http_conn_id="slack_webhook",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="Airflow Alert Bot",
    )

    try:
        slack_hook.execute()
        logging.info(f"Slack 알림 전송 완료: {dag_id}.{task_id}")
    except Exception as e:
        logging.error(f"Slack 알림 전송 실패: {e}")


def send_slack_success_alert(context: dict[str, Any]) -> None:
    """
    DAG 성공 시 Slack으로 알림을 전송.

    Args:
        context: Airflow task context
    """
    # Slack Webhook URL을 Airflow Variable에서 가져오기
    try:
        slack_webhook_token = Variable.get("SLACK_WEBHOOK_URL")
    except KeyError:
        logging.error("SLACK_WEBHOOK_URL 변수가 설정되지 않았습니다.")
        return

    # Context에서 필요한 정보 추출
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url if task_instance else "N/A"

    # 성공 메시지 구성
    slack_msg = f"""
:white_check_mark: *DAG 성공 알림*

*DAG ID:* `{dag_id}`
*Task ID:* `{task_id}`
*실행 시간:* {execution_date}
*로그 URL:* {log_url}

작업이 성공적으로 완료되었습니다.
    """

    # Slack Hook 생성 및 메시지 전송
    slack_hook = SlackWebhookHook(
        http_conn_id="slack_webhook",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="Airflow Alert Bot",
    )

    try:
        slack_hook.execute()
        logging.info(f"Slack 성공 알림 전송 완료: {dag_id}.{task_id}")
    except Exception as e:
        logging.error(f"Slack 알림 전송 실패: {e}")


def send_slack_retry_alert(context: dict[str, Any]) -> None:
    """
    DAG 재시도 시 Slack으로 알림을 전송.

    Args:
        context: Airflow task context
    """
    # Slack Webhook URL을 Airflow Variable에서 가져오기
    try:
        slack_webhook_token = Variable.get("SLACK_WEBHOOK_URL")
    except KeyError:
        logging.error("SLACK_WEBHOOK_URL 변수가 설정되지 않았습니다.")
        return

    # Context에서 필요한 정보 추출
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    try_number = task_instance.try_number if task_instance else "N/A"
    max_tries = task_instance.max_tries if task_instance else "N/A"
    log_url = task_instance.log_url if task_instance else "N/A"

    # 재시도 메시지 구성
    slack_msg = f"""
:warning: *DAG 재시도 알림*

*DAG ID:* `{dag_id}`
*Task ID:* `{task_id}`
*실행 시간:* {execution_date}
*재시도 횟수:* {try_number}/{max_tries}
*로그 URL:* {log_url}

작업이 재시도되고 있습니다.
    """

    # Slack Hook 생성 및 메시지 전송
    slack_hook = SlackWebhookHook(
        http_conn_id="slack_webhook",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="Airflow Alert Bot",
    )

    try:
        slack_hook.execute()
        logging.info(f"Slack 재시도 알림 전송 완료: {dag_id}.{task_id}")
    except Exception as e:
        logging.error(f"Slack 알림 전송 실패: {e}")
