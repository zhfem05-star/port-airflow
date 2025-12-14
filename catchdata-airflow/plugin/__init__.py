"""Airflow 커스텀 플러그인."""

from airflow.plugins_manager import AirflowPlugin

from plugin.slack_alert import (
    send_slack_alert,
    send_slack_retry_alert,
    send_slack_success_alert,
)


class SlackAlertPlugin(AirflowPlugin):
    """Slack 알림 플러그인."""

    name = "slack_alert_plugin"
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []

    # 플러그인에서 제공하는 함수들
    operators = []
    sensors = []

    # 콜백 함수를 전역에서 사용 가능하도록 export
    callbacks = {
        "send_slack_alert": send_slack_alert,
        "send_slack_success_alert": send_slack_success_alert,
        "send_slack_retry_alert": send_slack_retry_alert,
    }
