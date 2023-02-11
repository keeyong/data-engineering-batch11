from airflow.models import Variable

import logging
import requests

def on_failure_callback(context):
    """
    https://airflow.apache.org/_modules/airflow/operators/slack_operator.html
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    text = str(context['task_instance'])
    text += "```" + str(context.get('exception')) +"```"
    send_message_to_a_slack_channel(text, ":scream:")


# def send_message_to_a_slack_channel(message, emoji, channel, access_token):
def send_message_to_a_slack_channel(message, emoji):
    # url = "https://slack.com/api/chat.postMessage"
    url = "https://hooks.slack.com/services/"+Variable.get("slack_url")
    headers = {
        'content-type': 'application/json',
    }
    data = { "username": "Data GOD", "text": message, "icon_emoji": emoji }
    r = requests.post(url, json=data, headers=headers)
    return r
