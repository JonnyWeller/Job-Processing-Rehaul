"""
TECHNICAL ALERTS

- API DOWN DETECTOR
- LANDING PAGES DOWN DETECTOR
- WEBAPP DOWN DETECTOR

"""
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import os
import json
from loguru import logger
from notifiers.logging import NotificationHandler
from datetime import datetime, timedelta
from twilio.rest import Client
import time
from dotenv import load_dotenv


# load_dotenv('/home/charliegv2/.env')
# FILEPATH = '/home/charliegv2/ad_operations/alerts/'
FILEPATH = "./"

SLACK_WEBHOOK = os.getenv('slack_webhook')
API_CSV_FILEPATH = FILEPATH + 'api_down.csv'
LANDING_CSV_FILEPATH = FILEPATH + 'landing_pages_down.csv'
WEBAPP_CSV_FILEPATH = FILEPATH + 'webapp_down.csv'
RECOVERY_TIME_HOURS = 1
RECOVERY_TIME_MINUTES = 0
CHECK_INTERVAL_MINUTES = 2
os.environ['TZ'] = 'Europe/London'


def send_slack_notification(message=None):
    if message is None:
        print("No message provided - not sending notification")
        return

    defaults = {'webhook_url': SLACK_WEBHOOK}
    handler = NotificationHandler("slack", defaults=defaults)
    logger.add(handler, level="WARNING")
    logger.warning(message)
    logger.remove()
    return


def send_text_alert(check_type, action):
    account_sid = os.getenv('account_sid')
    auth_token = os.getenv('auth_token')
    client = Client(account_sid, auth_token)

    mobile_numbers = [
        '07111111111'
    ]

    for number in mobile_numbers:

        response = client.messages.create(
            body=u"\U0001F6A8 {} IS DOWN \U0001F6A8 {}".format(check_type, action),
            from_='+447123456789',  # Technical Alerts
            to=number
        )
        print(response)

    return


def is_api_down():
    password = os.getenv('api_password')
    user = os.getenv('api_user')
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    url = 'https://app.placed-app.com/api/v4/servant/candidate/languages/363943'
    data = json.dumps(
        {"languages": ["en"]}, indent=4
    )

    try:
        response = requests.post(url, data=data, auth=HTTPBasicAuth(user, password), headers=headers, timeout=20)

        if response.status_code in {200, 201}:
            print(f"{datetime.now()} API STATUS: OK")
            return False, response.status_code, "OK"

        print(f"{datetime.now()} API STATUS: DOWN (BAD RESPONSE)")
        return True, response.status_code, "BAD RESPONSE"

    except requests.exceptions.ReadTimeout:
        print(f"{datetime.now()} API STATUS: DOWN (TIMEOUT)")
        return True, 504, "TIMEOUT"


def is_landing_pages_down():
    url = 'https://apply.placed-app.com/jobs/postcode-search'

    try:
        response = requests.get(url, timeout=10)

        if response.status_code in {200, 201}:
            print(f"{datetime.now()} LANDING_PAGES STATUS: OK")
            return False, response.status_code, "OK"

        return True, response.status_code, "BAD RESPONSE"

    except requests.exceptions.ReadTimeout:
        print(f"{datetime.now()} LANDING_PAGES STATUS: DOWN (TIMEOUT)")
        return True, 504, "TIMEOUT"


def is_webapp_down():
    url = 'https://app.placed-app.com/user-register/candidate-register'

    try:
        response = requests.get(url, timeout=10)

        if response.status_code in {200, 201}:
            print(f"{datetime.now()} WEBAPP STATUS: OK")
            return False, response.status_code, "OK"

        return True, response.status_code, "BAD RESPONSE"

    except requests.exceptions.ReadTimeout:
        print(f"{datetime.now()} WEBAPP STATUS: DOWN (TIMEOUT)")
        return True, 504, "TIMEOUT"


def check_latest_down_time(check_type):
    if check_type == 'API':
        filepath = API_CSV_FILEPATH
    elif check_type == 'LANDING_PAGES':
        filepath = LANDING_CSV_FILEPATH
    elif check_type == 'WEBAPP':
        filepath = WEBAPP_CSV_FILEPATH
    else:
        raise ValueError(f"Unsupported check_type: {check_type}")

    df = pd.read_csv(filepath)
    df = df[df['is_down'] == True]
    if df.empty:
        return True

    try:
        last_down_time = datetime.strptime(df.created_at.max(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        last_down_time = datetime.strptime(df.created_at.max(), "%Y-%m-%d %H:%M:%S.%f")

    if last_down_time > datetime.now() - timedelta(hours=RECOVERY_TIME_HOURS, minutes=RECOVERY_TIME_MINUTES):
        return False

    return True


def check_is_planned(check_type):
    if check_type in {'API', 'WEBAPP'}:  # Never planned outage for these types
        return False

    hour = datetime.now().hour
    minute = datetime.now().minute

    if hour == 3 and 0 < minute < 20:
        return True

    return False


def update_api_csv(check_type, status_text, status_code, is_down, time_now):
    if check_type == 'API':
        filepath = API_CSV_FILEPATH
    elif check_type == 'LANDING_PAGES':
        filepath = LANDING_CSV_FILEPATH
    elif check_type == 'WEBAPP':
        filepath = WEBAPP_CSV_FILEPATH
    else:
        raise ValueError(f"Unsupported check_type: {check_type}")

    df = pd.read_csv(filepath)
    last_id = df.id.max()
    new_entry_df = pd.DataFrame(
        {
            'id': [last_id + 1],
            'status_text': [status_text],
            'status_code': [status_code],
            'is_down': [is_down],
            'created_at': [str(time_now)]
        }
    )
    df = pd.concat([df, new_entry_df])
    df.to_csv(filepath, index=False)
    return


def check_down(check_type):
    has_had_recovery_time = check_latest_down_time(check_type)

    if check_type == 'API':
        is_down, response_code, response_text = is_api_down()
        action = "reset webserver_upgraded"

    elif check_type == 'LANDING_PAGES':
        is_down, response_code, response_text = is_landing_pages_down()
        action = "refresh apply.placed-app.com on pythonAnywhere"

    elif check_type == 'WEBAPP':
        is_down, response_code, response_text = is_webapp_down()
        action = "reset webserver_upgraded"

    else:
        raise ValueError(f"Unsupported check_type: {check_type}")

    if has_had_recovery_time and is_down:
        is_planned = check_is_planned(check_type)

        if is_planned:
            message = f"\n{check_type} IS DOWN (EXPECTED!!) :sweat_smile: {action}"
            print(message)
            send_slack_notification(message)

        else:
            message = f"\n:rotating_light: {check_type} IS DOWN :rotating_light: {action}"
            print(message)
            send_slack_notification(message)
            send_text_alert(check_type, action)

    time_now = datetime.now()
    update_api_csv(check_type, response_text, response_code, is_down, time_now)
    return


def main():
    check_types = [
        'API',
        'LANDING_PAGES',
        'WEBAPP'
    ]

    for check_type in check_types:
        check_down(check_type)


if __name__ == '__main__':
    main()

    while True:
        time.sleep(CHECK_INTERVAL_MINUTES * 60)
        main()
