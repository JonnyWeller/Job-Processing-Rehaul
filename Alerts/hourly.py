import pyplaced
from loguru import logger
from notifiers.logging import NotificationHandler
from dotenv import load_dotenv
import pandas as pd
from pyplaced.db_helpers import create_conn


SLACK_WEBHOOK = os.getenv('slack_webhook')
CHECK_FEED_DAYS = 1
load_dotenv('C:/Users/Jonny W/PycharmProjects/Job Processing Rehaul/Feed Monitoring/.env')
API_CSV_FILEPATH = './api_down.csv'
LANDING_CSV_FILEPATH = './landing_pages_down.csv'
WEBAPP_CSV_FILEPATH = './webapp_down.csv'


def GetPlaced(query):
    df = pyplaced.run_query(query, target_db='placed')
    return df


def GetInsights(query):
    df = pyplaced.run_query(query, target_db='insights')
    return df


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


def get_locked_feeds():
    query = f"""
    select id, NAME, COMMAND, last_execution
    from scheduled_command
    where id not in (11, 17)  # Exclude old commands from 2020
    and locked = 1
    and last_execution < now() - interval {CHECK_FEED_DAYS} day
    """
    df = GetPlaced(query)
    return df


def check_locked_feeds():
    # Check if any feeds are locked
    feeds_df = get_locked_feeds()
    if feeds_df.empty:
        print("No Locked Commands Found.")
        pass
    else:
        for index, row in feeds_df.iterrows():
            command = row['COMMAND']
            last_execution = row['last_execution']

            message = f"\nTEST TEST COMMAND `{command}`" \
                      f"\nNOT RUN SINCE {last_execution}"
            send_slack_notification(message)


def update_db(df, table_name):
    conn, cur = create_conn(credentials_dict=None)

    sql = f"""
    INSERT IGNORE INTO insights.{table_name}
    (id, status_text, status_code, is_down, created_at)
    VALUES (%s, %s, %s, %s, %s)
    """
    values = list(df.itertuples(index=False))

    cur.executemany(sql, values)
    conn.commit()
    cur.close()
    conn.close()
    return


def update_down_tables():
    df_api = pd.read_csv(API_CSV_FILEPATH)
    df_landing = pd.read_csv(LANDING_CSV_FILEPATH)
    df_webapp = pd.read_csv(WEBAPP_CSV_FILEPATH)

    update_db(df_api, 'status_api')
    update_db(df_landing, 'status_landing_pages')
    update_db(df_webapp, 'status_webapp')


def generate_csv_from_tables():
    df_api = GetInsights("select * from insights.status_api order by id desc")
    df_landing = GetInsights("select * from insights.status_landing_pages order by id desc")
    df_webapp = GetInsights("select * from insights.status_webapp order by id desc")

    df_api.to_csv(API_CSV_FILEPATH, index=False)
    df_landing.to_csv(LANDING_CSV_FILEPATH, index=False)
    df_webapp.to_csv(WEBAPP_CSV_FILEPATH, index=False)


def main():
    check_locked_feeds()
    update_down_tables()


if __name__ == '__main__':
    # main()
    generate_csv_from_tables()
