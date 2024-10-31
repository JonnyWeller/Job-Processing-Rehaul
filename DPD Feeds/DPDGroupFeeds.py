"""
Get Feeds
Split by keyword in title / location

Get IDs
Update venues with correct jobs to import

"""
import pandas as pd
import requests
import json
import pygsheets
import os
from dotenv import load_dotenv
import pyplaced
import time
from loguru import logger
from notifiers.logging import NotificationHandler
import os

SLACK_WEBHOOK = os.getenv('slack_webhook')
defaults = {'webhook_url': SLACK_WEBHOOK}
handler = NotificationHandler("slack", defaults=defaults)
logger.add(handler, level="ERROR")

load_dotenv()

with open(os.getenv('credentials_dict_file_location'), 'r') as f:
    CREDENTIALS_DICT = json.loads(f.read())


def update_job_offer_references(venue_id: int, job_offer_references_to_import: str):
    ssh_tunnel, conn = pyplaced.db_helpers.ssh_conn(credentials_dict=CREDENTIALS_DICT)

    cur = conn.cursor()
    sql = f"""
    UPDATE venue
    SET job_offer_references_to_import = "{job_offer_references_to_import}"
    WHERE id = {venue_id}
    """
    cur.execute(sql)
    conn.commit()
    print(sql)
    print("SUCCESSFULLY EXECUTED")
    cur.close()
    conn.close()
    return


def get_dpd_feed():
    url = 'https://api.smartrecruiters.com/v1/companies/DPDGroupUK1/postings'
    x = requests.get(url)
    job_data = json.loads(x.content)
    df = pd.json_normalize(job_data['content'])
    print(f"{df['id'].size} Jobs Found")
    df = df.rename(columns={'location.city': 'location_city', 'name': 'job_title'})
    return df


def get_filter_table():
    gc = pygsheets.authorize(service_file=os.environ.get("google_service_account_file_path"))
    worksheet_key = os.getenv('worksheet_key')
    sh = gc.open_by_key(worksheet_key)
    wks = sh.worksheet(property='id', value=0)
    df = wks.get_as_df()
    print(df)
    return df


def filter_feed(df_filter, title, location, refNumber):

    for index, row in df_filter.iterrows():
        if refNumber == row['ref_number']:
            print(refNumber)
            return row['priority']
        if row['title_keyword'] in title and row['location_keyword'] in location:
            return row['priority']

    return "low"


def get_priority_lists(df):
    low = df[df['priority'] == 'low']['uuid'].astype(str).str.cat(sep=',')
    print(f"{df[df['priority'] == 'low']['uuid'].size} Low Priority Jobs")
    medium = df[df['priority'] == 'medium']['uuid'].astype(str).str.cat(sep=',')
    print(f"{df[df['priority'] == 'medium']['uuid'].size} Medium Priority Jobs")
    high = df[df['priority'] == 'high']['uuid'].astype(str).str.cat(sep=',')
    print(f"{df[df['priority'] == 'high']['uuid'].size} High Priority Jobs")
    monthly = df[df['priority'] == 'monthly']['uuid'].astype(str).str.cat(sep=',')
    print(f"{df[df['priority'] == 'monthly']['uuid'].size} Monthly Jobs")
    return low, medium, high, monthly


def main():
    df = get_dpd_feed()
    df_filter = get_filter_table()
    df['priority'] = df[['job_title', 'location_city', 'refNumber']].apply(lambda x: filter_feed(df_filter, x.job_title, x.location_city, x.refNumber), axis=1)

    low_roles_list, medium_roles_list, high_roles_list, monthly_roles_list = get_priority_lists(df)

    update_job_offer_references(14787, low_roles_list)      # low
    update_job_offer_references(14786, medium_roles_list)   # medium
    update_job_offer_references(14790, high_roles_list)     # high
    update_job_offer_references(14783, monthly_roles_list)  # monthly
    return


def update_royal_mail_jobs(job_offer_id):
    ssh_tunnel, conn = pyplaced.db_helpers.ssh_conn(credentials_dict=CREDENTIALS_DICT)

    cur = conn.cursor()
    sql = f"""
    update venue_match set venue_id=14791 where job_offer_id = {job_offer_id};
    """
    cur.execute(sql)

    sql = f"""
    update venue_match_candidate set venue_id=14791 where job_offer_id = {job_offer_id};
    """
    cur.execute(sql)

    sql = f"""
    update job_offer set venue_id=14791, user_id=644532 where id = {job_offer_id};
    """
    cur.execute(sql)

    sql = f"""
    update job_offer set created_by_user_id=14791 where id = {job_offer_id}; -- do we want that?
    """
    cur.execute(sql)

    sql = f"""
    update conversation set venue_id=14791 where job_offer_id = {job_offer_id};
    """
    cur.execute(sql)

    sql = f"""
    update job_offer_imported set venue_id=14791 where job_offer_id = {job_offer_id};
    """
    cur.execute(sql)

    sql = f"""
    update interview set venue_id=14791 where job_offer_id = {job_offer_id};
    """
    cur.execute(sql)

    sql = f"""
    update candidate_rating set venue_id=14791 where job_offer_id = {job_offer_id};
    """
    cur.execute(sql)
    conn.commit()
    print(job_offer_id)
    cur.close()
    conn.close()
    time.sleep(0.1)
    return


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        message = f"Error with DPD Group feeds: {e}"
        logger.error(message)
