import pyplaced.db_helpers
import requests
import base64
import pandas as pd
import json
from dotenv import load_dotenv
from loguru import logger
from notifiers.logging import NotificationHandler
import os

with open(os.getenv('credentials_dict_file_location'), 'r') as f:
    CREDENTIALS_DICT = json.loads(f.read())


SLACK_WEBHOOK = os.gentenv('slack_webhook')


def get_greenhouse(url='https://harvest.greenhouse.io/v1/job_posts?per_page=500&live=true&active=true', header_api_token_string=os.getenv('header_api_token_string'), pages=2):
    header_api_token = f'{header_api_token_string}:'.encode('utf-8')
    encoded_password = str(base64.b64encode(header_api_token).decode('utf-8'))

    headers = {'Authorization': f'Basic {encoded_password}'}
    data_bytes = bytearray()

    for page in range(pages):
        page = page + 1
        print(f"{page=}")
        if page == 1:
            x = requests.get(url, headers=headers)
            print(x.status_code)
            data_bytes = bytearray(x.content)

        else:
            new_url = url + f'&page={page}'
            x = requests.get(new_url, headers=headers)
            data_bytes = data_bytes[:-1] + b','
            data_bytes.extend(bytearray(x.content)[1:])

    return data_bytes


def get_salaried(job_id: int, boards_api_token=os.getenv('boards_api_token')):
    url = f'https://boards-api.greenhouse.io/v1/boards/hellofresh/jobs/{job_id}'
    header_api_token = f'{boards_api_token}:'.encode('utf-8')
    encoded_password = str(base64.b64encode(header_api_token).decode('utf-8'))

    headers = {'Authorization': f'Basic {encoded_password}'}
    x = requests.get(url, headers=headers)

    if x.status_code == 404:  # Job Filled
        return pd.Series({"salary_type": "Position Filled"})

    job_data = json.loads(x.content)
    salary_type = job_data["metadata"][1]["value"]
    salary_type = "Salary" if salary_type is None else salary_type

    return pd.Series({"salary_type": salary_type})


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


def update_imported_jobs():
    ssh_tunnel, conn = pyplaced.db_helpers.ssh_conn(credentials_dict=CREDENTIALS_DICT)

    cur = conn.cursor()
    sql = """
    UPDATE job_offer_imported
    SET applydata = "5902c7c31us"
    WHERE venue_id in (14775, 14778)
    AND on_feed = 1
    AND applydata IS NULL
    """
    cur.execute(sql)
    conn.commit()
    print(sql)
    print("SUCCESSFULLY EXECUTED")
    cur.close()
    conn.close()
    return


def get_job_offer_references():
    data = get_greenhouse()
    df = pd.json_normalize(json.loads(data))
    df = df[df['location.name'].str.contains('United Kingdom|England|London|Banbury|Nuneaton|Derby', regex=True, na=False)]
    df = df[~df['location.name'].str.contains('Berlin', regex=True, na=False)]

    df['salary_type'] = df.apply(lambda x: get_salaried(x.id), axis=1)
    df = df[['id', 'location.name', 'salary_type']]
    df = df[df['salary_type'] != 'Position Filled']
    df = df[df['id'] != 6149591]  # Excluded Roles
    print(df)

    df_salary = df[df['salary_type'] == 'Salary']
    df_hourly = df[df['salary_type'] == 'Hourly']

    salary_jobs_str = df_salary.id.astype(str).str.cat(sep=',')
    hourly_jobs_str = df_hourly.id.astype(str).str.cat(sep=',')
    print(f"{salary_jobs_str=}")
    print(f"{hourly_jobs_str=}")
    return salary_jobs_str, hourly_jobs_str


def get_current_salaried_roles():
    return pyplaced.run_query(
        "select job_offer_references_to_import from venue where id = 14775",
        target_db='placed'
    )['job_offer_references_to_import'][0].split(",")


def get_promoted_salaried_roles():
    return pyplaced.run_query(
        """select group_concat(url) as reference_ids
        from landing.promotion_request
        where client = 'hellofresh'
        and response is null""",
        target_db='insights'
    )['reference_ids'][0].split(",")


def check_new_salaried_roles_on_feed(job_ids):
    check_job_ids = job_ids.split(",")
    current_job_ids = get_current_salaried_roles()
    promoted_job_ids = get_promoted_salaried_roles()

    has_new_jobs = any(job_id not in current_job_ids and job_id not in promoted_job_ids for job_id in check_job_ids)
    new_job_ids = [job_id for job_id in check_job_ids if job_id not in current_job_ids and job_id not in promoted_job_ids]

    return has_new_jobs, new_job_ids


def get_job_title(job_id: int, boards_api_token=os.getenv('boards_api_token')):
    url = f'https://boards-api.greenhouse.io/v1/boards/hellofresh/jobs/{job_id}'
    header_api_token = f'{boards_api_token}:'.encode('utf-8')
    encoded_password = str(base64.b64encode(header_api_token).decode('utf-8'))

    headers = {'Authorization': f'Basic {encoded_password}'}
    x = requests.get(url, headers=headers)

    if x.status_code == 404:  # Job Filled
        return None

    job_data = json.loads(x.content)
    return job_data["title"]


def find_all_job_titles(job_ids):
    return [{"job_id": job_id, "title": get_job_title(job_id)} for job_id in job_ids]


def send_slack_notification(new_job_data):
    jobs_text = ",\n".join([str(job) for job in new_job_data])
    message = f"""
<@U057RRV59QX>
New Job(s) Added to HelloFresh Feed:
{jobs_text}
    """
    print(message)

    defaults = {'webhook_url': SLACK_WEBHOOK}
    handler = NotificationHandler("slack", defaults=defaults)

    logger.add(handler, level="INFO")
    logger.info(message)

    logger.remove()
    return


def main():
    salary_job_ids, hourly_job_ids = get_job_offer_references()
    has_new_jobs, new_job_ids = check_new_salaried_roles_on_feed(salary_job_ids)

    if has_new_jobs:
        job_data = find_all_job_titles(new_job_ids)
        send_slack_notification(job_data)

    update_job_offer_references(14775, salary_job_ids)  # Set import for salaried jobs
    update_job_offer_references(14778, hourly_job_ids)  # Set import for hourly jobs
    update_imported_jobs()
    return


def test_get_details(job_id: int, boards_api_token=os.getenv('boards_api_token')):
    url = f'https://boards-api.greenhouse.io/v1/boards/hellofresh/jobs/{job_id}?full_content=true'
    header_api_token = f'{boards_api_token}:'.encode('utf-8')
    encoded_password = str(base64.b64encode(header_api_token).decode('utf-8'))

    headers = {'Authorization': f'Basic {encoded_password}'}
    x = requests.get(url, headers=headers)

    if x.status_code == 404:  # Job Filled
        return None

    job_data = json.loads(x.content)
    print(job_data)
    return job_data["questions"]


if __name__ == '__main__':
    main()
