import re
import requests
import urllib3
import xmltodict
import traceback
import pandas as pd
from datetime import datetime
import pgeocode
import time
from pyplaced.db_helpers import create_conn
from dotenv import load_dotenv
import os


def UpdateFeedLiveJobs(df):
    load_dotenv()
    conn, cur = create_conn(credentials_dict=None)
    sql = """
    INSERT INTO landing.feed_live_jobs
    (guid, title, description, postcode, latitude, longitude, redirect_url, company_group_name, on_feed, logo_url, 
    salary, city, benefits, part_time, full_time, experience_required, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    values = list(df.itertuples(index=False))

    cur.executemany(sql, values)
    conn.commit()

    conn, cur = create_conn(credentials_dict=None)
    sql = """
    UPDATE landing.feed_live_jobs
    SET job_offer_id = CONCAT('P', id)
    WHERE job_offer_id = '';
    """

    cur.execute(sql)
    conn.commit()

    cur.close()
    conn.close()
    return


def check_existing(x):
    if x is None:
        return False
    return True


def get_part_time(x):
    if x is None:
        return False
    regexp = re.compile(r'[Pp]art[\s*-*]*[Tt]ime\s*')
    if regexp.search(x):
        return True
    return False


def get_full_time(x):
    if x is None:
        return False
    regexp = re.compile(r'[Ff]ull[\s*-*]*[Tt]ime\s*')
    if regexp.search(x):
        return True
    return False


def get_latitude(post_code: str):
    nomi = pgeocode.Nominatim('gb')
    result = nomi.query_postal_code(post_code)
    return result['latitude']


def get_longitude(post_code: str):
    nomi = pgeocode.Nominatim('gb')
    result = nomi.query_postal_code(post_code)
    return result['longitude']


def main():
    tic = time.perf_counter()
    url = os.getenv('feed_url')

    http = urllib3.PoolManager()

    response = http.request('GET', url)
    try:
        data = xmltodict.parse(response.data)
    except:
        print("Failed to parse xml from response (%s)" % traceback.format_exc())
        return

    df = pd.DataFrame.from_dict(data['jobs']['job'])

    # Apply transformations to all
    """
    Required:
    - title (must exist) DONE
    - description (format)
    - post_code (if not existing, find using places API)
    - lat / lng (from postcode) DONE
    - reference_id (generate)
    - guid (must exist) DONE
    - apply_url (must exist) DONE
    - company_name (must exist) DONE
    - on_feed = 1 DONE
    
    Nice to have:
    - logo
    - salary DONE
    - city DONE
    - benefits
    - part time / full time DONE
    - experience
    """

    df = df[~df['title'].isnull()]
    df = df[~df['url'].isnull()]
    df = df[~df['reference'].isnull()]
    df = df[~df['description'].isnull()]
    df = df[~df['postcode'].isnull()]

    df['part_time'] = df.employment_type.apply(get_part_time)
    df['full_time'] = df.employment_type.apply(get_full_time)

    df['on_feed'] = 1
    df['created_at'] = datetime.now()
    df['updated_at'] = datetime.now()
    df['logo_url'] = None
    df['benefits'] = None

    # filter out NaN values from lat / lng
    df = df[~df['latitude'].isna()]

    # Re-order columns
    df = df[
        [
            'reference',
            'title',
            'description',
            'postcode',
            'latitude',
            'longitude',
            'url',
            'company',
            'on_feed',
            'logo_url',
            'salary',
            'city',
            'benefits',
            'part_time',
            'full_time',
            'required_experience',
            'created_at',
            'updated_at'
        ]
    ]
    print(df.columns)

    UpdateFeedLiveJobs(df)
    toc = time.perf_counter()

    print(
        f"Process Finished. Total {toc - tic:0.4f} seconds\n"
        f"{len(df)} Jobs @ {(toc - tic) / len(df):0.4f} seconds per Job"
    )
    return


if __name__ == '__main__':
    main()
