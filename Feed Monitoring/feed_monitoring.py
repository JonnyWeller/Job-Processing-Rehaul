"""
FEED MONITORING

Purpose is to monitor feeds from all stages

SOURCE
MIDDLEWARE
IMPORTED
ROBOTAN
PLACED
LANDING PAGES
PROGRAMMATIC

source feeds, use link from active ATSes DONE
count elements - will differ per feed DONE
will have to split harri feeds by company <organization_brand_id> DONE
"""
import base64
import datetime
import json
import pandas as pd
import pyplaced
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import gzip
import shutil
from lxml import etree as ET
import os

# print(os.path.abspath(os.path.join(os.getcwd(), os.pardir)))
FILEPATH = 'C:/Users/Jonny W/PycharmProjects/Job Processing Rehaul/Feed Monitoring/'
load_dotenv(FILEPATH + '.env')


def GetPlaced(query):
    df = pyplaced.run_query(query, target_db='placed')
    return df


def GetInsights(query):
    df = pyplaced.run_query(query, target_db='insights')
    return df


def UpdateDatabase(df_feeds):
    with open(FILEPATH + 'queries/imported.sql', 'r') as f:
        query = f.read()

    df_placed = GetPlaced(query)

    with open(FILEPATH + 'queries/landing_live_jobs.sql', 'r') as f:
        query = f.read()

    df_landing = GetInsights(query)

    df_imported = df_placed.merge(df_landing, on=['ats_id', 'ats_name'], how='left')

    df_all = df_feeds.merge(df_imported, on=['ats_id', 'ats_name'], how='left').fillna(0)
    df_all['updated_at'] = datetime.datetime.now()

    print(df_all)
    print(df_all.columns)
    # df_all.to_csv(FILEPATH + 'test.csv')

    sql = """
    INSERT IGNORE INTO insights.feed_monitoring
    (ats_id, ats_name, source_feed_jobs, middleware_feed_jobs, ats_type, job_offer_imported_jobs, robotan_attempted,
    placed_app_jobs, placed_app_jobs_live, last_synced_date, landing_live_jobs, landing_live_jobs_boosted,
    programmatic_live_jobs, updated_at)
    VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    ats_name = VALUES(ats_name)
    , ats_type = VALUES(ats_type)
    , source_feed_jobs = VALUES(source_feed_jobs)
    , middleware_feed_jobs = VALUES(middleware_feed_jobs)
    , job_offer_imported_jobs = VALUES(job_offer_imported_jobs)
    , robotan_attempted = VALUES(robotan_attempted)
    , placed_app_jobs = VALUES(placed_app_jobs)
    , placed_app_jobs_live = VALUES(placed_app_jobs_live)
    , landing_live_jobs = VALUES(landing_live_jobs)
    , landing_live_jobs_boosted = VALUES(landing_live_jobs_boosted)
    , programmatic_live_jobs = VALUES(programmatic_live_jobs)
    , updated_at = VALUES(updated_at)
    , last_synced_date = VALUES(last_synced_date);
    """
    values = list(df_all.itertuples(index=False))

    conn, cur = pyplaced.db_helpers.create_conn(credentials_dict=None)
    cur.executemany(sql, values)
    conn.commit()

    cur.close()
    conn.close()
    return


def GetFeeds(from_csv=True):
    with open(FILEPATH + 'queries/feeds.sql', 'r') as f:
        query = f.read()

    if from_csv:
        df = pd.read_csv(FILEPATH + "feed_list_source.csv")

    else:
        df = GetPlaced(query)
        df.to_csv(FILEPATH + "feed_list_source.csv", index_label='index')

    return df.fillna(0)


def CountJobsFromJSON(json_data, target_key, max_depth=0, current_depth=0):
    """Counts occurrences of a specific key within a JSON structure, up to a specified depth.

    Args:
        json_data: The JSON data (as a dictionary, list, or nested structure).
        target_key: The key to count.
        max_depth: The maximum depth to traverse within the JSON (default: 0, no limit).
        current_depth: The current depth during recursion (internal use).

    Returns:
        int: The total count of the specified key within the specified depth.
    """

    count = 0
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if key == target_key and current_depth <= max_depth:
                count += 1
            if current_depth < max_depth:  # Decrement only if not at max depth yet
                count += CountJobsFromJSON(value, target_key, max_depth, current_depth + 1)
    elif isinstance(json_data, list):
        for item in json_data:
            if current_depth < max_depth:
                count += CountJobsFromJSON(item, target_key, max_depth, current_depth + 1)
    return count


def CountJobsFromXML(file, target_xml_element):
    filename = os.fsdecode(file)
    doc = ET.parse(filename)
    root = doc.getroot()

    namespaces = {}  # A dictionary to store namespace prefixes and URIs

    for elem in root.iter():
        if elem.tag.startswith('{'):  # Check for namespace syntax
            _, namespace_uri = elem.tag[1:].split('}')
            prefix = namespace_uri.split('/')[-1]  # Isolate the prefix
            namespaces[prefix] = namespace_uri

    # Find the <jobs_count> element
    jobs_count_element = root.find('jobs_count')
    jobs_count_element2 = root.find('jobcount')

    # Check if it exists and get the value (if it does)
    if jobs_count_element is not None:
        jobs_count = jobs_count_element.text
        return jobs_count

    if jobs_count_element2 is not None:
        jobs_count = jobs_count_element2.text
        return jobs_count

    if bool(namespaces):
        print(f"{namespaces=}")
        print(f"{target_xml_element=}")
        namespaces = {'wd': 'urn:com.workday.report/CR26-Workday-to-placed-app-Job-Postings'}
        return sum(len(job) > 0 for job in root.findall(target_xml_element, namespaces=namespaces))

    if not root.findall(target_xml_element) and root.find("jobs"):
        return len(root.find("jobs").findall(target_xml_element))

    elif root.tag == "response":  # for Lidl response format
        return len(root.find("joboffers").findall(target_xml_element))

    else:
        return sum(len(job) > 0 for job in root.findall(target_xml_element))


def get_greenhouse(url='https://harvest.greenhouse.io/v1/job_posts?per_page=500&live=true&active=true', header_api_token_string=os.getenv('header_api_token_string'), pages=2):
    # soho_house_header_api_token_string="708539fb5a5e17d91f2ea1da5adb0664-101"
    # &full_content=true
    # soho_house_url='https://harvest.greenhouse.io/v1/job_posts'
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


def GetTotalJobsFromSource(source_df: pd.DataFrame, url_title='fetch_url', ats_name='name', download=False):
    df_passwords = pd.read_csv(FILEPATH + 'passwords.csv')  # local

    # Store Data to new dataframe
    df_totals = pd.DataFrame(
        columns=[
            'ats_id',
            'ats_name',
            'source_feed_jobs',
            'middleware_feed_jobs',
        ]
    )

    # Extract XMLs from links
    for index, row in source_df.iterrows():
        url = row[url_title]
        name = row[ats_name]
        ats = row[ats_name].replace(" ", "_")
        ats_id = row['ats_id']
        ats_type = row['type']
        is_harri = True if row['type'] == "harri" else False
        is_middleware = True if row['is_middleware'] == 1 else False
        requires_password = row['requires_password']
        is_json = row['is_json']
        target_key = row['target_key']
        json_max_depth = row['json_max_depth']
        target_xml_element = row['target_xml_element']
        header_api_token = row['header_api_token']

        if is_json:
            file = FILEPATH + f"source_xmls/{ats}.json"

        else:
            file = FILEPATH + f"source_xmls/{ats}.xml"

        # Finds Fresh Feeds
        if download:
            if requires_password:
                # Get passwords from df_passwords csv file
                df_passwords_slice = df_passwords.loc[df_passwords.client == name].reset_index()
                username = df_passwords_slice.username[0]
                password = df_passwords_slice.password[0]

                response = requests.get(url, auth=HTTPBasicAuth(username=username, password=password))

            else:
                response = requests.get(url)

            if ats_type == 'GreenHouse':
                data = get_greenhouse(url, header_api_token)

            else:
                data = response.content

            if url.endswith(".gz"):
                with open(f"{file}.gz", 'wb+') as f:
                    f.write(data)

                with gzip.open(f"{file}.gz", 'rb') as f_in:
                    with open(file, 'wb+') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                with open(file, 'wb+') as f:
                    f.write(data)

        # HARRI FEED
        # Split feeds from harri_brand_id
        if is_harri is True:
            desired_brand_id = str(int(row['harri_brand_id']))
            tree = ET.parse(file)  # Replace with the path to your XML file
            root = tree.getroot()

            # Filter jobs based on placed_account_id / organization_brand_id
            filtered_jobs = []
            for job in root.findall('job'):
                brand_id = job.find('organization_brand_id').text
                if brand_id == desired_brand_id:
                    filtered_jobs.append(job)

            # Create new XML
            new_root = ET.Element('jobs')  # Create a root element named 'jobs'
            for job in filtered_jobs:
                new_root.append(job)

            new_tree = ET.ElementTree(new_root)
            new_tree.write(file, encoding='utf-8', xml_declaration=True)

        try:
            if is_json:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                result = CountJobsFromJSON(data, target_key=target_key, max_depth=json_max_depth)

            else:
                result = CountJobsFromXML(file, target_xml_element)

            middleware_result = result
            print(f"{name} = {result} Jobs")

            if is_middleware is True:
                middleware_url = row['middleware_url']

                response = requests.get(middleware_url)
                xml = response.content

                if middleware_url.endswith('.json'):
                    file = FILEPATH + f"middleware_xmls/{ats}.json"
                else:
                    file = FILEPATH + f"middleware_xmls/{ats}.xml"

                if middleware_url.endswith(".gz"):
                    with open(f"{file}.gz", 'wb+') as f:
                        f.write(xml)

                    with gzip.open(f"{file}.gz", 'rb') as f_in:
                        with open(file, 'wb+') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                else:
                    with open(file, 'wb+') as f:
                        f.write(xml)

                if middleware_url.endswith('.json'):
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    middleware_result = CountJobsFromJSON(data, target_key=target_key, max_depth=json_max_depth)

                else:
                    if name == 'HCA':
                        target_xml_element = 'job'
                    middleware_result = CountJobsFromXML(file, target_xml_element=target_xml_element)

            data = {
                'ats_id': ats_id,
                'ats_name': name,
                'source_feed_jobs': result,
                'middleware_feed_jobs': middleware_result
            }

            df_totals = pd.concat(
                [df_totals, pd.DataFrame(data, index=[0])],
                ignore_index=True
            )
        except Exception as generic_exception:
            print(f"{file} ERROR: {generic_exception}")

    return df_totals


if __name__ == '__main__':
    df = GetFeeds(from_csv=False)
    df = GetTotalJobsFromSource(df, download=True)

    UpdateDatabase(df)
