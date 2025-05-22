# Featured in https://blog.coupler.io/pipedrive-api-guide/

import requests
import os
from dotenv import load_dotenv
import pandas as pd
import time
import concurrent.futures
from google.cloud import bigquery

load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.path.dirname(__file__), "service_account.json")
client = bigquery.Client()
token = os.getenv('PIPEDRIVE_API_KEY')

def extract_party_info(party_list):
    return {
        'emails': ", ".join(filter(None, [p.get('email_address') for p in party_list])),
        'names': ", ".join(filter(None, [p.get('name') for p in party_list]))
    }

def get_email_threads(start):

    url = f'https://api.pipedrive.com/v1/mailbox/mailThreads'
    params = {
        'api_token': token,
        'folder': 'sent',
        'start': start,
        'limit': 30
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    response_json = response.json()
    thread_fields = response_json.get('data', [])

    records = []

    for thread in thread_fields:
        thread_id = thread['id']
        subject = thread['subject']
        message_count = thread['message_count']
        deal_id = thread['deal_id']
        add_time = thread['add_time']
        snippet = thread['snippet']
        
        to_info = extract_party_info(thread.get('parties', {}).get('to', []) or [])
        from_info = extract_party_info(thread.get('parties', {}).get('from', []) or [])
        cc_info = extract_party_info(thread.get('parties', {}).get('cc', []) or [])
        bcc_info = extract_party_info(thread.get('parties', {}).get('bcc', []) or [])

        records.append({
            'thread_id': thread_id,
            'subject': subject,
            'message_count': message_count,
            'deal_id': deal_id,
            'add_time': add_time,
            'snippet': snippet,
            'to_emails': to_info['emails'],
            'from_emails': from_info['emails'],
            'cc_emails': cc_info['emails'],
            'bcc_emails': bcc_info['emails'],
        })

    more_items_in_collection = response_json.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection')
    start = response_json.get('additional_data', {}).get('pagination', {}).get('next_start')

    return records, more_items_in_collection, start

def get_email_messages(threads_2plus_responses):
    threads_2plus_messages = []

    def fetch_first_message(row):
        thread_id = row['thread_id']
        message_count = row['message_count']
        deal_id = row['deal_id']
        message_url = f'https://api.pipedrive.com/v1/mailbox/mailThreads/{thread_id}/mailMessages'
        try:
            time.sleep(0.1)
            response = requests.get(message_url, params={'api_token': token})
            response.raise_for_status()
            messages = response.json().get('data', [])
            first_message = messages[0] if messages else {}

            from_emails = extract_party_info(first_message.get('from', []) or [])
            to_emails = extract_party_info(first_message.get('to', []) or [])
            cc_emails = extract_party_info(first_message.get('cc', []) or [])
            bcc_emails = extract_party_info(first_message.get('bcc', []) or [])
            subject = first_message.get('subject', '')
            snippet = first_message.get('snippet', '')
            add_time = first_message.get('add_time', '')

            return {
                'thread_id': thread_id,
                'subject': subject,
                'message_count': message_count,
                'deal_id': deal_id,
                'add_time': add_time,
                'snippet': snippet,
                'to_emails': to_emails['emails'],
                'from_emails': from_emails['emails'],
                'cc_emails': cc_emails['emails'],
                'bcc_emails': bcc_emails['emails'],
            }
        except Exception as e:
            print(f"Failed to fetch messages for thread {thread_id}: {e}")
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_first_message, row) for row in threads_2plus_responses]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                threads_2plus_messages.append(result)

    return threads_2plus_messages

def export_to_bigquery(df):
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = os.getenv('BIGQUERY_TABLE')
    dataset_id = os.getenv('BIGQUERY_DATASET')
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    return job.result()

if __name__ == "__main__":

    max_iterations = 100
    iteration = 0
    start = 0

    all_records = []
    
    while iteration < max_iterations:

        thread_records, more_items_in_collection, next_start = get_email_threads(start)
        time.sleep(0.2)

        # Single-message threads have sufficient data to determine sender, recipient, etc. and can be processed directly.
        threads_one_response = [row for row in thread_records if row['message_count'] == 1]
        # Threads with more than one message require additional API calls to fetch the first message for each thread.
        threads_2plus_responses = [row for row in thread_records if row['message_count'] > 1]

        if threads_2plus_responses:
            threads_2plus_messages = get_email_messages(threads_2plus_responses)
            all_threads = threads_one_response + threads_2plus_messages
        else:
            all_threads = threads_one_response

        all_records.extend(all_threads)

        if not more_items_in_collection:
            break
    
        start = next_start
        iteration += 1
        if iteration >= max_iterations:
            print(f"Reached the maximum number of iterations: {max_iterations}, stopping.")
            break

    all_threads_df = pd.DataFrame(all_records)
    all_threads_df['add_time'] = pd.to_datetime(all_threads_df['add_time'])

    export_to_bigquery(all_threads_df)

