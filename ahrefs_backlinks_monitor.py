# Featured in https://blog.coupler.io/ahrefs-api-backlinks-monitoring/

import requests
import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas_gbq
import json
from dotenv import load_dotenv

load_dotenv()

project_id = os.getenv('PROJECT_ID')
dataset_id = os.getenv('DATASET_ID')
domains_table_id = os.getenv('DOMAINS_TABLE_ID')
backlinks_table_id = os.getenv('BACKLINKS_TABLE_ID')
refdomains_table_id = os.getenv('REFDOMAINS_TABLE_ID')
api_token = os.getenv('AHREFS_API_KEY')

HEADERS = {
    'Accept': 'application/json, application/xml',
    'Authorization': f"Bearer {api_token}"
}

# Fetch the list of competitors from BigQuery
def get_domains_from_bq():
    client = bigquery.Client()
    input_table = f"{project_id}.{dataset_id}.{domains_table_id}"
    query = f"SELECT DISTINCT REGEXP_REPLACE(domain, r'^https?://|/$', '') as domain FROM `{input_table}`"
    domains = client.query(query).to_dataframe()['domain'].to_list()
    print(f"Fetched {len(domains)} domains from BigQuery")
    return domains

# Fetch backlinks
def get_backlinks(target, current_date, previous_date):
    where_clause = {
        "and": [
            {"field": "is_content", "is": ["eq", 1]},
            {"field": "domain_rating_source", "is": ["gte", 24.5]},
            {"field": "traffic_domain", "is": ["gte", 500]},
            {"field": "first_seen_link", "is": ["gte", previous_date]},
            {"field": "first_seen_link", "is": ["lte", current_date]},
            {"field": "last_seen", "is": "is_null"}
        ]
    }
    url = 'https://api.ahrefs.com/v3/site-explorer/all-backlinks'
    params = {
        'history': f'since:{previous_date}',
        'target': target,
        'where': json.dumps(where_clause),
        'select': (
            'url_from,url_to,anchor,domain_rating_source,url_rating_source,traffic_domain,'
            'refdomains_source,linked_domains_source_page,traffic,positions,links_external,'
            'is_dofollow,is_nofollow,is_ugc,first_seen_link'
        )
    }
    r = requests.get(url=url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json().get('backlinks', [])

# Fetch number of referring domains for the new backlinks
def get_refdomain_numbers(target, current_date, previous_date):
    where_clause = {
        "and": [
            {"field": "domain_rating", "is": ["gte", 24.5]},
            {"field": "first_seen", "is": ["gte", previous_date]},
            {"field": "first_seen", "is": ["lte", current_date]}
        ]
    }
    url = 'https://api.ahrefs.com/v3/site-explorer/refdomains'
    params = {
        'history': f'since:{previous_date}',
        'target': target,
        'where': json.dumps(where_clause),
        'select': 'domain,domain_rating'
    }
    r = requests.get(url=url, headers=HEADERS, params=params)
    r.raise_for_status()
    refdomains = r.json().get('refdomains', [])
    return len(refdomains), len([domain for domain in refdomains if domain['domain_rating'] >= 80])

# Export data to BigQuery
def export_to_bigquery(data, table_id):
    print(f"Exporting data to BigQuery table: {table_id}")
    pandas_gbq.to_gbq(data, table_id, project_id="couplerio-demo", if_exists="replace")
    print("Export completed successfully.")

# Main function
def fetch_ahrefs_data():
    domains = get_domains_from_bq()
    current_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    previous_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    previous_date_iso = f"{previous_date}T00:00:00Z"
    current_date_iso = f"{current_date}T23:59:59Z"

    backlinks_results = []
    refdomains_results = []

    for domain in domains:
        try:
            print(f"Fetching data for {domain}")
            backlinks = get_backlinks(domain, current_date_iso, previous_date_iso)
            for backlink in backlinks:
                backlinks_results.append({
                    'domain': domain,  
                    'url_from': backlink.get('url_from'),
                    'url_to': backlink.get('url_to'),
                    'anchor': backlink.get('anchor'),
                    'DR': backlink.get('domain_rating_source'),
                    'UR': backlink.get('url_rating_source'),
                    'domain_traffic': backlink.get('traffic_domain'),
                    'referring_domains': backlink.get('refdomains_source'),
                    'linked_domains': backlink.get('linked_domains_source_page'),
                    'page_traffic': backlink.get('traffic'),
                    'keywords': backlink.get('positions'),
                    'external_links': backlink.get('links_external'),
                    'is_dofollow': backlink.get('is_dofollow'),
                    'is_nofollow': backlink.get('is_nofollow'),
                    'is_ugc': backlink.get('is_ugc'),
                    'first_seen': backlink.get('first_seen_link')
                })
            refdomains_25, refdomains_80 = get_refdomain_numbers(domain, current_date_iso, previous_date_iso)
            refdomains_results.append({
                'domain': domain,
                'refdomains_25+': refdomains_25,
                'refdomains_80+': refdomains_80
            })
        except requests.exceptions.RequestException as e:
            print(f"API error for {domain}: {e}")
        except Exception as e:
            print(f"Error processing {domain}: {e}")

    if backlinks_results:
        export_to_bigquery(pd.DataFrame(backlinks_results), f"{project_id}.{dataset_id}.{backlinks_table_id}")
    if refdomains_results:
        export_to_bigquery(pd.DataFrame(refdomains_results), f"{project_id}.{dataset_id}.{refdomains_table_id}")

if __name__ == "__main__":
    fetch_ahrefs_data()