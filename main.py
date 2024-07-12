#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ElasticSearch Bulk Document Updater Script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: July 12, 2024
GitHub: https://github.com/ugurcandede

This Python script is designed to perform add a field to all Elasticsearch docs with given schema.
It accomplishes the following tasks:
1. Reads document data from a JSON file.
2. Parallelizes document insertion.
3. Sends each update operation to Elasticsearch in bulk.

Usage:
- This script reads document data from a JSON file and performs document updates on Elasticsearch.
- The JSON file should have the following format:
{
  "develop": 37,
  "release": 487
}
- After the execution, it prints start and end times along with the processing time.
"""

import requests
import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor

environments = {
    "local": "dev",
    "net": "preprod",
    "prod": "prod",
}

environment_addresses = {
    "local": "http://localhost:9200",
    "dev": "http://xxx:9200",
    "net": "http://xxx:9200",
    "prod": "http://xxx:9200",
}

headers = {"Content-Type": "application/json"}


def get_update_url(env, tenant_id):
    if env in environments:
        return f"{environment_addresses[env]}/{tenant_id}_tickets_{environments[env]}/_update_by_query?refresh=true"
    else:
        raise EnvironmentError(f"Unknown environment: {env}")


def generate_elastic_query(option_id):
    query = {
        "script": {
            "source": f"ctx._source.fieldMap['ts.scope'] = ['id':{option_id},'value':{option_id},'type':'SELECT','order':1,'name':'TICKET']",
            "lang": "painless",
        },
        "query": {
            "match_all": {}
        },
    }
    return json.dumps(query)


def send_update_request_bulk(tenant_id, value, env):
    try:
        url = get_update_url(env, tenant_id)
        data = generate_elastic_query(value)

        response = requests.post(url=url, data=data, headers=headers)
        response.raise_for_status()
        print(f"[SUCCESS] tenantId: {tenant_id} field added to fieldMap")
    except requests.exceptions.RequestException as e:
        print(
            f"[ERROR] error while updating documents with tenantId: {tenant_id}, field added not to fieldMap {str(e)}")


def print_elapsed_time(start_time, end_time):
    elapsed_time = end_time - start_time

    milliseconds = int((elapsed_time - int(elapsed_time)) * 1000)

    seconds = int(elapsed_time % 60)
    minutes = int((elapsed_time // 60) % 60)
    hours = int(elapsed_time // 3600)

    print(f"{hours}h:{minutes}m:{seconds}sec:{milliseconds}ms elapsed")


def main():
    parser = argparse.ArgumentParser(description="Update Elasticsearch documents.")
    parser.add_argument("--env", required=True, choices=environments.keys(), help="Environment")

    args = parser.parse_args()

    start_time = time.time()
    print("Starting to update documents...\n")
    print(f"Start Time: {start_time}")

    try:
        with open("result.json", "r") as file:
            json_file = json.load(file)

            with ThreadPoolExecutor(max_workers=4) as executor:
                for tenant_id, values in json_file.items():
                    if values is not None:
                        executor.submit(send_update_request_bulk, tenant_id, values, args.env)

    except Exception as e:
        print(f"An error occurred: {e}")

    end_time = time.time()
    print("\nFinished updating documents.")

    print(f"End Time: {end_time}")

    print_elapsed_time(start_time, end_time)


if __name__ == "__main__":
    main()
