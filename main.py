#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ElasticSearch Bulk Document Updater Script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: October 27, 2023
GitHub: https://github.com/ugurcandede

This Python script is designed to perform bulk Elasticsearch data update operations.
It accomplishes the following tasks:
1. Reads document data from a JSON file.
2. Parallelizes document updates.
3. Sends each update operation to Elasticsearch in bulk.

Usage:
- This script reads document data from a JSON file and performs document updates on Elasticsearch.
- The JSON file should have the following format:
  {
      "tenantId1": [
        {"field_key2": ["ticket_key1", "ticket_key2"]},
        {"field_key2": ["ticket_key3", "ticket_key4"]}
      ],
      "tenantId2": null
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
    "dev": "staging",
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


def generate_elastic_query(field_key, ticket_key):
    query = {
        "script": {
            "source": f"ctx._source.fieldMap['{field_key}'].put('type','NUMBER_DECIMAL')",
            "lang": "painless",
        },
        "query": {
            "bool": {
                "should": [{"term": {"key.keyword": ticket_key}}]
            }
        },
    }
    return json.dumps(query)


def send_update_request_bulk(tenant_id, values, env):
    for value in values:
        for cf_key, ticket_keys in value.items():
            for ticket_key in ticket_keys:
                try:
                    url = get_update_url(env, tenant_id)
                    data = generate_elastic_query(cf_key, ticket_key)

                    response = requests.post(url=url, data=data, headers=headers)
                    response.raise_for_status()
                    print(f"[SUCCESS] tenantId: {tenant_id}, ticketKey: {ticket_key}, customFieldKey: {cf_key}")
                except requests.exceptions.RequestException as e:
                    print(
                        f"[ERROR] error while updating documents with tenantId: {tenant_id}, ticketKey: {ticket_key}, customFieldKey: {cf_key} {str(e)}")


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
    elapsed_time = end_time - start_time

    print("\nFinished updating documents.")

    print(f"End Time: {end_time}")
    print(f"Elapsed Time: {elapsed_time} seconds")


if __name__ == "__main__":
    main()
