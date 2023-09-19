#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ElasticSearch Bulk Document Updater Script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: September 18, 2023
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
      "organizationId1": ["requesterId1", "requesterId2", ...],
      "organizationId2": ["requesterId3", "requesterId4", ...],
      ...
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


def decide_environment(env: str, tenant_id: str) -> str:
    if env in environments:
        return f"{environment_addresses[env]}/{tenant_id}_tickets_{environments[env]}/_update_by_query"
    else:
        raise EnvironmentError(f"Unknown environment: {env}")


def elastic_query(organization_id, requester_id):
    query = {
        "script": {
            "source": f"ctx._source.organization = {organization_id}",
            "lang": "painless",
        },
        "query": {
            "bool": {
                "should": [{"term": {"fieldMap.ts.requester.value": requester_id}}]
            }
        },
    }
    return json.dumps(query)


def send_update_request_bulk(organization_id, requester_ids, env, tenant_id):
    headers = {"Content-Type": "application/json"}
    url = decide_environment(env, tenant_id)

    print(f"Sending update request for organization id: {organization_id}")
    for rid in requester_ids:
        data = elastic_query(organization_id, rid)
        try:
            response = requests.post(url=url, data=data, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while updating documents with organization id {organization_id}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description="Update Elasticsearch documents.")
    parser.add_argument("--tenantId", required=True, help="Tenant ID")
    parser.add_argument("--env", required=True, choices=environments.keys(), help="Environment")

    args = parser.parse_args()

    start_time = time.time()
    print("Starting to update documents...\n")
    print(f"Start Time: {start_time}")

    try:
        with open("result.json", "r") as file:
            json_file = json.load(file)

            with ThreadPoolExecutor(max_workers=4) as executor:
                for key, values in json_file:
                    executor.submit(send_update_request_bulk, key, values, args.env, args.tenantId)

    except Exception as e:
        print(f"An error occurred: {e}")

    end_time = time.time()
    elapsed_time = end_time - start_time

    print("\nFinished updating documents.")

    print(f"End Time: {end_time}")
    print(f"Elapsed Time: {elapsed_time} seconds")


if __name__ == "__main__":
    main()
