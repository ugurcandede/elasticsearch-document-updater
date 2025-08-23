#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Document Update Operation Module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: August 23, 2025

This module handles updating document organization fields in Elasticsearch.
"""

import json
import requests
from concurrent.futures import ThreadPoolExecutor
from .utils import get_update_url
from .config import HEADERS, DEFAULT_MAX_WORKERS


def generate_document_update_query(doc_id, doc_key, query_field_key, value):
    """Generate Elasticsearch query for updating document organization."""
    query = {
        "script": {
            "source": f"ctx._source.{doc_key} = {doc_id}",
            "lang": "painless",
        },
        "query": {
            "bool": {
                "should": [{"term": {f"fieldMap.{query_field_key}.value": value}}]
            }
        },
    }
    return json.dumps(query)


def send_document_update_request(doc_id, doc_key,query_field_key,doc_values, env, tenant_id):
    """Send request to update documents for a tenant."""
    url = get_update_url(env, tenant_id)

    print(f"Sending update request for organization id: {doc_id}")
    for value in doc_values:
        data = generate_document_update_query(doc_id, doc_key, query_field_key, value)
        try:
            response = requests.post(url=url, data=data, headers=HEADERS)
            response.raise_for_status()
            print(f"[SUCCESS] tenantId: {tenant_id}, doc_id: {doc_id}, value: {value}")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] error while updating documents with tenantId: {tenant_id}, doc_id: {doc_id}, value: {value} {str(e)}")


def execute_document_update_operation(json_data, doc_key,query_field_key, env, tenant_id, max_workers=DEFAULT_MAX_WORKERS):
    """Execute document update operation for all organizations in parallel."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            futures = []
            for key, values in json_data.items():
                if values is not None:
                    future = executor.submit(send_document_update_request, key, doc_key,query_field_key, values, env, tenant_id)
                    futures.append(future)

            # Wait for all tasks to complete with timeout
            for future in futures:
                future.result(timeout=90)  # 90 second timeout per organization
        except KeyboardInterrupt:
            print("Cancelling running tasks...")
            executor.shutdown(wait=False, cancel_futures=True)
            raise
