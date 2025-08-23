#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Add Field Operation Module
~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: August 23, 2025

This module handles adding fields to all Elasticsearch documents.
"""

import json
import requests
from concurrent.futures import ThreadPoolExecutor
from .utils import get_update_url
from .config import HEADERS, DEFAULT_MAX_WORKERS


def generate_add_field_query(field_key, option_id):
    """Generate Elasticsearch query for adding a field to all documents."""
    query = {
        "script": {
            "source": f"ctx._source.fieldMap['{field_key}'] = ['id':{option_id},'value':{option_id},'type':'SELECT','order':1,'name':'TICKET']",
            "lang": "painless",
        },
        "query": {
            "match_all": {}
        },
    }
    return json.dumps(query)


def send_add_field_request(tenant_id, value, env, field_key):
    """Send request to add field to all documents for a tenant."""
    try:
        url = get_update_url(env, tenant_id)
        data = generate_add_field_query(field_key, value)

        response = requests.post(url=url, data=data, headers=HEADERS)
        response.raise_for_status()
        print(f"[SUCCESS] tenantId: {tenant_id}, field '{field_key}' added to fieldMap")
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] error while updating documents with tenantId: {tenant_id}, field '{field_key}' not added to fieldMap {str(e)}")


def execute_add_field_operation(json_data, env, field_key, max_workers=DEFAULT_MAX_WORKERS):
    """Execute add field operation for all tenants in parallel."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            futures = []
            for tenant_id, values in json_data.items():
                if values is not None:
                    future = executor.submit(send_add_field_request, tenant_id, values, env, field_key)
                    futures.append(future)

            # Wait for all tasks to complete with timeout
            for future in futures:
                future.result(timeout=30)  # 30 second timeout per request
        except KeyboardInterrupt:
            print("Cancelling running tasks...")
            executor.shutdown(wait=False, cancel_futures=True)
            raise
