#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Field Type Update Operation Module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: August 23, 2025

This module handles updating field types in Elasticsearch documents.
"""

import json
import requests
from concurrent.futures import ThreadPoolExecutor
from .utils import get_update_url
from .config import HEADERS, DEFAULT_MAX_WORKERS


def generate_field_type_query(field_key, field_type, ticket_key):
    """Generate Elasticsearch query for updating field type."""
    query = {
        "script": {
            "source": f"ctx._source.fieldMap['{field_key}'].put('type','{field_type}')",
            "lang": "painless",
        },
        "query": {
            "bool": {
                "should": [{"term": {"key.keyword": ticket_key}}]
            }
        },
    }
    return json.dumps(query)


def send_field_type_update_request(tenant_id, values, env, field_type):
    """Send request to update field types for a tenant."""
    for value in values:
        for cf_key, ticket_keys in value.items():
            for ticket_key in ticket_keys:
                try:
                    url = get_update_url(env, tenant_id)
                    data = generate_field_type_query(cf_key, ticket_key, field_type)

                    response = requests.post(url=url, data=data, headers=HEADERS)
                    response.raise_for_status()
                    print(f"[SUCCESS] tenantId: {tenant_id}, ticketKey: {ticket_key}, customFieldKey: {cf_key}")
                except requests.exceptions.RequestException as e:
                    print(f"[ERROR] error while updating documents with tenantId: {tenant_id}, ticketKey: {ticket_key}, customFieldKey: {cf_key} {str(e)}")


def execute_field_type_operation(json_data, env, field_type, max_workers=DEFAULT_MAX_WORKERS):
    """Execute field type update operation for all tenants in parallel."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            futures = []
            for tenant_id, values in json_data.items():
                if values is not None:
                    future = executor.submit(send_field_type_update_request, tenant_id, values, env, field_type)
                    futures.append(future)

            # Wait for all tasks to complete with timeout
            for future in futures:
                future.result(timeout=60)  # 60 second timeout per tenant
        except KeyboardInterrupt:
            print("Cancelling running tasks...")
            executor.shutdown(wait=False, cancel_futures=True)
            raise
