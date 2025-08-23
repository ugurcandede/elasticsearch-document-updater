#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Elasticsearch Configuration Module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: August 23, 2025

This module contains common configuration settings for Elasticsearch operations.
"""

# Environment configurations
ENVIRONMENTS = {
    "local": "dev",
    "dev": "staging",
    "net": "preprod",
    "prod": "prod",
}

ENVIRONMENT_ADDRESSES = {
    "local": "http://localhost:9200",
    "dev": "http://xxx:9200",
    "net": "http://xxx:9200",
    "prod": "http://xxx:9200",
}

HEADERS = {"Content-Type": "application/json"}

# Default thread pool settings
DEFAULT_MAX_WORKERS = 4
