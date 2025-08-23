#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Elasticsearch Utilities Module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: August 23, 2025

This module contains common utility functions for Elasticsearch operations.
"""

import time
from datetime import datetime
from .config import ENVIRONMENTS, ENVIRONMENT_ADDRESSES


def get_update_url(env, tenant_id):
    """Generate Elasticsearch update URL for given environment and tenant."""
    if env in ENVIRONMENTS:
        return f"{ENVIRONMENT_ADDRESSES[env]}/{tenant_id}_tickets_{ENVIRONMENTS[env]}/_update_by_query?refresh=true"
    else:
        raise EnvironmentError(f"Unknown environment: {env}")


def print_elapsed_time(start_time, end_time):
    """Print formatted elapsed time."""
    elapsed_time = end_time - start_time

    milliseconds = int((elapsed_time - int(elapsed_time)) * 1000)
    seconds = int(elapsed_time % 60)
    minutes = int((elapsed_time // 60) % 60)
    hours = int(elapsed_time // 3600)

    print(f"\n{hours}h:{minutes}m:{seconds}sec:{milliseconds}ms elapsed")


def print_start_info():
    """Print start information."""
    start_time = time.time()
    start_datetime = datetime.fromtimestamp(start_time)
    print("Starting to update documents...\n")
    print(f"Start Time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    return start_time


def print_end_info(start_time):
    """Print end information and elapsed time."""
    end_time = time.time()
    end_datetime = datetime.fromtimestamp(end_time)
    print("\nFinished updating documents.")
    print(f"End Time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    print_elapsed_time(start_time, end_time)
    return end_time
