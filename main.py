#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ElasticSearch Bulk Document Updater - Unified Main Script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Author: Ugurcan Dede
Date: August 23, 2025
GitHub: https://github.com/ugurcandede

This unified script combines three different Elasticsearch operations:
1. Add Field Operations - Adds a field to all documents
2. Field Type Operations - Updates field types in documents
3. Document Operations - Updates organization information in documents

Usage:
    python main.py --operation add_field --env local --data result.json
    python main.py --operation field_type --env local --data result.json
    python main.py --operation update_document --env local --tenant_id develop --data result.json
"""

import argparse
import json
import sys
from src.config import ENVIRONMENTS
from src.utils import print_start_info, print_end_info
from src.add_field_operations import execute_add_field_operation
from src.field_type_operations import execute_field_type_operation
from src.document_operations import execute_document_update_operation


def load_json_data(file_path):
    """Load JSON data from file."""
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{file_path}'.")
        sys.exit(1)


def validate_args(args):
    """Validate command line arguments."""
    if args.operation == "update_document" and not args.tenant_id:
        print("Error: --tenant_id is required for update_document operation.")
        sys.exit(1)

    if args.operation == "add_field" and not args.field_key:
        print("Error: --field_key is required for add_field operation.")
        sys.exit(1)

    if args.operation == "field_type" and not args.field_key:
        print("Error: --field_type is required for field_type operation.")
        sys.exit(1)

    if args.operation == "update_document" and not args.field_key:
        print("Error: --doc_key is required for update_document operation.")
        sys.exit(1)

    if args.operation == "update_document" and not args.field_key:
        print("Error: --query_field_key is required for update_document operation.")
        sys.exit(1)


def confirm_operation(args, json_data):
    """Ask user to confirm the operation before proceeding."""
    print("=" * 60)
    print("OPERATION CONFIRMATION")
    print("=" * 60)
    print(f"Operation Type: {args.operation.upper()}")
    print(f"Environment: {args.env.upper()}")
    print(f"Data file: {args.data}")
    if args.tenant_id:
        print(f"Tenant ID: {args.tenant_id}")
    print(f"Max workers: {args.max_workers}")

    # Show data summary
    if json_data:
        print(f"Data entries: {len(json_data)}")
        if args.operation == "add_field":
            print(f"Field key: {args.field_key}")
            valid_entries = sum(1 for v in json_data.values() if v is not None)
            print(f"Valid tenant entries: {valid_entries}")
        elif args.operation == "field_type":
            valid_entries = sum(1 for v in json_data.values() if v is not None)
            total_operations = sum(len(v) if v else 0 for v in json_data.values())
            print(f"Valid tenant entries: {valid_entries}")
            print(f"Total field type operations: {total_operations}")
        elif args.operation == "update_document":
            total_requesters = sum(len(v) if v else 0 for v in json_data.values())
            print(f"Organizations: {len(json_data)}")
            print(f"Total requesters: {total_requesters}")

    print("-" * 60)

    # Warning for production
    if args.env == "prod":
        print("⚠️  WARNING: You are about to run this operation on PRODUCTION!")
        print("⚠️  This will modify live data in production environment!")
        print("-" * 60)

    while True:
        response = input("Do you want to proceed with this operation? (yes/no): ").strip().lower()
        if response in ['yes', 'y']:
            return True
        elif response in ['no', 'n']:
            print("Operation cancelled by user.")
            return False
        else:
            print("Please enter 'yes' or 'no'")


def main():
    parser = argparse.ArgumentParser(
        description="Unified Elasticsearch Document Updater",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Operation Types:
  add_field      - Add a field to all documents
  field_type     - Update field types in documents  
  update_document- Update organization information in documents

Examples:
  python main.py --operation add_field --env local --data result.json
  python main.py --operation field_type --env local --data result.json
  python main.py --operation update_document --env local --tenant_id develop --data result.json
        """
    )

    parser.add_argument(
        "--operation",
        required=True,
        choices=["add_field", "field_type", "update_document"],
        help="Type of operation to perform"
    )

    parser.add_argument(
        "--env",
        required=True,
        choices=ENVIRONMENTS.keys(),
        help="Environment to update"
    )

    parser.add_argument(
        "--data",
        default="result.json",
        help="JSON data file path (default: result.json)"
    )

    parser.add_argument(
        "--tenant_id",
        help="Tenant ID (required for update_document operation)"
    )

    parser.add_argument(
        "--max_workers",
        type=int,
        default=4,
        help="Maximum number of worker threads (default: 4)"
    )

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation and proceed automatically"
    )

    # Arguments specific to operations
    parser.add_argument(
        "--field_key",
        help="Field key to add (required for add_field operation)",
        type=str
    )

    parser.add_argument(
        "--field_type",
        help="Field type to add (required for field_type operation)",
        type=str
    )

    parser.add_argument(
        "--doc_key",
        help="Doc key to add (required for update_document operation)",
        type=str
    )

    parser.add_argument(
        "--query_field_key",
        help="Doc key to add (required for update_document operation)",
        type=str
    )

    args = parser.parse_args()
    validate_args(args)

    # Load data
    json_data = load_json_data(args.data)

    # Confirm operation with user
    if not args.yes and not confirm_operation(args, json_data):
        sys.exit(0)

    # Start operation
    start_time = print_start_info()

    try:
        if args.operation == "add_field":
            execute_add_field_operation(json_data, args.env, args.field_key, args.max_workers)
        elif args.operation == "field_type":
            execute_field_type_operation(json_data, args.env, args.field_type, args.max_workers)
        elif args.operation == "update_document":
            execute_document_update_operation(json_data, args.env, args.doc_key, args.query_field_key, args.tenant_id,
                                              args.max_workers)
    except KeyboardInterrupt:
        print("\n\nOperation interrupted by user (Ctrl+C)")
        print("Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    # End operation
    print_end_info(start_time)


if __name__ == "__main__":
    main()
