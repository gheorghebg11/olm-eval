#!/usr/bin/env python3
"""
Script to update ground truth JSONL files.
Automatically backs up the original file with a datetime stamp before saving updates.
"""

import argparse
import json
import os
import shutil
from datetime import datetime


def load_jsonl(file_path: str) -> list[dict]:
    """Load a JSONL file and return a list of dictionaries."""
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                data.append(json.loads(line))
    return data


def save_jsonl(data: list[dict], file_path: str) -> None:
    """Save a list of dictionaries to a JSONL file."""
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')


def backup_file(file_path: str) -> str:
    """
    Create a backup of the file with a datetime stamp.
    If file is NAME.jsonl, backup will be NAME_YYYYMMDD_HHMMSS.jsonl
    Returns the backup file path.
    """
    base_name = os.path.splitext(file_path)[0]
    extension = os.path.splitext(file_path)[1]

    # Generate datetime stamp in format YYYYMMDD_HHMMSS
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    backup_path = f"{base_name}_{timestamp}{extension}"

    # Copy the original file to the backup location
    shutil.copy2(file_path, backup_path)
    print(f"Backed up original file to: {backup_path}")

    return backup_path


def update_gt_data(data: list[dict], args) -> list[dict]:
    """
    Update the ground truth data based on command-line arguments.

    Args:
        data: List of dictionaries loaded from the JSONL file
        args: Parsed command-line arguments

    Returns:
        Updated list of dictionaries
    """
    updated_data = data

    # Operation 1: Remove tests where field equals value
    if args.remove_where:
        field, value = args.remove_where
        initial_count = len(updated_data)
        updated_data = [item for item in updated_data if str(item.get(field)) != value]
        removed_count = initial_count - len(updated_data)
        print(f"  Removed {removed_count} tests where {field}='{value}'")

    # Operation 2: Update field from old value to new value
    if args.update_field:
        field, old_value, new_value = args.update_field
        updated_count = 0
        for item in updated_data:
            if str(item.get(field)) == old_value:
                item[field] = new_value
                updated_count += 1
        print(f"  Updated field '{field}' from '{old_value}' to '{new_value}' for {updated_count} tests")

    return updated_data


def main():
    parser = argparse.ArgumentParser(
        description="Update a ground truth JSONL file with automatic backup."
    )
    parser.add_argument(
        "gt_file",
        type=str,
        help="Path to the ground truth JSONL file to update"
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually save changes (default is dry-run mode)"
    )

    # Default operations
    parser.add_argument(
        "--remove-where",
        nargs=2,
        metavar=("FIELD", "VALUE"),
        help="Remove all tests where FIELD equals VALUE (e.g., --remove-where type baseline)"
    )
    parser.add_argument(
        "--update-field",
        nargs=3,
        metavar=("FIELD", "OLD_VALUE", "NEW_VALUE"),
        help="Update FIELD from OLD_VALUE to NEW_VALUE (e.g., --update-field status pending reviewed)"
    )

    args = parser.parse_args()

    # Process the gt_file path
    gt_file = args.gt_file

    # If it's just a filename (not a path), use default folder
    if not os.path.dirname(gt_file):
        default_folder = "olmOCR-bench/bench_data"
        gt_file = os.path.join(default_folder, gt_file)

    # If no extension, add .jsonl
    if not os.path.splitext(gt_file)[1]:
        gt_file = gt_file + ".jsonl"

    # Check if file exists
    if not os.path.exists(gt_file):
        print(f"Error: File not found: {gt_file}")
        return 1

    print(f"Loading GT file: {gt_file}")

    # Load the JSONL file
    data = load_jsonl(gt_file)
    print(f"Loaded {len(data)} entries")

    # Update the data
    print("Updating data...")
    updated_data = update_gt_data(data, args)
    print(f"Updated to {len(updated_data)} entries")

    if not args.no_dry_run:
        print("\n=== DRY RUN MODE - NOT SAVING CHANGES ===")
        print("Sample of updated data:")
        for item in updated_data[:3]:
            print(f"  {item}")
        print("\nTo actually save changes, run with --no-dry-run flag")
        return 0

    # Backup the original file
    backup_path = backup_file(gt_file)

    # Save the updated data
    print(f"Saving updated data to: {gt_file}")
    save_jsonl(updated_data, gt_file)
    print("Done!")

    return 0


if __name__ == "__main__":
    exit(main())
