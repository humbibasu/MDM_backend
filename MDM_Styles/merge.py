import random
import string
from typing import List, Dict, Any
from datetime import datetime
import json
import pandas as pd
from collections import defaultdict, Counter
from fastapi import HTTPException
from enum import Enum

# Enum for Rules
class RuleType(str, Enum):
    conflict_resolution = "conflict_resolution"
    data_aggregation = "data_aggregation"
    survivorship = "survivorship"
    deduplication_and_consolidation = "deduplication_and_consolidation"

class MergingStyle(str, Enum):
    registry_style = "registry_style"
    transaction_style = "transaction_style"

# Helper function to generate a unique golden ID
def generate_golden_id(length=6) -> str:
    return ''.join(random.choices(string.digits, k=length))

# Helper function to generate a unique numeric reference ID
def generate_unique_id(existing_ids: set, length=6) -> str:
    while True:
        new_id = ''.join(random.choices(string.digits, k=length))
        if new_id not in existing_ids:
            existing_ids.add(new_id)
            return new_id

# Helper function to parse dates
def parse_date(date_string: str) -> datetime:
    try:
        return datetime.strptime(date_string, "%d-%m-%Y")
    except ValueError:
        try:
            return datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError(f"Unable to parse date: {date_string}")

# Define rule functions for Survivorship
def most_recent_update(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    most_recent = max(records, key=lambda r: parse_date(r['Last_Updated']))
    return most_recent

def most_frequent_value(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    column_values = {column: [] for column in records[0].keys() if column != 'Last_Updated'}
    for record in records:
        for column in column_values:
            column_values[column].append(record.get(column, ''))

    most_frequent = {}
    for column, values in column_values.items():
        most_common_value = Counter(values).most_common(1)[0][0]
        most_frequent[column] = most_common_value

    return most_frequent

def custom_business_rules(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    for record in records:
        if 'example.com' in record.get('Email', ''):
            return record
    return records[0]  # fallback

def most_complete_record(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    complete_record = max(records, key=lambda r: sum(1 for v in r.values() if v))
    return complete_record

def apply_survivorship_rules(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    most_recent = most_recent_update(records)
    frequent_values = most_frequent_value(records)
    custom_record = custom_business_rules(records)
    complete_record = most_complete_record(records)

    golden_record = {}
    for column in records[0].keys():
        if column == 'Last_Updated':
            continue

        values = {
            "most_recent_update": most_recent.get(column),
            "most_frequent": frequent_values.get(column),
            "custom_rule": custom_record.get(column),
            "most_complete": complete_record.get(column)
        }

        # Apply rules in a prioritized manner
        golden_record[column] = values["most_recent_update"]  # Start with the most recent update

        # Update with most frequent value if it differs from the current
        if golden_record[column] != values["most_frequent"]:
            golden_record[column] = values["most_frequent"]

        # Update with custom rule value if it differs from the current
        if golden_record[column] != values["custom_rule"]:
            golden_record[column] = values["custom_rule"]

        # Update with most complete record value if it differs from the current
        if golden_record[column] != values["most_complete"]:
            golden_record[column] = values["most_complete"]

    # Generate a shorter golden ID
    golden_record['Golden_ID'] = generate_golden_id()

    return golden_record

# Existing functions for conflict resolution and data aggregation
def resolve_conflicts(suspects: List[Dict]) -> Dict:
    golden_record = {}
    for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
        values = [suspect[field] for suspect in suspects if suspect[field]]

        if field == 'Last_Updated':
            golden_record[field] = max(values, key=lambda d: parse_date(d))
        else:
            golden_record[field] = max(set(values), key=values.count)

    # Generate a shorter golden ID
    golden_record['Golden_ID'] = generate_golden_id()

    return golden_record

def aggregate_data(suspects: List[Dict]) -> Dict:
    aggregated_record = {}
    for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
        values = [suspect[field] for suspect in suspects if suspect[field]]

        if field == 'Last_Updated':
            aggregated_record[field] = max(values, key=lambda d: parse_date(d))
        else:
            aggregated_record[field] = values[0]  # Keep the first non-null value

    # Generate a shorter golden ID
    aggregated_record['Golden_ID'] = generate_golden_id()

    return aggregated_record

def deduplication_and_consolidation(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {}
    
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(records)
    
    # De-Duplication based on all columns except the identifier (_id)
    subset_fields = df.columns.difference(['_id'])
    df_dedup = df.drop_duplicates(subset=subset_fields, keep='first')
    
    # Dynamically define aggregation rules based on data type
    aggregation_functions = defaultdict(lambda: 'first')  # Default to 'first'
    
    for column in df_dedup.columns:
        if df_dedup[column].dtype == 'object':
            aggregation_functions[column] = lambda x: x.mode()[0] if not x.mode().empty else None
        elif pd.api.types.is_numeric_dtype(df_dedup[column]):
            aggregation_functions[column] = 'mode'
        else:
            aggregation_functions[column] = 'mode'  # Default behavior for other types

    # Apply aggregation across all records to create a single consolidated record
    consolidated_series = df_dedup.agg(aggregation_functions)
    
    # Convert Series to dictionary
    golden_record = consolidated_series.to_dict()
    
    # Generate a shorter golden ID
    golden_record['Golden_ID'] = generate_golden_id()

    return golden_record

# Modified apply_rules function to include new rule
def apply_rules(rule_type: RuleType, suspect_file_path: str, merging_style: MergingStyle) -> Dict[str, Any]:
    try:
        if not suspect_file_path:
            raise HTTPException(status_code=404, detail="No suspects file found")

        with open(suspect_file_path, 'r') as file:
            suspects = json.load(file)

        if not suspects:
            raise HTTPException(status_code=404, detail="No suspects found")

        # Generate unique numeric IDs for suspects
        existing_ids = set()
        for suspect in suspects:
            suspect['id'] = generate_unique_id(existing_ids)
            suspect.pop('_id', None)  # Remove _id from suspects

        if rule_type == RuleType.conflict_resolution:
            golden_record = resolve_conflicts(suspects)
        elif rule_type == RuleType.data_aggregation:
            golden_record = aggregate_data(suspects)
        elif rule_type == RuleType.survivorship:
            golden_record = apply_survivorship_rules(suspects)
        elif rule_type == RuleType.deduplication_and_consolidation:
            golden_record = deduplication_and_consolidation(suspects)
        else:
            raise HTTPException(status_code=400, detail="Invalid rule type")

        golden_id = generate_golden_id()
        golden_record['Golden_ID'] = golden_id

        if merging_style == MergingStyle.registry_style:
            for suspect in suspects:
                suspect['reference'] = golden_id

            return {
                "GoldenRecord": golden_record,
                "Suspects": suspects
            }

        elif merging_style == MergingStyle.transaction_style:
            for suspect in suspects:
                suspect['reference'] = golden_id
                suspect['status'] = 'end-dated'
                suspect['end_dated_at'] = datetime.utcnow().isoformat()

            return {
                "GoldenRecord": golden_record,
                "Suspects": suspects
            }

        else:
            raise HTTPException(status_code=400, detail="Invalid merging style")

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Suspects file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))