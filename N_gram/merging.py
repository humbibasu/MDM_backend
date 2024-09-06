# # merging.py

# from typing import List, Dict
# from datetime import datetime
# import json
# import os
# from fastapi import HTTPException
# from enum import Enum

# # Enum for Rules
# class RuleType(str, Enum):
#     conflict_resolution = "conflict_resolution"
#     data_aggregation = "data_aggregation"

# def resolve_conflicts(suspects: List[Dict]) -> Dict:
#     golden_record = {}
#     for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
#         values = [suspect[field] for suspect in suspects if suspect[field]]

#         if field == 'Last_Updated':
#             golden_record[field] = max(values, key=lambda d: datetime.strptime(d, "%d-%m-%Y"))
#         else:
#             golden_record[field] = max(set(values), key=values.count)

#     golden_record['Record_ID'] = str(suspects[0]['Record_ID'])
#     golden_record['UUID'] = suspects[0].get('UUID')

#     return golden_record

# def aggregate_data(suspects: List[Dict]) -> Dict:
#     aggregated_record = {}
#     for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
#         values = [suspect[field] for suspect in suspects if suspect[field]]

#         if field == 'Last_Updated':
#             aggregated_record[field] = max(values, key=lambda d: datetime.strptime(d, "%d-%m-%Y"))
#         else:
#             aggregated_record[field] = values[0]  # Keep the first non-null value

#     aggregated_record['Record_ID'] = str(suspects[0]['Record_ID'])
#     aggregated_record['UUID'] = suspects[0].get('UUID')

#     return aggregated_record

# def apply_rules(rule_type: RuleType, suspect_file_path: str) -> Dict:
#     try:
#         if not suspect_file_path:
#             raise HTTPException(status_code=404, detail="No suspects file found")

#         with open(suspect_file_path, 'r') as file:
#             suspects = json.load(file)
        
#         if not suspects:
#             raise HTTPException(status_code=404, detail="No suspects found")

#         if rule_type == RuleType.conflict_resolution:
#             return resolve_conflicts(suspects)
#         elif rule_type == RuleType.data_aggregation:
#             return aggregate_data(suspects)
#         else:
#             raise HTTPException(status_code=400, detail="Invalid rule type")
#     except FileNotFoundError:
#         raise HTTPException(status_code=404, detail="Suspects file not found")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


from typing import List, Dict, Any
from datetime import datetime
import json
import os
from fastapi import HTTPException
from enum import Enum
from collections import Counter
import pandas as pd

# Enum for Rules
class RuleType(str, Enum):
    conflict_resolution = "conflict_resolution"
    data_aggregation = "data_aggregation"
    survivorship = "survivorship"

# Define rule functions for Survivorship
def most_recent_update(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    most_recent = max(records, key=lambda r: datetime.strptime(r['Last_Updated'], "%d-%m-%Y"))
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

    return golden_record

# Existing functions for conflict resolution and data aggregation
def resolve_conflicts(suspects: List[Dict]) -> Dict:
    golden_record = {}
    for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
        values = [suspect[field] for suspect in suspects if suspect[field]]

        if field == 'Last_Updated':
            golden_record[field] = max(values, key=lambda d: datetime.strptime(d, "%d-%m-%Y"))
        else:
            golden_record[field] = max(set(values), key=values.count)

    golden_record['Record_ID'] = str(suspects[0]['Record_ID'])
    golden_record['UUID'] = suspects[0].get('UUID')

    return golden_record

def aggregate_data(suspects: List[Dict]) -> Dict:
    aggregated_record = {}
    for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
        values = [suspect[field] for suspect in suspects if suspect[field]]

        if field == 'Last_Updated':
            aggregated_record[field] = max(values, key=lambda d: datetime.strptime(d, "%d-%m-%Y"))
        else:
            aggregated_record[field] = values[0]  # Keep the first non-null value

    aggregated_record['Record_ID'] = str(suspects[0]['Record_ID'])
    aggregated_record['UUID'] = suspects[0].get('UUID')

    return aggregated_record

# Modified apply_rules function to include survivorship
def apply_rules(rule_type: RuleType, suspect_file_path: str) -> Dict:
    try:
        if not suspect_file_path:
            raise HTTPException(status_code=404, detail="No suspects file found")

        with open(suspect_file_path, 'r') as file:
            suspects = json.load(file)
        
        if not suspects:
            raise HTTPException(status_code=404, detail="No suspects found")

        if rule_type == RuleType.conflict_resolution:
            return resolve_conflicts(suspects)
        elif rule_type == RuleType.data_aggregation:
            return aggregate_data(suspects)
        elif rule_type == RuleType.survivorship:
            return apply_survivorship_rules(suspects)
        else:
            raise HTTPException(status_code=400, detail="Invalid rule type")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Suspects file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

