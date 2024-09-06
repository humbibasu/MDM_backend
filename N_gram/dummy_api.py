# from fastapi import FastAPI, HTTPException, Query
# from pymongo import MongoClient
# from typing import List, Dict
# from fuzzywuzzy import fuzz
# from fastapi.middleware.cors import CORSMiddleware
# import json
# import os

# app = FastAPI()

# # Define the data model manually without Pydantic for ObjectId
# def convert_objectid_to_str(doc):
#     """Convert ObjectId to string in the document."""
#     if "_id" in doc:
#         doc["_id"] = str(doc["_id"])
#     if "Record_ID" in doc:
#         doc["Record_ID"] = str(doc["Record_ID"])
#     return doc

# # Function to perform fuzzy matching
# def fuzzy_match(target: str, choices: List[Dict], threshold: int, top_n: int) -> List[Dict]:
#     match_results = []
#     target_lower = target.lower()
#     for choice in choices:
#         if isinstance(choice['Name'], str):
#             choice_lower = choice['Name'].lower()
#             score = fuzz.ratio(target_lower, choice_lower)
#             if score >= threshold:
#                 match_results.append((choice, score))
    
#     sorted_matches = sorted(match_results, key=lambda x: x[1], reverse=True)
#     top_matches = [doc for doc, score in sorted_matches[:top_n]]
#     return top_matches

# # Function to perform fuzzy matching within a single MongoDB collection
# def fuzzy_match_single_collection(input_name: str, threshold: int, top_n: int) -> List[Dict]:
#     client = MongoClient('mongodb://localhost:27017/')
#     db = client['MDM']
#     collection = db['business_rules_data']

#     # Fetch all documents with the necessary fields, use alias for UUID
#     cursor = collection.find({}, {
#         'Record_ID': 1,
#         "UUID('5466fad3-1a0c-4816-a468-785173b5a48a')": 1,
#         'Name': 1,
#         'Address': 1,
#         'Phone': 1,
#         'Email': 1,
#         'Last_Updated': 1,
#         'Source_System': 1
#     })

#     all_docs = []
#     for doc in cursor:
#         doc['UUID'] = doc.pop("UUID('5466fad3-1a0c-4816-a468-785173b5a48a')", None)
#         all_docs.append(convert_objectid_to_str(doc))

#     matches = fuzzy_match(input_name, all_docs, threshold, top_n)
#     if not matches:
#         raise HTTPException(status_code=404, detail="No matches found")

#     return matches

# # Function to store suspects locally
# def store_suspects_locally(suspects: List[Dict], filename: str) -> str:
#     filepath = os.path.join(os.getcwd(), filename)
#     with open(filepath, 'w') as file:
#         json.dump(suspects, file, default=str, indent=4)  # Use indent=4 for pretty-printing
#     return filepath

# # Function to apply conflict resolution and generate a golden record
# def generate_golden_record(suspects: List[Dict]) -> Dict:
#     # Implement your conflict resolution rules here
#     golden_record = {}
#     for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
#         values = [suspect[field] for suspect in suspects if suspect[field]]
#         if field == 'Last_Updated':
#             golden_record[field] = max(values)  # Use the most recent date
#         else:
#             golden_record[field] = values[0]  # Choose the first value (customize as needed)

#     # Include Record_ID and UUID from the first suspect (or customize as needed)
#     golden_record['Record_ID'] = str(suspects[0]['Record_ID'])
#     golden_record['UUID'] = suspects[0].get('UUID')

#     return golden_record

# # Define the API endpoint to perform fuzzy matching, store suspects, and generate a golden record
# @app.get("/generate-golden-record")
# def generate_golden_record_api(
#     input_name: str = Query(..., description="The input name to match against the collection"),
#     threshold: int = Query(..., description="The threshold for fuzzy matching"),
#     top_n: int = Query(10, description="The number of top matches to return")
# ):
#     try:
#         # Step 1: Perform fuzzy matching and retrieve suspects
#         suspects = fuzzy_match_single_collection(input_name, threshold, top_n)
        
#         # Step 2: Store the suspects locally
#         filepath = store_suspects_locally(suspects, "suspects.json")
        
#         # Step 3: Generate a golden record by applying conflict resolution rules
#         golden_record_data = generate_golden_record(suspects)
        
#         # Return the golden record
#         return {"GoldenRecord": golden_record_data}
#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# # Allow CORS for all origins
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Run the FastAPI application
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8080)



from fastapi import FastAPI, HTTPException, Query, Body
from pymongo import MongoClient
from typing import List, Dict
from fuzzywuzzy import fuzz
from fastapi.middleware.cors import CORSMiddleware
import json
import os
from datetime import datetime
from enum import Enum
from statistics import mean, mode

app = FastAPI()

# Enum for Conflict Resolution Rules
class ConflictResolutionRule(str, Enum):
    most_recent = "most_recent"
    most_frequent = "most_frequent"
    highest_value = "highest_value"
    lowest_value = "lowest_value"
    custom = "custom"

# Enum for Data Aggregation Rules
class DataAggregationRule(str, Enum):
    sum = "sum"
    average = "average"
    max = "max"
    min = "min"
    mode = "mode"
    custom = "custom"

def convert_objectid_to_str(doc):
    """Convert ObjectId to string in the document."""
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    if "Record_ID" in doc:
        doc["Record_ID"] = str(doc["Record_ID"])
    return doc

def fuzzy_match(target: str, choices: List[Dict], threshold: int, top_n: int) -> List[Dict]:
    match_results = []
    target_lower = target.lower()
    for choice in choices:
        if isinstance(choice['Name'], str):
            choice_lower = choice['Name'].lower()
            score = fuzz.ratio(target_lower, choice_lower)
            if score >= threshold:
                match_results.append((choice, score))
    
    sorted_matches = sorted(match_results, key=lambda x: x[1], reverse=True)
    top_matches = [doc for doc, score in sorted_matches[:top_n]]
    return top_matches

def fuzzy_match_single_collection(input_name: str, threshold: int, top_n: int) -> List[Dict]:
    client = MongoClient('mongodb://localhost:27017/')
    db = client['MDM']
    collection = db['business_rules_data']

    cursor = collection.find({}, {
        'Record_ID': 1,
        "UUID('5466fad3-1a0c-4816-a468-785173b5a48a')": 1,
        'Name': 1,
        'Address': 1,
        'Phone': 1,
        'Email': 1,
        'Last_Updated': 1,
        'Source_System': 1
    })

    all_docs = []
    for doc in cursor:
        doc['UUID'] = doc.pop("UUID('5466fad3-1a0c-4816-a468-785173b5a48a')", None)
        all_docs.append(convert_objectid_to_str(doc))

    matches = fuzzy_match(input_name, all_docs, threshold, top_n)
    if not matches:
        raise HTTPException(status_code=404, detail="No matches found")

    return matches

def store_suspects_locally(suspects: List[Dict], filename: str) -> str:
    filepath = os.path.join(os.getcwd(), filename)
    with open(filepath, 'w') as file:
        json.dump(suspects, file, default=str, indent=4)
    return filepath

def resolve_conflicts(suspects: List[Dict], rule: ConflictResolutionRule) -> Dict:
    golden_record = {}
    for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
        values = [suspect[field] for suspect in suspects if suspect[field]]

        if not values:
            golden_record[field] = None
            continue

        if rule == ConflictResolutionRule.most_recent and field == 'Last_Updated':
            golden_record[field] = max(values, key=lambda d: datetime.strptime(d, "%d-%m-%Y"))
        elif rule == ConflictResolutionRule.most_frequent:
            golden_record[field] = max(set(values), key=values.count)
        elif rule == ConflictResolutionRule.highest_value and isinstance(values[0], (int, float)):
            golden_record[field] = max(values)
        elif rule == ConflictResolutionRule.lowest_value and isinstance(values[0], (int, float)):
            golden_record[field] = min(values)
        else:
            golden_record[field] = values[0]

    golden_record['Record_ID'] = str(suspects[0]['Record_ID'])
    golden_record['UUID'] = suspects[0].get('UUID')

    return golden_record

def aggregate_data(suspects: List[Dict], rule: DataAggregationRule) -> Dict:
    aggregated_record = {}
    for field in ['Name', 'Address', 'Phone', 'Email', 'Last_Updated', 'Source_System']:
        values = [suspect[field] for suspect in suspects if suspect[field]]

        if not values:
            aggregated_record[field] = None
            continue

        if rule == DataAggregationRule.sum:
            if all(isinstance(v, (int, float)) for v in values):
                aggregated_record[field] = sum(values)
            else:
                raise ValueError(f"Cannot apply sum on non-numeric field {field}")

        elif rule == DataAggregationRule.average:
            if all(isinstance(v, (int, float)) for v in values):
                aggregated_record[field] = mean(values)
            else:
                raise ValueError(f"Cannot apply average on non-numeric field {field}")

        elif rule == DataAggregationRule.max:
            if all(isinstance(v, (int, float)) for v in values):
                aggregated_record[field] = max(values)
            else:
                raise ValueError(f"Cannot apply max on non-numeric field {field}")

        elif rule == DataAggregationRule.min:
            if all(isinstance(v, (int, float)) for v in values):
                aggregated_record[field] = min(values)
            else:
                raise ValueError(f"Cannot apply min on non-numeric field {field}")

        elif rule == DataAggregationRule.mode:
            try:
                aggregated_record[field] = mode(values)
            except:
                aggregated_record[field] = None  # Handle no mode found

        elif rule == DataAggregationRule.custom:
            aggregated_record[field] = values[0] if values else None

        else:
            aggregated_record[field] = values[0] if values else None

    aggregated_record['Record_ID'] = str(suspects[0]['Record_ID'])
    aggregated_record['UUID'] = suspects[0].get('UUID')

    return aggregated_record

@app.get("/fuzzy-match")
def fuzzy_match_api(
    input_name: str = Query(..., description="The input name to match against the collection"),
    threshold: int = Query(..., description="The threshold for fuzzy matching"),
    top_n: int = Query(10, description="The number of top matches to return")
):
    try:
        suspects = fuzzy_match_single_collection(input_name, threshold, top_n)
        filepath = store_suspects_locally(suspects, "suspects.json")
        return {"Suspects": suspects, "SuspectsFile": filepath}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/apply-conflict-resolution")
def apply_conflict_resolution(
    rule: ConflictResolutionRule = Query(..., description="Conflict resolution rule to apply"),
    suspects: List[Dict] = Body(..., description="List of suspects to resolve conflicts")
):
    try:
        if not suspects:
            raise HTTPException(status_code=404, detail="No suspects provided")

        golden_record_data = resolve_conflicts(suspects, rule)
        return {"GoldenRecord": golden_record_data}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/apply-data-aggregation")
def apply_data_aggregation(
    rule: DataAggregationRule = Query(..., description="Data aggregation rule to apply"),
    suspects: List[Dict] = Body(..., description="List of suspects to aggregate")
):
    try:
        if not suspects:
            raise HTTPException(status_code=404, detail="No suspects provided")

        if rule == DataAggregationRule.concat:
            raise HTTPException(status_code=400, detail="Concatenation is not supported in this aggregation endpoint")

        golden_record_data = aggregate_data(suspects, rule)
        return {"GoldenRecord": golden_record_data}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

