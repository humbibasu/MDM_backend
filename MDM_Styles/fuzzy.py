# fuzzy_matching.py
from pymongo import MongoClient
from fuzzywuzzy import fuzz
from typing import List, Dict
import json
import os
from fastapi import HTTPException

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
    collection = db['Transaction_sets']

    cursor = collection.find({}, {
        # 'Record_ID': 1,
        # "UUID('5466fad3-1a0c-4816-a468-785173b5a48a')": 1,
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

    suspect_file_path = store_suspects_locally(matches, "suspects.json")
    return matches, suspect_file_path

def store_suspects_locally(suspects: List[Dict], filename: str) -> str:
    filepath = os.path.join(os.getcwd(), filename)
    with open(filepath, 'w') as file:
        json.dump(suspects, file, default=str, indent=4)
    return filepath