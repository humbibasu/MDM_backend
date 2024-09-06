from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import pandas as pd
from fuzzywuzzy import fuzz
from typing import List, Dict, Any
from enum import Enum
import json

app = FastAPI()

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['mdm_example']

# Enum for Rules
class RuleType(str, Enum):
    conflict_resolution = "conflict_resolution"
    data_aggregation = "data_aggregation"
    survivorship = "survivorship"

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

def load_data(collection_name):
    collection = db[collection_name]
    data = list(collection.find({}))
    return pd.DataFrame(json.loads(JSONEncoder().encode(data)))

def normalize_columns(df, id_col, name_col, email_col, **kwargs):
    column_mapping = {
        id_col: 'id',
        name_col: 'name',
        email_col: 'email'
    }
    column_mapping.update(kwargs)
    return df.rename(columns=column_mapping)

def fuzzy_match(df: pd.DataFrame, search_name: str, threshold: int) -> pd.DataFrame:
    matches = []
    for _, row in df.iterrows():
        score = fuzz.token_set_ratio(search_name, row['name'])
        if score >= threshold:
            matches.append({
                'id': row['id'],
                'name': row['name'],
                'email': row['email'],
                'score': score
            })
    return pd.DataFrame(matches)

def resolve_conflicts(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    golden_record = {}
    for field in records[0].keys():
        if field == 'id':
            golden_record[field] = records[0][field]
        else:
            values = [record[field] for record in records if pd.notna(record[field])]
            if values:
                golden_record[field] = max(set(values), key=values.count)
    return golden_record

def aggregate_data(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    golden_record = {}
    for field in records[0].keys():
        if field == 'id':
            golden_record[field] = records[0][field]
        else:
            values = [record[field] for record in records if pd.notna(record[field])]
            if values:
                golden_record[field] = values[0]  # Keep the first non-null value
    return golden_record

def apply_survivorship_rules(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    golden_record = {}
    for field in records[0].keys():
        if field == 'id':
            golden_record[field] = records[0][field]
        else:
            values = [record[field] for record in records if pd.notna(record[field])]
            if values:
                if field == 'email':
                    golden_record[field] = next((v for v in values if 'example.com' in v), values[0])
                else:
                    golden_record[field] = max(values, key=lambda x: len(str(x)))
    return golden_record

def create_golden_record(df: pd.DataFrame, rule_type: RuleType) -> Dict[str, Any]:
    if df.empty:
        return {}

    all_records = df.to_dict('records')
    
    if rule_type == RuleType.conflict_resolution:
        return resolve_conflicts(all_records)
    elif rule_type == RuleType.data_aggregation:
        return aggregate_data(all_records)
    elif rule_type == RuleType.survivorship:
        return apply_survivorship_rules(all_records)
    else:
        raise ValueError(f"Invalid rule type: {rule_type}")

def safe_convert(val):
    try:
        return str(val).encode('utf-8', errors='ignore').decode('utf-8')
    except Exception as e:
        return None

def save_to_mongodb(df: pd.DataFrame, collection_name: str):
    if not df.empty:
        collection = db[collection_name]
        collection.delete_many({})

        df = df.applymap(safe_convert)
        records = json.loads(df.to_json(orient='records', date_format='iso'))
        
        collection.insert_many(records)
        print(f"Data saved to collection: {collection_name}")
    else:
        print(f"No data to save for collection: {collection_name}")

@app.get("/search")
async def search(
    name: str,
    threshold: int = Query(..., ge=0, le=100),
    top_suspects: int = Query(..., ge=1)
):
    try:
        # Load and normalize data from all collections
        crm_df = normalize_columns(load_data('crm_system'), 'customer_id', 'name', 'email', phone='phone')
        erp_df = normalize_columns(load_data('erp_system'), 'customer_id', 'name', 'email', billing_address='address')
        ecommerce_df = normalize_columns(load_data('ecommerce_system'), 'user_id', 'name', 'email', shipping_address='address')

        # Combine all data
        all_data = pd.concat([crm_df, erp_df, ecommerce_df], ignore_index=True)

        # Perform fuzzy matching
        matches = fuzzy_match(all_data, name, threshold)

        # Get top suspects
        top_suspects_df = matches.sort_values('score', ascending=False).head(top_suspects)

        # Save suspects to MongoDB
        save_to_mongodb(top_suspects_df, 'Suspects')

        return {
            "message": "Search completed successfully",
            "suspects": json.loads(JSONEncoder().encode(top_suspects_df.to_dict(orient='records'))),
            "total_suspects": len(top_suspects_df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/merge")
async def merge(rule_type: RuleType):
    try:
        # Load suspects from MongoDB
        suspects_df = load_data('Suspects')

        if suspects_df.empty:
            raise HTTPException(status_code=400, detail="No suspects found. Please perform a search first.")

        # Create a single golden record
        golden_record = create_golden_record(suspects_df, rule_type)
        golden_record_df = pd.DataFrame([golden_record])

        # Remove the golden record from suspects
        suspects_df = suspects_df[suspects_df['id'] != golden_record['id']]

        # Mark remaining suspects as end-dated
        suspects_df['status'] = 'end-dated'
        suspects_df['end_date'] = datetime.now()

        # Save golden record and updated suspects to MongoDB
        save_to_mongodb(golden_record_df, 'records_golden')
        save_to_mongodb(suspects_df, 'End_Dated_Suspects2')

        return {
            "message": "Merge completed successfully",
            "golden_record": json.loads(JSONEncoder().encode(golden_record)),
            "end_dated_suspects": json.loads(JSONEncoder().encode(suspects_df.to_dict(orient='records'))),
            "total_end_dated": len(suspects_df)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)