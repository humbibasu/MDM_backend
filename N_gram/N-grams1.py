from fastapi import FastAPI, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from typing import List

app = FastAPI()

# MongoDB connection
client = AsyncIOMotorClient("mongodb://localhost:27017")
db = client.MDM
collection = db.n_gram_records

# Pydantic models
class Record(BaseModel):
    name: str
    address: str
    phone: str

class MatchResult(BaseModel):
    record: Record
    similarity: float

# N-gram matching functions
def generate_ngrams(string, n):
    return [string[i:i+n] for i in range(len(string)-n+1)]

def calculate_similarity(str1, str2, n):
    ngrams1 = set(generate_ngrams(str1.lower(), n))
    ngrams2 = set(generate_ngrams(str2.lower(), n))
    shared_ngrams = ngrams1.intersection(ngrams2)
    similarity = (2 * len(shared_ngrams)) / (len(ngrams1) + len(ngrams2))
    return similarity * 100

# API endpoints
@app.get("/match/", response_model=MatchResult)
async def match_strings(
    str1: str = Query(..., description="First string to match"),
    str2: str = Query(..., description="Second string to match"),
    threshold: float = Query(40.0, description="Similarity threshold")
):
    similarity = calculate_similarity(str1, str2, 2)  # Using bigrams
    
    # Find the closest matching record in the database
    all_records = await collection.find().to_list(length=None)
    best_match = None
    best_similarity = 0
    
    for record in all_records:
        record_similarity = max(
            calculate_similarity(str1, record['name'], 2),
            calculate_similarity(str2, record['name'], 2)
        )
        
        # Debugging: Print the similarity score for each record
        print(f"Comparing '{str1}' and '{str2}' with '{record['name']}': Similarity = {record_similarity:.2f}%")
        
        if record_similarity > best_similarity:
            best_similarity = record_similarity
            best_match = record
    
    # Apply the threshold to determine if a match should be returned
    if best_similarity >= threshold:
        print(f"Best match: {best_match['name']} with {best_similarity:.2f}% similarity.")
        return MatchResult(
            record=Record(**best_match),
            similarity=best_similarity
        )
    else:
        print("No match found above the threshold.")
        raise HTTPException(status_code=404, detail="No matching record found")

@app.get("/records/", response_model=List[Record])
async def get_all_records():
    records = await collection.find().to_list(length=None)
    return [Record(**record) for record in records]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

