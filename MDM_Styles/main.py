from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fuzzy import fuzzy_match_single_collection
from merge import apply_rules, RuleType, MergingStyle
from datetime import datetime

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Store the path to the suspect file globally
suspect_file_path = ""

@app.get("/fuzzy-match")
def fuzzy_match_api(
    input_name: str = Query(..., description="The input name to match against the collection"),
    threshold: int = Query(..., description="The threshold for fuzzy matching"),
    top_n: int = Query(10, description="The number of top matches to return")
):
    try:
        global suspect_file_path
        suspects, suspect_file_path = fuzzy_match_single_collection(input_name, threshold, top_n)
        return {"Suspects": suspects, "SuspectsFile": suspect_file_path}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/apply-rules")
def apply_rules_api(
    rule_type: RuleType = Query(..., description="Rule type to apply "),
    merging_style: MergingStyle = Query(..., description="Merging style (registry_style or transaction_style)")
):
    try:
        if not suspect_file_path:
            raise HTTPException(status_code=400, detail="Suspect file path not set. Please run the fuzzy-match endpoint first.")

        result = apply_rules(rule_type, suspect_file_path, merging_style)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)