# # main.py

# from fastapi import FastAPI, HTTPException, Query
# from fastapi.middleware.cors import CORSMiddleware
# from fuzzy_matching import fuzzy_match_single_collection
# from merging import apply_rules, RuleType

# app = FastAPI()

# # Store the path to the suspect file globally
# suspect_file_path = ""

# @app.get("/fuzzy-match")
# def fuzzy_match_api(
#     input_name: str = Query(..., description="The input name to match against the collection"),
#     threshold: int = Query(..., description="The threshold for fuzzy matching"),
#     top_n: int = Query(10, description="The number of top matches to return")
# ):
#     try:
#         global suspect_file_path
#         suspects, suspect_file_path = fuzzy_match_single_collection(input_name, threshold, top_n)
#         return {"Suspects": suspects, "SuspectsFile": suspect_file_path}
#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/apply-rules")
# def apply_rules_api(
#     rule_type: RuleType = Query(..., description="Rule type to apply (conflict_resolution or data_aggregation)")
# ):
#     try:
#         golden_record_data = apply_rules(rule_type, suspect_file_path)
#         return {"GoldenRecord": golden_record_data}
#     except HTTPException as e:
#         raise e
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8080)




from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fuzzy_matching import fuzzy_match_single_collection
from merging import apply_rules, RuleType

app = FastAPI()

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
    rule_type: RuleType = Query(..., description="Rule type to apply (conflict_resolution, data_aggregation, or survivorship)")
):
    try:
        golden_record_data = apply_rules(rule_type, suspect_file_path)
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
    uvicorn.run(app, host="0.0.0.0", port=8000)
