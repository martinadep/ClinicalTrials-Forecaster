"""
FastAPI wrapper around models.predict.predict_ranking() -- lets the dashboard
call the model over HTTP instead of importing PySpark in-process. See
dashboard/DASHBOARD_INTEGRATION.md section 4, "Option 1 -- via the inference API".
"""
from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from models.predict import predict_ranking

app = FastAPI()


class PredictRequest(BaseModel):
    trial_params: dict
    candidate_sites: List[dict]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict(req: PredictRequest):
    try:
        ranked = predict_ranking(req.trial_params, req.candidate_sites)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))

    return {
        "ranked_results": [
            {"site": site, "predicted_velocity": velocity} for site, velocity in ranked
        ]
    }
