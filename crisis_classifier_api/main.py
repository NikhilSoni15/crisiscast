from fastapi import FastAPI, Request
from pydantic import BaseModel
from model_utils import classify_text
from model_utils import classify_batch_texts
from typing import List

app = FastAPI()

class CrisisInput(BaseModel):
    title: str
class TextInput(BaseModel):
    text: str


@app.post("/predict")
def predict_crisis(data: CrisisInput):
    label = classify_text(data.title)
    return {"crisis_type": label}

@app.post("/classify")
def classify(input: TextInput):
    prediction = classify_text(input.text)
    return {"crisis_type": prediction}

class BatchRequest(BaseModel):
    texts: List[str]    

@app.post("/classify/batch")
def classify_batch(input: BatchRequest):
    # print("📝 Incoming titles:", input.texts)
    texts = input.texts[:2]
    return {"crisis_types": classify_batch_texts(texts)}
