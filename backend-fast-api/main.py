from typing import Any, Dict

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()


origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:3000",
]



app.add_middleware(

    CORSMiddleware,

    allow_origins=origins,

    allow_credentials=True,

    allow_methods=["*"],

    allow_headers=["*"],

)

class Weight(BaseModel):
    weight: int


@app.post("/dummy/forecast")
async def main(weight1 : Weight):
    w = weight1.weight
    df = pd.DataFrame({"Project month": [1 *w,2*w,3*w,4*w]})
    return df.to_dict("r")
