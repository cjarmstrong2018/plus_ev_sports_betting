

from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Session
from sqlalchemy.sql import select
import json
import pandas as pd

app = FastAPI()
SQLALCHEMY_DATABASE_URI = 'sqlite:///../sports_betting.db'
# Create SQLAlchemy engine
engine = create_engine(SQLALCHEMY_DATABASE_URI)


# Define endpoint for "all_best_lines"
@app.get("/all_best_lines")
def get_all_best_lines():
    df = pd.read_sql("SELECT * FROM best_lines_model_probabilities", engine)
    return df.to_json(orient='records')

# Define endpoint for "filtered_lines"
@app.get("/reccommended_bets")
def get_filtered_lines():
    df = pd.read_sql("SELECT * FROM plus_ev_bets", engine)
    return df.to_json(orient='records')

if __name__ == "__main__":
    app.run()