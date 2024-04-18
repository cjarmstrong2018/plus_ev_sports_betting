from flask import Flask, render_template, jsonify
import requests
import time
from apscheduler.schedulers.background import BackgroundScheduler
import json

app = Flask(__name__)

def fetch_data():
    response = requests.get("http://localhost:8000/reccommended_bets")
    if response.status_code == 200:
        return json.loads(response.json())
    else:
        print(response.status_code)
        return []

scheduler = BackgroundScheduler()
scheduler.add_job(func=fetch_data, trigger="interval", seconds=60)
scheduler.start()

@app.route("/")
def index():
    data = fetch_data()
    return render_template("index.html", bets=data)

if __name__ == "__main__":
    app.run(debug=True)
