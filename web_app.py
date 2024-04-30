from flask import Flask, render_template, jsonify, send_from_directory
import requests
import time
from apscheduler.schedulers.background import BackgroundScheduler
import json
import pandas as pd
from sqlalchemy import create_engine

SQLALCHEMY_DATABASE_URI = 'sqlite:///sports_betting.db'


app = Flask(__name__, static_folder='static')

engine = create_engine(SQLALCHEMY_DATABASE_URI)



@app.route("/")
def home():
    return render_template("home.html")

@app.route('/about')
def about():
    return render_template('about.html')


@app.route("/plus_ev_bets")
def plus_ev():
    df = pd.read_sql("SELECT * FROM plus_ev_bets", engine)
    if df.empty:
        return render_template("no_bets_right_now.html", bets=None, image_url='jontay_porter.jpeg')
    df['mean_implied_probability'] = df['mean_implied_probability'].round(2)
    df['predicted_probability'] = df['predicted_probability'].round(2)
    df['expected_value'] = df['expected_value'].round(2)
    df['kelly'] = df['kelly'].round(3)
    df['half_kelly'] = df['half_kelly'].round(3)
    df['best_implied_probability'] = df['best_implied_probability'].round(2)
    
    df['thresh'] = df['thresh'].round(2)
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['best_odds_update_time'] = pd.to_datetime(df['best_odds_update_time'])
    df['avg_odds_update_time'] = pd.to_datetime(df['avg_odds_update_time'])
    df['start_time'] = df['start_time'].dt.strftime('%Y-%m-%d %H:%M') # add time zone
    df['best_odds_update_time'] = df['best_odds_update_time'].dt.strftime('%Y-%m-%d %H:%M')
    df['avg_odds_update_time'] = df['avg_odds_update_time'].dt.strftime('%Y-%m-%d %H:%M')
    return render_template("plus_ev.html", bets=json.loads(df.to_json(orient='records')), image_url='static/jontay_porter.jpeg')

@app.route("/best_lines")
def best_lines():
    df = pd.read_sql("SELECT * FROM best_lines_model_probabilities", engine)
    df['mean_implied_probability'] = df['mean_implied_probability'].round(2)
    df['predicted_probability'] = df['predicted_probability'].round(2)
    df['expected_value'] = df['expected_value'].round(2)
    df['kelly'] = df['kelly'].round(3)
    df['half_kelly'] = df['half_kelly'].round(3)
    df['best_implied_probability'] = df['best_implied_probability'].round(2)
    
    df['thresh'] = df['thresh'].round(2)
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['best_odds_update_time'] = pd.to_datetime(df['best_odds_update_time'])
    df['avg_odds_update_time'] = pd.to_datetime(df['avg_odds_update_time'])
    df['start_time'] = df['start_time'].dt.strftime('%Y-%m-%d %H:%M') # add time zone
    df['best_odds_update_time'] = df['best_odds_update_time'].dt.strftime('%Y-%m-%d %H:%M')
    df['avg_odds_update_time'] = df['avg_odds_update_time'].dt.strftime('%Y-%m-%d %H:%M')
    data = json.loads(df.to_json(orient='records'))
    return render_template("all_lines.html", bets=data)

@app.get("/all_best_lines")
def get_all_best_lines():
    df = pd.read_sql("SELECT * FROM best_lines_model_probabilities", engine)
    
    return df.to_json(orient='records')

# Define endpoint for "filtered_lines"
@app.get("/reccommended_bets")
def get_filtered_lines():
    df = pd.read_sql("SELECT * FROM plus_ev_bets", engine)
    return df.to_json(orient='records')


@app.route('/image/<path:filename>')
def serve_image(filename):
    return send_from_directory(app.static_folder + '/images', filename)

if __name__ == "__main__":
    app.run(host = '0.0.0.0', port = 5000)
