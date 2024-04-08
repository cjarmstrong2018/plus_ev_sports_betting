from datetime import datetime
import os
import time
import uuid 
from dotenv import load_dotenv
import requests
import json
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine


load_dotenv()

ODDS_API_KEY = os.getenv("ODDS_API_KEY")
REGIONS = 'us'
MARKETS = 'h2h'
ODDS_FORMAT = 'decimal'
DATE_FORMAT = 'iso'


SPORT_KEYS = [
    # "americanfootball_ncaaf",
    # "americanfootball_nfl",
    "baseball_mlb",
    # "basketball_nba",
    # "basketball_ncaab",
    # "hockey_nhl",
    # "soccer_usa_mls"
]

SQLALCHEMY_DATABASE_URI = 'sqlite:///sports_betting.db'
SLEEP_TIME_MINUTES = 5


class OddsAPI(object):
    def __init__(self, api_key=ODDS_API_KEY):
        self.api_key = api_key
        self.base_url = 'https://api.the-odds-api.com/v4'
    
    def get_sports(self):
        url = f"{self.base_url}/sports"
        params = {
            'api_key': self.api_key
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            int(f'Failed to get sports: status_code {response.status_code}, response body {response.text}')
        return response.json()
    
    def get_odds(self, sport_key, region=REGIONS, mkt=MARKETS, odds_format=ODDS_FORMAT, date_format=DATE_FORMAT):
        url = f"{self.base_url}/sports/{sport_key}/odds/"
        params = {
            'api_key': self.api_key,
            'regions': region,
            'markets': mkt,
            'oddsFormat': odds_format,
            'dateFormat': date_format
        }
        response = requests.get(url, params=params)
        if response.status_code != 200:
            int(f'Failed to get odds: status_code {response.status_code}, response body {response.text}')
        return response.json()
    


class OddsAPIExtractor:
    def __init__(self, api_key=ODDS_API_KEY):
        self.api = OddsAPI(api_key)
        self.engine = create_engine(SQLALCHEMY_DATABASE_URI)
        self.extracted_sports = None
        self.sports_table = None
        self.extracted_odds = None
        self.odds_table = None
        
    def extract_sports(self):
        """
        Gets the raw sports data from the API and stores it in the extracted_sports attribute

        Returns: 0 if successful, -1 if failed
        """
        try:
            sports = self.api.get_sports()
            self.extracted_sports = sports
            return 0
        except:
            return -1
    
    def transform_sports(self):
        """
        Transforms the extracted sports data into a table format and stores it in the sports_table attribute
        """
        self.sports_table = pd.DataFrame(self.extracted_sports)
    
    def load_sports(self):
        """
        Writes sports table to database
        """
        pass
    
    def extract_odds(self):
        """
        Extracts odds data from the API and stores it in the extracted_odds attribute

        Returns:
        0 if successful
        """
        self.extracted_odds = []
        for sport_key in SPORT_KEYS:
            try:
                odds = self.api.get_odds(sport_key)
                self.extracted_odds.extend(odds)
            except:
                print(f'Failed to extract odds for {sport_key}')
        return 0
            
    def transform_odds(self):
        """
        Transforms the extracted odds data into a table format and stores it in the odds_table attribute
        """
        self.odds_table = pd.json_normalize(self.extracted_odds, record_path=["bookmakers", "markets", "outcomes"], 
                    meta=["sport_key", "commence_time", "home_team", "away_team", 
                          ["bookmakers", "key"], ["bookmakers", "title"], ["markets", "key"]],
                    errors="ignore")
        self.odds_table = self.odds_table.rename(columns={
                                                        "name": "outcome", 
                                                        "price": "decimal_odds",
                                                        'sport_key': 'sport',
                                                        "commence_time": "start_time", 
                                                        "bookmakers.title": "sportsbook",}
                                                        )
        self.odds_table['update_time'] = pd.to_datetime('now')
        self.odds_table["id"] = [str(uuid.uuid4()) for _ in range(len(self.odds_table))]
        # convert start_time to from utc time to central time
        self.odds_table["start_time"] = pd.to_datetime(self.odds_table["start_time"])
        self.odds_table["start_time"] = self.odds_table["start_time"].dt.tz_convert('US/Central')
        self.odds_table = self.odds_table.loc[:, ['id', "sport", "home_team", "away_team", "start_time", "sportsbook", "outcome", "decimal_odds", "update_time"]]
        
    def load_odds(self):
        """
        Writes odds table to database
        """
        print(self.odds_table.head())
        self.odds_table.to_sql('all_betting_lines', self.engine, if_exists='replace', index=False)
        return 0
        
    def run_etl(self):
        """
        Runs one iteration of the ETL process

        Returns: 0 if successful, -1 if failed
            
        """
        r = self.extract_odds()
        if r != 0:
            return r
        print(self.extracted_odds[:5])
        r = self.transform_odds()
        if r != 0:
            return r
        r = self.load_odds()
        print(self.odds_table.head())
        if r != 0:
            return r
        return 0
    
    def run(self):
        """
        Runs the full ETL process every 5 minutes
        """
        # end at 9:30 pm central time
        end_time = datetime.now().replace(hour=21, minute=30, second=0, microsecond=0)
        while datetime.now() < end_time:
            self.run_etl()
            time.sleep(SLEEP_TIME_MINUTES * 60)
            
        
        
    
    
if __name__ == '__main__':
    extractor = OddsAPIExtractor()
    extractor.run()