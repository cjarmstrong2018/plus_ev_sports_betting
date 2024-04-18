import pickle
import traceback
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
# import fuzzy_pandas as fpd
import recordlinkage
import os
from dotenv import load_dotenv

from utils import basic_kelly_criterion

load_dotenv()
SQLALCHEMY_DATABASE_URI = 'sqlite:///../sports_betting.db'

ALPHA = os.getenv("ALPHA")
ALPHA = 0.05

BOOKMAKERS = {
    "BetOnline.ag": {"bookmaker_key": "betonlineag", "can_bet": False},
    "BetMGM": {"bookmaker_key": "betmgm", "can_bet": True},
    "BetRivers": {"bookmaker_key": "betrivers", "can_bet": True},
    "BetUS": {"bookmaker_key": "betus", "can_bet": False},
    "Bovada": {"bookmaker_key": "bovada", "can_bet": True},
    "DraftKings": {"bookmaker_key": "draftkings", "can_bet": True},
    "FanDuel": {"bookmaker_key": "fanduel", "can_bet": True},
    "LowVig.ag": {"bookmaker_key": "lowvig", "can_bet": False},
    "MyBookie.ag": {"bookmaker_key": "mybookieag", "can_bet": False},
    "PointsBet (US)": {"bookmaker_key": "pointsbetus", "can_bet": True},
    "SuperBook": {"bookmaker_key": "superbook", "can_bet": False},
    "Unibet": {"bookmaker_key": "unibet_us", "can_bet": False},
    "William Hill (Caesars)": {"bookmaker_key": "williamhill_us", "can_bet": True},
    "WynnBET": {"bookmaker_key": "wynnbet", "can_bet": False},
    "betPARX": {"bookmaker_key": "betparx", "can_bet": False},
    "ESPN BET": {"bookmaker_key": "espnbet", "can_bet": True},
    "Fliff": {"bookmaker_key": "fliff", "can_bet": False},
    "Hard Rock Bet": {"bookmaker_key": "hardrockbet", "can_bet": False},
    "SI Sportsbook": {"bookmaker_key": "sisportsbook", "can_bet": False},
    "Tipico": {"bookmaker_key": "tipico_us", "can_bet": False},
    "Wind Creek (Betfred PA)": {"bookmaker_key": "windcreek", "can_bet": False},
}


class LineFilter(object):
    def __init__(self):
        """
        This is the object that performs the ETL process of merging the two tables
        all_betting_lines and avg_odds into a single table and filtering the lines
        for those that are beyond the necessary threshold form the mean odds. 
        writes those lines to the database in the table plus_ev_bets
        """ 
        self.engine = create_engine(SQLALCHEMY_DATABASE_URI)
        self._alpha = float(ALPHA)
        self.model = pickle.load(open('model.pkl', 'rb'))
        self.average_odds = pd.DataFrame()
        self.all_betting_lines = pd.DataFrame()
        self.merged_df = pd.DataFrame()
        self.best_lines = pd.DataFrame()
        self.merged_df = pd.DataFrame()
        self.plus_ev_bets = pd.DataFrame()
        
    def extract(self):
        """
        Extracts data for all_betting_lines and avg_odds tables from the databasedata   
        stores them in the all_betting_lines and avg_odds attributes
        """
        self.all_betting_lines = pd.read_sql_query("SELECT * from all_betting_lines", self.engine)
        self.average_odds = pd.read_sql_query('SELECT * FROM avg_odds', self.engine)

    def transform(self):
        """
        Merges the two dataframes into a single betting dataframe and then filters the lines
        for those that are beyond the necessary threshold from the mean odds. For those that 
        are beyond the necessary threshold, it calculates the kelly criterion and writes those
        lines to the database in the table plus_ev_bets. Also assigns an ID to each plus_ev line to 
        track the bets and only alert me on new bets that hit
        """
        self.filter_bookmakers()
        self.compute_best_lines()
        self.merge_tables()
        self.necessary_calculations()
        self.find_plus_ev_bets()
    
    def load(self):
        """
        Writes the plus_ev_bets table to the database
        """
        self.merged_df.to_sql('best_lines_model_probabilities', self.engine, if_exists='replace', index=False)
        self.plus_ev_bets.to_sql('plus_ev_bets', self.engine, if_exists='replace', index=False)

    def merge_tables(self) -> None:
        """
        mergest the two tables best_lines and avg_odds into a single table. Currently this is done using an exact only approach but eventually this will be done
        using fuzzy_wuzzy to determine the best matches for each line
        """
        best_lines = self.best_lines.copy()
        best_lines = best_lines[['sport', 'home_team', 'away_team','start_time', 'sportsbook', 'outcome', 'decimal_odds', 'update_time']]
        best_lines = best_lines.rename(columns={'update_time': 'best_odds_update_time'})
        avg_odds = self.average_odds.copy()
        avg_odds = avg_odds[['sport', 'home_team', 'away_team','start_time', 'outcome', 'decimal_odds', 'update_time']]
        avg_odds = avg_odds.rename(columns={'update_time': 'avg_odds_update_time', 'decimal_odds': 'avg_odds'})
        df = pd.merge(best_lines, avg_odds, on=['sport', 'home_team', 'away_team', 'outcome'], how='inner', suffixes=('', '_y'))
        df = df[['sport', 'start_time', 'home_team', 'away_team', 'outcome','sportsbook', 
                 'decimal_odds', 'avg_odds', 'best_odds_update_time', 'avg_odds_update_time']]
        self.merged_df = df
      
    def compute_best_lines(self) -> None:
        """
        Selects only the best line for each outcome across all sportsbooks
        """
        # select the best line for each outcome using group by
        idx_max = self.all_betting_lines.groupby(['sport', 'home_team', 'away_team', 'start_time', 'outcome'])['decimal_odds'].idxmax()
        self.best_lines =  self.all_betting_lines.iloc[idx_max].sort_values('start_time')
        
    def necessary_calculations(self):
        """
        Uses model to calculate the predicted probability of each line and then calculates the kelly criterion for each line
        """
        self.merged_df['mean_implied_probability'] = 1 / self.merged_df['avg_odds']
        self.merged_df['best_implied_probability'] = 1 / self.merged_df['decimal_odds']
        self.merged_df['predicted_probability'] = self.merged_df['mean_implied_probability'] - self._alpha
        self.merged_df['thresh'] = 1 / (self.merged_df['mean_implied_probability'] - self._alpha)
        self.merged_df['expected_value'] = self.merged_df['predicted_probability'] * (self.merged_df['decimal_odds'] - 1)
        self.merged_df['kelly'] = self.merged_df.apply(lambda x: basic_kelly_criterion(x['predicted_probability'], x['decimal_odds']), axis=1)
        self.merged_df['half_kelly'] = self.merged_df.apply(lambda x: basic_kelly_criterion(x['predicted_probability'], x['decimal_odds'], kelly_size=0.5), axis=1)
        
    def find_plus_ev_bets(self) -> None:
        """
        Filters the merged_df for lines that are beyond the necessary threshold from the mean odds
        and calculates the kelly criterion for each line
        """
        self.plus_ev_bets = self.merged_df[self.merged_df['decimal_odds'] > self.merged_df['thresh']]
        
    def run_etl(self):
        """
        Runs the ETL process
        """
        self.extract()
        self.transform()
        self.load()
        
    def filter_bookmakers(self):
        """
        Filters the lines for those that are from bookmakers that I can bet with
        """
        book_makers = pd.DataFrame(BOOKMAKERS).T
        book_makers.index.name = 'sportsbook'
        book_makers = book_makers.reset_index(drop = False)
        book_makers = book_makers[book_makers['can_bet']]['sportsbook'].to_list()
        self.all_betting_lines = self.all_betting_lines[self.all_betting_lines['sportsbook'].isin(book_makers)]
        self.all_betting_lines = self.all_betting_lines.reset_index(drop = True)

        
        
