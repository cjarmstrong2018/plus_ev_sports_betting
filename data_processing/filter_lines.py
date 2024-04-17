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
        self.plus_ev_bets = pd.DataFrame()
        self.merged_df = pd.DataFrame()
        
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
        self.compute_best_lines()
        self.merge_tables()
        self.find_plus_ev_bets()
        self.calculate_probabilities_and_kelly_criterion()
    
    def load(self):
        """
        Writes the plus_ev_bets table to the database
        """
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
        
        
    def find_plus_ev_bets(self) -> None:
        """
        Filters the merged_df for lines that are beyond the necessary threshold from the mean odds
        and calculates the kelly criterion for each line
        """
        df = self.merged_df.copy()
        df['mean_implied_probability'] = 1 / df['avg_odds']
        df['best_implied_probability'] = 1 / df['decimal_odds']
        df['thresh'] = 1 / (df['mean_implied_probability'] - self._alpha)
        df = df[df['decimal_odds'] >= df['thresh']]
        self.plus_ev_bets = df
        
    def calculate_probabilities_and_kelly_criterion(self):
        """
        Calculates the implied probability for each line and then calculates the kelly criterion for each line
        """
        df = self.plus_ev_bets.copy()
        implied = df['mean_implied_probability']
        implied.name = 'implied_probability'
        df['predicted_probability'] = self.model.predict(implied)
        df['kelly'] = df.apply(lambda x: basic_kelly_criterion(x['predicted_probability'], x['decimal_odds']), axis=1)
        df['half_kelly'] = df.apply(lambda x: basic_kelly_criterion(x['predicted_probability'], x['decimal_odds'], kelly_size=0.5), axis=1)
        self.plus_ev_bets = df
        
        
    def run_etl(self):
        """
        Runs the ETL process
        """
        self.extract()
        self.transform()
        self.load()
        
        
    # def find_trades(self, df) -> pd.DataFrame:
    #     """
    #     Runs the necessary calculations on a merged df of mean and highest
    #     odds and returns a DataFrame of all trades spotted

    #     Args:
    #         df (pd.DataFrame): DataFrame from self.create_league_format

    #     Returns:
    #         pd.DataFrame: a DataFrame ready to be iterated over to send alerts to
    #         the channel
    #     """
    #     df = df[df['date'] < central_time_now() + pd.Timedelta(3, 'h')]
    #     print(f"Checking {len(df)} lines within window")
    #     self.valid_lines += len(df)
    #     df['mean_implied_probability'] = 1 / df['mean_odds']
    #     df['highest_implied_probability'] = 1 / df['odds']
    #     df['thresh'] = 1 / (df['mean_implied_probability'] - self._alpha)
    #     df = df[df['odds'] >= df['thresh']]
    #     return df

    # def necessary_calculations(self, df) -> pd.DataFrame:
    #     """
    #     Performs necessary calculations before iterating through df to send 
    #     notifications

    #     Args:
    #         df (pd.DataFrame): DataFrame of identified trades

    #     Returns:
    #         pd.DataFrame: original df but with updated columns for notifications
    #     """
    #     df['american_thresh'] = df['thresh'].apply(
    #         lambda x: convert_odds(x, cat_in="dec")['American'])
    #     df['american_thresh'] = df['american_thresh'].round()
    #     df['american_odds_best'] = df['odds'].apply(
    #         lambda x: convert_odds(x, cat_in="dec")['American'])
    #     df['american_odds_best'] = df['american_odds_best'].round()

    #     implied = df['mean_implied_probability']
    #     implied.name = 'implied_probability'
    #     df['predicted_prob'] = self.model.predict(implied)
    #     df['kelly'] = df.apply(lambda x: basic_kelly_criterion(
    #         x['predicted_prob'], x['odds']), axis=1)
    #     df['half_kelly'] = df.apply(lambda x: basic_kelly_criterion(
    #         x['predicted_prob'], x['odds'], kelly_size=0.5), axis=1)

    #     df['id'] = df.apply(lambda x: self.generate_game_id(x), axis=1)
    #     current_bankroll = self.current_bankroll()
    #     df['cja_wager'] = df['kelly'] * current_bankroll
    #     return df


        
        
