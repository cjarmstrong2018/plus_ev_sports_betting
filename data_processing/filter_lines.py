import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

SQLALCHEMY_DATABASE_URI = 'sqlite:///../sports_betting.db'


class LineFilter(object):
    def __init__(self):
        """
        This is the object that performs the ETL process of merging the two tables
        all_betting_lines and avg_odds into a single table and filtering the lines
        for those that are beyond the necessary threshold form the mean odds. 
        writes those lines to the database in the table plus_ev_bets
        """ 
        self.engine = create_engine(SQLALCHEMY_DATABASE_URI)
        self.average_odds = pd.DataFrame()
        self.all_betting_lines = pd.DataFrame()
        self.merged_df = pd.DataFrame()
        self.plus_ev_bets = pd.DataFrame()
        
    def extract(self):
        """
        Extracts data for all_betting_lines and avg_odds tables from the databasedata   
        stores them in the all_betting_lines and avg_odds attributes
        """
        self.all_betting_lines = pd.read_sql_query('SELECT * FROM all_betting_lines', self.engine)
        self.average_odds = pd.read_sql_query('SELECT * FROM avg_odds', self.engine)

    def transform(self):
        """
        Merges the two dataframes into a single betting dataframe and then filters the lines
        for those that are beyond the necessary threshold from the mean odds. For those that 
        are beyond the necessary threshold, it calculates the kelly criterion and writes those
        lines to the database in the table plus_ev_bets. Also assigns an ID to each plus_ev line to 
        track the bets and only alert me on new bets that hit
        """
        pass
    
    def load(self):
        """
        Writes the plus_ev_bets table to the database
        """
        self.plus_ev_bets.to_sql('plus_ev_bets', self.engine, if_exists='replace', index=False)


df_all_betting_lines = pd.read_sql_query('SELECT * FROM all_betting_lines', engine)
df_avg_odds = pd.read_sql_query('SELECT * FROM avg_odds', engine)

# Define fuzzy merge conditions
fuzzy_merge_conditions = {
    "start_time": "exact",
    "sport": "exact",
    "home_team": "fuzzy",
    "away_team": "fuzzy",
    "outcome": "fuzzy"
}

# Perform the fuzzy merge
merged_df = fpd.fuzzy_merge(
    df_all_betting_lines, df_avg_odds, on=["start_time", "sport"], on_type=fuzzy_merge_conditions
)