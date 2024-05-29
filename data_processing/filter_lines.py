from datetime import datetime
import hashlib
import pickle
import time
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process
from DiscordAlerts import DiscordAlert
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging

from utils import basic_kelly_criterion

load_dotenv()
SQLALCHEMY_DATABASE_URI = 'sqlite:///../sports_betting.db'
WEBSITE_URL = ''
TIME_SLEEP_MINUTES = 1
ALPHA = os.getenv("ALPHA")

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
        self.svc_name = "line_filter"
        self.logger = None
        self.init_logger()
        self.engine = create_engine(SQLALCHEMY_DATABASE_URI)
        self.svc_name = "line_filter"
        self._alpha = float(ALPHA)
        # self.model = pickle.load(open('model.pkl', 'rb'))
        self.discord = DiscordAlert()
        self.team_names = pd.DataFrame()
        self.average_odds = pd.DataFrame()
        self.all_betting_lines = pd.DataFrame()
        self.merged_df = pd.DataFrame()
        self.best_lines = pd.DataFrame()
        self.merged_df = pd.DataFrame()
        self.plus_ev_bets = pd.DataFrame()
        self.reccommended_bets_archive = pd.DataFrame()
        self.bets_to_reccommend = pd.DataFrame()
        
    def extract(self):
        """
        Extracts data for all_betting_lines and avg_odds tables from the databasedata   
        stores them in the all_betting_lines and avg_odds attributes
        """
        self.all_betting_lines = pd.read_sql_query("SELECT * from all_betting_lines", self.engine)
        self.average_odds = pd.read_sql_query('SELECT * FROM avg_odds', self.engine)
        self.team_names = pd.read_sql('SELECT * FROM team_names', self.engine)
        try:
            self.reccommended_bets_archive = pd.read_sql('SELECT * FROM reccommended_bets_archive', self.engine)
        except:
            self.reccommended_bets_archive = pd.DataFrame()

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
        self.get_bets_to_notify()
        self.merge_with_reccommended_bets_archive()
        
    def load(self):
        """
        Writes the plus_ev_bets table to the database
        """
        self.merged_df.to_sql('best_lines_model_probabilities', self.engine, if_exists='replace', index=False)
        self.plus_ev_bets.to_sql('plus_ev_bets', self.engine, if_exists='replace', index=False)
        self.reccommended_bets_archive.to_sql('reccommended_bets_archive', self.engine, if_exists='replace', index=False)
        
    def merge_tables(self) -> None:
        """
        mergest the two tables best_lines and avg_odds into a single table. Currently this is done using an exact only approach but eventually this will be done
        using fuzzy_wuzzy to determine the best matches for each line
        """
        self.clean_team_names()
        best_lines = self.best_lines.copy()
        best_lines = best_lines[['sport', 'home_team', 'away_team','start_time', 'sportsbook', 'outcome', 'decimal_odds', 'update_time']]
        best_lines = best_lines.rename(columns={'update_time': 'best_odds_update_time'})
        avg_odds = self.average_odds.copy()
        avg_odds = avg_odds[['sport', 'home_team', 'away_team','start_time', 'outcome', 'decimal_odds', 'update_time']]
        avg_odds = avg_odds.rename(columns={'update_time': 'avg_odds_update_time', 'decimal_odds': 'avg_odds'})
        df = pd.merge(best_lines, avg_odds, on=['sport', 'home_team', 'away_team', 'outcome'], how='inner', suffixes=('', '_y'))
        df = df[['sport', 'start_time', 'home_team', 'away_team', 'outcome','sportsbook', 
                 'decimal_odds', 'avg_odds', 'best_odds_update_time', 'avg_odds_update_time']]
        df['start_time'] = pd.to_datetime(df['start_time'])
        # Get current time in US/Central timezone
        # now = datetime.now(timezone('US/Central'))

        # # Convert 'now' to Pandas Timestamp object
        # now_timestamp = pd.Timestamp(now)

        df = df[df['start_time'] > datetime.now()]
        self.merged_df = df
      
    def compute_best_lines(self) -> None:
        """
        Selects only the best line for each outcome across all sportsbooks
        """
        # select the best line for each outcome using group by
        idx_max = self.all_betting_lines.groupby(['sport', 'home_team', 'away_team', 'start_time', 'outcome'])['decimal_odds'].idxmax()
        self.best_lines =  self.all_betting_lines.iloc[idx_max].sort_values('start_time')
        self.logger.debug(f"Best lines shape: {self.best_lines.shape}")
        
    def necessary_calculations(self):
        """
        Uses model to calculate the predicted probability of each line and then calculates the kelly criterion for each line
        """
        self.merged_df['mean_implied_probability'] = 1 / self.merged_df['avg_odds']
        self.merged_df['best_implied_probability'] = 1 / self.merged_df['decimal_odds']
        self.merged_df['predicted_probability'] = self.merged_df['mean_implied_probability'] - self._alpha
        self.merged_df['thresh'] = 1 / (self.merged_df['mean_implied_probability'] - self._alpha)
        self.merged_df['expected_value'] = (self.merged_df['predicted_probability'] * (self.merged_df['decimal_odds'] - 1)) + ((1 - self.merged_df['predicted_probability']) * -1)
        self.merged_df['kelly'] = self.merged_df.apply(lambda x: basic_kelly_criterion(x['predicted_probability'], x['decimal_odds']), axis=1)
        self.merged_df['half_kelly'] = self.merged_df.apply(lambda x: basic_kelly_criterion(x['predicted_probability'], x['decimal_odds'], kelly_size=0.5), axis=1)
        
    def find_plus_ev_bets(self) -> None:
        """
        Filters the merged_df for lines that are beyond the necessary threshold from the mean odds
        and calculates the kelly criterion for each line
        """
        self.plus_ev_bets = self.merged_df[self.merged_df['decimal_odds'] > self.merged_df['thresh']]
        self.plus_ev_bets['id'] = self.plus_ev_bets.apply(lambda x: self.generate_unique_hash(x['home_team'], x['away_team'], x['outcome'], x['start_time']), axis=1)
        
    def run_etl(self):
        """
        Runs the ETL process
        """
        self.extract()
        self.transform()
        self.load()
        self.notify()
        
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

    def run(self):
        """
        Runs the full ETL process every 5 minutes
        """
        end_time = datetime.now().replace(hour=21, minute=30, second=0, microsecond=0)
        while datetime.now() < end_time:
            self.run_etl()
            if self.all_betting_lines.empty:
                self.logger.debug("No more games today, shutting down")
                break
            time.sleep(TIME_SLEEP_MINUTES * 60)
      
    def clean_team_names(self):
        """
        Uses fuzzywuzzy to clean the team names in the avg_odds table, 
        extracts the set of teams for each sport and then uses fuzzywuzzy process to match the team names
        """
        self.best_lines['home_team'] = self.best_lines.apply(lambda x: self.lambda_fuzzy_wuzzy(x['home_team'], x['sport']), axis=1)
        self.best_lines['away_team'] = self.best_lines.apply(lambda x: self.lambda_fuzzy_wuzzy(x['away_team'], x['sport']), axis=1)
        self.best_lines['outcome'] = self.best_lines.apply(lambda x: self.lambda_fuzzy_wuzzy(x['outcome'], x['sport']), axis=1)
        self.average_odds['home_team'] = self.average_odds.apply(lambda x: self.lambda_fuzzy_wuzzy(x['home_team'], x['sport']), axis=1)
        self.average_odds['away_team'] = self.average_odds.apply(lambda x: self.lambda_fuzzy_wuzzy(x['away_team'], x['sport']), axis=1) 
        self.average_odds['outcome'] = self.average_odds.apply(lambda x: self.lambda_fuzzy_wuzzy(x['outcome'], x['sport']), axis=1)
        
    def lambda_fuzzy_wuzzy(self, team_name, sport) -> str:
        """
        Helper function for fuzzywuzzy to match team names
        """
        teams = self.team_names[self.team_names['sport'] == sport]['team_name'].to_list()
        
        if not teams:
            self.logger.debug(f"No teams found for {sport} {team_name}")
            return team_name    
        if team_name.lower() == 'draw':
            return 'draw'
        out = process.extractOne(team_name, teams, scorer=fuzz.token_set_ratio, score_cutoff= 80)
        self.logger.debug(f"{team_name}, {out}, {sport}")
        if out is None:
            return np.nan
        else:
            return out[0]
      
    def generate_unique_hash(self, home_team, away_team, outcome, start_datetime):
        """
        Generates a unique hash for each line based on the team names, outcome, and start time
        in order to avoid duplicate lines in the database
        
        Args:
            home_team (str): the home team name
            away_team (str): the away team name
            outcome (str): the outcome of the line
            start_datetime (datetime.datetime): the event start time

        Returns:
            str: the unique hash for the line
        """
        # Concatenate the relevant information into a string
        data_string = f"{home_team}-{away_team}-{outcome}-{start_datetime}"

        # Hash the string using SHA-256
        hashed_data = hashlib.sha256(data_string.encode()).hexdigest()
        return hashed_data

    def merge_with_reccommended_bets_archive(self):
        """
        Merges the plus_ev_bets table with the reccommended_bets_archive table to filter out the bets that have already been reccommended
        """
        self.reccommended_bets_archive = pd.concat([self.reccommended_bets_archive, self.bets_to_reccommend])
        self.reccommended_bets_archive = self.reccommended_bets_archive.drop_duplicates(subset=['id']) # should be redundant

    def get_bets_to_notify(self):
        """
        Filters the plus_ev_bets table for the bets that have not already been reccommended
        """
        if self.reccommended_bets_archive.empty:
            self.bets_to_reccommend = self.plus_ev_bets
        else:
            self.bets_to_reccommend = self.plus_ev_bets[~self.plus_ev_bets['id'].isin(self.reccommended_bets_archive['id'])]
        
    def send_alerts(self):
        """
        Sends alerts for the bets that have not already been reccommended
        """
        for index, row in self.bets_to_reccommend.iterrows():
            msg = f"{row['home_team']} vs {row['away_team']} {row['outcome']} at {row['sportsbook']} has a {round(row['expected_value'], 2)} EV. \n "
            msg += f"Bet on {row['outcome']} with {row['sportsbook']} at {row['decimal_odds']} odds. And bet on nothing lower than {row['thresh']}\n"
            self.discord.send_msg(msg)
            
    def create_and_send_notification(self) -> None:
        """
        Generates the notification that will be sent to the discord server to
        notify of treades
        """
        if self.bets_to_reccommend.empty:
            return
        intro_msg = ":rotating_light::rotating_light: Potential Bets Found! :rotating_light::rotating_light:\n"
        self.discord.send_msg(intro_msg)
        for i, row in self.bets_to_reccommend.iterrows():
            date = row['date'].strftime("%m/%d %I:%M %p")
            american_thresh = "+" + \
                str(row['american_thresh']) if row['american_thresh'] > 0 else str(
                    row['american_thresh'])
            american_best = "+" + \
                str(row['american_odds_best']) if row['american_odds_best'] > 0 else str(
                    row['american_odds_best'])
            msg = f"{date} {row['away_team']} @ {row['home_team']}\n"
            msg += f"Bet on {row['odds_team']} Moneyline with {row['bookmaker']}\n"
            msg += f"Current Odds: {row['odds']} ({american_best}).\n"
            msg += f"The bet is good if the odds are at least {round(row['thresh'], 2)} ({american_thresh})\n"
            msg += "Using Kelly Criterion, we reccommend betting the following percentages of your betting bankroll: \n"
            msg += f"Full Kelly: {round(row['kelly'] * 100)}%\n"
            msg += f"Half Kelly: {round(row['half_kelly'] * 100)}%\n"
            msg += "\n\n"
            self.discord.send_msg(msg)

        self.discord.send_msg("Good Luck!!")
    
    def post_archive_to_sheets(self):
        """
        Posts the desired columns of the archive to google sheets
        """
        json_perms = "sportsbook-scraping-363802-5ed6e9e4d35c.json" 

        # Define the scope and credentials
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(json_perms, scope)

        # Authorize the client
        client = gspread.authorize(creds)

        # Open the Google Sheet by its title
        sheet = client.open('Sports Betting Log').sheet1  # Replace 'Your Google Sheet Title' with your sheet's title
        
        desired_columns = ['sport', 'start_time', 'home_team', 'away_team', 'outcome','sportsbook', 
                 'decimal_odds', 'avg_odds', "thresh", "kelly", "half_kelly", "expected_value"]
        df = self.reccommended_bets_archive[desired_columns]
        if df.empty:
            return

        sheet.update(df.values.tolist(), 'A2')
        self.logger.debug(f"Posted Reccommended bets archive to Google Sheets")
        
    def notify(self):
        """
        Sends the alerts and posts the archive to google sheets
        """
        try:
            self.send_alerts()
            self.post_archive_to_sheets()
        except Exception as e:
            self.logger.error(f"Error sending alerts: {e}")
            self.discord.send_msg(f"Error sending alerts: {e}")
        
    def init_logger(self, log_lvl=logging.DEBUG, verbose=True) -> None:
        logger = logging.getLogger("line_filter")
        logger.setLevel(log_lvl)
        formatter = logging.Formatter(
            "[%(asctime)s][%(name)s][%(levelname)s]: %(message)s"
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_lvl)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        current_file_path = os.path.abspath(__file__)
        current_file_directory = os.path.dirname(current_file_path)
        logs_dir = os.path.join(current_file_directory, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_file_name = f"{self.svc_name}.log"
        log_file_path = os.path.join(logs_dir, log_file_name)
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        self.logger = logger
    
if __name__ == "__main__":
    lf = LineFilter()
    lf.run()    
