from datetime import datetime
import logging
import time
import uuid
from bs4 import BeautifulSoup
from dateutil import parser
import pandas as pd
import numpy as np
import selenium
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementClickInterceptedException
from utils import get_sqlalchemy_engine
import os
from dotenv import load_dotenv

load_dotenv()

ODDS_PORTAL_USERNAME = os.getenv("ODDS_PORTAL_USERNAME")
ODDS_PORTAL_PASSWORD = os.getenv("ODDS_PORTAL_PASSWORD")
SQLALCHEMY_DATABASE_URI = 'sqlite:///../sports_betting.db'
SLEEP_TIME_MINUTES = 5

LEAGUE_URLS = {
            "americanfootball_nfl": "https://www.oddsportal.com/american-football/usa/nfl/",
            "baseball_mlb": "https://www.oddsportal.com/baseball/usa/mlb/",
            "basketball_nba": "https://www.oddsportal.com/basketball/usa/nba/",
            "basketball_ncaab": "https://www.oddsportal.com/basketball/usa/ncaa/",
            "americanfootball_ncaaf": "https://www.oddsportal.com/american-football/usa/ncaa/",
            "soccer_epl": "https://www.oddsportal.com/soccer/england/premier-league/",
            "soccer_spain_la_liga": "https://www.oddsportal.com/soccer/spain/laliga/",
            "soccer_italy_serie_a": "https://www.oddsportal.com/soccer/italy/serie-a/",
            "soccer_uefa_champs_league": "https://www.oddsportal.com/soccer/europe/champions-league/",
            "soccer_france_ligue_one": "https://www.oddsportal.com/soccer/france/ligue-1/",
            "soccer_usa_mls": "https://www.oddsportal.com/soccer/usa/mls/",
            "soccer_germany_bundesliga": "https://www.oddsportal.com/soccer/germany/bundesliga/"
        }


class OddsPortalScraper:
    def __init__(self, league_urls=LEAGUE_URLS):
        self.logger = logging.getLogger("odds_portal")   
        self.configure_logger()
        self.league_urls = league_urls
        self.engine = get_sqlalchemy_engine()
        op = webdriver.ChromeOptions()
        op.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
        )
        # op.add_argument('headless')
        op.add_argument("--disable-web-security")
        op.add_argument("no-sandbox")
        op.add_argument("--disable-blink-features=AutomationControlled")
        op.add_argument("--log-level=3")
        self.web = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=op
        )
        self.data = pd.DataFrame()
        
    def configure_logger(self):
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        # add file handler
        fh = logging.FileHandler("odds_portal.log")
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        

    def odds_portal_login(self):
        self.web.get("https://www.oddsportal.com/login/")
        login_xpath = (
            "/html/body/div[1]/div[1]/div[1]/div/main/div[3]/div[2]/div/div/form/div[4]"
        )
        WebDriverWait(self.web, 10).until(
            EC.element_to_be_clickable((By.XPATH, login_xpath))
        )
        time.sleep(5)
        user = self.web.find_elements(By.ID, "login-username-sign")[-1]
        user.send_keys(ODDS_PORTAL_USERNAME)
        pswd = self.web.find_elements(By.ID, "login-password-sign")[-1]
        pswd.send_keys(ODDS_PORTAL_PASSWORD)
        login = self.web.find_element(By.XPATH, login_xpath)
        login.click()
        self.logger.info("Logged in to OddsPortal")

    def get_avg_odds(self, sport_key, league_url) -> pd.DataFrame:
        self.web.get(league_url)
        table_xpath = '//*[@id="app"]/div[1]/div[1]/div/main/div[3]/div[4]'
        WebDriverWait(self.web, 4).until(
            EC.element_to_be_clickable((By.XPATH, table_xpath)))
        time.sleep(.5)
        soup = BeautifulSoup(self.web.page_source, "html.parser")
        rows = soup.find_all("div", class_="eventRow flex w-full flex-col text-xs")
        entries = []
        event_date = datetime.now().date()
        for row in rows:
            try:
                date_tag = row.find("div", class_="text-black-main font-main w-full truncate text-xs font-normal leading-5")
                if date_tag is not None:
                    date_tag = date_tag.text
                    if 'yesterday' in date_tag.lower():
                        continue
                    if 'today' in date_tag.lower():
                        continue
                    if 'tomorrow' in date_tag.lower():
                        event_date = datetime.today() + pd.Timedelta(days=1)
                    else:
                        try:
                            event_date = parser.parse(date_tag).date()
                        except:
                            print(date_tag, "not parsed")
                            continue
                teams = row.find_all("p", class_="participant-name truncate")
                home_team, away_team = teams[0].text, teams[1].text
                time_div = row.find("div", class_="next-m:flex-col min-md:flex-row min-md:gap-1 text-gray-dark flex flex-row self-center text-[12px] w-full")
                start_time = time_div.find('p').text
                start_time = datetime.time(datetime.strptime(start_time, "%H:%M"))
                odds = row.find_all("div", class_="flex-center border-black-borders min-w-[60px] flex-col gap-1 pb-0.5 pt-0.5 relative")
                odds = [odd.find("p").text for odd in odds] 
                if len(odds) == 3:
                    home_odds, draw_odds, away_odds = odds
                else:
                    home_odds, away_odds = odds
                    draw_odds = None
                start_time = datetime.combine(event_date, start_time)
                home_entry = {
                    'id': uuid.uuid4(),
                    "sport": sport_key, # sport key
                    'home_team': home_team,
                    'away_team': away_team,
                    'start_time': start_time,
                    'outcome': home_team,
                    "decimal_odds": home_odds,
                    'update_time': datetime.now(),
                }
                away_entry = {
                    'id': uuid.uuid4(),
                    "sport": sport_key, # sport key
                    'home_team': home_team,
                    'away_team': away_team,
                    'start_time': start_time,
                    'outcome': away_team,
                    "decimal_odds": away_odds,
                    'update_time': datetime.now(),
                }
                entries.append(home_entry)
                entries.append(away_entry)
                if draw_odds is not None:
                    draw_entry = {
                        'id': uuid.uuid4(),
                        "sport": sport_key, # sport key
                        'home_team': home_team,
                        'away_team': away_team,
                        'start_time': start_time,
                        'outcome': 'Draw',
                        "decimal_odds": draw_odds,
                        'update_time': datetime.now(),
                    }
                    entries.append(draw_entry)
            except Exception as e:
                print(e)
                continue
        # select only rows that are today and not tomorrow or beyond
        df = pd.DataFrame(entries)
        if df.empty:
            return df
        df['start_time'] = pd.to_datetime(df['start_time'])
        df = df[df['start_time'].dt.date == datetime.now().date()]
        df = df[df['start_time'].dt.time > datetime.now().time()]
        df = df.reset_index(drop = True)
        return df
    
    def extract_odds(self):
        dfs = []
        for league, url in self.league_urls.items():
            try:
                league_df = self.get_avg_odds(league, url)
            except Exception as e:
                print(e)
                continue
            dfs.append(league_df)
        df = pd.concat(dfs)
        df = df.reset_index(drop=True)
        self.data = df
        self.logger.debug(f"Extracted {len(df)} odds avg odds entries")
        return 0

    def transform_odds(self):
        """
        If any transformation needs to be done, it can be added here
        """
        try:
            self.data['id'] = self.data['id'].astype(str)
            self.data['decimal_odds'] = self.data['decimal_odds'].astype(float)
            self.data['update_time'] = pd.to_datetime(self.data['update_time'])
            return 0
        except:
            return -1

    def load_odds(self):
        r = self.data.to_sql('avg_odds', self.engine, if_exists='replace', index=False)
        self.logger.info(f"Loaded {r} rows into avg_odds")

    def run_etl(self):
        try:
            r = self.extract_odds()
        except Exception as e:
            self.logger.error("Failed to extract odds")
            self.logger.error(e)
        try:
            r = self.transform_odds()
        except Exception as e:
            self.logger.error("Failed to transform odds")
            self.logger.error(e)
        try:
            r = self.load_odds()
        except Exception as e:
            self.logger.error("Failed to transform odds")
            self.logger.error(e)
        
    def run(self):
        self.odds_portal_login()
        end_time = datetime.now().replace(hour=21, minute=30, second=0, microsecond=0)
        while datetime.now() < end_time:
            self.run_etl()
            time.sleep(SLEEP_TIME_MINUTES * 60)
        self.logger.info("Shutting down gracefully")
            


if __name__ == "__main__":
    scraper = OddsPortalScraper()
    scraper.run()
