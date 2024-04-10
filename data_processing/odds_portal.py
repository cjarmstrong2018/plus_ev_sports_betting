import time
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
import os
from dotenv import load_dotenv

load_dotenv()

ODDS_PORTAL_USERNAME = os.getenv("ODDS_PORTAL_USERNAME")
ODDS_PORTAL_PASSWORD = os.getenv("ODDS_PORTAL_PASSWORD")

LEAGUE_URLS = {""}


class OddsPortalScraper:
    def __init__(self, league_urls=LEAGUE_URLS):
        self.league_urls = league_urls
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

    def get_avg_odds(self, league_url) -> pd.DataFrame:
        self.web.get(league_url)

        pass

    def get_odds_for_leagues(self):
        dfs = []
        for league, url in self.league_urls.items():
            league_df = self.get_avg_odds(url)
            dfs.append(league_df)
        df = pd.concat(dfs)

    def transform_avg_odds(self):
        pass

    def save_avg_odds(self):
        self.transformed_df.to_sql()

    def run(self):
        self.odds_portal_login()
        # While datetime < shutdown time
        self.get_odds_for_leagues()
        self.transform_avg_odds()
        self.save_avg_odds()


if __name__ == "__main__":
    scraper = OddsPortalScraper()
    scraper.run()
