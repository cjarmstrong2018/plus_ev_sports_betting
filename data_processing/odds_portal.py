import pandas as pd
import numpy as np
import selenium
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import os
from dotenv import load_dotenv

load_dotenv()

CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH")

print(CHROMEDRIVER_PATH)

LEAGUE_URLS = {
}


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


op = webdriver.ChromeOptions()
op.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36")
# op.add_argument('headless')
op.add_argument("--disable-web-security")
op.add_argument("no-sandbox")
op.add_argument("--disable-blink-features=AutomationControlled")
op.add_argument("--log-level=3")
web = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

web.get("https://www.oddsportal.com/login/")

