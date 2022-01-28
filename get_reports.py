import json
import time

import pytest

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
<<<<<<< HEAD
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
#Configure and launch driver for selenium data pull from Fidelity
# driver = webdriver.Chrome()
driver = Service("/home/joe/chromedriver/stable/chromedriver")
vars = {}
driver.get('https://vistula.onesite.realpage.com/')
=======

def test_untitled(self):
    self.driver.get("https://vistula.onesite.realpage.com/")
    self.driver.set_window_size(1162, 646)
    self.driver.switch_to.frame(0)
    self.driver.find_element(By.ID, "txtLogin").click()
    self.driver.find_element(By.ID, "txtLogin").click()
    self.vars["window_handles"] = self.driver.window_handles
    self.driver.find_element(By.ID, "btnLogin").click()
    self.vars["win711"] = self.wait_for_window(2000)
    self.vars["root"] = self.driver.current_window_handle
    self.driver.switch_to.window(self.vars["win711"])
    self.driver.switch_to.window(self.vars["root"])
    self.driver.switch_to.window(self.vars["win711"])

class GetReports:

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless") # Ensure GUI is off
        chrome_options.add_argument("--no-sandbox")
        webdriver_service = Service("/home/joe/chromedriver/stable/chromedriver")
        self.driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
        self.vars = {}

    def wait_for_window(self, timeout=2):
        vars = {}
        time.sleep(round(timeout / 1000))
        wh_now = self.driver.window_handles
        wh_then = self.vars["window_handles"]
        if len(wh_now) > len(wh_then):
            set(wh_now).difference(set(wh_then)).pop()
        print('debug stop')

    def make_request(self):
        self.driver.get('https://vistula.onesite.realpage.com/')
        self.driver.set_window_size(1162, 646)
        self.driver.switch_to.frame(0)
        self.driver.find_element(By.ID, "txtLogin").click()
        self.driver.find_element(By.ID, "txtLogin").click()
        self.vars["window_handles"] = self.driver.window_handles
        self.driver.find_element(By.ID, "btnLogin").click()
        self.vars["win711"] = self.wait_for_window(2000)
        self.vars["root"] = self.driver.current_window_handle
        self.driver.switch_to.window(self.vars["win711"])
        self.driver.switch_to.window(self.vars["root"])
        self.driver.switch_to.window(self.vars["win711"])


report = GetReports()
report.setup_driver()
report.make_request()
  

>>>>>>> a992b36... setup get_reports.py
# driver.set_window_size(1587, 942)
# driver.find_element(By.ID, "txtLogin").click()
# driver.find_element(By.ID, "txtLogin").send_keys("JWalsh4")
# driver.find_element(By.ID, "password").send_keys("JWalsh41205")
# driver.find_element(By.ID, "btnLogin").click()
# #Wait for enough time to web page to render
# time.sleep(5)
# driver.find_element(By.ID, "tab-2").click()
# #Wait for enough time to web page to render
# time.sleep(5)
# driver.find_element(By.CSS_SELECTOR, ".posweb-grid_top-download-button").click()
# #Wait for enough time to web page to render
# time.sleep(5)
# driver.find_element(By.CSS_SELECTOR, ".pntlt > .pnlogin > .pnls > a").click()
# driver.quit()
