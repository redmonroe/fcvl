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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
#Configure and launch driver for selenium data pull from Fidelity
# driver = webdriver.Chrome()
driver = Service("/home/joe/chromedriver/stable/chromedriver")
vars = {}
driver.get('https://vistula.onesite.realpage.com/')
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
