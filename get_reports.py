import pytest
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
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
# driver.find_element(By.ID, "userId-input").click()
# driver.find_element(By.ID, "userId-input").send_keys("USERNAME")
# driver.find_element(By.ID, "password").send_keys("PASSWORD")
# driver.find_element(By.ID, "fs-login-button").click()
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