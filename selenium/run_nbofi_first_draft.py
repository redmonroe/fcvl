
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

## Setup chrome options
chrome_options = Options()
chrome_options.headless = True
# chrome_options.add_argument("--headless") # Ensure GUI is off
# chrome_options.add_argument("--no-sandbox")

# Set path to chromedriver as per your configuration
service_path = Service("/home/joe/chromedriver/stable/chromedriver")

browser = webdriver.Chrome(service=service_path, options=chrome_options)
site_url1 = 'http://www.google.com' 
site_url2 = 'http://www.nbofi.com' 
sleep = 3

def nbofi_working_scrape(browser=browser, sleep=sleep):
    ### this worked
    browser.get("https://bank.nbofi.com/")
    time.sleep(sleep)
    browser.get_screenshot_as_file('page.png')
    print(browser.title)
    time.sleep(sleep)
    browser.find_element(By.ID, "username").click()
    browser.find_element(By.ID, "username").send_keys("jwalsh")
    browser.find_element(By.ID, "username").send_keys(Keys.ENTER)
    browser.get_screenshot_as_file('page2.png')
    browser.find_element(By.ID, "password").send_keys("izow162nb")
    browser.find_element(By.ID, "password").send_keys(Keys.ENTER)
    browser.get_screenshot_as_file('page3.png')
    time.sleep(sleep)
    browser.get_screenshot_as_file('page4.png')
    print(browser.title, browser.current_url)
    ids = browser.find_elements(By.XPATH, ('//*[@id]'))
    for ii in ids:
        print(ii.tag_name, '*', ii.get_attribute('id'), ii.is_displayed()) 
           # id name as string
    inputs = browser.find_elements(By.CSS_SELECTOR, ('CSS: input[title="Enter code"]'))
    for ii in inputs:
        print(ii, '**', ii.is_displayed())    # id name as string
    breakpoint()
    browser.find_element(By.TAG_NAME, "jha-form-floating-group")
    keys = input('put authy code here >>>')
    browser.find_element(By.TAG_NAME, "input").send_keys(keys)
    browser.get_screenshot_as_file('page5.png')
    ele.send_keys('')
    # browser.set_window_size(1051, 798)
    # browser.find_element(By.CSS_SELECTOR, ".olb-toggle").click()
    # time.sleep(sleep)
    # browser.find_element(By.CSS_SELECTOR, ".text-right > jha-button").click()
    # time.sleep(sleep)
    # print(browser.title)
    # for x in ele:
    #     print(x.tag_name, x.get_attribute('id'))
    # browser.get_screenshot_as_file('page2.png')
    # time.sleep(sleep)
    # browser.get_screenshot_as_file('page3.png')
    breakpoint()

nbofi_working_scrape(browser=browser, sleep=sleep)




