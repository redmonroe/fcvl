"""
# Filename: run_selenium.py
"""

## Run selenium and chrome driver to scrape data from cloudbytes.dev
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

## Setup chrome options
chrome_options = Options()
chrome_options.add_argument("--headless") # Ensure GUI is off
chrome_options.add_argument("--no-sandbox")

# Set path to chromedriver as per your configuration
webdriver_service = Service("/home/joe/chromedriver/stable/chromedriver")

# Choose Chrome Browser
browser = webdriver.Chrome(service=webdriver_service, options=chrome_options)

# Get page
# browser.get("https://www.nytimes.com")
browser.get('https://www.nbofi.com')
browser.find_element_by_class_name('olb-area').click()
browser.find_element(By.NAME, 'username')
browser.find_element(By.NAME, 'username').send_keys('jwalsh')
browser.find_element(By.XPATH, ("//input[@value='Login']")).click()
time.sleep(10)
ids = browser.find_elements(By.XPATH, ('//*[@id]'))
browser.find_element(By.ID, ('password')).send_keys('izow162nb')
for ii in ids:
    #print ii.tag_name
    print('*', ii.get_attribute('id'))    # id name as string


breakpoint()
# Extract description from page and print
description = browser.find_element(By.NAME, "description").get_attribute("content")
print(f"{description}")

#Wait for 10 seconds
browser.quit()