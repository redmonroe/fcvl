## Run selenium and chrome driver to scrape data from cloudbytes.dev
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import unittest

class ChromeSetupTest(unittest.TestCase):

    def setUp(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless") # Ensure GUI is off
        chrome_options.add_argument("--no-sandbox")
        webdriver_service = Service("/home/joe/chromedriver/stable/chromedriver")
        self.browser = webdriver.Chrome(service=webdriver_service, options=chrome_options)

    def tearDown(self):
        self.browser.quit()

    def test_with_google_searchbox(self):
        # assert 'chrome' in browser.name
        self.browser.get("https://www.google.com")
        # Extract description from page and print
        search_box = self.browser.find_element(By.NAME, "q")
        self.assertEqual('Search', search_box.accessible_name)
        # self.fail('Finish the test!')
        print('stop line for debugger')


# cst = ChromeSetupTest()
# cst.setUp()
# cst.test_with_google_searchbox()
# cst.tearDown()

if __name__ == '__main__':
    unittest.main()
        
