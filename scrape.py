from playwright.sync_api import Playwright, sync_playwright, expect
from errors import Errors

class PWScrape:

    # @Errors.playwright_timeerror(times=1)
    # def hello(self, genus=None):
    #     print('hello from hello')
    #     return genus

    @Errors.playwright_timeerror
    def pw_deposits(self, path=None, times=None):
        with sync_playwright() as playwright:
            self.run_deposits_realpage(playwright, path)
            
    @Errors.playwright_timeerror
    def playwright_nbofi_opcash(self, path=None, times=None):
        with sync_playwright() as playwright:
            self.run_opcash_test(playwright, path)
            
    @Errors.playwright_timeerror
    def playwright_nbofi_scrape(self, path=None, times=None):
        with sync_playwright() as playwright:
            self.run_nbofi_mtd_test(playwright, path)
            
    @Errors.playwright_timeerror
    def playwright_rentroll_scrape(self, path=None, times=None):
        with sync_playwright() as playwright:
            self.run_rentroll_realpage(playwright, path)
            
    def run_opcash_test(self, playwright, path):
        print('run opcash test')
        
    def run_nbofi_mtd_test(self, playwright, path):
        print('run nbofi mtd test')
        
    def run_rentroll_realpage(self, playwright, path):
        print('run rentroll from realpage')
    
    def run_deposits_realpage(self, playwright, path):
        print('run deposits from realpage')
        print(f'any successful downloads will go to {path}')
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()

        page = context.new_page()

        page.goto("https://www.realpage.com/home")

        page.goto("https://www.realpage.com/login/identity/Account/SignIn")

        page.get_by_placeholder("Username").click()

        page.get_by_placeholder("Username").fill("JWalsh4")

        page.get_by_role("button").click()
        page.wait_for_url("https://www.realpage.com/login/identity/Account/Local")

        page.get_by_placeholder("Password").click()
        page.get_by_placeholder("Password").fill("Freedom7!")
        page.locator("button").click()
        page.wait_for_url("https://www.realpage.com/home/")  

        page.get_by_role("menuitem", name="Reports").get_by_role("link", name="Reports").click()
        page.wait_for_url("https://www.realpage.com/reporting/")

        page.goto("https://www.realpage.com/reporting/")

        page.get_by_text("Bank Deposit Details (Excel)").click()

        page.get_by_role("button", name="Select Property").click()
        page.get_by_role("option", name="Falls Creek Village I").locator("label div").click()
        page.get_by_role("option", name="Falls Creek Village I").press("Tab")
        page.get_by_role("button", name="Current AR Period").press("Tab")
        page.get_by_role("button", name="1").click()
        page.get_by_role("textbox", name="Search").fill("all")
        page.get_by_role("option", name="ALL").get_by_text("ALL").click()

        page.screenshot(path="screenshot.png")
        page.get_by_role("button", name="Generate").click()

        page.locator("raul-grid-row[role=\"listitem\"]:has-text(\"Bank Deposit Details (Excel) Generate & Schedule Add to Group View Scheduled Vie\")").get_by_role("button").click()

        page.get_by_role("button", name="View Completed").click()

        page.get_by_role("button", name="Select Properties").click()

        page.get_by_role("option", name="Falls Creek Village I").locator("label div").click()

        page.locator("raul-grid-row[role=\"listitem\"]:has-text(\"Completed Date12/07/2022 01:50 AMPropertyFalls Creek Village IUser / Report Grou\")").get_by_role("button").click()

        with page.expect_download() as download_info:
            page.get_by_role("button", name="Download").click()
        download = download_info.value
        print(download)
        print(f'saving to {path}')
        download.save_as(path)

        page.close()


        # ---------------------
        context.close()
        browser.close()