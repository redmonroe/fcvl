from playwright.sync_api import Playwright, sync_playwright, expect

class Scrape:

    def hello(self):
        print('hello from scrape')

    def pw_context(self, path):
        with sync_playwright() as playwright:
            self.run_realpage_test(playwright, path)
    
    def run_realpage_test(self, playwright, path):
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        print(context)

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

        #take screenshot with playwright    

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
        download.save_as(path)
        breakpoint()

        page.close()


        # ---------------------
        context.close()
        browser.close()