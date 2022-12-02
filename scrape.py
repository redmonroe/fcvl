from playwright.sync_api import Playwright, sync_playwright, expect

class Scrape:

    def hello(self):
        print('hello from scrape')

    def pw_context(self):
        with sync_playwright() as playwright:
            self.run_realpage_test(playwright)
    
    def run_realpage_test(self, playwright):
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()

        page = context.new_page()

        page.goto("https://www.realpage.com/home")

        page.goto("https://www.realpage.com/login/identity/Account/SignIn")

        page.get_by_placeholder("Username").click()

        page.get_by_placeholder("Username").click()

        page.get_by_placeholder("Username").fill("JWalsh4")

        page.get_by_placeholder("Username").press("Enter")
        page.wait_for_url("https://www.realpage.com/login/identity/Account/Local")

        page.locator("button").click()
        page.wait_for_url("https://www.realpage.com/home/")

        with page.expect_popup() as popup_info:
            page.get_by_role("link", name="OneSite OneSite L&R, Budgeting, Payments, Screening, Purchasing, and Doc. Mgmt").click()
        page1 = popup_info.value
        page.wait_for_url("https://vistula.onesite.realpage.com/multishell_cb/shell.htm?c=101101000011000000001111001000000010000000000000000000000000000100000000000100000000000000,818328946,JWALSH4&u=221,1111110110000000010000010000&t=0")
        breakpoint()

        page2.goto("https://vistula.onesite.realpage.com/ui2/dashboards/#/")

        page.get_by_role("menuitem", name="Reports").get_by_role("link", name="Reports").click()
        page.wait_for_url("https://www.realpage.com/reporting/")

        page.goto("https://www.realpage.com/reporting/")

        page.get_by_placeholder("Search...").click()

        page.get_by_placeholder("Search...").fill("rent roll")

        page2.close()

        page1.close()

        page.close()

        # ---------------------
        context.close()
        browser.close()



# with sync_playwright() as playwright:
#     run(playwright)