## Config file

[config backup: 04/02/22](https://docs.google.com/document/d/1ppJAYvWywn7t-yLFdu12K11rRn6a3_f2HNRroSR76o8/edit)

## Purpose

The purpose of this collection of code is to automate the entire financial cycle for my work. This includes:

1. scraping reports and data from the web
2. preparing monthly financials
3. preparing annual financials
4. generating receipts and work orders

This project is to be realtime. It will use Python, Flask, Selenium, sqlite, and Postgres for the backend. The front end will be made using Google Sheets Tailwind and Remix/React.

##

Roadmap

- testing coverage for autors & checklist (generation and population of checklist and rent sheets for rent roll and tenant payments by date: )
  - major issues: is access to deposits before month end
- fill down formula(efficiently?)
- damages list
- autogenerate
- scraping kata
- scraping from nbofi: shadow-dom link
- scraping from intuit
- investigate plaid
- distributing processed files to audit checklist pre-packaged for auditors
- must make it must faster (obviously the bottleneck it the calls to google sheets)
- async
