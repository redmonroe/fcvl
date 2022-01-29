## top level

[ ] define globals for year: rent sheets doc, audit 2022 folder, any subfolders

- income output globals:

[markdown guide](https://www.markdownguide.org/basic-syntax/)

### income

- start again when I have an actual bank statement from Jan?

### rent sheets flow:

- what we know: master imported rent roll & deposits correctly, master also generated rent receipts correctly
- what we don't know: whether merges from master to fcvl_rebuild will work with without additional issues: ESPECIALLY IMPORTS FROM LILTILITIES V UTILS & api call syntax
  -GAMEPLAN: work through with tests for core functionality on fcvl_rebuild, then that branch will become production, we don't need to keep master for anything other than job automation until fcvl_rebuild is ready to push to production
  -ALSO IMPORTANT: be sure to rebuild xlsx autoconverted, just move old xls to another folder, change name to modified date + type (ie deposits_1282022.xlx then convert?)
  - proposed features: bank site parsing for rent sheet reconciliation, download google doc as docx or pdf labeled and ready to email to peg, double checking of match between rent receipts and rent roll, late payer warning system
  -

### pdf

- bank statements: can I loop through 2, can I get them from audit 2022/2021
- define export folder structure (I don't want them in pwd)
- rclone or rsync to get these files out of pwd

## get_reports.py

- can selenium ide deal with onesite
- what is the front end experience for selenium?

- links
- [selenium for fidelity](https://wire.insiderfinance.io/exporting-portfolio-data-from-fidelity-for-analysis-d212ac83ad99)
- [selenium docs](https://selenium-python.readthedocs.io/installation.html)
