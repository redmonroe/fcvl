## top level

- requirements:

  - testing:
    - april endbal 2920(test) v. 2933(rs): wait to til I process month to freak out (deactivating april in tests for now)
    - need to write tests for statusrs and balance letters (this is not an auto-flow now; triggered from cli )
  - incremental processing of files (comes from statusRS)
  - make rent sheets (JUST USE PANDAS)
    - midmonth scrape (how does this work with rent sheets)
    - mm scrape: make load a loop instead of just one shot
    - we don't have an answer yet for inputing move-in like in feb (greiner)
  - RENT RECEIPTS:
    - make rent receipts flow
    - rent receipts and letter class
    - add letter generated col to db
    - need to either loop on fcv_workflows or run automatically
  - S/M: export files for audit once they are processed, async, profiling, cython, frontend api structure
  - structure:
    - use interfaces and getters when referencing other classes

- links
- [markdown guide](https://www.markdownguide.org/basic-syntax/)
- [selenium for fidelity](https://wire.insiderfinance.io/exporting-portfolio-data-from-fidelity-for-analysis-d212ac83ad99)
- [selenium docs](https://selenium-python.readthedocs.io/installation.html)
