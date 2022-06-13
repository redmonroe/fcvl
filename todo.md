## top level

- requirements:

  - why is file_indexer func write_to_dep_list so ugly, comes from extract_deposits_by_type

  - operations:
    - segregate scrape_loading functionality
  - testing:
    - need to write tests for statusrs and balance letters (this is not an auto-flow now; triggered from cli )
  - incremental processing of files (comes from statusRS)
  - make rent sheets (JUST USE PANDAS)
  - S/M: export files for audit once they are processed, async, profiling, cython, frontend api structure
  - structure:
    - use interfaces and getters when referencing other classes
      -outside world
    - make convention for renaming pdfs

- links
- [markdown guide](https://www.markdownguide.org/basic-syntax/)
- [selenium for fidelity](https://wire.insiderfinance.io/exporting-portfolio-data-from-fidelity-for-analysis-d212ac83ad99)
- [selenium docs](https://selenium-python.readthedocs.io/installation.html)
