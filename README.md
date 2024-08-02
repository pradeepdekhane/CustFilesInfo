This repository consis of csv customer file with Schema changes.

Its a prototype for Schema Evolution.

PySpark code written in databricks to see what happens when Schema changes in source:
1] import_github_udf.py : 
    defines user defined function to load git files in databricks file system and a function to run the notebook with required parameters and tracking.
2] load_stg_customer.py : 
    1] import files from Github
    2] load din parquet delta table with mergeschema option enabled
    3] script to be run only once per source file.
    4] overwrite in first run and append in second run
3] run_notbook_load_customer_data :
    job to run load_stg_customer.py notebook for two csv files with second file have one column added.
