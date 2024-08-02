# Databricks notebook source
# MAGIC %md
# MAGIC ###### function for importing specific file in dataframe

# COMMAND ----------

import requests
import pandas as pd
from io import StringIO

# function for importing specific file in dataframe
def import_github_file_udf(in_username, in_repository, in_filename):
    url = f"https://raw.githubusercontent.com/{in_username}/{in_repository}/main/{in_filename}"

    # Download the CSV file content using requests
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful

    # Get the CSV content as a string
    csv_content = response.content.decode('utf-8')

    # Convert the CSV content to a pandas DataFrame
    csv_buffer = StringIO(csv_content)
    pandas_df = pd.read_csv(csv_buffer)
    
    # Convert the pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)

    return spark_df

# COMMAND ----------

print("------------------------------------------------------------------------------------------------")
print("imported UDF : `import_github_file_udf`")
print("------------------------------------------------------------------------------------------------")
print("""`import_github_directory_udf` :
      Call using import_github_directory_udf(v_username, v_repository, v_filename)
      Inputs parameters -->
           v_username   :  username of github account 
           v_repository :  repository name
           v_filename   :  file name
      Ouput return value:
           spark dataframe result for an input file from given repository from Github user""")
print("------------------------------------------------------------------------------------------------")

# COMMAND ----------

# MAGIC %md
# MAGIC ######function for importing a directory in dbfs

# COMMAND ----------

import os

# function for importing a directory in dbfs
def import_github_directory_udf(in_username, in_repository):
    # Define the repository URL and the target directory in DBFS
    url = f"https://github.com/{in_username}/{in_repository}.git"
    target_dir = f"/{repository}"
    # Clone the repository
    os.system(f"git clone {url} {target_dir}")
    dbutils.fs.mv(f"file:////{target_dir}", f"/importGithub{target_dir}", recurse=True)

    import_dir = f"/importGithub{target_dir}"

    return import_dir

# COMMAND ----------

print("------------------------------------------------------------------------------------------------")
print("imported UDF : `import_github_directory_udf`")
print("------------------------------------------------------------------------------------------------")
print("""`import_github_directory_udf` :
      Call using import_github_directory_udf(v_username, v_repository)
      Inputs parameters -->
           v_username   :  username of github account 
           v_repository :  repository name
      Ouput return value:
           importGithub/{repository}" dbfs folder name""")
print("------------------------------------------------------------------------------------------------")

# COMMAND ----------

# MAGIC %md
# MAGIC ######function for running notebook file to load customer

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# define function to run a notebook that load customer file
def run_stg_customer_udf(v_script_name, v_username, v_repository, v_file_name):
    dttm = spark.sql("SELECT current_timestamp()" ).collect()[0][0]
    url = f"https://github.com/{v_username}/{v_repository}.git"
    print(f"Initating File ingestion \n          File Name  : {v_file_name}\n          Github URL : {url}")
    print(f"{dttm} : Load started")
    v_result = dbutils.notebook.run(v_script_name, 0, {"p_username": v_username, "p_repository": v_repository, "p_file_name": v_file_name})
    if v_result == "Success":
        dttm = spark.sql("SELECT current_timestamp()" ).collect()[0][0]
        print(f"{dttm} : Load Success\n")
    else:
        dttm = spark.sql("SELECT current_timestamp()" ).collect()[0][0]
        print(f"{dttm} : Load Fail")

# COMMAND ----------

print("------------------------------------------------------------------------------------------------")
print("imported UDF : `import_github_directory_udf`")
print("------------------------------------------------------------------------------------------------")
print("""`run_stg_customer_udf` :
      Call using run_stg_customer_udf(v_script_name, v_username, v_repository, v_file_name)run_stg_customer_udf(v_username, v_repository)
      Inputs parameters -->
           v_script_name :  notebook file name to run
           v_username    :  username of github account 
           v_repository  :  repository name of github
           v_file_name   :  file name from github
      Ouput return value:
           NA""")
print("------------------------------------------------------------------------------------------------")
