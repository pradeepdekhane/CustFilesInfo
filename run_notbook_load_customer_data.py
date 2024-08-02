# Databricks notebook source
# MAGIC %run "/Workspace/StudyFolder/import_github_udf"

# COMMAND ----------

# run for file customers_27072024.csv
run_stg_customer_udf("load_stg_customer", "pradeepdekhane", "CustFilesInfo", "customers_27072024.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stg.customer;

# COMMAND ----------

# run for file customers_28072024.csv
# in this file column "Birth Date" is extra
# Lets see how it notebook react without any change

run_stg_customer_udf("load_stg_customer", "pradeepdekhane", "CustFilesInfo", "customers_28072024.csv") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stg.customer;

# COMMAND ----------

dbutils.notebook.exit("Success")
