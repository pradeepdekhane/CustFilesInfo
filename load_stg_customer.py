# Databricks notebook source
dbutils.widgets.text("p_username", "pradeepdekhane")
username = dbutils.widgets.get("p_username")

# COMMAND ----------

dbutils.widgets.text("p_repository", "CustFilesInfo")
repository = dbutils.widgets.get("p_repository")

# COMMAND ----------

dbutils.widgets.text("p_file_name", "customers_27072024.csv")
filename = dbutils.widgets.get("p_file_name")

# COMMAND ----------

# MAGIC %run "/StudyFolder/import_github_udf"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, current_timestamp, lit, asc, desc, current_date, to_date

# COMMAND ----------

dname = import_github_directory_udf(username, repository)

fname = f"dbfs:{dname}/{filename}"

stg_df = spark.read.csv(fname, header=True, inferSchema=True)

display(stg_df)

# COMMAND ----------

#spark.sql("drop table STG.CUSTOMER") # Dev purpose
#dbutils.fs.rm("/stg/customer", recurse=True) # Dev purpose

# COMMAND ----------

if spark.catalog.tableExists("stg.customer"):
    v_mode = "append"
else:
    v_mode = "overwrite"

# COMMAND ----------

stg_df.write.mode("append").option("mergeSchema", "true").format("delta").save("/stg/customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS STG;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS STG.CUSTOMER
# MAGIC USING DELTA
# MAGIC LOCATION "/stg/customer"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stg.customer;

# COMMAND ----------

dbutils.notebook.exit("Success")
