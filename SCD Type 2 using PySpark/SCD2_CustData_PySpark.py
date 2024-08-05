# Databricks notebook source
# cleanup for dev set for first run
#spark.sql("DROP TABLE STG.T_CUSTOMER_DELTA")
#dbutils.fs.rm("/STG/T_CUSTOMER_DELTA/", recurse = True)

# COMMAND ----------

# Use dbutils.widget define a "folder" variable with a default value
dbutils.widgets.text("file_date", "27072024")

# Now get the parameter value (if no value was passed, the default set above will be used)
v_file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

import urllib3

url=f"https://raw.githubusercontent.com/pradeepdekhane/CustFilesInfo/main/customers_{v_file_date}.csv"

# Download product data from GitHub
response = urllib3.PoolManager().request('GET', url)
data = response.data.decode("utf-8")

# Save the product data to the specified folder
path = f"dbfs:/importGithub/customer/customers_{v_file_date}.csv"
dbutils.fs.put(path, data, True)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, current_timestamp, lit, asc, desc

# COMMAND ----------

cust_schema = StructType(fields=[StructField("Index", IntegerType(), False),
                                 StructField("CustomerId", StringType(), True),
                                 StructField("FirstName", StringType(), True),
                                 StructField("LastName", StringType(), True),
                                 StructField("Company", StringType(), True),
                                 StructField("City", StringType(), True),
                                 StructField("Country", StringType(), True),
                                 StructField("Phone1", StringType(), True),
                                 StructField("Phone2", StringType(), True),
                                 StructField("Email", StringType(), True),
                                 StructField("SubscriptionDate", StringType(), True),
                                 StructField("Website", StringType(), True)
                                 ])

# COMMAND ----------

stg_df = spark.read.csv(f"dbfs:/importGithub/customer/customers_{v_file_date}.csv", header=True, schema=cust_schema)

# COMMAND ----------

src_df = stg_df.withColumn("FileDate", lit(v_file_date))

display(src_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS STG;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS STG.T_CUSTOMER_DELTA
# MAGIC (
# MAGIC Index integer,
# MAGIC CustomerId string,
# MAGIC FirstName string,
# MAGIC LastName string,
# MAGIC Company string,
# MAGIC City string,
# MAGIC Country string,
# MAGIC Phone1 string,
# MAGIC Phone2 string,
# MAGIC Email string,
# MAGIC SubscriptionDate string,
# MAGIC Website string,
# MAGIC FileDate string,
# MAGIC CustCd integer,
# MAGIC StartDate Date,
# MAGIC EndDate Date,
# MAGIC IsCurrent string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/STG/T_CUSTOMER_DELTA/'

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/STG/T_CUSTOMER_DELTA/")

tgt_df = delta_table.toDF()

display(tgt_df)

# COMMAND ----------

# Find new records
insert_df = src_df.alias("src").join(
    tgt_df.filter(col("IsCurrent") == "Y").alias("tgt"),
    col("src.Index") == col("tgt.Index"),
    "anti"
)


# COMMAND ----------

# Generate surrogate keys for new records
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, to_date, col

window_spec = Window.orderBy(lit(1))
surrogate_add_df = insert_df.withColumn("CustCd", row_number().over(window_spec) + tgt_df.count())
new_df = surrogate_add_df.withColumn("StartDate", to_date(lit(v_file_date),"ddmmyyyy"))\
    .withColumn("EndDate", (to_date(lit("9999-12-31"), "yyyy-mm-dd")))\
    .withColumn("IsCurrent", lit("Y"))

display(new_df)

# COMMAND ----------

# Find type 2 column update records e.f lets consider type 2 on Phone1, Phone2 and Email only 
update_t2_df = src_df.alias("src").join(
    tgt_df.filter(col("IsCurrent") == "Y").alias("tgt"),
    col("src.Index") == col("tgt.Index"),
    "inner")\
        .filter((col("src.Phone1") != col("tgt.Phone1")) | (col("src.Phone2") != col("tgt.Phone2")) | (col("src.Email") != col("tgt.Email")))\
        .select(col("src.*"))

window_spec = Window.orderBy(lit(1))
surrogate_add_t2_df = update_t2_df.drop("CustCd")\
    .withColumn("CustCd", row_number().over(window_spec) + tgt_df.count())

t2_df = surrogate_add_t2_df.withColumn("StartDate", to_date(lit(v_file_date),"ddmmyyyy"))\
    .withColumn("EndDate", to_date(lit("9999-12-31"),"yyyy-mm-dd"))\
    .withColumn("IsCurrent", lit("Y"))

display(t2_df)


# COMMAND ----------

# Find non type 2 column update records for which only update needed
t1_df = src_df.alias("src").join(
    tgt_df.filter(col("IsCurrent") == "Y").alias("tgt"),
    col("src.Index") == col("tgt.Index"),
    "inner"
).filter(((col("src.FirstName") != col("tgt.FirstName")) | (col("src.LastName") != col("tgt.LastName")) | (col("src.Company") != col("tgt.Company")) | (col("src.City") != col("tgt.City")) | (col("src.Country") != col("tgt.Country")) | (col("src.SubscriptionDate") != col("tgt.SubscriptionDate")) | (col("src.Website") != col("tgt.Website")) ))\
    .select(col("src.Index"),
            col("src.FirstName"),
            col("src.LastName"),
            col("src.Company"),
            col("src.City"),
            col("src.Country"),
            col("src.SubscriptionDate"),
            col("src.Website")
    )

display(t1_df)

# COMMAND ----------

# non type 2 column update in target:
delta_table.alias("tgt").merge(t1_df.alias("src"), "tgt.Index = src.Index") \
  .whenMatchedUpdate(
    set={
      "FirstName": "src.FirstName",
      "LastName": "src.LastName",
      "Company": "src.Company",
      "City": "src.City",
      "Country": "src.Country",
      "SubscriptionDate": "src.SubscriptionDate",
      "Website": "src.Website"
    }
  )\
  .execute()

# COMMAND ----------

#updated type 2 record identified : close old records

delta_table.alias("tgt").merge(t2_df.alias("src"), "tgt.Index = src.Index") \
  .whenMatchedUpdate(
    condition="src.Phone1 != tgt.Phone1 OR src.Phone2 != tgt.Phone2 OR src.Email != tgt.Email",
    set={
      "EndDate": to_date(lit(v_file_date),"ddmmyyyy"),
      "IsCurrent": "'N'"
    }
  )\
  .execute()

# COMMAND ----------

#insert record identified as new

delta_table.alias("tgt").merge(new_df.alias("src"), "tgt.Index = src.Index") \
  .whenNotMatchedInsert(values=dict(zip(new_df.columns, new_df.columns))) \
  .execute()

# COMMAND ----------

#updated type 2 record identified : insert new records

new_df.write.format("delta").mode("append").save("/STG/T_CUSTOMER_DELTA/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM STG.T_CUSTOMER_DELTA;

# COMMAND ----------

dbutils.notebook.exit("Success")
