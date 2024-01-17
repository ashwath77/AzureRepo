# Databricks notebook source
# DBTITLE 1,Creating the mount point from ADF.
def mounting_layer_json(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# COMMAND ----------

# DBTITLE 1,Reading the csv file.
def read(file_format,path,options):
     return spark.read.format(file_format).options(**options).load(path)

# COMMAND ----------

# DBTITLE 1,Creating snake_case for columns in lower case.
def snake_case(x):
    a = x.lower()
    return a.replace(' ','_')

# COMMAND ----------

from pyspark.sql.functions import col, split, substring, when

# COMMAND ----------

# DBTITLE 1,Creating two columns by first_name and last_name.
def split_name_column(df, name_column):
    return df.withColumn("first_name", split(col(name_column), " ")[0]) \
             .withColumn("last_name", split(col(name_column), " ")[1])

# COMMAND ----------

# DBTITLE 1,Creating domain column and extracting email columns.
def extract_domain_column(df, email_column, split_char, split_char2):
    return df.withColumn("domain", split(col(email_column),split_char)[1]).withColumn('domain', split(col(email_column), split_char2)[0])

# COMMAND ----------

# DBTITLE 1,Create a column gender where male = "M" and Female="F".
def create_gender_column(df, gender_column):
    return df.withColumn("gender", when(col(gender_column) == "male", "M").otherwise("F"))

# COMMAND ----------

# DBTITLE 1,From Joining date create two colums date and time by splitting based on " " delimiter.
def split_joining_date_column(df, joining_date_column):
    return df.withColumn("date", split(col(joining_date_column), " ")[0]) \
             .withColumn("time", split(col(joining_date_column), " ")[1])


# COMMAND ----------

# DBTITLE 1,Create a column expenditure-status, based on spent column is spent below 200 column value is "MIN" else "MAX"
def create_expenditure_status_column(df, spent_column):
    return df.withColumn("expenditure_status", when(col(spent_column) < 200, "MINIMUM").otherwise("MAXIMUM"))

# COMMAND ----------

# DBTITLE 1,Date column should be on "yyyy-MM-dd" format.
from pyspark.sql.functions import col, date_format, to_date

def format_date_column(df, date_column, input_format='dd-MM-yyyy', output_format='yyyy-MM-dd'):
    return df.withColumn(date_column, date_format(to_date(col(date_column), input_format), output_format))

# COMMAND ----------

# DBTITLE 1,Write based on upsert table.
from delta.tables import *
def save_as_delta(res_df, path, db_name, tb_name, mergecol):
    base_path = f"{path}/{db_name}/{tb_name}"
    mappedCol = " AND ".join(list(map((lambda x: f"old.{x} = new.{x} "),mergecol)))
    if not DeltaTable.isDeltaTable(spark, f"{base_path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        res_df.write.mode("overwrite").format("delta").option("path", base_path).saveAsTable(f"{db_name}.{tb_name}")
    else:
         deltaTable = DeltaTable.forPath(spark, f"{base_path}")
         deltaTable.alias("old").merge(res_df.alias("new"),mappedCol).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# DBTITLE 1,Create sub_category column based on category_id
def create_sub_category_column(df, category_column, sub_category_column):
    return df.withColumn(sub_category_column, 
                        when(col(category_column) == 1, "phone")
                        .when(col(category_column) == 2, "laptop")
                        .when(col(category_column) == 3, "playstation")
                        .when(col(category_column) == 4, "e-device")
                        .otherwise("unknown"))


# COMMAND ----------

# DBTITLE 1,Create a store category columns and the value is exatracted from email
from pyspark.sql.functions import col, split

def extract_email_column(df, email_column, split_char, split_char2):
    return df.withColumn("store_category", split(col(email_column), split_char)[1]) \
             .withColumn('store_category', split(col(email_column), split_char2)[0])

# COMMAND ----------

def transform_store_dates(df, created_at_column, updated_at_column, date_format='MM-dd-yyyy'):
    from pyspark.sql.functions import to_date
    return df.withColumn(created_at_column, to_date(col(created_at_column), date_format)) \
             .withColumn(updated_at_column, to_date(col(updated_at_column), date_format))

# COMMAND ----------

def select_required_columns(product_df, store_df):
    return product_df.select(
        "store_id", "store_name", "location", "manager_name",
        "product_name", "product_code", "description", "category_id",
        "price", "stock_quantity", "supplier_id",
        "product_created_at", "product_updated_at", "image_url",
        "weight", "expiry_date", "is_active", "tax_rate"
    ).join(
        store_df.select("store_id", "store_name", "location", "manager_name"),
        "store_id"
    )

# COMMAND ----------

def transform_data_for_gold_layer(customer_sales_df, gold_df):
    return customer_sales_df.join(
        gold_df,
        customer_sales_df.product_id == gold_df.product_code,
        "inner"
    ).select(
        "order_date", "category", "city", "customer_id", "order_id",
        "product_id", "profit", "region", "sales", "segment",
        "ship_date", "ship_mode", "latitude", "longitude",
        "store_name", "location", "manager_name", "product_name",
        "price", "stock_quantity", "image_url"
    )